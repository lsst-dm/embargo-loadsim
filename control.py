"""
control - flat S3 load, the boring companion to obsload

No visit grid, no pipeline: --writers users PUT and --readers users GET a
fixed keyspace of --objects keys, flat out, forever. Use it as a control to
tell endpoint behaviour apart from obsload's burst structure.

  write:  PUTs cycle the keyspace; after one pass it is the overwrite path
  read:   GETs uniformly random keys; fill the keyspace first
  mix:    give both --writers and --readers

--s3-endpoint takes a comma-separated list round-robined per request,
identical to obsload. -u is derived from --writers + --readers; do not pass
it. --fill writes every key exactly once, then exits (run it in a single
process; with --processes each process fills the whole keyspace). The leader
logs the cluster's throughput every --throughput-interval seconds.

  locust -f control.py --headless --fill --writers 32 \
         --objects 1000 --size 16MiB --s3-endpoint http://s3.example:9000
  locust -f control.py --headless -t 10m --readers 64 --objects 1000 \
         --s3-endpoint http://s3.example:9000 --csv=ctl1
"""

# MUST be first: see obsload.py.
from gevent import monkey

monkey.patch_all()

import io  # noqa: E402
import itertools  # noqa: E402
import logging  # noqa: E402
import os  # noqa: E402
import random  # noqa: E402
import time  # noqa: E402

import gevent  # noqa: E402

import boto3  # noqa: E402
from botocore.config import Config as BotoConfig  # noqa: E402

from locust import User, constant, events, task  # noqa: E402
from locust.exception import StopUser  # noqa: E402
from locust.runners import WORKER_REPORT_INTERVAL, MasterRunner, WorkerRunner  # noqa: E402

log = logging.getLogger("control")

# botocore loads MBs of JSON service models at first client build; pay
# that here, before a follower connects. See obsload.py.
boto3.client("s3", region_name="us-east-1")

_UNITS = {
    "": 1, "b": 1,
    "k": 1000, "kb": 1000, "kib": 1024,
    "m": 10**6, "mb": 10**6, "mib": 1024**2,
    "g": 10**9, "gb": 10**9, "gib": 1024**3,
}


def parse_size(s):
    s = s.strip().lower()
    i = 0
    while i < len(s) and (s[i].isdigit() or s[i] == "."):
        i += 1
    return int(float(s[:i]) * _UNITS[s[i:].strip()])


POOL = b""


class PoolReader(io.RawIOBase):
    """Seekable file-like of arbitrary length backed by a slice of POOL."""

    def __init__(self, size, offset):
        self.size = size
        self.offset = offset % len(POOL)
        self.pos = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, off, whence=io.SEEK_SET):
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.pos, io.SEEK_END: self.size}[whence]
        self.pos = max(0, min(self.size, base + off))
        return self.pos

    def readinto(self, b):
        src = (self.offset + self.pos) % len(POOL)
        n = min(len(b), self.size - self.pos, len(POOL) - src)
        if n <= 0:
            return 0
        b[:n] = POOL[src:src + n]
        self.pos += n
        return n


def fire(name, t0, length, exc, rtype):
    events.request.fire(request_type=rtype, name=name,
                        response_time=(time.perf_counter() - t0) * 1000.0,
                        response_length=length, exception=exc, context=None)


def put_obj(client, bucket, key, size, rng):
    body = PoolReader(size, rng.randrange(len(POOL)))
    t0 = time.perf_counter()
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentLength=size)
    except Exception as exc:
        fire("write", t0, 0, exc, "S3P")
        return
    count_bytes("write", size)
    fire("write", t0, size, None, "S3P")


def get_obj(client, bucket, key):
    t0 = time.perf_counter()
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        n = 0
        for chunk in resp["Body"].iter_chunks(1 << 20):
            n += len(chunk)
    except Exception as exc:
        fire("read", t0, 0, exc, "S3G")
        return
    count_bytes("read", n)
    fire("read", t0, n, None, "S3G")


def make_client(o, endpoint):
    cfg = BotoConfig(
        max_pool_connections=1024,
        retries={"total_max_attempts": 1, "mode": "standard"},
        connect_timeout=o.s3_timeout,
        read_timeout=o.s3_timeout,
        signature_version="s3v4",
        s3={"addressing_style": "path", "payload_signing_enabled": False},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return boto3.client("s3", endpoint_url=endpoint,
                        region_name=os.environ.get("AWS_REGION", "us-east-1"),
                        verify=False, config=cfg)


@events.init_command_line_parser.add_listener
def _args(parser):
    g = parser.add_argument_group("control")
    g.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"),
                   help="endpoint URL, or comma-separated URLs round-robined "
                        "per request")
    g.add_argument("--s3-timeout", type=float, default=30.0)
    g.add_argument("--bucket", default="obsload")
    g.add_argument("--prefix", default="control")
    g.add_argument("--objects", type=int, default=1000, help="keyspace size")
    g.add_argument("--size", default="8MiB", help="object size for writes")
    g.add_argument("--writers", type=int, default=0)
    g.add_argument("--readers", type=int, default=0)
    g.add_argument("--fill", action="store_true",
                   help="write every key exactly once, then exit")
    g.add_argument("--throughput-interval", type=float, default=10.0,
                   help="seconds per throughput window; the leader logs "
                        "cluster totals per window")


CLIENT = None
INTERVAL = 10.0                   # --throughput-interval, set at test start
# Bytes moved per wall-clock window, {window: {"read": n, "write": n}}; window
# k covers k*INTERVAL to (k+1)*INTERVAL. Followers send theirs to the leader,
# which adds them up and logs them.
BYTES = {}
WIDX = itertools.count()          # next write index, process-local
FILL = {"left": 0}                # writers still filling, process-local


def key(o, i):
    return f"{o.prefix}/{i:08d}"


@events.init.add_listener
def _init(environment, **_):
    o = environment.parsed_options
    if not os.environ.get("OBSLOAD_BOTOCORE_LOGS"):
        for name in ("botocore", "boto3", "urllib3", "s3transfer"):
            logging.getLogger(name).setLevel(logging.WARNING)

    # a follower only has its own CLI defaults here; the real options arrive
    # with the leader's spawn message, so the leader (or a single process)
    # validates them
    if not isinstance(environment.runner, WorkerRunner):
        parse_size(o.size)
        if o.writers + o.readers < 1:
            raise ValueError("give --writers and/or --readers")
        if o.fill and (not o.writers or o.readers):
            raise ValueError("--fill wants --writers and no --readers")
    Writer.fixed_count = o.writers
    Reader.fixed_count = o.readers
    need = o.writers + o.readers
    if getattr(o, "num_users", None) is None:
        o.num_users = need
        if not getattr(o, "spawn_rate", None):
            o.spawn_rate = need
    elif o.num_users != need:
        raise ValueError(f"-u {o.num_users} != --writers + --readers = {need}. "
                         f"Omit -u and it is derived for you.")


def count_bytes(kind, n):
    """Add n bytes, "read" or "write", to the current window."""
    moved = BYTES.setdefault(int(time.time() // INTERVAL), {"read": 0, "write": 0})
    moved[kind] += n


@events.report_to_master.add_listener
def _send_bytes(client_id, data, **_):
    # follower: send the counts with Locust's stats report (every 3s, and
    # once more after the users stop), then start again from zero
    data["control"] = dict(BYTES)
    BYTES.clear()


@events.worker_report.add_listener
def _add_bytes(client_id, data, **_):
    # leader: add one follower's counts to the totals
    for window, moved in data.get("control", {}).items():
        total = BYTES.setdefault(window, {"read": 0, "write": 0})
        total["read"] += moved["read"]
        total["write"] += moved["write"]


def _throughput():
    """Leader or single process: log each window once every follower's
    report for it has arrived. Reports come every 3s."""
    while True:
        gevent.sleep(INTERVAL - time.time() % INTERVAL)
        _log_windows(time.time() - WORKER_REPORT_INTERVAL - INTERVAL)


def _log_windows(cutoff):
    """Log each window that ended by cutoff, oldest first. Bytes that arrive
    after their window's line (from a stalled follower) get a second line
    with the same time; lines with the same time add up."""
    ready = [window for window in BYTES if (window + 1) * INTERVAL <= cutoff]
    for window in sorted(ready):
        moved = BYTES.pop(window)
        log.info("throughput %s %gs: read %.1f MiB/s write %.1f MiB/s (%dB %dB)",
                 time.strftime("%H:%M:%S", time.localtime(window * INTERVAL)),
                 INTERVAL, moved["read"] / INTERVAL / (1 << 20),
                 moved["write"] / INTERVAL / (1 << 20), moved["read"], moved["write"])


@events.quitting.add_listener
def _quitting(environment, **_):
    # leader or single process: the followers' last reports are in by now,
    # so log whatever is left
    if not isinstance(environment.runner, WorkerRunner):
        _log_windows(float("inf"))


@events.test_start.add_listener
def _test_start(environment, **_):
    global POOL, CLIENT, INTERVAL
    o = environment.parsed_options
    INTERVAL = o.throughput_interval
    if not isinstance(environment.runner, WorkerRunner):
        gevent.spawn(_throughput)  # the leader, or a single process, logs totals
    if isinstance(environment.runner, MasterRunner):
        return  # runs no users
    POOL = random.randbytes(64 << 20)
    urls = [u.strip() for u in (o.s3_endpoint or "").split(",") if u.strip()]
    CLIENT = itertools.cycle([make_client(o, u) for u in urls or [None]])


class Writer(User):
    wait_time = constant(0)

    def on_start(self):
        self.o = self.environment.parsed_options
        self.rng = random.Random()
        self.size = parse_size(self.o.size)
        FILL["left"] += 1  # each writer pairs this with one draw past the end

    @task
    def write(self):
        o = self.o
        i = next(WIDX)
        if o.fill and i >= o.objects:
            FILL["left"] -= 1
            if FILL["left"] == 0:  # the last local finisher quits the process
                log.info("fill complete: %d objects", o.objects)
                self.environment.runner.quit()
            raise StopUser()
        put_obj(next(CLIENT), o.bucket, key(o, i % o.objects), self.size,
                self.rng)


class Reader(User):
    wait_time = constant(0)

    def on_start(self):
        self.o = self.environment.parsed_options
        self.rng = random.Random()

    @task
    def read(self):
        o = self.o
        get_obj(next(CLIENT), o.bucket, key(o, self.rng.randrange(o.objects)))
