"""
batchload - flat worker-pool pipeline: preload inputs, compute, write out

No visit grid, no queues: --workers users each loop back-to-back jobs
  preload   GET a per-job sample of the warmed input keyspace
  compute   sleep(uniform(--process-delay))
  writeout  PUT --writeout-spec under job-unique keys

--warm-spec is the entire input keyspace, written by --warm (then exit):
      --warm-spec "seq:50000:12MiB,bias:20:110MiB,flat:20:99MiB"
--preload-spec is how many keys of each tag one job reads:
      --preload-spec "seq:20,bias:2,flat:1"
A big warmed count reads cold, a small one hot. The tag "seq" is special:
its keyspace is claimed in order and each key is read exactly once across
the cluster; workers stop when it runs out, so a seq run is bounded by the
walk. Leave "seq" out of --preload-spec to sample forever instead.

--slow-start N staggers first jobs to about N workers/sec cluster-wide.
-u is derived from --workers; do not pass it. Writeout keys are namespaced
by --run-id (default r<epoch>); input/ is outside it, so warm survives runs.

  locust -f batchload.py --headless --warm \
         --s3-endpoint http://s3.example:9000 --bucket batchload
  locust -f batchload.py --headless --run-time 30m --workers 200 \
         --s3-endpoint http://s3.example:9000 --bucket batchload --csv=b1
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
from collections import deque  # noqa: E402
from dataclasses import dataclass  # noqa: E402

import gevent  # noqa: E402
from gevent.pool import Pool as GPool  # noqa: E402

import boto3  # noqa: E402
from botocore.config import Config as BotoConfig  # noqa: E402

from locust import User, constant, events, task  # noqa: E402
from locust.exception import StopUser  # noqa: E402
from locust.runners import MasterRunner, WorkerRunner  # noqa: E402

log = logging.getLogger("batchload")

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


@dataclass
class Group:
    tag: str
    count: int
    lo: int
    hi: int

    def size(self, rng):
        return self.lo if self.lo == self.hi else rng.randint(self.lo, self.hi)


def parse_spec(spec):
    groups = []
    for part in (p.strip() for p in spec.split(",")):
        if not part:
            continue
        tag, count, size = part.split(":", 2)
        lo, _, hi = size.partition("-")
        groups.append(Group(tag, int(count), parse_size(lo), parse_size(hi or lo)))
    return groups


def parse_reads(spec):
    """'seq:20,bias:2' -> {tag: count}; no sizes, GETs read whatever warm wrote."""
    reads = {}
    for part in (p.strip() for p in spec.split(",")):
        if not part:
            continue
        tag, count = part.split(":")
        reads[tag] = int(count)
    return reads


def parse_range(s):
    lo, _, hi = s.strip().partition("-")
    return float(lo), float(hi or lo)


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


def fire_since(name, t0, length=0, exc=None, rtype="OBS"):
    events.request.fire(request_type=rtype, name=name,
                        response_time=(time.perf_counter() - t0) * 1000.0,
                        response_length=length, exception=exc, context=None)


def put_obj(client, bucket, key, size, rng, name):
    body = PoolReader(size, rng.randrange(len(POOL)))
    t0 = time.perf_counter()
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentLength=size)
    except Exception as exc:
        fire_since(name, t0, 0, exc, rtype="S3P")
        return False
    TP["write"] += size
    fire_since(name, t0, size, rtype="S3P")
    return True


def get_obj(client, bucket, key, name):
    t0 = time.perf_counter()
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        n = 0
        for chunk in resp["Body"].iter_chunks(1 << 20):
            n += len(chunk)
    except Exception as exc:
        fire_since(name, t0, 0, exc, rtype="S3G")
        return False
    TP["read"] += n
    fire_since(name, t0, n, rtype="S3G")
    return True


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


def input_key(o, tag, i):
    return f"{o.prefix}/input/{tag}/{i:08d}"


def out_key(o, wid, jobno, tag, i):
    return f"{o.prefix}/out/{STATE['run']}/w{wid:05d}-j{jobno:06d}/{tag}-{i:03d}"


@events.init_command_line_parser.add_listener
def _args(parser):
    g = parser.add_argument_group("batchload")
    g.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"),
                   help="endpoint URL, or comma-separated URLs round-robined "
                        "per request")
    g.add_argument("--preload-endpoint", default="",
                   help="endpoint(s) for preload GETs and --warm PUTs; "
                        "default --s3-endpoint")
    g.add_argument("--writeout-endpoint", default="",
                   help="endpoint(s) for writeout PUTs; default --s3-endpoint")
    g.add_argument("--s3-timeout", type=float, default=30.0)
    g.add_argument("--bucket", default="obsload")
    g.add_argument("--prefix", default="batchload")
    g.add_argument("--run-id", default="",
                   help="writeout key namespace; default r<epoch> is unique per run")

    g.add_argument("--workers", type=int, default=100,
                   help="worker users, cluster total")
    g.add_argument("--warm-spec", default="seq:5000:12MiB,bias:20:110MiB,flat:20:99MiB",
                   help="entire input keyspace, tag:count:size[-max]; written by --warm")
    g.add_argument("--preload-spec", default="seq:20,bias:2,flat:1",
                   help="keys one job reads per tag, tag:count; 'seq' walks in order")
    g.add_argument("--process-delay", default="20-28",
                   help="simulated compute seconds, 'min-max' or a scalar")
    g.add_argument("--writeout-spec", default="calexp:1:48MiB,src:1:2MiB,metric:12:8KiB")
    g.add_argument("--slow-start", type=float, default=0.0,
                   help="stagger first jobs to about this many workers/sec; "
                        "0 = all at once")

    g.add_argument("--concurrency", type=int, default=8,
                   help="parallel transfers per worker within one stage")
    g.add_argument("--warm", action="store_true",
                   help="populate the input keyspace, then exit; no load is run")
    g.add_argument("--throughput-interval", type=float, default=10.0,
                   help="seconds per throughput log window; per process, wall-"
                        "aligned so lines from NTP-synced hosts sum in post")


WARM, WRITEOUT = [], []
WARMN = {}                       # tag -> warmed count
READS = {}                       # tag -> keys per job
PROCESS_DELAY = (20.0, 28.0)
CLIENTS = {}
ENV = None
TP = {"read": 0, "write": 0}
GO = gevent.event.Event()
GO_DATA = {}
STATE = {"run": "", "procs": 1}

# The seq walk: each process owns a contiguous slice of the warmed "seq"
# keyspace, workers claim the next chunk without yielding. Claimed is
# consumed - a worker killed mid-preload does not requeue its chunk.
SEQ = {"alive": 0}               # next/end filled after GO, once per process

_free_slots = deque()
_next_slot = itertools.count()


@events.init.add_listener
def _init(environment, **_):
    global ENV
    ENV = environment
    o = environment.parsed_options

    if not os.environ.get("OBSLOAD_BOTOCORE_LOGS"):
        for name in ("botocore", "boto3", "urllib3", "s3transfer"):
            logging.getLogger(name).setLevel(logging.WARNING)

    # a --worker process only has its own defaults here (see obsload.py), so
    # validate the real args on the master/local runner only
    if not isinstance(environment.runner, WorkerRunner):
        warm = {g.tag: g.count for g in parse_spec(o.warm_spec)}
        for tag, n in parse_reads(o.preload_spec).items():
            if tag not in warm:
                raise ValueError(f"--preload-spec tag {tag!r} not in --warm-spec")
            if n > warm[tag]:
                raise ValueError(f"--preload-spec {tag}:{n} exceeds warmed count {warm[tag]}")
        parse_spec(o.writeout_spec)
        parse_range(o.process_delay)
        if o.workers < 1:
            raise ValueError("give --workers")

    Worker.fixed_count = o.workers
    if getattr(o, "num_users", None) is None:
        o.num_users = o.workers
        if not getattr(o, "spawn_rate", None):
            o.spawn_rate = o.workers
    elif o.num_users != o.workers:
        raise ValueError(f"-u {o.num_users} != --workers {o.workers}. "
                         f"Omit -u and it is derived for you.")

    # pay the botocore model load before a worker connects; see obsload.py
    boto3.client("s3", region_name="us-east-1")

    if isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message("batchload_go", _on_go)


def _build_globals(environment):
    """Must run at test_start, not init: see obsload.py."""
    global POOL, WARM, WARMN, READS, WRITEOUT, PROCESS_DELAY, CLIENTS
    o = environment.parsed_options
    WARM = parse_spec(o.warm_spec)
    WARMN = {g.tag: g.count for g in WARM}
    READS = parse_reads(o.preload_spec)
    WRITEOUT = parse_spec(o.writeout_spec)
    PROCESS_DELAY = parse_range(o.process_delay)

    if not isinstance(environment.runner, MasterRunner) or o.warm:
        POOL = random.randbytes(64 << 20)
        cycles = {}

        def clients(spec):
            spec = spec or o.s3_endpoint or ""
            if spec not in cycles:
                urls = [u.strip() for u in spec.split(",") if u.strip()]
                cycles[spec] = itertools.cycle(
                    [make_client(o, u) for u in urls or [None]])
            return cycles[spec]

        CLIENTS = {"preload": clients(o.preload_endpoint),
                   "writeout": clients(o.writeout_endpoint)}


def _on_go(environment, msg, **_):
    STATE.update(msg.data)
    log.info("go received: run=%s procs=%d", STATE["run"], STATE["procs"])
    GO.set()


def _throughput(w):
    """Same log line as obsload's; sum same-timestamp lines for cluster totals."""
    last, t0 = dict(TP), time.time()
    while True:
        gevent.sleep(w - time.time() % w)
        t1 = time.time()
        cur = dict(TP)
        rd, wr = cur["read"] - last["read"], cur["write"] - last["write"]
        dt = t1 - t0
        last, t0 = cur, t1
        if rd or wr:
            log.info("throughput: read %dB %.1f MiB/s write %dB %.1f MiB/s",
                     rd, rd / dt / (1 << 20), wr, wr / dt / (1 << 20))


@events.test_start.add_listener
def _test_start(environment, **_):
    _build_globals(environment)
    gevent.spawn(_throughput, environment.parsed_options.throughput_interval)
    if isinstance(environment.runner, WorkerRunner):
        return  # waits for the broadcast
    gevent.spawn(_prime, environment)  # never block test_start; see obsload.py


def _prime(environment):
    global GO_DATA
    o = environment.parsed_options
    runner = environment.runner

    if o.warm:
        _warm(environment)
        runner.quit()
        return

    procs = getattr(runner, "worker_count", 0) or 1
    GO_DATA = {"run": o.run_id or f"r{int(time.time())}", "procs": procs,
               "sent_at": time.time()}
    log.info("run=%s procs=%d", GO_DATA["run"], procs)
    if isinstance(runner, MasterRunner):
        runner.send_message("batchload_go", GO_DATA)
    else:
        STATE.update(GO_DATA)
        GO.set()


@events.worker_connect.add_listener
def _worker_connect(client_id, **_):
    # one-shot messages need a re-send on reconnect; see obsload.py
    if GO_DATA and ENV is not None and isinstance(ENV.runner, MasterRunner):
        ENV.runner.send_message("batchload_go", dict(GO_DATA, sent_at=time.time()),
                                client_id=client_id)


def _warm(environment):
    o = environment.parsed_options
    client = next(CLIENTS["preload"])
    rng = random.Random()

    def spawn(pool):
        for grp in WARM:
            for i in range(grp.count):
                pool.spawn(put_obj, client, o.bucket,
                           input_key(o, grp.tag, i), grp.size(rng), rng,
                           f"warm/{grp.tag}")

    _run_pool(64, spawn)
    log.info("warm complete: %d objects", sum(g.count for g in WARM))


def _run_pool(width, spawn):
    """See obsload.py: gevent does not kill pooled greenlets with their parent."""
    pool = GPool(width)
    try:
        spawn(pool)
        pool.join()
    finally:
        pool.kill(block=False)


class Worker(User):
    wait_time = constant(0)

    def on_start(self):
        # claim before blocking so on_stop pairs exactly; see obsload.py
        self.slot = _free_slots.popleft() if _free_slots else next(_next_slot)
        SEQ["alive"] += 1
        GO.wait()
        self.o = o = self.environment.parsed_options
        self.rng = random.Random()
        # stride by --workers so ids never depend on the live process count
        self.wid = getattr(self.environment.runner, "worker_index", 0) * o.workers + self.slot
        self.jobno = 0
        if "next" not in SEQ:
            n = WARMN.get("seq", 0)
            p, procs = getattr(self.environment.runner, "worker_index", 0), STATE["procs"]
            # a late-joining process (index >= procs) gets an empty slice
            # rather than re-reading someone else's keys
            SEQ["next"] = min(n * p // procs, n)
            SEQ["end"] = min(n * (p + 1) // procs, n)
        if o.slow_start:
            # uniform over workers/N seconds ~= N starts/sec cluster-wide,
            # with no knowledge of how workers are spread across processes
            gevent.sleep(self.rng.uniform(0, o.workers / o.slow_start))

    def on_stop(self):
        _free_slots.append(self.slot)
        SEQ["alive"] -= 1

    @task
    def job(self):
        o = self.o
        t0 = time.perf_counter()
        keys = []
        for tag, n in READS.items():
            if tag == "seq":
                take = min(n, SEQ["end"] - SEQ["next"])
                if take <= 0:
                    if SEQ["alive"] == 1:  # last local worker out quits
                        log.info("seq walk complete")
                        self.environment.runner.quit()
                    raise StopUser()
                keys += [(tag, i) for i in range(SEQ["next"], SEQ["next"] + take)]
                SEQ["next"] += take
            else:
                keys += [(tag, i) for i in self.rng.sample(range(WARMN[tag]), n)]

        t_pre = time.perf_counter()

        def spawn(pool):
            for tag, i in keys:
                pool.spawn(get_obj, next(CLIENTS["preload"]), o.bucket,
                           input_key(o, tag, i), f"preload/{tag}")

        _run_pool(o.concurrency, spawn)
        fire_since("preload/all", t_pre)

        gevent.sleep(self.rng.uniform(*PROCESS_DELAY))
        self._writeout()
        fire_since("job/all", t0)
        log.debug("w%05d j%06d done in %.2fs", self.wid, self.jobno,
                  time.perf_counter() - t0)
        self.jobno += 1

    def _writeout(self):
        o = self.o
        nbytes = 0
        t0 = time.perf_counter()

        def spawn(pool):
            nonlocal nbytes
            for grp in WRITEOUT:
                for i in range(grp.count):
                    size = grp.size(self.rng)
                    nbytes += size
                    pool.spawn(put_obj, next(CLIENTS["writeout"]), o.bucket,
                               out_key(o, self.wid, self.jobno, grp.tag, i), size,
                               self.rng, f"writeout/{grp.tag}")

        _run_pool(o.concurrency, spawn)
        fire_since("writeout/all", t0, nbytes)
