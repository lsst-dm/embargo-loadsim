r"""
obsload: S3 Rubin load simulator.

See README.md for basics. Internal cadence:

    visit time - --preload-lead A Detector puts a job on the queue.
                       A free Worker takes it and downloads the calibration
                       objects.
    visit time         Every Detector uploads its raw exposure file.
                       The Worker reads the exposure back, sleeps for
                       --process-delay, then uploads its work product.
    a few seconds on   A Consumer reads some of the work products (--consumers)
                       (optionally)

Visits repeat every --cadence seconds until --run-time ends.

How a run goes
    1. Locust starts a leader and its followers, or one single process.
    2. Each follower (or the single process) parses the options, builds its
       S3 clients, and starts its share of the Detector, Worker and Consumer
       users.
    3. The leader waits until every user is running, picks the time of
       visit 0, and sends it to every follower. Users wait for it.
    4. Every process computes the same visit times from visit 0 and
       --cadence, so detectors on every host upload together.
    5. Every 3 seconds, each follower sends the leader its byte and exposure
       counts. The leader logs cluster throughput and one line per visit.

Where code runs
    leader           user placement (EvenDispatcher), the visit schedule,
                     cluster logging, and the --warm uploads.
    follower         all workers. Sends its counts to the leader.
    single process   everything above (when neither --master nor --worker
                     is given)

A job stays in the process that made it: a Detector's job goes to a Worker in
the same process. EvenDispatcher gives each process its share of every user
class for that reason.

Concurrency
    Every user runs in a gevent greenlet: it will run until it
    waits (a sleep, a network call, a queue) and then lets another one run.
    Code runs concurrently instead of in parallel, so locks are not needed.

Glossary
    visit     one exposure, taken by all detectors at once
    job       one detector's exposure (raw), waiting for a Worker
    det       a detector id (det0000, det0001, ...), unique across processes
    spec      "tag:count:size[-maxsize],...": the objects one stage moves,
              for example --exposure-spec "raw:1:10MiB-14MiB,meta:1:4KiB"
    range     "min-max" or a single number, in seconds: --process-delay "20-28"
    window    a --throughput-interval slice of wall-clock time
    leader    the Locust process that coordinates a distributed run
    follower  a Locust process that runs users. Locust itself says master
              and worker, and its flags and classes keep those names:
              --master, --worker, MasterRunner, WorkerRunner.
    process   any Locust process: leader, follower or single process
    Worker    always a Worker user, from the pipeline's shared pool
    fire+X    in the logs: how late a detector started its exposure

Times named *_at are wall-clock readings (time.time()), which agree across
hosts. Times named t_* are local timer readings (time.perf_counter()), used
only to measure how long something took.

To run see README.md and example .sh files in configs/
"""

# Locust needs gevent's patched sockets, so patch before importing anything
# else. (Locust patches on import too; this covers python -c and pytest.)
from gevent import monkey

monkey.patch_all()

# The imports below follow patch_all() on purpose:
# ruff: noqa: E402
import io
import itertools
import logging
import os
import random
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from types import SimpleNamespace

import gevent
from gevent.event import Event
from gevent.pool import Pool as GPool
from gevent.queue import Empty, Queue

import boto3
import urllib3
from botocore.config import Config as BotoConfig

from locust import User, constant, events, task
from locust.runners import WORKER_REPORT_INTERVAL, MasterRunner, WorkerRunner

log = logging.getLogger("obsload")
throughput_log = logging.getLogger("obsload.throughput")  # its own logger, for the console

# Load botocore's S3 model now. Locust imports this file before it forks
# followers or connects to the leader, so this happens once per host. Loaded
# later, it can stall a process long enough to miss Locust's heartbeats.
boto3.client("s3", region_name="us-east-1")
# gevent imports its DNS resolver and thread pool on first use. Import them
# now, so that happens outside the first measured request.
gevent.config.resolver
gevent.config.threadpool
# The S3 clients skip TLS certificate checks. Hide the warning urllib3
# prints for every such request.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================================
# OPTIONS
# obsload's command line options. Locust adds its own: see locust --help.
# ============================================================================

@events.init_command_line_parser.add_listener
def _add_options(parser):
    g = parser.add_argument_group("obsload")

    # where requests go
    g.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"),
                   help="endpoint URL, or comma-separated URLs; each request "
                        "picks one at random")
    g.add_argument("--preload-endpoint", default="",
                   help="endpoint(s) for preload GETs and --warm PUTs; "
                        "default --s3-endpoint")
    g.add_argument("--exposure-endpoint", default="",
                   help="endpoint(s) for exposure PUTs and their readback GETs, "
                        "kept together for read-your-write; default --s3-endpoint")
    g.add_argument("--writeout-endpoint", default="",
                   help="endpoint(s) for writeout PUTs; default --s3-endpoint")
    g.add_argument("--consume-endpoint", default="",
                   help="endpoint(s) for consume GETs; default --s3-endpoint")
    g.add_argument("--s3-timeout", type=float, default=30.0,
                   help="connect/read timeout per attempt; there are no retries")
    g.add_argument("--bucket", default="obsload")
    g.add_argument("--prefix", default="obsload")
    g.add_argument("--run-id", default="",
                   help="raw/proc key namespace; default r<epoch> is unique per run")

    # users and the visit schedule
    g.add_argument("--detectors", type=int, default=189, help="N: Detector users")
    g.add_argument("--worker-pool", type=int, default=300, help="M: Worker users, M > N")
    g.add_argument("--cadence", default="36", help="seconds between visits, 'min-max' or a scalar")
    g.add_argument("--lead-in", type=float, default=5.0,
                   help="grace after all users spawn, before visit 0's preload")
    g.add_argument("--preload-lead", type=float, default=10.0,
                   help="enqueue the job this many seconds before the exposure")

    # what each stage moves
    g.add_argument("--preload-spec", default="bias:1:36MiB,dark:1:36MiB,flat:1:36MiB,cfg:20:4KiB")
    g.add_argument("--exposure-spec", default="raw:1:10MiB-14MiB,meta:1:4KiB")
    g.add_argument("--no-raw-readback", action="store_true",
                   help="skip re-reading the exposure's objects before the processing wait")
    g.add_argument("--process-delay", default="100-120",
                   help="simulated compute seconds, 'min-max' or a scalar")
    g.add_argument("--writeout-spec", default="calexp:1:48MiB,src:1:2MiB,metric:12:8KiB")

    # optional writeout consumers
    g.add_argument("--consumers", type=int, default=0,
                   help="downstream reader users; 0 disables the stage entirely")
    g.add_argument("--consume-delay", default="1-10",
                   help="seconds after writeout before the read, 'min-max' or a scalar")
    g.add_argument("--consume-tags", default="",
                   help="read back only these writeout tags, comma separated; empty = all")

    # everything else
    g.add_argument("--concurrency", type=int, default=8,
                   help="parallel transfers per user within one stage of one job")
    g.add_argument("--warm", action="store_true",
                   help="populate the preload keyspace, then exit; no load is run")
    g.add_argument("--throughput-interval", type=float, default=2.0,
                   help="seconds per throughput window; the leader logs "
                        "cluster totals per window")


# ============================================================================
# SHARED STATE
# Module-level values used by the code below. Every process has its own copy;
# processes exchange data only through Locust messages.
# ============================================================================

OPTS = None      # the command line options (see _on_init)
RUNNER = None    # this process's Locust runner: leader, follower or single process

# Built from OPTS at test start (see _setup_process).
SETUP = SimpleNamespace(
    preload_objects=None,   # ObjectSpecs a Worker downloads before each exposure
    exposure_objects=None,  # ObjectSpecs a Detector uploads at each visit
    writeout_objects=None,  # ObjectSpecs a Worker uploads after processing
    cadence=None,           # seconds between visits: (min, max)
    process_delay=None,     # seconds of simulated compute: (min, max)
    consume_delay=None,     # seconds from upload to a Consumer's read: (min, max)
    consume_tags=None,      # writeout tags Consumers read; empty means all
    clients=None,           # {stage: list of S3 clients, one per endpoint}
    payload=None,           # 64MiB of random bytes that every upload reads from
)

# The visit schedule, set by the leader and sent to every follower:
# {"visit0_at": when visit 0 happens, "run_id": key prefix for this run,
#  "followers": how many followers there were then}.
SCHEDULE = {}
SCHEDULE_READY = Event()  # users wait on this before their first visit

# Detectors pass jobs to Workers, and Workers pass read jobs to Consumers.
JOB_QUEUE = Queue()
READ_QUEUE = Queue()
# Worker and Consumer users in this process, and how many are busy with a job.
WORKERS = SimpleNamespace(total=0, busy=0)
CONSUMERS = SimpleNamespace(total=0, busy=0)

# Detector slot numbers in this process. A restarted Detector takes a freed
# slot, so it keeps the same detector id.
FREE_SLOTS = deque()
NEXT_SLOT = itertools.count()

# Counts kept by every process. Followers send theirs to the leader, which
# adds them up (see CLUSTER LOG). Window w covers wall-clock seconds
# w*interval to (w+1)*interval.
BYTES_MOVED = {}        # {window: {"read": bytes, "write": bytes}}
EXPOSURES_STARTED = {}  # {visit: {"detectors": count, "max_late": seconds}}
# Leader (or single process): its place in the log, and the numbers for the
# closing summary.
LOG_STATE = SimpleNamespace(
    next_window=None,           # next window to log; None until the test starts
    next_visit=0,               # next visit to log
    next_visit_at=0.0,          # and its time
    summary_from=float("inf"),  # the summary covers windows ending after this
    rates=[],                   # (read, write) MiB/s of each window in the summary
    stopped_at=None,            # when the test stopped
)


@dataclass
class Job:
    """One detector's exposure for one visit, waiting for a Worker."""
    visit: int
    det: int
    visit_at: float                   # the visit's time
    t_queued: float                   # when the job was queued
    exposure_done: Event              # set when the detector's upload ends, however it ends
    t_exposed: float = 0.0            # when the upload started
    outcome: str = "abandoned"        # becomes "uploaded" or "failed" when the upload returns
    keys: list = field(default_factory=list)  # (tag, key) of each object uploaded

    def __str__(self):  # what f"{job}" prints
        return f"v{self.visit:06d}/det{self.det:04d}"


@dataclass
class ReadJob:
    """A job's uploaded results, for a Consumer to read at read_at."""
    visit: int
    det: int
    keys: list        # (tag, key) of each object to read
    read_at: float    # when to read them
    t_queued: float   # when the read job was queued

    def __str__(self):
        return f"v{self.visit:06d}/det{self.det:04d}"


# ============================================================================
# DETECTOR
# One Detector user per simulated detector. Each visit it queues a job for
# the Workers, then uploads its exposure at the visit time.
# ============================================================================

def visit_gap(visit):
    """Seconds from this visit to the next. Every process gets the same
    answer: a random --cadence is seeded with the visit number."""
    lo, hi = SETUP.cadence
    # random() is the one method Python keeps stable across versions
    return lo if lo == hi else lo + random.Random(visit).random() * (hi - lo)


class Detector(User):
    wait_time = constant(0)  # call visit() again as soon as it returns

    def on_start(self):
        """Locust calls this once, before the first visit()."""
        # Take a slot before anything that can block. Locust calls on_stop
        # even when it stops this user during on_start, and on_stop returns it.
        self.slot = FREE_SLOTS.popleft() if FREE_SLOTS else next(NEXT_SLOT)
        SCHEDULE_READY.wait()
        self.rng = random.Random()
        # Each follower numbers its detectors from its worker_index times
        # --detectors, so ids stay unique as followers come and go.
        self.det = getattr(RUNNER, "worker_index", 0) * OPTS.detectors + self.slot
        self.next_visit = 0
        self.next_visit_at = SCHEDULE["visit0_at"]

    def on_stop(self):
        FREE_SLOTS.append(self.slot)

    @task
    def visit(self):
        """One visit: queue a job for the Workers, then upload the exposure
        at the visit time. Locust calls this in a loop."""
        visit, visit_at = self._pick_visit()
        sleep_until(visit_at - OPTS.preload_lead)
        job = self._queue_job(visit, visit_at)
        try:
            sleep_until(visit_at)
            self._upload_exposure(job)
        finally:
            job.exposure_done.set()  # a Worker waits on this, so set it however we leave

    def _pick_visit(self):
        """Return the next visit still ahead, and its time. Visits that
        already passed count as visit/skipped."""
        skipped, first_missed_at = 0, self.next_visit_at
        while self.next_visit_at <= time.time():
            self.next_visit_at += visit_gap(self.next_visit)
            self.next_visit += 1
            skipped += 1
        # A detector that starts mid-run catches up to the present silently.
        if skipped and self.next_visit > skipped:
            record_failure("visit/skipped", self.next_visit_at - first_missed_at,
                           f"det{self.det:04d} skipped {skipped} visit(s)")
        visit, visit_at = self.next_visit, self.next_visit_at
        self.next_visit_at += visit_gap(visit)
        self.next_visit += 1
        return visit, visit_at

    def _queue_job(self, visit, visit_at):
        job = Job(visit=visit, det=self.det, visit_at=visit_at,
                  t_queued=time.perf_counter(), exposure_done=Event())
        JOB_QUEUE.put(job)
        # How much of --preload-lead the Worker gets. It shrinks when the
        # last exposure ran long; sched/dispatch shows a saturated pool.
        record("sched/lead", max(0.0, visit_at - time.time()))
        idle = WORKERS.total - WORKERS.busy
        if JOB_QUEUE.qsize() > idle:
            record_failure("sched/pool-exhausted", 0.0,
                           f"qdepth={JOB_QUEUE.qsize()} idle={idle}")
        return job

    def _upload_exposure(self, job):
        job.t_exposed = time.perf_counter()
        late = time.time() - job.visit_at
        count_exposure(job.visit, 1, late)
        objects = [(obj.tag, raw_key(job.visit, job.det, obj), obj.size(self.rng))
                   for obj in SETUP.exposure_objects]
        job.keys = upload_all("exposure", objects, SETUP.clients["exposure"], self.rng)
        job.outcome = "uploaded" if len(job.keys) == len(objects) else "failed"

        overran = time.time() - (job.visit_at + visit_gap(job.visit))
        if overran > 0:
            record_failure("visit/overrun", overran,
                           f"det{job.det:04d} ran {overran:.1f}s past its slot")
        log.debug("v%06d det%04d fire%+.3fs exp=%.2fs%s", job.visit, job.det, late,
                  time.perf_counter() - job.t_exposed, "" if job.outcome == "uploaded" else " FAILED")


# ============================================================================
# WORKER
# The shared pool, larger than the number of Detectors. A Worker takes one job
# and keeps it from the calibration download to the result upload.
# ============================================================================

class Worker(User):
    wait_time = constant(0)  # call serve() again as soon as it returns

    def on_start(self):
        """Locust calls this once, before the first serve()."""
        # Count this user before anything that can block. Locust calls
        # on_stop even when it stops this user during on_start.
        WORKERS.total += 1
        SCHEDULE_READY.wait()
        self.rng = random.Random()

    def on_stop(self):
        WORKERS.total -= 1

    @task
    def serve(self):
        """Take the next job and see it through. Locust calls this in a loop."""
        try:
            # returns every second, so a graceful stop (--stop-timeout) can end this user
            job = JOB_QUEUE.get(timeout=1.0)
        except Empty:
            return
        WORKERS.busy += 1
        try:
            self._process(job)
        finally:
            WORKERS.busy -= 1

    def _process(self, job):
        """Download the calibration, wait for the exposure, read it back,
        "compute", and upload the results. Stops early, with a failure row,
        for a job too old to do or an exposure that failed or never ended."""
        # Time the job sat on the queue. It rises as the pool saturates.
        record_since("sched/dispatch", job.t_queued)

        late = time.time() - job.visit_at
        if late > sum(SETUP.cadence):  # two average visit gaps
            record_failure("sched/stale", late,
                           f"{job} claimed {late:.1f}s after its exposure; dropped")
            return
        # Skip the calibration when the exposure already ended without an upload.
        ended_badly = job.exposure_done.is_set() and job.outcome != "uploaded"
        if not ended_badly:
            if late > 0:
                # Processing needs the calibration, so a late job still downloads it.
                record_failure("sched/preload-late", late,
                               f"{job} claimed after its exposure; preload ran late")
            self._download_calibration(job)

        wait_limit = 2 * sum(SETUP.cadence)
        if not job.exposure_done.wait(timeout=wait_limit):
            record_failure("sched/abandoned", wait_limit, f"{job} exposure never completed")
            return
        if job.outcome == "abandoned":
            record_failure("sched/abandoned", 0.0,
                           f"{job} detector stopped before uploading its exposure")
            return
        if job.outcome != "uploaded":
            record_failure("pipeline/skipped", 0.0, f"{job} exposure {job.outcome}; no writeout")
            return

        # Read the exposure back, then "compute"; the Worker is busy for both.
        # These reads hit objects written seconds ago, so expect them cache-hot.
        if not OPTS.no_raw_readback:
            download_all("readback", job.keys, SETUP.clients["exposure"])
        gevent.sleep(self.rng.uniform(*SETUP.process_delay))
        self._upload_results(job)
        record_since("pipeline/e2e", job.t_exposed)  # exposure start to results uploaded

    def _download_calibration(self, job):
        # --warm filled calibration sets for detectors 0 to N-1. Spread the
        # followers' detectors over them, so each detector reads its own set.
        follower, slot = divmod(job.det, OPTS.detectors)
        det = (slot * SCHEDULE["followers"] + follower) % OPTS.detectors
        keys = [(obj.tag, calib_key(det, obj)) for obj in SETUP.preload_objects]
        download_all("preload", keys, SETUP.clients["preload"])

    def _upload_results(self, job):
        objects = [(obj.tag, proc_key(job.visit, job.det, obj), obj.size(self.rng))
                   for obj in SETUP.writeout_objects]
        uploaded = upload_all("writeout", objects, SETUP.clients["writeout"], self.rng)
        self._offer_to_consumers(job, uploaded)

    def _offer_to_consumers(self, job, uploaded):
        """Queue the uploaded results for a Consumer to read later. This never
        waits, so the Worker goes straight back to the pool."""
        if not OPTS.consumers:
            return
        if SETUP.consume_tags:
            uploaded = [(tag, key) for tag, key in uploaded if tag in SETUP.consume_tags]
        if not uploaded:
            return
        idle = CONSUMERS.total - CONSUMERS.busy
        READ_QUEUE.put(ReadJob(
            visit=job.visit, det=job.det, keys=uploaded,
            read_at=time.time() + self.rng.uniform(*SETUP.consume_delay),
            t_queued=time.perf_counter(),
        ))
        if READ_QUEUE.qsize() > idle:
            record_failure("sched/consume-exhausted", 0.0,
                           f"qdepth={READ_QUEUE.qsize()} idle={idle}")


# ============================================================================
# CONSUMER
# Reads results back a little while after a Worker uploads them.
# Off unless --consumers is above 0.
# ============================================================================

class Consumer(User):
    wait_time = constant(0)  # call consume() again as soon as it returns

    def on_start(self):
        """Locust calls this once, before the first consume()."""
        CONSUMERS.total += 1  # before anything that can block; see Worker.on_start
        SCHEDULE_READY.wait()

    def on_stop(self):
        CONSUMERS.total -= 1

    @task
    def consume(self):
        """Take the next read job, wait until its read time, and read it.
        Locust calls this in a loop."""
        try:
            # returns every second, so a graceful stop (--stop-timeout) can end this user
            job = READ_QUEUE.get(timeout=1.0)
        except Empty:
            return
        CONSUMERS.busy += 1
        try:
            record_since("sched/consume-dispatch", job.t_queued)
            # read_at was set at upload time, so a busy Consumer pool shows up
            # here as reads that start late.
            sleep_until(job.read_at)
            late = time.time() - job.read_at
            if late > 0.5:
                record_failure("sched/consume-late", late,
                               f"{job} read {late:.1f}s later than requested")
            download_all("consume", job.keys, SETUP.clients["consume"])
        finally:
            CONSUMERS.busy -= 1


# ============================================================================
# S3
# Uploads and downloads, each recorded in Locust's stats, and what they need:
# payload bytes, clients and object names.
# ============================================================================

def upload(client, key, size, rng, stat_name):
    """PUT size random bytes to key and record it as stat_name.
    Returns True if it worked."""
    body = PayloadReader(size, rng.randrange(len(SETUP.payload)))
    t0 = time.perf_counter()
    try:
        client.put_object(Bucket=OPTS.bucket, Key=key, Body=body, ContentLength=size)
    except Exception as exc:
        record_since(stat_name, t0, 0, exc, kind="S3P")
        return False
    record_since(stat_name, t0, size, kind="S3P")
    return True


def download(client, key, stat_name):
    """GET key, read the whole body, and record it as stat_name.
    Returns True if it worked."""
    t0 = time.perf_counter()
    try:
        response = client.get_object(Bucket=OPTS.bucket, Key=key)
        n = 0
        # read to the end, so the time covers the whole transfer
        for chunk in response["Body"].iter_chunks(1 << 20):
            n += len(chunk)
            count_bytes("read", len(chunk))
    except Exception as exc:
        record_since(stat_name, t0, 0, exc, kind="S3G")
        return False
    record_since(stat_name, t0, n, kind="S3G")
    return True


def download_all(stage, keys, clients):
    """Download every (tag, key) in keys, --concurrency at a time. Each object
    is recorded as stage/tag, and the whole batch as stage/all."""
    t0 = time.perf_counter()
    run_concurrently(OPTS.concurrency, [(download, random.choice(clients), key, f"{stage}/{tag}")
                                        for tag, key in keys])
    record_since(f"{stage}/all", t0)


def upload_all(stage, objects, clients, rng):
    """Upload every (tag, key, size) in objects, --concurrency at a time,
    recorded like download_all. Returns the (tag, key) of each upload that
    worked."""
    t0 = time.perf_counter()
    worked = run_concurrently(OPTS.concurrency,
                              [(upload, random.choice(clients), key, size, rng, f"{stage}/{tag}")
                               for tag, key, size in objects])
    uploaded, moved = [], 0
    for (tag, key, size), ok in zip(objects, worked):
        if ok:
            uploaded.append((tag, key))
            moved += size
    record_since(f"{stage}/all", t0, moved)
    return uploaded


def run_concurrently(width, calls):
    """Run every call, a (function, arg, ...) tuple, at most width at a time,
    wait for all of them, and return what each one returned, in order."""
    pool = GPool(width)
    greenlets = []
    try:
        for call in calls:
            greenlets.append(pool.spawn(*call))
        pool.join()
    finally:
        pool.kill(block=False)  # if we're killed mid-join, gevent would leave the calls running
    return [g.value for g in greenlets]


class PayloadReader(io.RawIOBase):
    """A read-only file of size bytes, taken from SETUP.payload starting at
    offset and wrapping around at its end. botocore reads each upload body
    from one of these, so an upload of any size needs no memory of its own."""

    def __init__(self, size, offset):
        self.size = size
        self.offset = offset % len(SETUP.payload)
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

    def readinto(self, buf):
        start = (self.offset + self.pos) % len(SETUP.payload)
        # stop at the end of the payload; botocore calls again for the rest
        n = min(len(buf), self.size - self.pos, len(SETUP.payload) - start)
        if n <= 0:
            return 0
        buf[:n] = SETUP.payload[start:start + n]
        self.pos += n
        count_bytes("write", n)  # counted as botocore sends it
        return n


def make_client(endpoint):
    """An S3 client for endpoint, set up to keep the load generator's own CPU
    out of the measured times."""
    config = BotoConfig(
        # Keep up to 4096 connections per client open for reuse. A burst can
        # open more, and urllib3 closes the extras when they finish.
        # Raise `ulimit -n` on big runs.
        max_pool_connections=4096,
        # One attempt per request, so every failure shows up in the stats.
        retries={"total_max_attempts": 1, "mode": "standard"},
        connect_timeout=OPTS.s3_timeout,
        read_timeout=OPTS.s3_timeout,
        signature_version="s3v4",
        s3={
            "addressing_style": "path",
            # Sign requests without hashing the body. Over http, SigV4 hashes
            # every byte, which takes enough CPU to show up in request times.
            "payload_signing_enabled": False,
        },
        # Checksum only when S3 requires it. botocore 1.36 and later
        # checksum every request by default.
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    return boto3.client("s3", endpoint_url=endpoint,
                        region_name=os.environ.get("AWS_REGION", "us-east-1"),
                        verify=False, config=config)


# Object names. Calibration objects sit outside the run id, so one --warm
# serves every run. Raw and processed objects are new in each run.
def calib_key(det, obj):
    return f"{OPTS.prefix}/calib/{obj.tag}/det{det:04d}/{obj.index:06d}"


def raw_key(visit, det, obj):
    return (f"{OPTS.prefix}/raw/{SCHEDULE['run_id']}/v{visit:06d}/det{det:04d}/"
            f"{obj.tag}-{obj.index:04d}")


def proc_key(visit, det, obj):
    return (f"{OPTS.prefix}/proc/{SCHEDULE['run_id']}/v{visit:06d}/det{det:04d}/"
            f"{obj.tag}-{obj.index:04d}")


# ============================================================================
# RUN SETUP
# Locust calls these hooks, in this order: _on_init when a process starts,
# then _on_test_start. The leader then starts the visit schedule and sends it
# to every follower.
# ============================================================================

@events.init.add_listener
def _on_init(environment, **_):
    """Locust calls this once per process at startup. A follower has only its
    own command line here; the leader's options arrive at test start."""
    global OPTS, RUNNER
    OPTS = environment.parsed_options
    RUNNER = environment.runner

    # botocore's DEBUG logging writes a line for every internal event and
    # buries everything else. Set OBSLOAD_BOTOCORE_LOGS=1 to keep it.
    if not os.environ.get("OBSLOAD_BOTOCORE_LOGS"):
        for name in ("botocore", "boto3", "urllib3", "s3transfer"):
            logging.getLogger(name).setLevel(logging.WARNING)

    # With --logfile, Locust sends log lines to the file only. The leader, or a
    # single process, also shows obsload's warnings, errors and throughput
    # lines on the console.
    if OPTS.logfile and not is_follower():
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s obsload: %(message)s")
        problems = logging.StreamHandler()
        problems.setFormatter(fmt)
        problems.setLevel(logging.WARNING)
        log.addHandler(problems)
        # delete these three lines to keep throughput off the console
        throughput_console = logging.StreamHandler()
        throughput_console.setFormatter(fmt)
        throughput_log.addHandler(throughput_console)

    environment.dispatcher_class = EvenDispatcher

    # Parse the spec and range strings now, so a typo stops the run at startup.
    for spec in (OPTS.preload_spec, OPTS.exposure_spec, OPTS.writeout_spec):
        parse_spec(spec)
    for text in (OPTS.cadence, OPTS.process_delay, OPTS.consume_delay):
        parse_range(text)
    endpoints = [OPTS.s3_endpoint or "", OPTS.preload_endpoint, OPTS.exposure_endpoint,
                 OPTS.writeout_endpoint, OPTS.consume_endpoint]
    for url in ",".join(endpoints).split(","):
        if url.strip() and "://" not in url:
            raise ValueError(f"endpoint {url.strip()!r} needs http:// or https://")

    # A warm runs until every object is written, whatever --run-time says.
    if OPTS.warm:
        OPTS.run_time = None

    # Run exactly this many users of each class.
    Detector.fixed_count = OPTS.detectors
    Worker.fixed_count = OPTS.worker_pool
    Consumer.fixed_count = OPTS.consumers

    # Locust checks -u against the counts above before this hook runs, while
    # they are all still 0. So obsload fills in -u here, and stops the run
    # when a given -u doesn't match.
    need = OPTS.detectors + OPTS.worker_pool + OPTS.consumers
    if getattr(OPTS, "num_users", None) is None:
        OPTS.num_users = need
        if not getattr(OPTS, "spawn_rate", None):
            OPTS.spawn_rate = need
    elif OPTS.num_users != need:
        raise ValueError(
            f"-u {OPTS.num_users} != --detectors {OPTS.detectors} + --worker-pool "
            f"{OPTS.worker_pool} + --consumers {OPTS.consumers} = {need}. "
            f"Omit -u and it is derived for you."
        )

    if is_follower():
        RUNNER.register_message("obsload_schedule", _on_schedule)


def _setup_process():
    """Fill SETUP from the options, and make the payload and S3 clients when
    this process moves data. Runs at test start, when a follower has the
    leader's options."""
    SETUP.preload_objects = parse_spec(OPTS.preload_spec)
    SETUP.exposure_objects = parse_spec(OPTS.exposure_spec)
    SETUP.writeout_objects = parse_spec(OPTS.writeout_spec)
    SETUP.cadence = parse_range(OPTS.cadence)
    SETUP.process_delay = parse_range(OPTS.process_delay)
    SETUP.consume_delay = parse_range(OPTS.consume_delay)
    SETUP.consume_tags = {t.strip() for t in OPTS.consume_tags.split(",") if t.strip()}

    if is_leader() and not OPTS.warm:
        return  # the leader moves data only when it warms
    SETUP.payload = random.randbytes(64 << 20)

    # One set of clients per process, shared by its users and built before
    # they start: building one per user takes long enough to miss visit 0.
    # Each stage gets one client per endpoint (its own option, or
    # --s3-endpoint), and each request picks one at random. Stages with the
    # same endpoints share clients.
    built = {}

    def clients_for(endpoints):
        endpoints = endpoints or OPTS.s3_endpoint or ""
        if endpoints not in built:
            urls = [u.strip() for u in endpoints.split(",") if u.strip()]
            # with no endpoint at all, boto3 talks to AWS itself
            built[endpoints] = [make_client(u) for u in urls or [None]]
        return built[endpoints]

    SETUP.clients = {"preload": clients_for(OPTS.preload_endpoint),
                     "exposure": clients_for(OPTS.exposure_endpoint),
                     "writeout": clients_for(OPTS.writeout_endpoint),
                     "consume": clients_for(OPTS.consume_endpoint)}


@events.test_start.add_listener
def _on_test_start(environment, **_):
    _setup_process()
    if is_follower():
        return  # its users wait for the schedule from the leader
    gevent.spawn(_log_loop)
    gevent.spawn(_start_visits)  # in the background: blocking here would stall heartbeats


def _start_visits():
    """Leader or single process. With --warm: upload the calibration objects
    and quit. Otherwise wait until every user is running, then set the time
    of visit 0 and send the schedule to every follower."""
    if OPTS.warm:
        LOG_STATE.summary_from = time.time()
        _warm()
        RUNNER.quit()
        return

    # A follower reports its users once its setup is done, so this also waits
    # out slow-starting followers.
    while RUNNER.user_count < RUNNER.target_user_count:
        log.info("waiting for users: %d of %d spawned",
                 RUNNER.user_count, RUNNER.target_user_count)
        gevent.sleep(1)

    visit0_at = time.time() + OPTS.lead_in + OPTS.preload_lead
    SCHEDULE.update(visit0_at=visit0_at, run_id=OPTS.run_id or f"r{int(visit0_at)}",
                    followers=RUNNER.worker_count)
    log.info("visit schedule: visit 0 at %.3f (%s), cadence %s, run %s",
             visit0_at, hhmmss(visit0_at), OPTS.cadence, SCHEDULE["run_id"])
    LOG_STATE.next_visit_at = visit0_at  # the log walks the visits from here
    LOG_STATE.summary_from = visit0_at   # and the summary starts here
    if is_leader():
        RUNNER.send_message("obsload_schedule", dict(SCHEDULE, sent_at=time.time()))
    SCHEDULE_READY.set()  # starts a single process's users; the leader has none


def _on_schedule(environment, msg, **_):
    """Follower: store the schedule from the leader, and start the users."""
    SCHEDULE.update(msg.data)
    log.info("visit schedule received %.3fs after the leader sent it",
             time.time() - msg.data["sent_at"])
    SCHEDULE_READY.set()


def _warm():
    """Upload the calibration objects Workers download: --preload-spec for
    each of --detectors, 64 at a time, through the first preload client."""
    client = SETUP.clients["preload"][0]
    rng = random.Random()
    calls = []
    for det in range(OPTS.detectors):
        for obj in SETUP.preload_objects:
            calls.append((upload, client, calib_key(det, obj), obj.size(rng), rng,
                          f"warm/{obj.tag}"))
    uploaded = run_concurrently(64, calls).count(True)
    level = logging.INFO if uploaded == len(calls) else logging.ERROR
    log.log(level, "warm complete: %d of %d objects uploaded, across %d detectors",
            uploaded, len(calls), OPTS.detectors)


# ============================================================================
# USER PLACEMENT
# How many users of each class each follower runs, and reports of followers
# lost during the run.
# ============================================================================

class EvenDispatcher:
    """Locust's dispatcher_class: decides how many users of each class each
    follower runs. Every follower gets an even share of every class, since a
    process's jobs can only go to its own Workers. Leftover users go to the
    same followers for each class, spread across hosts. All users start at
    once (-r is ignored). A single process counts as the one follower.

    Locust uses exactly these methods, plus the dispatch_in_progress flag,
    and passes worker_nodes and worker_node by keyword, so those names stay
    Locust's. It calls add_worker and remove_worker when a follower joins,
    comes back, stops responding or quits mid-run. It skips them at the
    normal end of a run, so remove_worker is the place to report a lost
    follower."""

    def __init__(self, worker_nodes, user_classes):
        self.followers = list(worker_nodes)
        self.user_classes = user_classes
        self.pending = []  # the assignment for Locust to take next
        self.dispatch_in_progress = False

    def add_worker(self, worker_node):
        # a follower that comes back may still be listed
        self.followers = [node for node in self.followers if node.id != worker_node.id]
        self.followers.append(worker_node)
        log.info("followers: %s joined; %d followers", worker_node.id, len(self.followers))

    def remove_worker(self, worker_node):
        self.followers = [node for node in self.followers if node.id != worker_node.id]
        # a failure row, so the loss shows up in the stats table and CSV
        record_failure("cluster/followers-lost", 0.0, f"{worker_node.id} lost")
        log.error("FOLLOWERS LOST: %s went missing or quit; %d followers left",
                  worker_node.id, len(self.followers))
        if not self.followers:
            log.error("TEST STOPPED: Locust stops when no followers are left; no load "
                      "runs for the rest of --run-time, even if followers come back")

    def new_dispatch(self, target_user_count, spawn_rate, user_classes=None):
        self.pending = [self._assign_users()]
        self.dispatch_in_progress = True

    def __iter__(self):
        return self

    def __next__(self):
        if not self.pending:
            self.dispatch_in_progress = False
            raise StopIteration
        return self.pending.pop()

    def _assign_users(self):
        """Return {follower id: {user class name: count}}."""
        # Order the followers: the first on each host, then the second on each
        # host, and so on, so leftover users spread across hosts. Followers
        # marked missing count too: Locust gives a returning follower its users
        # before it clears the mark.
        per_host, ranked = defaultdict(int), []
        for node in sorted(self.followers, key=lambda node: node.id):
            host = node.id.rsplit("_", 1)[0]  # Locust's ids are <hostname>_<hex>
            ranked.append((per_host[host], node.id))
            per_host[host] += 1
        order = [node_id for _, node_id in sorted(ranked)]

        counts = {node_id: {} for node_id in order}
        for cls in self.user_classes:
            share, extra = divmod(cls.fixed_count, len(order))
            for i, node_id in enumerate(order):
                counts[node_id][cls.__name__] = share + (1 if i < extra else 0)
        _check_mix(counts)
        return counts


def _check_mix(counts):
    """Log the users each process got, one line per distinct mix. Flag a
    process whose jobs would never be taken, or whose visits can't overlap."""
    mixes = Counter((c["Detector"], c["Worker"], c["Consumer"]) for c in counts.values())
    for (detectors, workers, consumers), processes in sorted(mixes.items()):
        who = (f"{processes} process(es) with {detectors} detectors, "
               f"{workers} workers, {consumers} consumers")
        log.info("census: %s", who)
        if detectors and not workers:
            log.error("census: %s: detectors with no local worker pool - jobs will "
                      "never be claimed. Raise --worker-pool or reduce the number of "
                      "Locust processes.", who)
        elif workers and workers <= detectors:
            log.warning("census: %s: worker pool <= detectors; visits cannot overlap",
                        who)
        if OPTS.consumers and workers and not consumers:
            log.error("census: %s: workers with no local consumer pool - read jobs "
                      "will never be claimed. Raise --consumers or reduce the number "
                      "of processes.", who)


# ============================================================================
# CLUSTER LOG
# Every process counts the bytes it moves and the exposures its detectors
# start. Followers send their counts to the leader with Locust's regular
# stats report. The leader adds them up and logs a throughput line per
# window and a line per visit.
# ============================================================================

def count_bytes(direction, n):
    """Add n bytes, "read" or "write", to the current window."""
    window = int(time.time() // OPTS.throughput_interval)
    moved = BYTES_MOVED.setdefault(window, {"read": 0, "write": 0})
    moved[direction] += n


def count_exposure(visit, detectors, late):
    """Add detectors that started visit's exposure, the latest late seconds
    after the visit time."""
    started = EXPOSURES_STARTED.setdefault(visit, {"detectors": 0, "max_late": 0.0})
    started["detectors"] += detectors
    started["max_late"] = max(started["max_late"], late)


@events.report_to_master.add_listener
def _send_counts(client_id, data, **_):
    """Follower: attach this follower's counts to Locust's stats report to
    the leader (every 3s, and once more after the users stop), then start
    counting from zero."""
    data["obsload"] = {"bytes": dict(BYTES_MOVED), "exposures": dict(EXPOSURES_STARTED)}
    BYTES_MOVED.clear()
    EXPOSURES_STARTED.clear()


@events.worker_report.add_listener
def _add_counts(client_id, data, **_):
    """Leader: add one follower's counts to the cluster totals. Counts for a
    line already logged (from a stalled follower) get a warning line of their
    own, so the log still adds up."""
    counts = data.get("obsload")
    if not counts or LOG_STATE.next_window is None:
        return  # the log starts at test start
    for window, moved in counts["bytes"].items():
        if window < LOG_STATE.next_window:
            log.warning("throughput: late report for the window at %s: read %dB "
                        "write %dB, missing from that window's line",
                        hhmmss(window * OPTS.throughput_interval),
                        moved["read"], moved["write"])
            continue
        total = BYTES_MOVED.setdefault(window, {"read": 0, "write": 0})
        total["read"] += moved["read"]
        total["write"] += moved["write"]
    for visit, started in counts["exposures"].items():
        if visit < LOG_STATE.next_visit:
            log.warning("v%06d: %d more detector(s) fired, max fire+%.3fs, reported "
                        "after the visit's line", visit, started["detectors"],
                        started["max_late"])
            continue
        count_exposure(visit, started["detectors"], started["max_late"])


def _log_loop():
    """Leader or single process: at the end of each window, log the windows
    and visits whose counts are complete. Followers report every 3s, so with
    2s windows a window is logged 5s after it ends, and a visit 7s after its
    time."""
    interval = OPTS.throughput_interval
    LOG_STATE.next_window = int(time.time() // interval)
    while True:
        gevent.sleep(interval - time.time() % interval)
        cutoff = time.time() - WORKER_REPORT_INTERVAL - interval
        _log_windows(cutoff)
        _log_visits(cutoff - interval)


def _log_windows(cutoff):
    """Log throughput for each window that ended by cutoff, empty ones too."""
    interval = OPTS.throughput_interval
    mib = 1 << 20
    while (LOG_STATE.next_window + 1) * interval <= cutoff:
        window = LOG_STATE.next_window
        moved = BYTES_MOVED.pop(window, {"read": 0, "write": 0})
        read, write = moved["read"] / interval / mib, moved["write"] / interval / mib
        throughput_log.info("throughput %s %gs: read %.1f MiB/s write %.1f MiB/s (%dB %dB)",
                            hhmmss(window * interval), interval, read, write,
                            moved["read"], moved["write"])
        if (window + 1) * interval > LOG_STATE.summary_from:
            LOG_STATE.rates.append((read, write))
        LOG_STATE.next_window = window + 1


def _log_visits(cutoff):
    """Log how many detectors started each visit whose time was by cutoff."""
    while SCHEDULE and LOG_STATE.next_visit_at <= cutoff:
        visit = LOG_STATE.next_visit
        started = EXPOSURES_STARTED.pop(visit, {"detectors": 0, "max_late": 0.0})
        level = logging.INFO if started["detectors"] == OPTS.detectors else logging.WARNING
        log.log(level, "v%06d: %d of %d detectors fired, max fire+%.3fs",
                visit, started["detectors"], OPTS.detectors, started["max_late"])
        LOG_STATE.next_visit_at += visit_gap(visit)
        LOG_STATE.next_visit = visit + 1


@events.test_stop.add_listener
def _on_test_stop(environment, **_):
    LOG_STATE.stopped_at = time.time()  # the final log stops here


@events.quitting.add_listener
def _on_quitting(environment, **_):
    """Leader or single process: log what is left, then the summary. Locust
    calls this after waiting briefly for the followers' last reports."""
    if is_follower() or LOG_STATE.next_window is None:
        return  # a follower, or the test never started
    interval = OPTS.throughput_interval
    stopped_at = LOG_STATE.stopped_at or time.time()
    # every window up to the stop, plus any later one that still holds bytes
    last = max([int(stopped_at // interval)] + list(BYTES_MOVED))
    _log_windows((last + 1) * interval)
    _log_visits(stopped_at)

    rates = LOG_STATE.rates
    if rates:
        reads = [read for read, _ in rates]
        writes = [write for _, write in rates]
        throughput_log.info("throughput summary, %d windows: read mean %.1f "
                            "peak %.1f MiB/s, write mean %.1f peak %.1f MiB/s",
                            len(rates), sum(reads) / len(reads), max(reads),
                            sum(writes) / len(writes), max(writes))


# ============================================================================
# HELPERS
# ============================================================================

def record(name, seconds, size=0, error=None, kind="OBS"):
    """Add one sample to row name of Locust's stats table; an error makes it
    a failure. kind fills the Type column: OBS for obsload's own rows, S3G
    and S3P for single GETs and PUTs."""
    events.request.fire(request_type=kind, name=name, response_time=seconds * 1000.0,
                        response_length=size, exception=error, context=None)


def record_since(name, t0, size=0, error=None, kind="OBS"):
    """record() the time since t0, a time.perf_counter() reading."""
    record(name, time.perf_counter() - t0, size, error, kind)


def record_failure(name, seconds, message):
    """record() a failure. message shows in Locust's error report."""
    record(name, seconds, error=Exception(message))


def sleep_until(when):
    """Sleep until the wall-clock time when, or return at once if it has
    passed. Other users run in the meantime."""
    gevent.sleep(max(0.0, when - time.time()))


def is_leader():
    return isinstance(RUNNER, MasterRunner)


def is_follower():
    return isinstance(RUNNER, WorkerRunner)


def hhmmss(when):
    return time.strftime("%H:%M:%S", time.localtime(when))


# ============================================================================
# PARSING OPTION STRINGS
# ============================================================================

UNITS = {
    "": 1, "b": 1,
    "k": 1000, "kb": 1000, "kib": 1024,
    "m": 10**6, "mb": 10**6, "mib": 1024**2,
    "g": 10**9, "gb": 10**9, "gib": 1024**3,
}


def parse_size(text):
    """'36MiB' -> 37748736. Units ignore case; a bare number is bytes."""
    text = text.strip().lower()
    i = 0
    while i < len(text) and (text[i].isdigit() or text[i] == "."):
        i += 1
    return int(float(text[:i]) * UNITS[text[i:].strip()])


@dataclass
class ObjectSpec:
    """One object from a spec: the index'th object of its tag, sized
    between min_size and max_size."""
    tag: str
    index: int
    min_size: int
    max_size: int

    def size(self, rng):
        if self.min_size == self.max_size:
            return self.min_size
        return rng.randint(self.min_size, self.max_size)


def parse_spec(spec):
    """'raw:1:10MiB-14MiB,meta:1:4KiB' -> one ObjectSpec per object."""
    objects = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        tag, count, size = part.split(":", 2)
        tag = tag.strip()
        if any(obj.tag == tag for obj in objects):
            raise ValueError(f"{spec!r}: tag {tag!r} appears twice, so its keys would collide")
        min_size, _, max_size = size.partition("-")
        min_size, max_size = parse_size(min_size), parse_size(max_size or min_size)
        if min_size > max_size:
            raise ValueError(f"{part!r}: the minimum size is above the maximum")
        for index in range(int(count)):
            objects.append(ObjectSpec(tag, index, min_size, max_size))
    return objects


def parse_range(text):
    """'20-28' -> (20.0, 28.0), and '34' -> (34.0, 34.0)."""
    low, _, high = text.strip().partition("-")
    return float(low), float(high or low)
