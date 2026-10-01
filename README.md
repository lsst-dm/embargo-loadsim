An S3 Rubin visit load simulator.

Before each visit, a pool of workers receives a preload notice and GETs
calibration data. On the visit every detector PUTs an exposure at the same
moment. The pool of workers then GET the exposures back, simulates
processing, and PUT results. Optional consumer workers then read a subset
of results.

Two other locust files are used for comparsion: `control.py`, which does flat
PUT and GETs as fast as possible, and `batchload.py`, which simulates batch
processing workers.

## Install

    pip install -r requirements.txt

## Running

Fill the preload objects once before starting:

    locust -f obsload.py --config configs/realistic.conf --headless --warm \
           --detectors 189 --s3-endpoint http://s3.example:9000 --bucket obsload

Then start the leader, which coordinates the workload but does not run any:

    locust -f obsload.py --headless --run-time 15m \
           --detectors 189 --worker-pool 1200 \
           --master --expect-workers 64 \
           --s3-endpoint http://s3.example:9000 --bucket obsload --csv=run1

Start followers across several hosts (in this case four):

    # Ensure AWS variables are exported in the environment
    locust -f - --processes 16 --worker --master-host <leader host>

See `configs/warm.sh`, `configs/leader.sh` and `configs/follower.sh` as well
as `configs/locust.conf` and `configs/realistic.conf`: replace placeholders
before running or customize files as needed.

Arguments can be tiered: `configs/locust.conf` can set common options, then the
contents of `--config configs/specifictest.conf` can be used to vary file
specs.

Caveats:

- `--run-time` needs `--headless`. Without, Locust starts its web
  UI and waits for someone to press Start.
- Leave out `-u`. obsload sets it internally.
- `--warm` fills the calibration keyspace (`--detectors` * `--preload-spec`)
  and exits. This can overwrite pre-existing files. See `configs/warm.sh`.
- `locust -f obsload.py --help` lists obsload's options under "obsload".
- If the test does not start after several minutes, add `--enable-rebalancing`
  to the leader. This _should_ be fixed and not necessary though.
- Big runs open thousands of connections: raise `ulimit -n`.
- Output files are partitioned by `--run-id` and do not clean themselves up.
- Payloads are random data, designed to be difficult to compress.
- Aggregated stats output at the end of the run is mixed with end to end
  timing, so ignore the OBS rows.

## Reading the output

With `--logfile`, Locust writes log lines to the file only. The leader's
console shows the stats table, plus obsload's warnings, errors and throughput
lines. The leader logs:

- `throughput [etc]`:
  for every `--throughput-interval`, about 6 seconds after it ends. The byte counts add up
  exactly across lines. Bytes that reach the leader after their window's
  line (from a stalled follower) get a `late report` warning line.
  `throughput summary` is output once at the end with averages and max.
- `v000012: 189 of 189 detectors fired, max fire+0.004s` for every visit: how
  many detectors started that visit's exposure, and how late the last one
  started. A warning when any detector is missing.
- `census: 21 process(es) with 8 detectors, 13 workers, 0 consumers`, one line
  per distinct mix each time users are assigned to processes. A warning or
  error flags when worker pool is exhausted.
- `FOLLOWERS LOST` when a follower stops responding or quits, and
  `TEST STOPPED` when none are left. Locust then stops the test internally but
  may still wait for `--run-time` to expire.

`--loglevel DEBUG` on a follower adds a line per detector per visit:
`v000000 det0000 fire+0.001s exp=0.01s`.

Set `OBSLOAD_BOTOCORE_LOGS=1` to keep botocore's own DEBUG lines when
troubleshooting (noisy)

### Stats rows

OBS: internal job stages
S3G: GETs against S3 endpoint
S3P: PUTs against S3 endpoint

| Row | Meaning |
|---|---|
| `preload/*`, `exposure/*`, `readback/*`, `writeout/*`, `consume/*` | one row per object tag, plus `/all` timing each whole batch |
| `sched/dispatch` | how long a job waited on the queue for a Worker |
| `sched/lead` | how much of `--preload-lead` a job got. It shrinks when the previous exposure runs into the next preload |
| `sched/pool-exhausted` | a job was queued while without available idle Workers |
| `sched/preload-late` | a Worker took the job after its exposure. The calibration download still runs |
| `sched/stale` | a Worker took the job more than two average visit gaps late, and dropped it |
| `sched/abandoned` | the exposure never finished (the Detector was probably stopped) |
| `sched/consume-dispatch`, `sched/consume-exhausted`, `sched/consume-late` | same as above but for optional Consumer pool |
| `visit/overrun` | a Detector's upload finished after the next visit's time |
| `visit/skipped` | a Detector missed a visit entirely |
| `pipeline/e2e` | exposure start to results uploaded |
| `pipeline/skipped` | the exposure failed, so no results were uploaded |
| `cluster/followers-lost` | one failure per follower lost mid-run. should not happen |

A summary histogram is printed at the end of the run.

Use the throughput lines to see high resolution information about burst
workload.

## Testing without Ceph

A stub S3 that answers 200 to everything is enough to check timing and the
pipeline:

```python
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_PUT(self):
        n = int(self.headers.get("Content-Length") or 0)
        while n > 0: n -= len(self.rfile.read(min(n, 1 << 16)))
        self.send_response(200); self.send_header("Content-Length", "0")
        self.send_header("ETag", '"0"'); self.end_headers()
    def do_GET(self):
        b = b"x" * 8192
        self.send_response(200); self.send_header("Content-Length", str(len(b)))
        self.end_headers(); self.wfile.write(b)
    def do_HEAD(self):
        self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a): pass
ThreadingHTTPServer(("127.0.0.1", 9998), H).serve_forever()
```

Quick check: `--detectors 4 --worker-pool 8 --cadence 8 --lead-in 4
--preload-lead 3` with small specs, and `--processes 3` to exercise the
leader and its followers. Then starve the pool (`--worker-pool 2
--process-delay 30`) and
confirm `sched/dispatch` climbs and `sched/pool-exhausted` fires.

## Limitations

This is not a perfect load simulation. It is designed to be slightly harsher
than reality and has some divergence:

- Raw upload timing are tighter than reality
- Preload timing cannot be longer than the visit cadence (this is fine for
  smulation purposes but in reality preload is longer than a visit)
- The worker pool is per-process rather than fully decoupled (any worker able
  to process any raw). This should still be close enough.
- Care must be taken to avoid incidental overloads: locust will warn if CPU is
  too high on processes, but the user must ensure the follower hosts are not
  overloading their CPU or network.
