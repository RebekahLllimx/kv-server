# KV Server

A single-process TCP service for binary values, expiring records and conditional
updates, with an append-only recovery log. Its always-mode commit coordinator
groups ordered requests into one append/flush/fsync operation while withholding
success until their effects have been published in memory.

[ArgScope](https://github.com/RebekahLllimx/ArgScope) uses the service to exchange
expiring tool inputs and results between processes. Its long-term review history
is stored separately in SQLite.

## Run

Python 3.10 or 3.11, macOS or Linux. The runtime uses the standard library and
Unix advisory file locking. Use a fresh local AOF, not another service's file.

```bash
python main.py --sync always --aof data/append.aof --port 6379
```

Without options, defaults remain `everysec`, `data/append.aof`, and loopback port
6379. `--sync` accepts `always`, `everysec` and `no`; `--port 0` requests an
ephemeral port. There is no authentication or TLS; keep it on a trusted local host.

```bash
redis-cli -h 127.0.0.1 -p 6379 SET example hello EX 30
redis-cli -h 127.0.0.1 -p 6379 GET example
redis-cli -h 127.0.0.1 -p 6379 CAS example hello updated EX 30
redis-cli -h 127.0.0.1 -p 6379 CAD example updated
```

## Commands

Requests are RESP2 arrays of bulk strings. PING, SET with EX/NX/XX, GET, DEL,
EXPIRE, TTL, CAS and CAD retain their existing response types. DEL accepts one
key. Plain SET/CAS clear the old TTL; a successful EX write starts a new deadline;
an unsuccessful condition leaves value and deadline unchanged.

CAS compares complete bytes, not a server revision number. It cannot detect an
A-to-B-to-A sequence; an expired token cannot stop external work by its old owner.

SET/CAS accept EX seconds in 1..2,147,483,647. EXPIRE accepts a decimal integer;
nonpositive values delete immediately. A positive expiry must fit the AOF's
unsigned Unix-millisecond timestamp of at most 20 digits. An invalid expiry
returns ERR without admitting work or stopping the service.

## Always-mode coordination

- Accepted requests are ordered in one bounded FIFO; conditions see preceding
  effects in a changed-key overlay. GET/TTL use the same serial order.
- At most 32 ready requests and 4 MiB of log records form one batch. There is no
  artificial batching timer. The batch changes I/O granularity, not transaction
  semantics.
- Outstanding work, including the active batch, is limited to 256 requests and
  8 MiB of accounted input bytes. Full capacity produces a RESP `BUSY` error
  **before admission**; that submission will not execute.
- GET/TTL and failed conditions may wait for preceding writes to synchronize.
  They do not expose effects from a batch whose persistence fails.
- Caller cancellation or disconnect does not withdraw an admitted request. If
  no response arrives, its outcome may be unknown; the server does not retry it.

These are commit-work bounds, not a bound on all connections, socket buffers,
stored data or process memory. The [engineering design](ENGINEERING_DESIGN.md)
defines the logical operation time, publication point and failure boundaries.

## Persistence and recovery

| Mode | Successful mutation waits for |
|---|---|
| always | Ordered preparation, append, flush, fsync, and memory publication |
| everysec | Per-write append/flush/publication; periodic fsync |
| no | Per-write append/flush/publication; no explicit fsync |

TTL uses monotonic time during a process and absolute Unix deadlines in AOF.
Preparation-to-response time consumes TTL, and downtime is not added back on
recovery. The log still contains SET/SETAT/DEL/EXPIREAT effect records, not CAS
conditions to re-evaluate.

A partial final record can be truncated at startup; malformed records fail
recovery. An unsuccessful batch can leave a durable prefix or more. Group commit
does not roll back a batch or provide exactly-once retries. An I/O/publication
failure stops admission and the standalone service rather than acknowledging
later work. Tests simulate errors and process exits, not a physical power cut.

On shutdown, stop admission and drain accepted work before releasing the file.
A five-second drain threshold is a warning, not permission to abandon a live
I/O worker or release its lock. Native I/O can make shutdown exceed that threshold.

## Tests and measurements

```bash
python -m pip install -r requirements-dev.txt
PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests
```

The tests cover command contracts, recovery, real TCP concurrency, capacity,
read barriers, cancellation, injected I/O failures and shutdown ordering. See
the [engineering design](ENGINEERING_DESIGN.md#validation-and-performance) for
the acceptance matrix and a matched always-mode comparison. Everysec has a
different acknowledgment contract and is not an equivalent performance baseline.
The current suite contains 207 tests.

The retained `benchmark_aof.py` measures direct file operations. Store-lock and
per-append probes in `benchmark_server.py` describe the legacy path; absent probe
samples do not measure the coordinator's queue or batch work. The
`benchmarks/benchmark_group_commit.py` driver records those counters, measures
the batch-size-one control and retains raw latencies and AOFs. Its
`--baseline` and `--candidate` arguments select two separate source snapshots.
The fixed per-write reference is retained in `benchmarks/baseline`; the project
root is the maintained implementation. Choose new output directories:

```bash
python benchmarks/benchmark_group_commit.py \
  --baseline benchmarks/baseline --candidate . \
  --output benchmark_results/group-commit-new
python benchmarks/review_benchmark.py \
  --run benchmark_results/group-commit-new \
  --baseline benchmarks/baseline --candidate . \
  --output benchmark_results/group-commit-check
```

The committed round summaries include the single-client overhead and event-loop
lag as well as concurrent throughput. Raw AOFs and latency samples are produced
by the commands above and are not repository inputs.

## Code map

`resp.py` parses frames, `commands.py` dispatches validated operations, and
`server.py` handles connection responses. `store.py` holds committed state and
the legacy write path. `commit.py` owns always-mode admission, staging, ordered
reads and drain. `aof.py` owns file operations, recovery and actual I/O completion.
`main.py` supervises the listener and shutdown.

Offline AOF maintenance remains in `rewrite_aof.py` and `aof_lock.py`. Stop the
server before previewing or applying a rewrite; preserve backups and the stable
lock sidecar.

## Rights

The original code and documentation are published for inspection with rights
reserved, without a general reuse licence. See [LICENSE](LICENSE). Third-party
materials retain their own terms.
