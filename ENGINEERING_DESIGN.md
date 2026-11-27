# Ordered group commits in the KV server

## Problem and requirements

The service stores binary keys and values for local TCP clients, supports
conditional updates and expiration, and recovers acknowledged writes from an
append-only log. ArgScope uses it for expiring tool inputs and outputs; durable
review history is held separately in SQLite. Concurrent clients need ordered
conditions, bounded admitted work and explicit outcomes when storage fails.

The per-write always path held the Store lock across each file handoff and
fsync. Increasing executor capacity could not remove that serialized service
time. A commit owner groups ready work while retaining fsync-before-success.

The external RESP command subset and AOF effect format stay unchanged. The new
path is limited to always. Everysec/no retain per-write ordering and their
existing synchronization promises. A shared file-operation cleanup now also
retains ownership under repeated cancellation; it does not add a sync to no.

## Interfaces and records

Requests are RESP2 arrays of bulk strings. The parser accepts at most 64 array
elements and 1 MiB per bulk value, with 64-byte frame headers. The listener binds
to loopback and has no authentication or TLS. Its command subset is:

| Command | Result |
| --- | --- |
| PING | Simple string PONG |
| SET key value [EX seconds] [NX or XX] | OK, or null when the condition fails |
| GET key | Binary bulk value, or null when absent/expired |
| DEL key | 1 when removed, otherwise 0 |
| EXPIRE key seconds | 1 when expiry/deletion is applied, otherwise 0 |
| TTL key | Remaining whole seconds; -1 for no expiry; -2 for absent/expired |
| CAS key expected replacement [EX seconds] | 1 on matching live bytes, otherwise 0 |
| CAD key expected | 1 on matching live bytes, otherwise 0 |

Plain SET/CAS clear an old expiry. NX/XX/CAS/CAD decisions use live values;
unsuccessful conditions do not change their deadline. SET/CAS EX accepts
1..2,147,483,647 seconds. Nonpositive EXPIRE means immediate deletion. Positive
EXPIRE must produce a finite monotonic deadline and an unsigned Unix-millisecond
timestamp of at most 20 digits, the same limit used for SETAT replay. Invalid
input is rejected before admission; malformed timestamp records fail recovery.

The committed dictionaries map bytes to bytes and optionally to monotonic
deadlines. AOF records describe effects: SET, SETAT, DEL and EXPIREAT. They do
not retain conditions or depend on a client's expected value during replay.
CAS is byte equality rather than a revision check; it does not prevent ABA or
provide fencing of external work.

## Ownership and data flow

```text
validated command
  -> reserve bounded request/input bytes, enqueue without await
  -> one coordinator prepares FIFO requests in a changed-key overlay
  -> concatenate bounded effect records
  -> exclusive AOF operation: append -> flush -> fsync
  -> publish prepared state without await
  -> resolve responses and release request capacity
```

The coordinator owns runtime mutation ordering in always mode. Store owns the
committed dictionaries and replay helpers. AOF owns the native file operation,
failure record and advisory lock. A small on-demand task drains accepted work;
there is no idle worker task or unbounded list of producer waits.

The first accepted request schedules the owner. Other ready requests can enter
before it runs; requests arriving during I/O accumulate for the next batch.
Finishing an empty queue and clearing its active task requires no intervening
await, so a new request cannot lose a wakeup. One sequential client generally
still gets one fsync per mutation.

## Admission and bounds

`Limits` in `commit.py` fixes the initial limits: 256 outstanding requests,
8 MiB input payload, 32 requests per batch and 4 MiB encoded records per batch.
Outstanding accounting includes work currently waiting for I/O. A conservative
per-record bound rejects an oversized request before admission. Key, replacement
and expected bytes count toward the payload limit. Native file work is one at
a time; the changed-key overlay does not copy the entire database.

Capacity rejection returns `BUSY ... request not admitted`. Closing admission
returns `UNAVAILABLE ... request not executed`. Neither is an automatic retry.
There is no promise that total connections, socket buffers, the data dictionaries
or all Python overhead fit inside these commit-work byte bounds.

## Ordered effects and reads

SET, NX/XX, CAS/CAD, DEL and EXPIRE consult the logical state after earlier
prepared requests. The overlay stores only changed keys, values and monotonic
deadlines. Successful writes emit the same SET/SETAT/DEL/EXPIREAT records as the
baseline; failed conditions do not create an effect record. Expired physical
entries may be reclaimed without adding another log record.

Network GET/TTL also enter the queue in always mode. For example, SET x=1,
GET x, CAS x 1->2 can form one batch: GET returns 1 only after that ordered
prefix is durable. Its result is not a dirty read merely because x is already 2
by delivery. A no-op whose condition depends on staged data waits with the
batch, and does not return a successful/failed-condition result after batch I/O
fails. Read-only batches do not call fsync.

The serial preparation step is the logical operation time for successful TCP
calls, conditional on later successful commit. Fsync completion and memory
publication are subsequent durability/visibility boundaries, and response
receipt is later still. This distinction matters for TTL: EX starts at write
preparation, not at admission or response delivery. Direct synchronous
`Store.get/ttl` are committed-memory snapshots for startup/tests/diagnostics;
network commands use the ordered asynchronous methods, not those bypass reads.

Without the barrier, CAS could check a key while live, wait through its expiry,
and succeed after GET had already returned absent. The barrier is a deliberate
read-latency tradeoff in this mode. Everysec/no keep the older read
path with their existing contract.

## Commit, failure and cancellation

The file worker appends all bounded records, flushes and completes fsync before
the owner publishes any in-memory effect or resolves a success. If a batch has
no records, the owner can publish cleanup/read results without a file operation.
This is not a multi-command transaction: a failed or interrupted append may
recover only a prefix, and there is no rollback marker or batch-atomic recovery.

After append/flush/fsync or publication failure, the owner stops admission,
fails current-batch waits with an unknown-outcome error, rejects queued work
known not to have executed, and notifies the service supervisor once. No new
network read is served from a failed instance. Memory publication after fsync
can itself fail; restart then uses the durable log, not a claim of rollback.

An admitted request is owned independently of its caller Future. Shielding the
wait means cancellation detaches the client, not the request. Resources remain
charged until completion. Strong references and a completion observer also
prevent abandoned error Futures from becoming unhandled warnings.

A native executor Future retains file-operation ownership. Repeated caller
cancellation waits for the actual worker before releasing the AOF lock. Context
variables are copied into that worker, as with the previous to_thread path.
Client disconnect, server error, lost response and retry are not an exactly-once
protocol; even an unacknowledged write can reappear after restart.

## Shutdown and launchers

The standalone service closes its listener and stops commit admission, drains
accepted work, then cancels remaining connections. Accepted operations also
survive a detached caller wait. AOF's
`close_async()` drains its attached commit owner before acquiring the file lock
to close. Embedded services use the same supervision and drain order.

After five seconds of drain, log a warning and continue awaiting the owner.
Do not release the file or stable sidecar lock while native I/O remains active.
Synchronous close refuses outstanding work. A forced process exit is a crash,
not a successful graceful timeout. Shutdown has no hard bound while an OS call
remains indefinitely blocked.

If event-loop teardown cancels a newly scheduled owner before its first
instruction, its admitted requests are still in the unprepared FIFO. Drain can
start that known queue. It must not restart a batch whose file work began or
retry an unknown I/O outcome; unresolved ownership instead prevents file close.

Both the standalone entry and ArgScope's managed session use `run_service` for
listener/failure supervision and `run_until_signal` for process signals. The
signal wrapper cancels the service owner once and waits for its cleanup;
repeated signals log that drain is in progress. This lets shutdown finish before
event-loop teardown cancels remaining tasks. The embedding application owns its child
process: a slow graceful stop warns and keeps waiting rather than killing the
commit owner at a fixed deadline. A forced external termination remains a crash.

## Recovery and fault model

Startup recovery precedes coordinator attachment. SETAT retains original Unix
deadlines, so offline time is not refunded. Incomplete final RESP frames are
truncated; malformed records fail recovery. No new record type or transaction
marker is introduced, and offline rewrite keeps its previous policy.

Tests use injected short writes, flush/fsync errors, controlled native-worker
barriers, publication exceptions and real process exits. They establish those
scenarios, not survival of a physical power cut, controller-cache loss or broken
filesystem synchronization. Initial directory-entry persistence is not upgraded
by this change; baseline and candidate use the same OS fsync-level contract.

## Validation and performance

The maintained source passes 207 tests. The earlier per-write implementation
passed 141. Its source is retained in `benchmarks/baseline` for matched I/O
comparisons; new runtime changes belong at the project root. The current tests
cover these invariants:

| Boundary | Verification |
| --- | --- |
| Capacity | Count and payload include the active batch; rejected work is absent from state and log |
| Ordering | Same-batch SET, GET, CAS, NX/CAD and expiry use preceding staged effects |
| Visibility | Held fsync withholds read/condition results; failed batches expose no successful result |
| Cancellation | Detached clients and repeated native-wait cancellation preserve the commit owner |
| Failure | Short write, flush, fsync and publication faults distinguish uncertain active work from unexecuted queued work |
| Shutdown | Soft drain retains file ownership; clients are cancelled after accepted work completes; SIGINT/SIGTERM share that path |
| Recovery | Process exits before/after publication, partial tails and corrupt records are checked |
| Input isolation | Extreme expiry values return ERR; invalid programmatic requests never enter the FIFO |
| Other modes | Everysec/no retain their per-write and synchronization contracts |

On 27 November 2026, three rotated rounds compared the per-write path, natural
grouping and a batch-size-one control. The workload used macOS 26.6.2 arm64,
Python 3.10.9, real loopback TCP and local file fsync. Each case used 128-byte
values, 500 warm-up writes and five measured seconds, with one outstanding
request per client. All paths wait for fsync before success. Medians of the three
rounds, not pooled-request percentiles, are:

| Clients | Per-write writes/s | Grouped writes/s | Per-write P99 ms | Grouped P99 ms |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 6,109 | 5,190 | 0.259 | 0.408 |
| 10 | 6,964 | 18,890 | 3.225 | 1.086 |
| 50 | 7,021 | 30,393 | 10.810 | 2.981 |

One client has no grouping opportunity and loses 15.0% throughput. At fifty
clients, batch size one reaches 8,925 writes/s, so dispatch/serialization changes
also contribute; grouping adds a further 3.41 times that rate. Event-loop P99
wakeup lag increases from 0.152 to 0.971 ms at the higher achieved request rate.
The closed-loop experiment does not hold offered load constant. These runs used
a shared development workstation, with other local checks during collection;
rotating run order does not remove system-load confounding.

[Complete round summaries and source hashes](benchmarks/group_commit_20261127.json)
retain P50/P99/P99.9, observed ranges, batch sizes and actual successful fsync
counts. [Independent verification](benchmarks/verification_20261127.json)
recalculates all 1,465,704 measured request latencies and validates 1,479,204
effect records including warm-up. Raw samples and AOFs are retained in the
local experiment archive; the compact records do not contain them. Earlier
experiments remain separate from this source-bound comparison.

Run `benchmarks/benchmark_group_commit.py` with separate `--baseline` and
`--candidate` snapshots and a new `--output` directory. Run
`benchmarks/review_benchmark.py` against those same snapshots and output to
recompute every quantile, inspect each AOF and verify source hashes. Sequential
SET is the benchmark workload; it does not measure the application's workload
or establish production capacity. Fault injection and process termination also
do not establish survival of a physical power cut.

Legacy Store-lock/per-append probes in `benchmark_server.py` do not observe
coordinator ownership; absent samples do not mean zero queue or batch work.

## Design review questions and answers

1. **Why count the active batch against capacity?** Deque length drops during I/O,
   but its payloads and Futures are still owned. Counting only waiting requests
   would admit more work than the claimed bound.
2. **Why an overlay instead of a full copy?** Each batch changes a bounded number
   of keys. A database copy would make each batch's cost depend on all stored
   records; an overlay still lets later conditions observe earlier effects.
3. **Why make reads and failed conditions wait?** Their answers may depend on an
   earlier staged effect. Returning before its sync would expose an order that
   can disappear on failure and recreates the TTL/CAS read-bypass inconsistency.
4. **Why preserve intermediate effect records?** Recovery must reproduce the
   ordered prefix that actually reached the file. Coalescing them would change
   that recovery history and is not needed for the initial I/O batching goal.
5. **Why is a batch not a transaction?** It has no atomic recovery marker or
   rollback protocol. One successful fsync covers several records, but partial
   failure can leave a prefix; independent commands retain independent results.
6. **What does cancellation prove?** Only that a particular caller stopped
   waiting. After admission, another task owns the write, and after sync its
   effect can survive even if no success response was received.
7. **Why is the shutdown deadline soft?** Cancelling an await cannot stop an
   arbitrary native I/O call. Closing/unlocking early would permit overlapping
   ownership; safe cleanup waits, or external forced termination invokes recovery.
8. **Why can one client become slower?** Without concurrent arrivals there is
   nothing to group. Admission, staging and Future handling add work while the
   same one-command fsync remains on the response path.
9. **Why benchmark batch size one too?** It retains the coordinator/dispatch
   changes but removes shared fsync. The contrast helps attribute an observed
   multi-client gain instead of assigning all differences to batching.
10. **What does a passing local benchmark not prove?** Production capacity,
    real-world power-loss guarantees, or an existing ArgScope capacity problem.
    Those require their own workloads, environments and evidence.
