import argparse
import asyncio
import json
import math
import platform
import random
import statistics
import tempfile
import time

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from aof import AOF, encode_command
from store import Store
from server import handle_client
from main import aof_sync_loop


POLICIES = ("always", "everysec", "no")
LOCAL_TIMEZONE = ZoneInfo("Asia/Shanghai")


class ObservedAOF(AOF):
    """AOF with benchmark-only timing around file operations."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fsync_durations_ms = []
        self.append_io_durations_ms = []

    def _append_sync(self, command):
        start = time.perf_counter_ns()

        try:
            return super()._append_sync(command)
        finally:
            self.append_io_durations_ms.append(
                (time.perf_counter_ns() - start) / 1_000_000
            )

    def _sync_sync(self):
        fsync_count_before = self.fsync_count
        start = time.perf_counter_ns()

        try:
            return super()._sync_sync()
        finally:
            if self.fsync_count > fsync_count_before:
                duration_ms = (
                    time.perf_counter_ns() - start
                ) / 1_000_000
                self.fsync_durations_ms.append(duration_ms)


class ObservedAsyncLock:
    """Benchmark-only asyncio lock that records acquisition wait."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self.wait_durations_ms = []
        self.hold_durations_ms = []
        self.contended_acquisitions = 0
        self._hold_started_ns = None

    async def acquire(self):
        was_contended = self._lock.locked()
        start = time.perf_counter_ns()
        await self._lock.acquire()
        self.wait_durations_ms.append(
            (time.perf_counter_ns() - start) / 1_000_000
        )

        if was_contended:
            self.contended_acquisitions += 1

        self._hold_started_ns = time.perf_counter_ns()
        return True

    def release(self):
        self.hold_durations_ms.append(
            (time.perf_counter_ns() - self._hold_started_ns)
            / 1_000_000
        )
        self._hold_started_ns = None
        self._lock.release()

    def locked(self):
        return self._lock.locked()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.release()


def measure_executor_start_delay(submitted_ns):
    """Runs in the worker and returns submit-to-start delay."""
    return (time.perf_counter_ns() - submitted_ns) / 1_000_000


def percentile(values, p):
    'Compute a percentile using the nearest-rank definition.'
    if not values:
        return 0.0

    ordered = sorted(values)
    index = math.ceil(p * len(ordered)) - 1
    index = max(0, min(index, len(ordered) - 1))

    return ordered[index]


def policy_order_for_round(round_index, mode, rng):
    """Return a reproducible policy order for one zero-based round."""
    policies = list(POLICIES)

    if mode == "fixed":
        return policies

    if mode == "rotating":
        shift = round_index % len(policies)
        return policies[shift:] + policies[:shift]

    if mode == "random":
        rng.shuffle(policies)
        return policies

    raise ValueError(f"Unsupported order mode: {mode}")


def rotating_order(values, round_index):
    values = list(values)
    shift = round_index % len(values)
    return values[shift:] + values[:shift]


def parse_client_counts(value):
    try:
        counts = [int(item.strip()) for item in value.split(",")]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--clients must be comma-separated positive integers"
        ) from exc

    if not counts or any(count <= 0 for count in counts):
        raise argparse.ArgumentTypeError(
            "--clients must be comma-separated positive integers"
        )

    if len(set(counts)) != len(counts):
        raise argparse.ArgumentTypeError(
            "--clients cannot contain duplicates"
        )

    return counts


def summarize_results(results):
    """Aggregate repeated runs without hiding their observed range."""
    summaries = []

    client_counts = sorted({
        result.get("clients", 1)
        for result in results
    })

    for clients in client_counts:
        for policy in POLICIES:
            policy_results = [
                result for result in results
                if result.get("clients", 1) == clients
                and result["policy"] == policy
            ]

            if not policy_results:
                continue

            summary = {
                "clients": clients,
                "policy": policy,
                "rounds": len(policy_results),
            }

            for metric in (
                "throughput",
                "average_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
                "p999_latency_ms",
                "max_latency_ms",
                "event_loop_lag_p99_ms",
                "event_loop_lag_p999_ms",
                "event_loop_lag_max_ms",
                "store_lock_wait_median_ms",
                "store_lock_wait_p99_ms",
                "store_lock_wait_max_ms",
                "store_lock_hold_median_ms",
                "store_lock_hold_p99_ms",
                "store_lock_hold_max_ms",
                "store_lock_contended_ratio",
                "executor_queue_median_ms",
                "executor_queue_p99_ms",
                "executor_queue_max_ms",
                "append_io_median_ms",
                "append_io_p99_ms",
                "append_io_max_ms",
                "fsync_latency_median_ms",
                "fsync_latency_p99_ms",
                "fsync_latency_max_ms",
                "fsync_count",
            ):
                values = [result[metric] for result in policy_results]
                summary[metric] = {
                    "median": statistics.median(values),
                    "min": min(values),
                    "max": max(values),
                }

            summaries.append(summary)

    return summaries


async def run_policy(
    policy,
    duration,
    interval,
    warmup,
    payload_size,
    clients=1,
    lag_interval_ms=1.0,
    executor_probe_interval_ms=5.0,
):
    'Benchmark one durability policy through real local TCP, using an isolated temporary AOF.'

    with tempfile.TemporaryDirectory() as temp_dir:
        aof_path = Path(temp_dir) / "append.aof"

        
        store = Store()
        store_lock = ObservedAsyncLock()
        store._lock = store_lock

        aof = ObservedAOF(
            aof_path,
            sync_policy=policy,
        )

        aof.load(store)
        store.attach_aof(aof)

        
        active_clients = 0
        clients_drained = asyncio.Event()
        clients_drained.set()

        async def client_handler(reader, writer):
            nonlocal active_clients

            active_clients += 1
            clients_drained.clear()

            try:
                await handle_client(
                    reader,
                    writer,
                    store,
                )
            finally:
                active_clients -= 1

                if active_clients == 0:
                    clients_drained.set()

        
        server = await asyncio.start_server(
            client_handler,
            "127.0.0.1",
            0,  
        )

        sync_task = None
        connections = []

        try:
            
            if policy == "everysec":
                sync_task = asyncio.create_task(
                    aof_sync_loop(
                        aof,
                        interval=interval,
                    )
                )

            host, port = server.sockets[0].getsockname()[:2]

            for _ in range(clients):
                connections.append(
                    await asyncio.open_connection(host, port)
                )

            payload = b"x" * payload_size

            
            async def warmup_client(
                client_index,
                reader,
                writer,
                request_count,
            ):
                for request_index in range(request_count):
                    request = encode_command([
                        b"SET",
                        (
                            f"warmup:{client_index}:"
                            f"{request_index}"
                        ).encode(),
                        payload,
                    ])

                    writer.write(request)
                    await writer.drain()

                    response = await reader.readline()

                    if response != b"+OK\r\n":
                        raise RuntimeError(
                            "Unexpected warmup response: "
                            f"{response!r}"
                        )

            warmup_counts = [
                warmup // clients + (index < warmup % clients)
                for index in range(clients)
            ]
            await asyncio.gather(*(
                warmup_client(index, reader, writer, warmup_counts[index])
                for index, (reader, writer) in enumerate(connections)
            ))

            
            # Keep warm-up writes out of the measured fsync interval.
            if policy != "no":
                await aof.sync_async()

            fsync_before = aof.fsync_count
            fsync_duration_before = len(aof.fsync_durations_ms)
            append_io_before = len(aof.append_io_durations_ms)
            store_lock.wait_durations_ms.clear()
            store_lock.hold_durations_ms.clear()
            store_lock.contended_acquisitions = 0

            
            loop = asyncio.get_running_loop()
            start_signal = loop.create_future()

            async def measure_client(client_index, reader, writer):
                deadlines = await start_signal
                deadline = deadlines["perf_counter"]
                client_latencies = []
                client_writes = 0

                while time.perf_counter() < deadline:
                    key = (
                        f"key:{client_index}:{client_writes}"
                    ).encode()
                    request = encode_command([
                        b"SET",
                        key,
                        payload,
                    ])

                    request_start = time.perf_counter_ns()

                    writer.write(request)
                    await writer.drain()
                    response = await reader.readline()

                    request_end = time.perf_counter_ns()

                    if response != b"+OK\r\n":
                        raise RuntimeError(
                            f"Unexpected SET response: {response!r}"
                        )

                    client_latencies.append(
                        (request_end - request_start) / 1_000_000
                    )
                    client_writes += 1

                return {
                    "client": client_index,
                    "writes": client_writes,
                    "latencies_ms": client_latencies,
                }

            async def measure_event_loop_lag():
                deadlines = await start_signal
                loop_deadline = deadlines["loop_time"]
                interval_seconds = lag_interval_ms / 1000.0
                lag_samples_ms = []
                next_wakeup = loop.time() + interval_seconds

                while next_wakeup < loop_deadline:
                    await asyncio.sleep(
                        max(0.0, next_wakeup - loop.time())
                    )
                    actual_wakeup = loop.time()
                    lag_samples_ms.append(
                        max(0.0, actual_wakeup - next_wakeup)
                        * 1000.0
                    )
                    next_wakeup = actual_wakeup + interval_seconds

                return lag_samples_ms

            async def measure_executor_queue():
                deadlines = await start_signal
                loop_deadline = deadlines["loop_time"]
                interval_seconds = (
                    executor_probe_interval_ms / 1000.0
                )
                queue_delays_ms = []

                while loop.time() < loop_deadline:
                    submitted_ns = time.perf_counter_ns()
                    queue_delays_ms.append(
                        await asyncio.to_thread(
                            measure_executor_start_delay,
                            submitted_ns,
                        )
                    )
                    await asyncio.sleep(interval_seconds)

                return queue_delays_ms

            measurement_tasks = [
                asyncio.create_task(
                    measure_client(index, reader, writer)
                )
                for index, (reader, writer) in enumerate(connections)
            ]
            lag_task = asyncio.create_task(measure_event_loop_lag())
            executor_probe_task = asyncio.create_task(
                measure_executor_queue()
            )

            await asyncio.sleep(0)
            start_time = time.perf_counter()
            start_signal.set_result({
                "perf_counter": start_time + duration,
                "loop_time": loop.time() + duration,
            })
            client_measurements = await asyncio.gather(
                *measurement_tasks
            )
            elapsed = time.perf_counter() - start_time
            event_loop_lags_ms = await lag_task
            executor_queue_delays_ms = await executor_probe_task

            latencies_ms = [
                latency
                for measurement in client_measurements
                for latency in measurement["latencies_ms"]
            ]
            client_write_counts = [
                measurement["writes"]
                for measurement in client_measurements
            ]
            write_count = sum(client_write_counts)

            
            measured_fsync_count = (
                aof.fsync_count - fsync_before
            )
            measured_fsync_durations_ms = aof.fsync_durations_ms[
                fsync_duration_before:
            ]
            measured_append_io_durations_ms = (
                aof.append_io_durations_ms[append_io_before:]
            )
            store_lock_waits_ms = list(
                store_lock.wait_durations_ms
            )
            store_lock_holds_ms = list(
                store_lock.hold_durations_ms
            )
            store_lock_acquisitions = len(store_lock_waits_ms)

        finally:
            
            for _, writer in connections:
                writer.close()

            await asyncio.gather(*(
                writer.wait_closed()
                for _, writer in connections
            ), return_exceptions=True)

            
            try:
                await asyncio.wait_for(
                    clients_drained.wait(),
                    timeout=5.0,
                )
            finally:
                
                server.close()
                await server.wait_closed()

                
                if sync_task is not None:
                    sync_task.cancel()

                    sync_result = await asyncio.gather(
                        sync_task,
                        return_exceptions=True,
                    )

                    for result in sync_result:
                        if isinstance(result, Exception):
                            raise result

            
            final_sync_ms = None
            final_sync_performed = False

            try:
                if policy != "no" and not aof.failed:
                    sync_start = time.perf_counter_ns()

                    final_sync_performed = await aof.sync_async()

                    sync_end = time.perf_counter_ns()

                    final_sync_ms = (
                        sync_end - sync_start
                    ) / 1_000_000

            finally:
                await aof.close_async()

        
        return {
            "policy": policy,
            "clients": clients,
            "writes": write_count,
            "elapsed_seconds": elapsed,
            "throughput": write_count / elapsed,
            "average_latency_ms": statistics.fmean(
                latencies_ms
            ),
            "p95_latency_ms": percentile(
                latencies_ms,
                0.95,
            ),
            "p99_latency_ms": percentile(
                latencies_ms,
                0.99,
            ),
            "p999_latency_ms": percentile(
                latencies_ms,
                0.999,
            ),
            "max_latency_ms": max(latencies_ms),
            "event_loop_lag_samples": len(event_loop_lags_ms),
            "event_loop_lag_p99_ms": percentile(
                event_loop_lags_ms,
                0.99,
            ),
            "event_loop_lag_p999_ms": percentile(
                event_loop_lags_ms,
                0.999,
            ),
            "event_loop_lag_max_ms": max(
                event_loop_lags_ms,
                default=0.0,
            ),
            "store_lock_samples": store_lock_acquisitions,
            "store_lock_wait_median_ms": statistics.median(
                store_lock_waits_ms
            ),
            "store_lock_wait_p99_ms": percentile(
                store_lock_waits_ms,
                0.99,
            ),
            "store_lock_wait_max_ms": max(store_lock_waits_ms),
            "store_lock_hold_median_ms": statistics.median(
                store_lock_holds_ms
            ),
            "store_lock_hold_p99_ms": percentile(
                store_lock_holds_ms,
                0.99,
            ),
            "store_lock_hold_max_ms": max(store_lock_holds_ms),
            "store_lock_contended_ratio": (
                store_lock.contended_acquisitions
                / store_lock_acquisitions
            ),
            "executor_queue_samples": len(
                executor_queue_delays_ms
            ),
            "executor_queue_median_ms": statistics.median(
                executor_queue_delays_ms
            ),
            "executor_queue_p99_ms": percentile(
                executor_queue_delays_ms,
                0.99,
            ),
            "executor_queue_max_ms": max(
                executor_queue_delays_ms
            ),
            "append_io_median_ms": statistics.median(
                measured_append_io_durations_ms
            ),
            "append_io_p99_ms": percentile(
                measured_append_io_durations_ms,
                0.99,
            ),
            "append_io_max_ms": max(
                measured_append_io_durations_ms
            ),
            "fsync_count": measured_fsync_count,
            "fsync_latency_median_ms": (
                statistics.median(measured_fsync_durations_ms)
                if measured_fsync_durations_ms else 0.0
            ),
            "fsync_latency_p99_ms": percentile(
                measured_fsync_durations_ms,
                0.99,
            ),
            "fsync_latency_max_ms": (
                max(measured_fsync_durations_ms)
                if measured_fsync_durations_ms else 0.0
            ),
            "final_sync_ms": final_sync_ms,
            "final_sync_performed": final_sync_performed,
            "client_writes": {
                "median": statistics.median(client_write_counts),
                "min": min(client_write_counts),
                "max": max(client_write_counts),
            },
        }


def print_result(result):
    print()
    print("=" * 60)
    print(f"Policy: {result['policy']}")
    print(f"Clients: {result['clients']}")
    print("=" * 60)

    print(f"Writes:              {result['writes']}")
    print(
        f"Elapsed:             "
        f"{result['elapsed_seconds']:.3f} s"
    )
    print(
        f"Throughput:          "
        f"{result['throughput']:.2f} ops/s"
    )
    print(
        f"Average latency:     "
        f"{result['average_latency_ms']:.4f} ms"
    )
    print(
        f"P95 latency:         "
        f"{result['p95_latency_ms']:.4f} ms"
    )
    print(
        f"P99 latency:         "
        f"{result['p99_latency_ms']:.4f} ms"
    )
    print(
        f"P99.9 latency:       "
        f"{result['p999_latency_ms']:.4f} ms"
    )
    print(
        f"Max latency:         "
        f"{result['max_latency_ms']:.4f} ms"
    )
    print(
        "Event-loop lag:      "
        f"p99={result['event_loop_lag_p99_ms']:.4f} ms, "
        f"p99.9={result['event_loop_lag_p999_ms']:.4f} ms, "
        f"max={result['event_loop_lag_max_ms']:.4f} ms"
    )
    print(
        "Store lock wait:     "
        f"median={result['store_lock_wait_median_ms']:.4f} ms, "
        f"p99={result['store_lock_wait_p99_ms']:.4f} ms, "
        f"max={result['store_lock_wait_max_ms']:.4f} ms, "
        f"contended={result['store_lock_contended_ratio']:.1%}"
    )
    print(
        "Store lock hold:     "
        f"median={result['store_lock_hold_median_ms']:.4f} ms, "
        f"p99={result['store_lock_hold_p99_ms']:.4f} ms, "
        f"max={result['store_lock_hold_max_ms']:.4f} ms"
    )
    print(
        "Executor queue:      "
        f"median={result['executor_queue_median_ms']:.4f} ms, "
        f"p99={result['executor_queue_p99_ms']:.4f} ms, "
        f"max={result['executor_queue_max_ms']:.4f} ms"
    )
    print(
        "Append worker I/O:   "
        f"median={result['append_io_median_ms']:.4f} ms, "
        f"p99={result['append_io_p99_ms']:.4f} ms, "
        f"max={result['append_io_max_ms']:.4f} ms"
    )
    print(
        "FSYNC latency:       "
        f"median={result['fsync_latency_median_ms']:.4f} ms, "
        f"p99={result['fsync_latency_p99_ms']:.4f} ms, "
        f"max={result['fsync_latency_max_ms']:.4f} ms"
    )
    print(
        f"FSYNC during test:   "
        f"{result['fsync_count']}"
    )
    print(
        f"Final sync performed: "
        f"{result['final_sync_performed']}"
    )

    if result["final_sync_ms"] is not None:
        print(
            f"Final sync latency:  "
            f"{result['final_sync_ms']:.4f} ms"
        )

    client_writes = result["client_writes"]
    print(
        "Writes per client:    "
        f"median={client_writes['median']:.1f}, "
        f"min={client_writes['min']}, "
        f"max={client_writes['max']}"
    )


def print_aggregate_summary(summaries):
    print("\n\nAGGREGATE SUMMARY (median [min, max])")
    print("-" * 154)
    print(
        f"{'Clients':>8} "
        f"{'Policy':<12}"
        f"{'Rounds':>8}"
        f"{'Ops/s':>28}"
        f"{'Avg ms':>25}"
        f"{'P95 ms':>25}"
        f"{'P99 ms':>25}"
        f"{'FSYNC':>29}"
    )

    for summary in summaries:
        throughput = summary["throughput"]
        average = summary["average_latency_ms"]
        p95 = summary["p95_latency_ms"]
        p99 = summary["p99_latency_ms"]
        fsync = summary["fsync_count"]

        print(
            f"{summary['clients']:>8} "
            f"{summary['policy']:<12}"
            f"{summary['rounds']:>8}"
            f"{throughput['median']:>12.2f} "
            f"[{throughput['min']:.2f}, {throughput['max']:.2f}]"
            f"{average['median']:>10.4f} "
            f"[{average['min']:.4f}, {average['max']:.4f}]"
            f"{p95['median']:>10.4f} "
            f"[{p95['min']:.4f}, {p95['max']:.4f}]"
            f"{p99['median']:>10.4f} "
            f"[{p99['min']:.4f}, {p99['max']:.4f}]"
            f"{fsync['median']:>11.1f} "
            f"[{fsync['min']:.0f}, {fsync['max']:.0f}]"
        )


def print_observability_summary(summaries):
    print("\nOBSERVABILITY SUMMARY (median across rounds)")
    print("-" * 132)
    print(
        f"{'Clients':>8} "
        f"{'Policy':<12}"
        f"{'Req P99.9':>12}"
        f"{'Req Max':>12}"
        f"{'Loop P99':>12}"
        f"{'Loop P99.9':>14}"
        f"{'Loop Max':>12}"
        f"{'FSYNC P50':>12}"
        f"{'FSYNC P99':>12}"
        f"{'FSYNC Max':>12}"
    )

    for summary in summaries:
        print(
            f"{summary['clients']:>8} "
            f"{summary['policy']:<12}"
            f"{summary['p999_latency_ms']['median']:>12.4f}"
            f"{summary['max_latency_ms']['median']:>12.4f}"
            f"{summary['event_loop_lag_p99_ms']['median']:>12.4f}"
            f"{summary['event_loop_lag_p999_ms']['median']:>14.4f}"
            f"{summary['event_loop_lag_max_ms']['median']:>12.4f}"
            f"{summary['fsync_latency_median_ms']['median']:>12.4f}"
            f"{summary['fsync_latency_p99_ms']['median']:>12.4f}"
            f"{summary['fsync_latency_max_ms']['median']:>12.4f}"
        )


def print_bottleneck_summary(summaries):
    print("\nBOTTLENECK BREAKDOWN (median across rounds)")
    print("-" * 132)
    print(
        f"{'Clients':>8} "
        f"{'Policy':<12}"
        f"{'Lock P50':>12}"
        f"{'Lock P99':>12}"
        f"{'Hold P50':>12}"
        f"{'Contended':>12}"
        f"{'ExecQ P50':>12}"
        f"{'ExecQ P99':>12}"
        f"{'I/O P50':>12}"
        f"{'I/O P99':>12}"
        f"{'Loop P99.9':>14}"
    )

    for summary in summaries:
        print(
            f"{summary['clients']:>8} "
            f"{summary['policy']:<12}"
            f"{summary['store_lock_wait_median_ms']['median']:>12.4f}"
            f"{summary['store_lock_wait_p99_ms']['median']:>12.4f}"
            f"{summary['store_lock_hold_median_ms']['median']:>12.4f}"
            f"{summary['store_lock_contended_ratio']['median']:>11.1%}"
            f"{summary['executor_queue_median_ms']['median']:>12.4f}"
            f"{summary['executor_queue_p99_ms']['median']:>12.4f}"
            f"{summary['append_io_median_ms']['median']:>12.4f}"
            f"{summary['append_io_p99_ms']['median']:>12.4f}"
            f"{summary['event_loop_lag_p999_ms']['median']:>14.4f}"
        )


def write_json_results(path, args, results, summaries, round_orders):
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(LOCAL_TIMEZONE).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
        },
        "configuration": {
            "duration_seconds": args.duration,
            "interval_seconds": args.interval,
            "warmup_requests": args.warmup,
            "payload_size_bytes": args.payload_size,
            "rounds": args.rounds,
            "order": args.order,
            "seed": args.seed,
            "clients": args.clients,
            "event_loop_lag_interval_ms": args.lag_interval_ms,
            "executor_probe_interval_ms": (
                args.executor_probe_interval_ms
            ),
            "warmup_definition": "total requests divided across clients",
            "request_pattern": (
                "each client sequentially sends SET and waits for its "
                "response; clients run concurrently"
            ),
        },
        "round_orders": round_orders,
        "results": results,
        "summaries": summaries,
    }
    path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


async def main():
    parser = argparse.ArgumentParser(
        description="Benchmark TCP KV Server with AOF policies"
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=15.0,
        help="Seconds to measure each policy",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="AOF background sync interval",
    )

    parser.add_argument(
        "--warmup",
        type=int,
        default=500,
        help="Number of warmup requests",
    )

    parser.add_argument(
        "--payload-size",
        type=int,
        default=128,
        help="Value size in bytes",
    )

    parser.add_argument(
        "--clients",
        type=parse_client_counts,
        default=[1],
        help="Comma-separated concurrent client counts, e.g. 1,10,50",
    )

    parser.add_argument(
        "--lag-interval-ms",
        type=float,
        default=1.0,
        help="Event-loop lag sampling interval in milliseconds",
    )

    parser.add_argument(
        "--executor-probe-interval-ms",
        type=float,
        default=5.0,
        help="Thread-pool queue probe interval in milliseconds",
    )

    parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="Number of complete comparison rounds",
    )

    parser.add_argument(
        "--order",
        choices=("rotating", "random", "fixed"),
        default="rotating",
        help="Policy order across rounds",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=20261117,
        help="Random seed used when --order=random",
    )

    parser.add_argument(
        "--json-output",
        type=Path,
        help="Optional path for raw results and metadata",
    )

    args = parser.parse_args()

    if args.duration <= 0:
        parser.error("--duration must be positive")

    if args.interval <= 0:
        parser.error("--interval must be positive")

    if args.warmup < 0:
        parser.error("--warmup cannot be negative")

    if args.payload_size < 0:
        parser.error("--payload-size cannot be negative")

    if args.rounds <= 0:
        parser.error("--rounds must be positive")

    if args.lag_interval_ms <= 0:
        parser.error("--lag-interval-ms must be positive")

    if args.executor_probe_interval_ms <= 0:
        parser.error("--executor-probe-interval-ms must be positive")

    rng = random.Random(args.seed)
    results = []
    round_orders = []

    for round_index in range(args.rounds):
        policies = policy_order_for_round(
            round_index,
            args.order,
            rng,
        )
        client_counts = rotating_order(args.clients, round_index)
        round_orders.append({
            "round": round_index + 1,
            "client_order": client_counts,
            "policy_order": policies,
        })

        print(
            f"\nROUND {round_index + 1}/{args.rounds}\n"
            f"Client order: "
            + " -> ".join(str(value) for value in client_counts)
            + "\nPolicy order: "
            + " -> ".join(policies)
        )

        for client_position, clients in enumerate(
            client_counts,
            start=1,
        ):
            for policy_position, policy in enumerate(
                policies,
                start=1,
            ):
                print(
                    "\nRunning TCP benchmark: "
                    f"clients={clients}, policy={policy}"
                )

                result = await run_policy(
                    policy=policy,
                    duration=args.duration,
                    interval=args.interval,
                    warmup=args.warmup,
                    payload_size=args.payload_size,
                    clients=clients,
                    lag_interval_ms=args.lag_interval_ms,
                    executor_probe_interval_ms=(
                        args.executor_probe_interval_ms
                    ),
                )
                result["round"] = round_index + 1
                result["client_position"] = client_position
                result["policy_position"] = policy_position

                results.append(result)
                print_result(result)

    print("\n\nFINAL SUMMARY")
    print("-" * 85)

    print(
        f"{'Clients':>8} "
        f"{'Policy':<12}"
        f"{'Writes':>10}"
        f"{'Ops/s':>14}"
        f"{'Avg ms':>12}"
        f"{'P95 ms':>12}"
        f"{'P99 ms':>12}"
        f"{'FSYNC':>10}"
    )

    for result in results:
        print(
            f"{result['clients']:>8} "
            f"{result['policy']:<12}"
            f"{result['writes']:>10}"
            f"{result['throughput']:>14.2f}"
            f"{result['average_latency_ms']:>12.4f}"
            f"{result['p95_latency_ms']:>12.4f}"
            f"{result['p99_latency_ms']:>12.4f}"
            f"{result['fsync_count']:>10}"
        )

    summaries = summarize_results(results)
    print_aggregate_summary(summaries)
    print_observability_summary(summaries)
    print_bottleneck_summary(summaries)

    if args.json_output is not None:
        write_json_results(
            args.json_output,
            args,
            results,
            summaries,
            round_orders,
        )
        print(f"\nRaw results written to: {args.json_output}")


if __name__ == "__main__":
    asyncio.run(main())
