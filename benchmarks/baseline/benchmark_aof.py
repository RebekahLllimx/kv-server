import argparse
import asyncio
import os
import statistics
import tempfile
import time

from aof import AOF
from store import Store

async def run_policy(
    policy,
    duration,
    interval,
    yield_every,
    payload_size,
):
    payload = b"x" * payload_size

    with tempfile.TemporaryDirectory() as tmpdir:
        aof = AOF(
            os.path.join(tmpdir, "append.aof"),
            sync_policy=policy,
        )

        store = Store()
        aof.load(store)
        store.attach_aof(aof)

        sync_task = None

        
        if policy == "everysec":
            async def sync_loop():
                while True:
                    await asyncio.sleep(interval)
                    await aof.sync_async()

            sync_task = asyncio.create_task(sync_loop())

        latencies_ms = []
        writes = 0
        key_index = 0

        start = time.perf_counter()

        try:
            while True:
                if time.perf_counter() - start >= duration:
                    break

                key = f"key:{key_index}".encode("ascii")
                key_index += 1

                before = time.perf_counter()
                await aof.append_async([b"SET", key, payload])
                latencies_ms.append(
                    (time.perf_counter() - before) * 1000.0
                )

                writes += 1

                # Yield so periodic fsync can run during the benchmark.
                if yield_every and writes % yield_every == 0:
                    await asyncio.sleep(0)
        finally:
            if sync_task is not None:
                sync_task.cancel()
                await asyncio.gather(sync_task, return_exceptions=True)

        
        final_sync_ms = None
        if policy != "no":
            before = time.perf_counter()
            await aof.sync_async()
            final_sync_ms = (time.perf_counter() - before) * 1000.0

        fsync_count = aof.fsync_count

        await aof.close_async()

    if writes == 0:
        return {
            "policy": policy,
            "writes": 0,
            "throughput": 0.0,
            "average_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "p99_latency_ms": 0.0,
            "fsync_count": fsync_count,
            "final_sync_ms": final_sync_ms,
        }

    sorted_latencies = sorted(latencies_ms)

    def percentile(p):
        index = int(len(sorted_latencies) * p)
        index = min(index, len(sorted_latencies) - 1)
        return sorted_latencies[index]

    return {
        "policy": policy,
        "writes": writes,
        "throughput": writes / duration,
        "average_latency_ms": statistics.mean(latencies_ms),
        "p95_latency_ms": percentile(0.95),
        "p99_latency_ms": percentile(0.99),
        "fsync_count": fsync_count,
        "final_sync_ms": final_sync_ms,
    }

def print_result(result):
    print("-" * 75)
    print(f"Policy:            {result['policy']}")
    print(f"Writes:            {result['writes']}")
    print(f"Throughput:        {result['throughput']:.2f} ops/s")
    print(f"Avg latency:       {result['average_latency_ms']:.4f} ms")
    print(f"P95 latency:       {result['p95_latency_ms']:.4f} ms")
    print(f"P99 latency:       {result['p99_latency_ms']:.4f} ms")
    print(f"Successful fsyncs: {result['fsync_count']}")

    if result["final_sync_ms"] is not None:
        print(f"Final sync:        {result['final_sync_ms']:.4f} ms")
    else:
        print("Final sync:        (skipped, policy=no)")

async def main():
    parser = argparse.ArgumentParser(
        description="Benchmark AOF sync policies."
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Seconds to run each policy",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Background sync interval for everysec",
    )
    parser.add_argument(
        "--yield-every",
        type=int,
        default=100,
        help="Yield to event loop after this many appends",
    )
    parser.add_argument(
        "--payload-size",
        type=int,
        default=128,
        help="Value size in bytes",
    )

    args = parser.parse_args()

    if args.duration <= 0:
        parser.error("--duration must be positive")

    if args.interval <= 0:
        parser.error("--interval must be positive")

    if args.yield_every <= 0:
        parser.error("--yield-every must be positive")

    if args.payload_size < 0:
        parser.error("--payload-size cannot be negative")

    policies = ["always", "everysec", "no"]

    results = []

    for policy in policies:
        print(f"\nRunning benchmark: {policy}")

        result = await run_policy(
            policy=policy,
            duration=args.duration,
            interval=args.interval,
            yield_every=args.yield_every,
            payload_size=args.payload_size,
        )

        results.append(result)
        print_result(result)

    print("\n\nSUMMARY")
    print("-" * 75)
    print(
        f"{'Policy':<12}"
        f"{'Writes':>10}"
        f"{'Ops/s':>14}"
        f"{'Avg ms':>12}"
        f"{'P95 ms':>12}"
        f"{'fsyncs':>10}"
    )

    for result in results:
        print(
            f"{result['policy']:<12}"
            f"{result['writes']:>10}"
            f"{result['throughput']:>14.2f}"
            f"{result['average_latency_ms']:>12.4f}"
            f"{result['p95_latency_ms']:>12.4f}"
            f"{result['fsync_count']:>10}"
        )

if __name__ == "__main__":
    asyncio.run(main())
