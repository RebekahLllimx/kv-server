"""Fixed real-TCP comparison: retained always, candidate, and batch-size-one control."""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import math
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * p) - 1)] if ordered else None


async def measure(args):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.root))
    from aof import AOF, encode_command
    from server import handle_client
    from store import Store

    if args.kind == "baseline":
        store = Store()
    else:
        from commit import Limits

        store = Store(
            commit_limits=Limits(batch_requests=1 if args.kind == "batch1" else 32)
        )
    args.output.mkdir(parents=True, exist_ok=False)
    source = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in args.root.glob("*.py")
    }
    log = AOF(args.output / "session.aof", sync_policy="always")
    log.load(store)
    store.attach_aof(log)
    connections, active = [], set()

    async def handler(reader, writer):
        task = asyncio.current_task()
        active.add(task)
        try:
            await handle_client(reader, writer, store)
        finally:
            active.discard(task)

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    last_values, latencies, per_client = {}, [], []
    counters = [0] * args.clients

    async def request(client):
        reader, writer = connections[client]
        counters[client] += 1
        key = f"client-{client}".encode()
        value = str(counters[client]).encode().ljust(128, b"x")
        start = time.perf_counter_ns()
        writer.write(encode_command([b"SET", key, value]))
        await writer.drain()
        reply = await reader.readline()
        elapsed = (time.perf_counter_ns() - start) / 1_000_000
        if reply != b"+OK\r\n":
            raise AssertionError(reply)
        last_values[key] = value
        return elapsed

    async def warm(client):
        for _ in range(500 // args.clients):
            await request(client)

    lag, stop_probe = [], False

    async def probe():
        expected = time.perf_counter() + 0.001
        while not stop_probe:
            await asyncio.sleep(max(0, expected - time.perf_counter()))
            now = time.perf_counter()
            lag.append(max(0, now - expected) * 1000)
            expected += 0.001 * max(1, math.floor((now - expected) / 0.001) + 1)

    try:
        for _ in range(args.clients):
            connections.append(await asyncio.open_connection("127.0.0.1", port))
        await asyncio.gather(*(warm(i) for i in range(args.clients)))
        fsync_before = log.fsync_count
        coordinator = getattr(store, "_committer", None)
        if coordinator is not None:
            coordinator.stats.clear()
            coordinator.batch_sizes.clear()
        probe_task = asyncio.create_task(probe())
        start = time.perf_counter()
        deadline = start + 5.0

        async def workload(client):
            own = []
            while time.perf_counter() < deadline:
                own.append(await request(client))
            return own

        samples = await asyncio.gather(*(workload(i) for i in range(args.clients)))
        elapsed = time.perf_counter() - start
        stop_probe = True
        await probe_task
        latencies = [value for own in samples for value in own]
        per_client = [len(own) for own in samples]
        fsyncs = log.fsync_count - fsync_before
        stats = dict(coordinator.stats) if coordinator is not None else None
        batches = dict(coordinator.batch_sizes) if coordinator is not None else None
        if stats is not None:
            assert stats["committed_records"] == len(latencies)
            assert stats.get("rejected", 0) == 0
        for key, value in last_values.items():
            assert store.get(key) == value
    finally:
        server.close()
        await server.wait_closed()
        for _, writer in connections:
            writer.close()
        for _, writer in connections:
            await writer.wait_closed()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        await log.close_async()
    recovered = Store()
    reader = AOF(log.path)
    reader.load(recovered)
    try:
        assert all(recovered.get(k) == v for k, v in last_values.items())
    finally:
        reader.close()
    assert source == {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in args.root.glob("*.py")
    }
    result = {
        "kind": args.kind,
        "clients": args.clients,
        "round": args.round,
        "measured_seconds": elapsed,
        "warmup_successes": sum(counters) - len(latencies),
        "successes": len(latencies),
        "errors": 0,
        "throughput_ops_s": len(latencies) / elapsed,
        "p50_ms": percentile(latencies, 0.5),
        "p99_ms": percentile(latencies, 0.99),
        "p999_ms": percentile(latencies, 0.999),
        "maximum_ms": max(latencies),
        "event_loop_lag_p99_ms": percentile(lag, 0.99),
        "event_loop_lag_max_ms": max(lag),
        "actual_successful_fsyncs": fsyncs,
        "writes_per_fsync": len(latencies) / fsyncs,
        "per_client_successes": per_client,
        "coordinator_stats": stats,
        "batch_size_counts": batches,
        "recovered_last_values": True,
        "durability": "always: fsync and memory publication before OK",
        "source_sha256": source,
        "platform": platform.platform(),
        "python": platform.python_version(),
    }
    with gzip.open(args.output / "latencies_ms.json.gz", "wt") as stream:
        json.dump({"requests": latencies, "event_loop_lag": lag}, stream)
    (args.output / "result.json").write_text(json.dumps(result, indent=2))
    print(
        json.dumps(
            {
                k: result[k]
                for k in (
                    "kind",
                    "clients",
                    "round",
                    "throughput_ops_s",
                    "p99_ms",
                    "writes_per_fsync",
                )
            }
        )
    )


def compare(args):
    args.output.mkdir(parents=True, exist_ok=False)
    rows = []
    kinds = ["baseline", "candidate", "batch1"]
    counts = [1, 10, 50]
    for repeat in range(3):
        for clients in counts[repeat:] + counts[:repeat]:
            order = kinds[repeat:] + kinds[:repeat]
            for kind in order:
                output = args.output / f"round-{repeat + 1}-clients-{clients}-{kind}"
                root = args.baseline if kind == "baseline" else args.candidate
                command = [
                    sys.executable,
                    "-B",
                    __file__,
                    "--worker",
                    "--root",
                    str(root),
                    "--kind",
                    kind,
                    "--clients",
                    str(clients),
                    "--round",
                    str(repeat + 1),
                    "--output",
                    str(output),
                ]
                result = subprocess.run(
                    command, capture_output=True, text=True, timeout=40
                )
                if result.returncode:
                    raise RuntimeError(result.stdout + result.stderr)
                print(result.stdout.strip(), flush=True)
                rows.append(json.loads((output / "result.json").read_text()))
    summary = []
    for clients in counts:
        for kind in kinds:
            group = [r for r in rows if r["clients"] == clients and r["kind"] == kind]
            row = {"clients": clients, "kind": kind, "rounds": len(group)}
            for field in (
                "throughput_ops_s",
                "p50_ms",
                "p99_ms",
                "p999_ms",
                "event_loop_lag_p99_ms",
                "writes_per_fsync",
            ):
                values = [r[field] for r in group]
                row[field] = {
                    "median": statistics.median(values),
                    "min": min(values),
                    "max": max(values),
                }
            summary.append(row)
    (args.output / "summary.json").write_text(
        json.dumps({"rounds": rows, "summary": summary}, indent=2)
    )
    print(json.dumps({"cases": len(rows), "output": str(args.output)}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--kind", choices=["baseline", "candidate", "batch1"])
    parser.add_argument("--clients", type=int)
    parser.add_argument("--round", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.worker:
        asyncio.run(measure(args))
    else:
        compare(args)
