import asyncio
import random
from argparse import ArgumentTypeError

import pytest

from benchmark_server import (
    ObservedAOF,
    ObservedAsyncLock,
    parse_client_counts,
    percentile,
    policy_order_for_round,
    rotating_order,
    summarize_results,
)
from store import Store


BOTTLENECK_METRICS = {
    "store_lock_wait_median_ms": 0.7,
    "store_lock_wait_p99_ms": 0.8,
    "store_lock_wait_max_ms": 0.9,
    "store_lock_hold_median_ms": 0.4,
    "store_lock_hold_p99_ms": 0.5,
    "store_lock_hold_max_ms": 0.6,
    "store_lock_contended_ratio": 0.5,
    "executor_queue_median_ms": 0.01,
    "executor_queue_p99_ms": 0.02,
    "executor_queue_max_ms": 0.03,
    "append_io_median_ms": 0.04,
    "append_io_p99_ms": 0.05,
    "append_io_max_ms": 0.06,
}


def run(coroutine):
    return asyncio.run(coroutine)


def test_percentile_uses_nearest_rank():
    values = [4.0, 1.0, 3.0, 2.0]

    assert percentile(values, 0.50) == 2.0
    assert percentile(values, 0.95) == 4.0


def test_rotating_order_changes_each_round():
    rng = random.Random(1)

    assert policy_order_for_round(0, "rotating", rng) == [
        "always", "everysec", "no"
    ]
    assert policy_order_for_round(1, "rotating", rng) == [
        "everysec", "no", "always"
    ]
    assert policy_order_for_round(2, "rotating", rng) == [
        "no", "always", "everysec"
    ]


def test_random_order_is_reproducible_with_same_seed():
    first_rng = random.Random(42)
    second_rng = random.Random(42)

    first = [
        policy_order_for_round(index, "random", first_rng)
        for index in range(5)
    ]
    second = [
        policy_order_for_round(index, "random", second_rng)
        for index in range(5)
    ]

    assert first == second


def test_client_counts_are_parsed_and_validated():
    assert parse_client_counts("1,10,50") == [1, 10, 50]

    with pytest.raises(ArgumentTypeError, match="positive integers"):
        parse_client_counts("1,0,50")

    with pytest.raises(ArgumentTypeError, match="duplicates"):
        parse_client_counts("1,10,10")


def test_client_order_rotates_across_rounds():
    assert rotating_order([1, 10, 50], 0) == [1, 10, 50]
    assert rotating_order([1, 10, 50], 1) == [10, 50, 1]
    assert rotating_order([1, 10, 50], 2) == [50, 1, 10]


def test_observed_aof_times_only_real_fsync(tmp_path, monkeypatch):
    monkeypatch.setattr("aof.os.fsync", lambda fd: None)

    aof = ObservedAOF(tmp_path / "append.aof")
    store = Store()
    aof.load(store)

    try:
        assert run(aof.sync_async()) is False
        assert aof.fsync_durations_ms == []

        run(aof.append_async([b"SET", b"key", b"value"]))
        assert run(aof.sync_async()) is True
        assert len(aof.fsync_durations_ms) == 1
        assert aof.fsync_durations_ms[0] >= 0.0

        assert run(aof.sync_async()) is False
        assert len(aof.fsync_durations_ms) == 1
    finally:
        aof.close()


def test_observed_lock_records_wait_and_hold_times():
    async def scenario():
        lock = ObservedAsyncLock()
        await lock.acquire()

        waiter = asyncio.create_task(lock.acquire())
        await asyncio.sleep(0)
        lock.release()

        await waiter
        lock.release()

        assert len(lock.wait_durations_ms) == 2
        assert len(lock.hold_durations_ms) == 2
        assert lock.contended_acquisitions == 1
        assert all(value >= 0.0 for value in lock.wait_durations_ms)
        assert all(value >= 0.0 for value in lock.hold_durations_ms)

    run(scenario())


def test_summary_reports_median_and_observed_range():
    results = []

    for policy, throughputs in (
        ("always", [8.0, 10.0, 9.0]),
        ("everysec", [12.0, 11.0, 13.0]),
        ("no", [14.0, 16.0, 15.0]),
    ):
        for throughput in throughputs:
            results.append({
                "clients": 1,
                "policy": policy,
                "throughput": throughput,
                "average_latency_ms": 1.0,
                "p95_latency_ms": 2.0,
                "p99_latency_ms": 3.0,
                "p999_latency_ms": 4.0,
                "max_latency_ms": 5.0,
                "event_loop_lag_p99_ms": 0.1,
                "event_loop_lag_p999_ms": 0.2,
                "event_loop_lag_max_ms": 0.3,
                **BOTTLENECK_METRICS,
                "fsync_latency_median_ms": 0.4,
                "fsync_latency_p99_ms": 0.5,
                "fsync_latency_max_ms": 0.6,
                "fsync_count": 0,
            })

    summaries = {
        item["policy"]: item
        for item in summarize_results(results)
    }

    assert summaries["always"]["throughput"] == {
        "median": 9.0,
        "min": 8.0,
        "max": 10.0,
    }
    assert summaries["everysec"]["throughput"]["median"] == 12.0
    assert summaries["no"]["throughput"]["max"] == 16.0


def test_summary_keeps_client_counts_separate():
    results = []

    for clients in (1, 10):
        for policy in ("always", "everysec", "no"):
            results.append({
                "clients": clients,
                "policy": policy,
                "throughput": clients * 100.0,
                "average_latency_ms": 1.0,
                "p95_latency_ms": 2.0,
                "p99_latency_ms": 3.0,
                "p999_latency_ms": 4.0,
                "max_latency_ms": 5.0,
                "event_loop_lag_p99_ms": 0.1,
                "event_loop_lag_p999_ms": 0.2,
                "event_loop_lag_max_ms": 0.3,
                **BOTTLENECK_METRICS,
                "fsync_latency_median_ms": 0.4,
                "fsync_latency_p99_ms": 0.5,
                "fsync_latency_max_ms": 0.6,
                "fsync_count": 0,
            })

    summaries = summarize_results(results)

    assert len(summaries) == 6
    assert {
        (summary["clients"], summary["policy"])
        for summary in summaries
    } == {
        (clients, policy)
        for clients in (1, 10)
        for policy in ("always", "everysec", "no")
    }
