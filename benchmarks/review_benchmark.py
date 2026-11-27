"""Recalculate benchmark summaries from raw samples and parse every retained AOF."""

import argparse
import csv
import gzip
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path


def review(run, baseline, candidate, output):
    output.mkdir(parents=True, exist_ok=False)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(baseline))
    from resp import NEED_MORE_DATA, RESPParser

    results = json.loads((run / "summary.json").read_text())
    assert len(results["rounds"]) == 27
    checked = []
    for row in results["rounds"]:
        path = run / f"round-{row['round']}-clients-{row['clients']}-{row['kind']}"
        assert json.loads((path / "result.json").read_text()) == row
        with gzip.open(path / "latencies_ms.json.gz", "rt") as stream:
            raw = json.load(stream)
        requests, lag = raw["requests"], raw["event_loop_lag"]
        assert len(requests) == row["successes"] == sum(row["per_client_successes"])
        assert row["warmup_successes"] == 500 and row["errors"] == 0
        assert row["measured_seconds"] >= 5
        assert math.isclose(
            len(requests) / row["measured_seconds"], row["throughput_ops_s"]
        )
        for field, values, q in (
            ("p50_ms", requests, 0.5),
            ("p99_ms", requests, 0.99),
            ("p999_ms", requests, 0.999),
            ("event_loop_lag_p99_ms", lag, 0.99),
        ):
            expected = sorted(values)[math.ceil(q * len(values)) - 1]
            assert math.isclose(row[field], expected), (path, field)
        assert max(requests) == row["maximum_ms"]
        if row["kind"] != "candidate":
            assert row["actual_successful_fsyncs"] == row["successes"]
        if row["kind"] != "baseline":
            distribution = row["batch_size_counts"]
            assert (
                sum(int(size) * count for size, count in distribution.items())
                == row["successes"]
            )
            assert sum(distribution.values()) == row["actual_successful_fsyncs"]
            assert max(map(int, distribution)) <= (1 if row["kind"] == "batch1" else 32)
            stats = row["coordinator_stats"]
            assert (
                stats["max_outstanding"] <= 256
                and stats["max_payload_bytes"] <= 8 * 1024 * 1024
            )
            assert stats["max_batch_bytes"] <= 4 * 1024 * 1024
        root = baseline if row["kind"] == "baseline" else candidate
        for name, expected in row["source_sha256"].items():
            assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, (
                root,
                name,
            )

        parser = RESPParser()
        latest, count = {}, 0
        with (path / "session.aof").open("rb") as stream:
            while block := stream.read(65536):
                parser.feed(block)
                while True:
                    command = parser.next_command()
                    if command is NEED_MORE_DATA:
                        break
                    assert command[0] == b"SET" and len(command) == 3
                    latest[command[1]] = command[2]
                    count += 1
        assert not parser.buffer and count == 500 + row["successes"]
        for i, completions in enumerate(row["per_client_successes"]):
            expected = (
                str(500 // row["clients"] + completions).encode().ljust(128, b"x")
            )
            assert latest[f"client-{i}".encode()] == expected
        checked.append(
            {
                "round": row["round"],
                "kind": row["kind"],
                "clients": row["clients"],
                "aof_records_verified": count,
                "successes": row["successes"],
            }
        )

    fields = (
        "throughput_ops_s",
        "p50_ms",
        "p99_ms",
        "p999_ms",
        "event_loop_lag_p99_ms",
        "writes_per_fsync",
    )
    summaries = []
    for clients in (1, 10, 50):
        for kind in ("baseline", "candidate", "batch1"):
            group = [
                r
                for r in results["rounds"]
                if r["clients"] == clients and r["kind"] == kind
            ]
            assert len(group) == 3
            summary = {"clients": clients, "kind": kind, "rounds": 3}
            for metric in fields:
                values = [r[metric] for r in group]
                for label, value in (
                    ("median", statistics.median(values)),
                    ("min", min(values)),
                    ("max", max(values)),
                ):
                    summary[f"{metric}_{label}"] = value
            summaries.append(summary)
    with (output / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    record = {
        "status": "passed",
        "cases": 27,
        "latency_samples_verified": sum(r["successes"] for r in checked),
        "aof_records_verified": sum(r["aof_records_verified"] for r in checked),
        "same_code_hashes": True,
        "fsync_and_batch_counts_reconciled": True,
        "complete_aof_streams_and_final_values": True,
        "cases_verified": checked,
    }
    (output / "verification.json").write_text(json.dumps(record, indent=2))
    print(
        json.dumps({k: v for k, v in record.items() if k != "cases_verified"}, indent=2)
    )
    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    review(args.run, args.baseline, args.candidate, args.output)
