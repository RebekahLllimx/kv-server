"""Bounded, ordered always-mode commits and durable TCP read barriers."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import Counter, deque
from dataclasses import dataclass

from aof import encode_command


class CommitBusy(Exception):
    """This submission was rejected before admission and will not execute."""


class CommitClosed(Exception):
    """This request was not executed because admission or the instance stopped."""


class CommitFailed(Exception):
    """The in-flight outcome is unknown; do not infer rollback or retry safety."""


@dataclass(frozen=True)
class Limits:
    requests: int = 256
    payload_bytes: int = 8 * 1024 * 1024
    batch_requests: int = 32
    batch_bytes: int = 4 * 1024 * 1024
    drain_warning_seconds: float = 5.0

    def __post_init__(self):
        bounds = (
            self.requests,
            self.payload_bytes,
            self.batch_requests,
            self.batch_bytes,
        )
        if any(type(bound) is not int or bound < 1 for bound in bounds):
            raise ValueError("Commit bounds must be positive integers")
        warning = self.drain_warning_seconds
        try:
            valid_warning = (
                not isinstance(warning, bool)
                and isinstance(warning, (int, float))
                and math.isfinite(warning)
                and warning > 0
            )
        except OverflowError:
            valid_warning = False
        if not valid_warning:
            raise ValueError("Drain warning threshold must be finite and positive")


@dataclass(frozen=True)
class Entry:
    value: bytes
    deadline: float | None


@dataclass
class Request:
    operation: str
    key: bytes
    value: bytes | None
    expected: bytes | None
    seconds: int | None
    condition: str | None
    result: asyncio.Future
    admitted_ns: int
    size: int


def _observe_result(future):
    # A disconnected/cancelled waiter may never retrieve a later failure.
    if not future.cancelled():
        future.exception()


class CommitCoordinator:
    def __init__(self, store, aof, limits=None):
        self.store, self.aof = store, aof
        self.limits = limits or Limits()
        self.state = "OPEN"
        self._queue = deque()
        self._runner = None
        self._count = self._bytes = 0
        self.stats = Counter()
        self.batch_sizes = Counter()

    async def submit(
        self,
        operation,
        key=b"",
        *,
        value=None,
        expected=None,
        seconds=None,
        condition=None,
    ):
        if self.state != "OPEN" or self.aof.failed:
            raise CommitClosed("Commit admission is closed; request not executed")
        if operation not in {
            "GET",
            "TTL",
            "PURGE",
            "SET",
            "CAS",
            "CAD",
            "DEL",
            "EXPIRE",
        }:
            raise ValueError("Unknown coordinator operation")
        if not isinstance(key, bytes):
            raise TypeError("Commit keys must be bytes")
        if operation in ("SET", "CAS") and not isinstance(value, bytes):
            raise TypeError("Replacement value must be bytes")
        if operation in ("CAS", "CAD") and not isinstance(expected, bytes):
            raise TypeError("Expected value must be bytes")
        parts = (key, value, expected)
        if any(part is not None and not isinstance(part, bytes) for part in parts):
            raise TypeError("Commit keys and values must be bytes")
        size = sum(len(part) for part in parts if part is not None)
        # The conservative overhead keeps every admitted effect within a batch.
        record_bound = len(key) + (len(value) if value is not None else 0) + 128
        if (
            self._count >= self.limits.requests
            or self._bytes + size > self.limits.payload_bytes
            or record_bound > self.limits.batch_bytes
        ):
            self.stats["rejected"] += 1
            raise CommitBusy("Commit capacity unavailable; request not admitted")
        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(_observe_result)
        request = Request(
            operation,
            key,
            value,
            expected,
            seconds,
            condition,
            future,
            time.perf_counter_ns(),
            size,
        )
        # Admission, accounting and wakeup are atomic with respect to this loop.
        self._count += 1
        self._bytes += size
        self._queue.append(request)
        self.stats["admitted"] += 1
        self.stats["max_outstanding"] = max(self.stats["max_outstanding"], self._count)
        self.stats["max_payload_bytes"] = max(
            self.stats["max_payload_bytes"], self._bytes
        )
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(self._run(), name="kv-commit-owner")
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            self.stats["cancelled_waiters"] += 1
            raise

    def _raw(self, key, overlay):
        if key in overlay:
            return overlay[key]
        if key not in self.store._data:
            return None
        return Entry(self.store._data[key], self.store._expires_at.get(key))

    def _prepare(self, request, overlay):
        s, key, operation = self.store, request.key, request.operation
        now = s._clock()
        raw = self._raw(key, overlay)
        live = (
            raw
            if raw is not None and (raw.deadline is None or now < raw.deadline)
            else None
        )
        if operation == "GET":
            return (live.value if live is not None else None), None, {}
        if operation == "TTL":
            return (
                (
                    -2
                    if live is None
                    else -1
                    if live.deadline is None
                    else int(live.deadline - now)
                ),
                None,
                {},
            )
        if operation == "PURGE":
            keys = self.store._expires_at.keys() | overlay.keys()
            expired = {
                k: None
                for k in keys
                if (entry := self._raw(k, overlay)) is not None
                and entry.deadline is not None
                and entry.deadline <= now
            }
            return len(expired), None, expired
        if operation == "SET":
            if (request.condition == "NX" and live is not None) or (
                request.condition == "XX" and live is None
            ):
                return False, None, {}
        elif operation == "CAS":
            if live is None or live.value != request.expected:
                return 0, None, {}
        elif operation in ("DEL", "CAD", "EXPIRE"):
            if live is None:
                return 0, None, ({key: None} if raw is not None else {})
            if operation == "CAD" and live.value != request.expected:
                return 0, None, {}
            if operation in ("DEL", "CAD") or request.seconds <= 0:
                return 1, encode_command([b"DEL", key]), {key: None}
            deadline, unix_ms = s._expiration_deadline(request.seconds, now=now)
            return (
                1,
                encode_command([b"EXPIREAT", key, str(unix_ms).encode()]),
                {key: Entry(live.value, deadline)},
            )
        else:
            raise ValueError("Unknown coordinator operation")
        if request.seconds is None:
            record, deadline = [b"SET", key, request.value], None
        else:
            deadline, unix_ms = s._expiration_deadline(request.seconds, now=now)
            record = [b"SETAT", key, request.value, str(unix_ms).encode()]
        return (
            (True if operation == "SET" else 1),
            encode_command(record),
            {key: Entry(request.value, deadline)},
        )

    def _publish(self, overlay):
        # No await: only this owner publishes runtime state in always mode.
        for key, entry in overlay.items():
            if entry is None:
                self.store._data.pop(key, None)
                self.store._expires_at.pop(key, None)
            else:
                self.store._data[key] = entry.value
                if entry.deadline is None:
                    self.store._expires_at.pop(key, None)
                else:
                    self.store._expires_at[key] = entry.deadline

    def _finish(self, request, *, value=None, error=None):
        self._count -= 1
        self._bytes -= request.size
        if error is None:
            request.result.set_result(value)
        else:
            request.result.set_exception(error)

    async def _run(self):
        batch = []
        try:
            while self._queue:
                overlay, records, batch = {}, [], []
                encoded_bytes = 0
                while self._queue and len(batch) < self.limits.batch_requests:
                    request = self._queue[0]
                    result, record, changes = self._prepare(request, overlay)
                    size = len(record) if record else 0
                    if batch and encoded_bytes + size > self.limits.batch_bytes:
                        break
                    self._queue.popleft()
                    wait = time.perf_counter_ns() - request.admitted_ns
                    self.stats["queue_wait_ns"] += wait
                    self.stats["max_queue_wait_ns"] = max(
                        self.stats["max_queue_wait_ns"], wait
                    )
                    batch.append((request, result))
                    overlay.update(changes)
                    if record:
                        records.append(record)
                        encoded_bytes += size
                self.stats["batches"] += 1
                self.batch_sizes[len(batch)] += 1
                self.stats["max_batch_bytes"] = max(
                    self.stats["max_batch_bytes"], encoded_bytes
                )
                if records:
                    await self.aof.append_batch_async(tuple(records))
                self._publish(overlay)
                self.stats["committed_requests"] += len(batch)
                self.stats["committed_records"] += len(records)
                for request, result in batch:
                    self._finish(request, value=result)
                batch = []
        except BaseException as exc:
            self.state = "FAILED"
            self.stats["failures"] += 1
            for request, _ in batch:
                self._finish(
                    request,
                    error=CommitFailed("In-flight outcome unknown; instance stopped"),
                )
            while self._queue:
                self._finish(
                    self._queue.popleft(),
                    error=CommitClosed("Earlier commit failed; request not executed"),
                )
            self.aof.report_failure(exc)

    def stop_admission(self):
        if self.state == "OPEN":
            self.state = "DRAINING"

    @property
    def pending(self):
        return self._count

    async def drain(self):
        self.stop_admission()
        while self._runner is not None:
            task = self._runner
            if not task.done():
                try:
                    done, _ = await asyncio.wait(
                        {task}, timeout=self.limits.drain_warning_seconds
                    )
                except asyncio.CancelledError:
                    done = {task}  # Cancellation is not a timeout; still drain below.
                if not done:
                    self.stats["drain_soft_timeouts"] += 1
                    logging.getLogger(__name__).warning(
                        "Commit drain exceeded soft deadline; retaining I/O ownership"
                    )
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
            if task.cancelled() and self._queue and self.state != "FAILED":
                # A cancelled Task that never entered _run cannot execute its
                # failure cleanup. These requests are still unprepared, not
                # uncertain file effects being retried. Drain that known queue.
                if self._count != len(self._queue):
                    raise RuntimeError("Cannot close with unresolved active ownership")
                self._runner = asyncio.create_task(self._run(), name="kv-commit-drain")
                continue
            break
        if self._count:
            raise RuntimeError("Commit owner stopped with unresolved requests")
        if self.state != "FAILED":
            self.state = "CLOSED"
