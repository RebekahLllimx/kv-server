import asyncio
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from aof import AOF, encode_command
from commands import execute_command
from commit import CommitBusy, CommitClosed, CommitFailed, Limits
from response import BulkString
from server import handle_client
from store import Store


class Clock:
    def __init__(self, value=1000.0):
        self.value = value

    def __call__(self):
        return self.value


class HeldAOF(AOF):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entered = threading.Event()
        self.release = threading.Event()

    def _append_bytes_sync(self, data):
        self.entered.set()
        if not self.release.wait(3):
            raise RuntimeError("Test did not release its I/O barrier")
        return super()._append_bytes_sync(data)


async def entered(log):
    assert await asyncio.to_thread(log.entered.wait, 2)


def opened(path, *, cls=AOF, limits=None, clock=None, on_failure=None):
    s = Store(clock=clock or Clock(), wall_clock=clock or Clock(), commit_limits=limits)
    log = cls(path, sync_policy="always", on_failure=on_failure)
    log.load(s)
    s.attach_aof(log)
    return s, log


def test_same_batch_conditions_reads_and_replay(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "batch.aof")
        jobs = [
            asyncio.create_task(s.set(b"x", b"1")),
            asyncio.create_task(s.get_for_command(b"x")),
            asyncio.create_task(s.compare_set(b"x", b"1", b"2")),
            asyncio.create_task(s.set(b"x", b"wrong", condition="NX")),
            asyncio.create_task(s.compare_delete(b"x", b"2")),
            asyncio.create_task(s.set(b"x", b"3", condition="NX")),
        ]
        assert await asyncio.gather(*jobs) == [True, b"1", 1, False, 1, True]
        assert log.fsync_count == 1
        assert s._committer.batch_sizes == {6: 1}
        await log.close_async()
        recovered = Store()
        reader = AOF(log.path)
        reader.load(recovered)
        assert recovered.get(b"x") == b"3"
        reader.close()

    asyncio.run(scenario())


def test_noop_and_read_do_not_escape_uncommitted_batch(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "visible.aof", cls=HeldAOF)
        jobs = [
            asyncio.create_task(s.set(b"x", b"1")),
            asyncio.create_task(s.set(b"x", b"2", condition="NX")),
            asyncio.create_task(s.get_for_command(b"x")),
        ]
        try:
            await entered(log)
            assert s.get(b"x") is None  # Diagnostic snapshot, not the TCP API.
            assert not any(t.done() for t in jobs)
        finally:
            log.release.set()
        assert await asyncio.gather(*jobs) == [True, False, b"1"]
        await log.close_async()

    asyncio.run(scenario())


def test_ttl_cas_read_barrier_and_absolute_deadline(tmp_path):
    async def scenario():
        clock = Clock()
        s = Store(clock=clock, wall_clock=clock)
        await s.set(b"x", b"old", seconds=1)
        log = HeldAOF(tmp_path / "ttl.aof", sync_policy="always")
        log.load(Store())
        s.attach_aof(log)
        clock.value += 0.5
        change = asyncio.create_task(s.compare_set(b"x", b"old", b"new", seconds=4))
        await entered(log)
        clock.value += 0.6
        read = asyncio.create_task(execute_command([b"GET", b"x"], s))
        await asyncio.sleep(0)
        assert not read.done()
        log.release.set()
        assert await change == 1
        assert await read == BulkString(b"new")
        assert await s.ttl_for_command(b"x") == 3
        await log.close_async()
        clock.value += 1
        recovery = Store(clock=clock, wall_clock=clock)
        reader = AOF(log.path)
        reader.load(recovery)
        assert recovery.get(b"x") == b"new" and recovery.ttl(b"x") == 2
        reader.close()

    asyncio.run(scenario())


def test_active_batch_counts_toward_admission_and_busy_is_unexecuted(tmp_path):
    async def scenario():
        limits = Limits(requests=2, payload_bytes=8, batch_requests=1)
        s, log = opened(tmp_path / "bounded.aof", cls=HeldAOF, limits=limits)
        a = asyncio.create_task(s.set(b"a", b"123"))
        await entered(log)
        b = asyncio.create_task(s.set(b"b", b"456"))
        await asyncio.sleep(0)
        try:
            with pytest.raises(CommitBusy):
                await s.set(b"c", b"7")
            assert s._committer.pending == 2 and s._committer._bytes == 8
            assert s.get(b"c") is None
        finally:
            log.release.set()
        await asyncio.gather(a, b)
        await log.close_async()
        assert b"c" not in s._data
        assert s._committer.pending == 0 and s._committer._bytes == 0

    asyncio.run(scenario())


def test_expected_value_bytes_and_oversize_are_bounded_before_admission(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "large.aof", limits=Limits(payload_bytes=5))
        with pytest.raises(CommitBusy):
            await s.compare_set(b"k", b"12345", b"v")
        assert s._committer.pending == 0 and log.fsync_count == 0
        await log.close_async()

    asyncio.run(scenario())


def test_cancellation_before_admission_has_no_effect(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "before.aof")
        task = asyncio.create_task(s.set(b"x", b"1"))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert s._committer.stats["admitted"] == 0
        await log.close_async()
        assert log.path.read_bytes() == b""

    asyncio.run(scenario())


def test_drain_handles_owner_cancelled_before_its_first_instruction(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "unstarted-owner.aof")
        waiter = asyncio.create_task(s.set(b"x", b"1"))
        await asyncio.sleep(0)
        assert s._committer.pending == 1
        s._committer._runner.cancel()
        await log.close_async()
        assert await waiter is True
        assert s.get(b"x") == b"1" and log.fsync_count == 1
        assert s._committer.pending == 0

    asyncio.run(scenario())


def test_cancelled_admitted_waiter_still_commits(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "cancel.aof", cls=HeldAOF)
        task = asyncio.create_task(s.set(b"x", b"1"))
        await entered(log)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert s._committer.pending == 1 and s.get(b"x") is None
        log.release.set()
        await log.close_async()
        assert s.get(b"x") == b"1" and log.fsync_count == 1

    asyncio.run(scenario())


def test_repeated_native_wait_cancellation_retains_file_lock(tmp_path):
    async def scenario():
        s = Store()
        log = HeldAOF(tmp_path / "repeat.aof", sync_policy="always")
        log.load(s)
        task = asyncio.create_task(log.append_async([b"SET", b"x", b"1"]))
        await entered(log)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and log._lock.locked()
        closing = asyncio.create_task(log.close_async())
        await asyncio.sleep(0)
        assert not closing.done() and log._path_lock is not None
        log.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await closing

    asyncio.run(scenario())


class PartialFile:
    def __init__(self, file, limit, *, fail_flush=False):
        self.file, self.limit, self.fail_flush = file, limit, fail_flush

    def write(self, data):
        if self.limit is None:
            return self.file.write(data)
        self.file.write(data[: self.limit])
        self.file.flush()
        return self.limit

    def flush(self):
        if self.fail_flush:
            raise OSError("injected flush failure")
        return self.file.flush()

    def fileno(self):
        return self.file.fileno()

    def close(self):
        return self.file.close()


@pytest.mark.parametrize("failure", ["short_write", "flush", "fsync"])
def test_failed_batch_is_unknown_not_rolled_back(tmp_path, monkeypatch, failure):
    async def scenario():
        notifications = []
        s, log = opened(tmp_path / f"{failure}.aof", on_failure=notifications.append)
        if failure == "short_write":
            log._file = PartialFile(
                log._file, len(encode_command([b"SET", b"x", b"1"])) + 5
            )
        elif failure == "flush":
            log._file = PartialFile(log._file, None, fail_flush=True)
        else:

            def fail(fd):
                raise OSError("injected fsync failure")

            monkeypatch.setattr("aof.os.fsync", fail)
        tasks = [
            asyncio.create_task(s.set(b"x", b"1")),
            asyncio.create_task(s.set(b"y", b"2")),
        ]
        tasks += [
            asyncio.create_task(s.set(b"x", b"wrong", condition="NX")),
            asyncio.create_task(s.get_for_command(b"x")),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(x, CommitFailed) for x in results)
        assert s.get(b"x") is None and s.get(b"y") is None
        assert log.failed and len(notifications) == 1 and s._committer.pending == 0
        with pytest.raises(CommitClosed):
            await s.get_for_command(b"x")
        await log.close_async()
        recovered = Store()
        reader = AOF(log.path)
        reader.load(recovered)
        assert recovered.get(b"x") == b"1"  # An unacknowledged effect can survive.
        if failure == "short_write":
            assert recovered.get(b"y") is None
        reader.close()

    asyncio.run(scenario())


def test_publication_failure_after_sync_is_fail_stop(tmp_path, monkeypatch):
    async def scenario():
        s, log = opened(tmp_path / "publish.aof")

        def fail(overlay):
            raise RuntimeError("injected publication failure")

        monkeypatch.setattr(s._committer, "_publish", fail)
        with pytest.raises(CommitFailed):
            await s.set(b"x", b"1")
        assert log.fsync_count == 1 and log.failed and s.get(b"x") is None
        await log.close_async()
        recovered = Store()
        reader = AOF(log.path)
        reader.load(recovered)
        assert recovered.get(b"x") == b"1"
        reader.close()

    asyncio.run(scenario())


def test_natural_batches_respect_encoded_byte_limit(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "split.aof", limits=Limits(batch_bytes=160))
        result = await asyncio.gather(
            *(s.set(str(i).encode(), b"v") for i in range(12))
        )
        assert all(result) and 1 < log.fsync_count < 12
        assert s._committer.stats["max_batch_bytes"] <= 160
        await log.close_async()

    asyncio.run(scenario())


def test_queued_waiter_cancel_does_not_remove_accepted_work(tmp_path):
    async def scenario():
        s, log = opened(
            tmp_path / "queued-cancel.aof", cls=HeldAOF, limits=Limits(batch_requests=1)
        )
        first = asyncio.create_task(s.set(b"x", b"1"))
        await entered(log)
        second = asyncio.create_task(s.compare_set(b"x", b"1", b"2"))
        await asyncio.sleep(0)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert s._committer.pending == 2
        log.release.set()
        await first
        await log.close_async()
        assert s.get(b"x") == b"2"

    asyncio.run(scenario())


def test_failed_inflight_and_unexecuted_queue_are_distinguished(tmp_path):
    async def scenario():
        s, log = opened(
            tmp_path / "failure-order.aof", cls=HeldAOF, limits=Limits(batch_requests=1)
        )
        log._file = PartialFile(log._file, None, fail_flush=True)
        first = asyncio.create_task(s.set(b"x", b"1"))
        await entered(log)
        second = asyncio.create_task(s.set(b"y", b"2"))
        await asyncio.sleep(0)
        log.release.set()
        out = await asyncio.gather(first, second, return_exceptions=True)
        assert isinstance(out[0], CommitFailed) and isinstance(out[1], CommitClosed)
        await log.close_async()
        recovered = Store()
        reader = AOF(log.path)
        reader.load(recovered)
        assert recovered.get(b"x") == b"1" and recovered.get(b"y") is None
        reader.close()

    asyncio.run(scenario())


def test_real_tcp_busy_request_is_not_logged(tmp_path):
    async def scenario():
        s, log = opened(
            tmp_path / "busy-wire.aof", cls=HeldAOF, limits=Limits(requests=1)
        )
        async with await asyncio.start_server(
            lambda r, w: handle_client(r, w, s), "127.0.0.1", 0
        ) as server:
            port = server.sockets[0].getsockname()[1]
            r1, w1 = await asyncio.open_connection("127.0.0.1", port)
            w1.write(encode_command([b"SET", b"x", b"1"]))
            await w1.drain()
            await entered(log)
            r2, w2 = await asyncio.open_connection("127.0.0.1", port)
            w2.write(encode_command([b"SET", b"y", b"2"]))
            await w2.drain()
            assert (await r2.readline()).startswith(b"-BUSY ")
            log.release.set()
            assert await r1.readline() == b"+OK\r\n"
            for writer in (w1, w2):
                writer.close()
                await writer.wait_closed()
        await log.close_async()
        assert s.get(b"y") is None

    asyncio.run(scenario())


def test_real_tcp_always_condition_competition_has_one_winner(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "contended.aof")
        async with await asyncio.start_server(
            lambda r, w: handle_client(r, w, s), "127.0.0.1", 0
        ) as server:
            port = server.sockets[0].getsockname()[1]

            async def send(command):
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                try:
                    writer.write(encode_command(command))
                    await writer.drain()
                    return await asyncio.wait_for(reader.readline(), 2)
                finally:
                    writer.close()
                    await writer.wait_closed()

            out = await asyncio.gather(
                *(
                    send([b"SET", b"lease", str(i).encode(), b"NX", b"EX", b"30"])
                    for i in range(20)
                )
            )
            assert out.count(b"+OK\r\n") == 1 and out.count(b"$-1\r\n") == 19
            old = s.get(b"lease")
            out = await asyncio.gather(
                *(
                    send([b"CAS", b"lease", old, b"next", b"EX", b"30"])
                    for _ in range(20)
                )
            )
            assert out.count(b":1\r\n") == 1 and out.count(b":0\r\n") == 19
            assert await send([b"CAD", b"lease", old]) == b":0\r\n"
            assert await send([b"CAD", b"lease", b"next"]) == b":1\r\n"
        await log.close_async()
        assert s.get(b"lease") is None

    asyncio.run(scenario())


def test_standalone_supervision_stops_listener_on_commit_failure(tmp_path, monkeypatch):
    from aof import AOFError
    from main import run_service

    async def scenario():
        failed = asyncio.Event()
        reason = {"error": None}

        def notify(error):
            reason["error"] = error
            failed.set()

        s, log = opened(tmp_path / "supervised.aof", on_failure=notify)
        clients = set()

        async def handler(reader, writer):
            task = asyncio.current_task()
            clients.add(task)
            try:
                await handle_client(reader, writer, s)
            finally:
                clients.discard(task)

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        service = asyncio.create_task(
            run_service(server, s, log, failed, reason, clients)
        )

        def disk_error(fd):
            raise OSError("injected supervised fsync failure")

        monkeypatch.setattr("aof.os.fsync", disk_error)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(encode_command([b"SET", b"x", b"1"]))
        await writer.drain()
        with pytest.raises(AOFError):
            await asyncio.wait_for(service, 2)
        assert not server.is_serving() and s._committer.state == "FAILED"
        writer.close()
        await writer.wait_closed()
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("127.0.0.1", port)
        await log.close_async()

    asyncio.run(scenario())


def test_service_drains_before_cancelling_remaining_connections():
    from aof import AOFError
    from main import run_service

    async def scenario():
        draining, release = asyncio.Event(), asyncio.Event()

        class Server:
            closed = False

            async def serve_forever(self):
                await asyncio.Event().wait()

            def close(self):
                self.closed = True

            async def wait_closed(self):
                pass

        class State:
            drained = False

            def stop_write_admission(self):
                pass

            async def drain_writes(self):
                draining.set()
                await release.wait()
                self.drained = True

        class Log:
            sync_policy = "always"

        server, state = Server(), State()

        async def client():
            try:
                await asyncio.Event().wait()
            finally:
                assert state.drained and server.closed

        connection = asyncio.create_task(client())
        failure = asyncio.Event()
        failure.set()
        service = asyncio.create_task(
            run_service(
                server, state, Log(), failure, {"error": OSError("stop")}, {connection}
            )
        )
        await draining.wait()
        assert server.closed and not connection.done()
        release.set()
        with pytest.raises(AOFError):
            await service
        assert connection.cancelled()

    asyncio.run(scenario())


def test_soft_shutdown_drains_and_never_releases_live_io(tmp_path, monkeypatch, caplog):
    async def scenario():
        s, log = opened(tmp_path / "drain.aof", cls=HeldAOF)
        task = asyncio.create_task(s.set(b"x", b"1"))
        await entered(log)
        original_wait = asyncio.wait

        async def timeout(tasks, **kwargs):
            return set(), set(tasks)

        monkeypatch.setattr("commit.asyncio.wait", timeout)
        closing = asyncio.create_task(log.close_async())
        await asyncio.sleep(0)
        assert s._committer.state == "DRAINING" and not closing.done()
        assert log._file is not None and log._path_lock is not None
        assert s._committer.stats["drain_soft_timeouts"] == 1
        with pytest.raises(CommitClosed):
            await s.set(b"y", b"2")
        with pytest.raises(RuntimeError, match="asynchronous close"):
            log.close()
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()
        monkeypatch.setattr("commit.asyncio.wait", original_wait)
        log.release.set()
        assert await task
        await closing
        assert s.get(b"x") == b"1" and log._path_lock is None
        assert "retaining I/O ownership" in caplog.text

    asyncio.run(scenario())


def test_real_tcp_disconnect_and_ordered_clients(tmp_path):
    async def scenario():
        s, log = opened(tmp_path / "tcp.aof", cls=HeldAOF)
        async with await asyncio.start_server(
            lambda r, w: handle_client(r, w, s), "127.0.0.1", 0
        ) as server:
            port = server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(encode_command([b"SET", b"x", b"1"]))
            await writer.drain()
            await entered(log)
            writer.close()
            await writer.wait_closed()
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(encode_command([b"GET", b"x"]))
            await w.drain()
            reply = asyncio.create_task(r.readexactly(7))
            await asyncio.sleep(0)
            assert not reply.done()
            log.release.set()
            assert await asyncio.wait_for(reply, 2) == b"$1\r\n1\r\n"
            w.close()
            await w.wait_closed()
        await log.close_async()
        assert s.get(b"x") == b"1"

    asyncio.run(scenario())


@pytest.mark.parametrize("point", ["after_ack", "before_publication"])
def test_process_exit_recovers_durable_effect(tmp_path, point):
    root = Path(__file__).resolve().parents[1]
    path = tmp_path / f"{point}.aof"
    script = """
import asyncio,os,sys
from aof import AOF
from store import Store
async def run():
 s=Store();log=AOF(sys.argv[1],sync_policy='always');log.load(s);s.attach_aof(log)
 if sys.argv[2]=='before_publication':
  s._committer._publish=lambda changes:os._exit(23)
 await s.set(b'x',b'1')
 os._exit(24)
asyncio.run(run())
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, str(path), point], cwd=root, timeout=5
    )
    assert result.returncode == (24 if point == "after_ack" else 23)
    s = Store()
    log = AOF(path)
    log.load(s)
    assert s.get(b"x") == b"1"
    log.close()


@pytest.mark.parametrize("policy", ["everysec", "no"])
def test_legacy_modes_do_not_construct_a_coordinator(tmp_path, policy):
    async def scenario():
        s = Store()
        log = AOF(tmp_path / f"{policy}.aof", sync_policy=policy)
        log.load(s)
        s.attach_aof(log)
        assert s._committer is None
        await s.set(b"x", b"1")
        assert await s.get_for_command(b"x") == b"1"
        assert log.fsync_count == 0
        await log.close_async()
        assert log.fsync_count == 0

    asyncio.run(scenario())
