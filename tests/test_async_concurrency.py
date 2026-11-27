import asyncio
import threading

import pytest

from aof import AOF, AOFError
from main import run_service
from store import Store


class FakeClock:
    def __init__(self, value=0.0):
        self.value = value

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


def run(coroutine):
    return asyncio.run(coroutine)


def test_concurrent_delete_is_serialized():
    async def scenario():
        store = Store()
        await store.set(b"x", b"1")

        results = await asyncio.gather(
            store.delete(b"x"),
            store.delete(b"x"),
        )

        assert sorted(results) == [0, 1]
        assert store.get(b"x") is None

    run(scenario())


def test_concurrent_set_matches_aof_replay(tmp_path):
    async def scenario():
        path = tmp_path / "append.aof"
        store = Store()
        aof = AOF(path, sync_policy="everysec")
        aof.load(store)
        store.attach_aof(aof)

        await asyncio.gather(
            store.set(b"x", b"first"),
            store.set(b"x", b"second"),
        )
        committed_value = store.get(b"x")
        await aof.close_async()

        recovered_store = Store()
        recovered_aof = AOF(path)
        recovered_aof.load(recovered_store)

        try:
            assert recovered_store.get(b"x") == committed_value
        finally:
            await recovered_aof.close_async()

    run(scenario())


def test_set_and_purge_cannot_delete_new_value():
    async def scenario():
        clock = FakeClock(100.0)
        store = Store(clock=clock)

        await store.set(b"x", b"old")
        await store.expire(b"x", 1)
        clock.advance(2)

        await asyncio.gather(
            store.purge_expired(),
            store.set(b"x", b"new"),
        )

        assert store.get(b"x") == b"new"
        assert store.ttl(b"x") == -1

    run(scenario())


def test_get_sees_old_value_until_set_commits():
    class BlockingAOF:
        def __init__(self):
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def append_async(self, command):
            self.started.set()
            await self.release.wait()

    async def scenario():
        store = Store()
        await store.set(b"x", b"old")

        aof = BlockingAOF()
        store.attach_aof(aof)

        write_task = asyncio.create_task(store.set(b"x", b"new"))
        await aof.started.wait()

        assert store.get(b"x") == b"old"

        aof.release.set()
        await write_task

        assert store.get(b"x") == b"new"

    run(scenario())


def test_expired_reads_do_not_mutate_store():
    async def scenario():
        clock = FakeClock(10.0)
        store = Store(clock=clock)

        await store.set(b"x", b"value")
        await store.expire(b"x", 1)
        clock.advance(2)

        assert store.get(b"x") is None
        assert store.ttl(b"x") == -2
        assert b"x" in store._data
        assert b"x" in store._expires_at

        assert await store.purge_expired() == 1
        assert b"x" not in store._data
        assert b"x" not in store._expires_at

    run(scenario())


def test_append_and_sync_do_not_overlap(tmp_path):
    class BlockingAOF(AOF):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.append_started = threading.Event()
            self.release_append = threading.Event()
            self.sync_started = threading.Event()

        def _append_sync(self, command):
            self.append_started.set()
            self.release_append.wait(timeout=2.0)
            return super()._append_sync(command)

        def _sync_sync(self):
            self.sync_started.set()
            return super()._sync_sync()

    async def scenario():
        aof = BlockingAOF(tmp_path / "append.aof")
        aof.load(Store())

        append_task = asyncio.create_task(
            aof.append_async([b"SET", b"x", b"1"])
        )
        assert await asyncio.to_thread(
            aof.append_started.wait,
            1.0,
        )

        sync_task = asyncio.create_task(aof.sync_async())
        await asyncio.sleep(0.02)
        assert not aof.sync_started.is_set()

        aof.release_append.set()
        await append_task
        assert await sync_task is True
        await aof.close_async()

    run(scenario())


def test_cancellation_waits_for_worker_before_releasing_aof_lock(tmp_path):
    class BlockingAOF(AOF):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.append_started = threading.Event()
            self.release_append = threading.Event()
            self.sync_started = threading.Event()

        def _append_sync(self, command):
            self.append_started.set()
            self.release_append.wait(timeout=2.0)
            return super()._append_sync(command)

        def _sync_sync(self):
            self.sync_started.set()
            return super()._sync_sync()

    async def scenario():
        aof = BlockingAOF(tmp_path / "append.aof")
        aof.load(Store())

        append_task = asyncio.create_task(
            aof.append_async([b"SET", b"x", b"1"])
        )
        assert await asyncio.to_thread(
            aof.append_started.wait,
            1.0,
        )

        append_task.cancel()
        sync_task = asyncio.create_task(aof.sync_async())
        await asyncio.sleep(0.02)

        assert not append_task.done()
        assert not aof.sync_started.is_set()

        aof.release_append.set()

        with pytest.raises(asyncio.CancelledError):
            await append_task

        assert await sync_task is True
        await aof.close_async()

    run(scenario())


def test_aof_failure_notifies_on_event_loop_and_rejects_writes(
    tmp_path,
    monkeypatch,
):
    def fail_fsync(fd):
        raise OSError("simulated fsync failure")

    monkeypatch.setattr("aof.os.fsync", fail_fsync)

    async def scenario():
        event_loop_thread = threading.get_ident()
        notifications = []

        def notify(exc):
            notifications.append((exc, threading.get_ident()))

        aof = AOF(
            tmp_path / "append.aof",
            sync_policy="always",
            on_failure=notify,
        )
        aof.load(Store())

        with pytest.raises(AOFError, match="Failed to sync AOF"):
            await aof.append_async([b"SET", b"x", b"1"])

        with pytest.raises(AOFError, match="previous I/O failure"):
            await aof.append_async([b"SET", b"x", b"2"])

        assert len(notifications) == 1
        assert notifications[0][1] == event_loop_thread
        await aof.close_async()

    run(scenario())


def test_run_service_stops_accepting_before_cancelling_clients():
    class FakeServer:
        def __init__(self):
            self.closed = False
            self.closed_before_client_cancelled = False

        async def serve_forever(self):
            await asyncio.Event().wait()

        def close(self):
            self.closed = True

        async def wait_closed(self):
            return None

    class FakeAOF:
        sync_policy = "no"

    async def scenario():
        server = FakeServer()
        client_cancelled = asyncio.Event()

        async def client():
            try:
                await asyncio.Event().wait()
            finally:
                server.closed_before_client_cancelled = server.closed
                client_cancelled.set()

        client_task = asyncio.create_task(client())
        await asyncio.sleep(0)

        failure_event = asyncio.Event()
        failure_event.set()

        with pytest.raises(AOFError, match="stopping service"):
            await run_service(
                server,
                Store(),
                FakeAOF(),
                failure_event,
                {"error": OSError("disk failed")},
                {client_task},
            )

        await client_cancelled.wait()
        assert server.closed
        assert server.closed_before_client_cancelled
        assert client_task.done()

    run(scenario())
