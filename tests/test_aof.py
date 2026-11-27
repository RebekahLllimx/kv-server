
from aof import AOF, AOFError, encode_command
from store import Store


class FakeClock:
    def __init__(self, initial_time=1000.0):
        self.current_time = initial_time

    def __call__(self):
        return self.current_time

    def advance(self, seconds):
        self.current_time += seconds


def test_encode_command():
    command = [b"SET", b"name", b"Alice"]

    assert encode_command(command) == (
        b"*3\r\n"
        b"$3\r\nSET\r\n"
        b"$4\r\nname\r\n"
        b"$5\r\nAlice\r\n"
    )


def test_encode_command_is_binary_safe():
    command = [b"SET", b"key", b"a\r\nb\x00c"]

    encoded = encode_command(command)

    assert encoded.endswith(b"$6\r\na\r\nb\x00c\r\n")


def test_aof_replays_set_and_delete(tmp_path):
    path = tmp_path / "appendonly.aof"

    store = Store()
    aof = AOF(str(path))
    aof.load(store)
    store.attach_aof(aof)

    run(store.set(b"name", b"Alice"))
    run(store.set(b"age", b"20"))
    run(store.delete(b"age"))

    aof.close()

    
    recovered_store = Store()

    recovered_aof = AOF(str(path))
    recovered_aof.load(recovered_store)

    assert recovered_store.get(b"name") == b"Alice"
    assert recovered_store.get(b"age") is None

    recovered_aof.close()


def test_aof_preserves_remaining_ttl(tmp_path):
    path = tmp_path / "appendonly.aof"

    wall_clock = FakeClock(1000.0)
    monotonic_clock = FakeClock(100.0)

    store = Store(
        clock=monotonic_clock,
        wall_clock=wall_clock
    )

    aof = AOF(str(path))
    aof.load(store)
    store.attach_aof(aof)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 10))

    aof.close()

    
    wall_clock.advance(4)

    
    recovered_store = Store(
        clock=FakeClock(500.0),
        wall_clock=wall_clock
    )

    recovered_aof = AOF(str(path))
    recovered_aof.load(recovered_store)

    assert recovered_store.get(b"name") == b"Alice"
    assert recovered_store.ttl(b"name") == 6

    recovered_aof.close()


def test_aof_removes_key_expired_during_downtime(tmp_path):
    path = tmp_path / "appendonly.aof"

    wall_clock = FakeClock(1000.0)

    store = Store(
        clock=FakeClock(100.0),
        wall_clock=wall_clock
    )

    aof = AOF(str(path))
    aof.load(store)
    store.attach_aof(aof)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 5))

    aof.close()

    
    wall_clock.advance(6)

    recovered_store = Store(
        clock=FakeClock(500.0),
        wall_clock=wall_clock
    )

    recovered_aof = AOF(str(path))
    recovered_aof.load(recovered_store)

    assert recovered_store.get(b"name") is None
    assert recovered_store.ttl(b"name") == -2

    recovered_aof.close()


def test_aof_truncates_incomplete_final_record(tmp_path):
    path = tmp_path / "appendonly.aof"

    valid_record = encode_command(
        [b"SET", b"name", b"Alice"]
    )

    incomplete_record = (
        b"*2\r\n"
        b"$3\r\nGET\r\n"
        b"$4\r\nna"
    )

    path.write_bytes(valid_record + incomplete_record)

    store = Store()
    aof = AOF(str(path))

    aof.load(store)

    assert store.get(b"name") == b"Alice"

    
    assert path.read_bytes() == valid_record

    store.attach_aof(aof)

    
    run(store.set(b"city", b"Paris"))

    aof.close()

    expected_file = (
        valid_record
        + encode_command([b"SET", b"city", b"Paris"])
    )

    assert path.read_bytes() == expected_file

    
    recovered_store = Store()
    recovered_aof = AOF(str(path))

    recovered_aof.load(recovered_store)

    assert recovered_store.get(b"name") == b"Alice"
    assert recovered_store.get(b"city") == b"Paris"

    recovered_aof.close()
import asyncio

import pytest

from aof import AOF
from store import Store


def run(coroutine):
    return asyncio.run(coroutine)


def test_sync_only_when_dirty(tmp_path, monkeypatch):
    fsync_calls = []

    def fake_fsync(fd):
        fsync_calls.append(fd)

    monkeypatch.setattr("aof.os.fsync", fake_fsync)

    aof = AOF(tmp_path / "append.aof")
    store = Store()
    aof.load(store)

    try:
        
        assert run(aof.sync_async()) is False
        assert fsync_calls == []

        
        run(aof.append_async([b"SET", b"name", b"Alice"]))
        assert aof._dirty is True

        
        assert run(aof.sync_async()) is True
        assert len(fsync_calls) == 1
        assert aof._dirty is False

        
        assert run(aof.sync_async()) is False
        assert len(fsync_calls) == 1

        
        run(aof.append_async([b"SET", b"name", b"Bob"]))
        assert aof._dirty is True

        assert run(aof.sync_async()) is True
        assert len(fsync_calls) == 2
        assert aof._dirty is False

    finally:
        aof.close()

def test_sync_failure_keeps_dirty(tmp_path, monkeypatch):
    def failing_fsync(fd):
        raise OSError("simulated fsync failure")

    monkeypatch.setattr("aof.os.fsync", failing_fsync)

    failure_notifications = []

    aof = AOF(
        tmp_path / "append.aof",
        on_failure=failure_notifications.append,
    )
    store = Store()
    aof.load(store)

    try:
        run(aof.append_async([b"SET", b"name", b"Alice"]))

        with pytest.raises(AOFError):
            run(aof.sync_async())

        assert aof._dirty is True
        assert aof._failed is True
        assert len(failure_notifications) == 1
        assert isinstance(failure_notifications[0], OSError)

    finally:
        aof.close()
