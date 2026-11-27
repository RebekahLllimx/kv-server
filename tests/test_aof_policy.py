import asyncio

import pytest

from aof import AOF, AOFError
from store import Store


def run(coroutine):
    return asyncio.run(coroutine)


def create_aof(tmp_path, sync_policy="everysec", on_failure=None):
    aof = AOF(
        tmp_path / "append.aof",
        sync_policy=sync_policy,
        on_failure=on_failure,
    )
    store = Store()
    aof.load(store)
    store.attach_aof(aof)
    return aof


def test_invalid_sync_policy_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="Invalid AOF sync policy"):
        AOF(tmp_path / "append.aof", sync_policy="sometimes")


def test_always_syncs_every_append(tmp_path, monkeypatch):
    fsync_calls = []
    monkeypatch.setattr("aof.os.fsync", fsync_calls.append)

    aof = create_aof(tmp_path, sync_policy="always")

    try:
        run(aof.append_async([b"SET", b"a", b"1"]))
        run(aof.append_async([b"SET", b"b", b"2"]))

        assert len(fsync_calls) == 2
        assert aof.fsync_count == 2
        assert aof._dirty is False
    finally:
        aof.close()


def test_everysec_waits_for_explicit_sync_and_skips_when_clean(
    tmp_path,
    monkeypatch,
):
    fsync_calls = []
    monkeypatch.setattr("aof.os.fsync", fsync_calls.append)

    aof = create_aof(tmp_path, sync_policy="everysec")

    try:
        run(aof.append_async([b"SET", b"a", b"1"]))

        assert fsync_calls == []
        assert aof._dirty is True

        assert run(aof.sync_async()) is True
        assert len(fsync_calls) == 1
        assert aof._dirty is False

        assert run(aof.sync_async()) is False
        assert len(fsync_calls) == 1
    finally:
        aof.close()


def test_no_policy_never_fsyncs_on_append_or_close(tmp_path, monkeypatch):
    fsync_calls = []
    monkeypatch.setattr("aof.os.fsync", fsync_calls.append)

    aof = create_aof(tmp_path, sync_policy="no")
    run(aof.append_async([b"SET", b"a", b"1"]))

    assert aof._dirty is True

    aof.close()

    assert fsync_calls == []


def test_fsync_failure_notifies_once_and_rejects_future_writes(
    tmp_path,
    monkeypatch,
):
    notifications = []

    def fail_fsync(fd):
        raise OSError("simulated fsync failure")

    monkeypatch.setattr("aof.os.fsync", fail_fsync)

    aof = create_aof(
        tmp_path,
        sync_policy="always",
        on_failure=notifications.append,
    )

    try:
        with pytest.raises(AOFError, match="Failed to sync AOF"):
            run(aof.append_async([b"SET", b"a", b"1"]))

        assert aof.failed is True
        assert aof._dirty is True
        assert len(notifications) == 1
        assert isinstance(notifications[0], OSError)

        with pytest.raises(AOFError, match="previous I/O failure"):
            run(aof.append_async([b"SET", b"b", b"2"]))

        assert len(notifications) == 1
    finally:
        aof.close()
