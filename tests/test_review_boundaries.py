"""Admission and recovery faults found during the public-source review."""

import asyncio
import select
import signal
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from aof import AOF, AOFError, encode_command
from commands import execute_command
from commit import Limits
from response import Error, Integer
from server import handle_client
from store import Store


@pytest.mark.parametrize("policy", ["always", "everysec", "no"])
def test_invalid_expiry_does_not_poison_service(tmp_path, policy):
    async def scenario():
        store = Store()
        log = AOF(tmp_path / "expiry.aof", sync_policy=policy)
        log.load(store)
        store.attach_aof(log)
        try:
            await store.set(b"key", b"before")
            for seconds in (b"9" * 400, b"9" * 25, b"9" * 10000):
                result = await execute_command([b"EXPIRE", b"key", seconds], store)
                assert result == Error(b"ERR invalid expire time")
                assert not log.failed
                assert await store.get_for_command(b"key") == b"before"
                assert await store.ttl_for_command(b"key") == -1
            assert await store.compare_set(b"key", b"before", b"after") == 1
            assert await execute_command([b"EXPIRE", b"key", b"60"], store) == Integer(
                1
            )
        finally:
            await log.close_async()

        recovered = Store()
        replay = AOF(log.path)
        replay.load(recovered)
        assert recovered.get(b"key") == b"after"
        assert 0 <= recovered.ttl(b"key") <= 60
        replay.close()

    asyncio.run(scenario())


def test_invalid_programmatic_request_is_rejected_before_admission(tmp_path):
    async def scenario():
        store = Store()
        log = AOF(tmp_path / "admission.aof", sync_policy="always")
        log.load(store)
        store.attach_aof(log)
        try:
            owner = store._committer
            for operation, kwargs in (
                ("SET", {"value": None}),
                ("GET", {"key": None}),
                ("CAS", {"value": b"v", "expected": None}),
                ("CAD", {"expected": None}),
            ):
                with pytest.raises(TypeError):
                    await owner.submit(operation, **kwargs)
            with pytest.raises(ValueError, match="Unknown"):
                await owner.submit("UNKNOWN")
            assert owner.pending == owner.stats["admitted"] == 0
            assert owner.state == "OPEN" and not log.failed
            await store.set(b"key", b"valid")
            assert await store.get_for_command(b"key") == b"valid"
        finally:
            await log.close_async()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "field", ["requests", "payload_bytes", "batch_requests", "batch_bytes"]
)
@pytest.mark.parametrize("value", [True, 1.5, 0, -1])
def test_commit_bounds_require_positive_integer(field, value):
    with pytest.raises(ValueError, match="positive integers"):
        Limits(**{field: value})


@pytest.mark.parametrize(
    "warning", [float("nan"), float("inf"), 10**400, 0, -1, True, "5"]
)
def test_drain_warning_is_finite_and_positive(warning):
    with pytest.raises(ValueError, match="finite and positive"):
        Limits(drain_warning_seconds=warning)


@pytest.mark.parametrize("name", [b"SETAT", b"EXPIREAT"])
@pytest.mark.parametrize("timestamp", [b"9" * 400, b"9" * 21, b"-1", b""])
def test_invalid_recovery_timestamp_is_aof_error_and_releases_lock(
    tmp_path, name, timestamp
):
    path = tmp_path / "corrupt.aof"
    arguments = (
        [name, b"key", b"value", timestamp]
        if name == b"SETAT"
        else [name, b"key", timestamp]
    )
    content = encode_command([b"SET", b"valid", b"prefix"]) + encode_command(arguments)
    path.write_bytes(content)
    for _ in range(2):
        log = AOF(path)
        with pytest.raises(AOFError, match="expiration timestamp"):
            log.load(Store())
        assert log._file is None and log._path_lock is None
        assert (
            path.read_bytes() == content
        )  # Corruption is not repaired as an EOF tail.


def test_very_negative_expiry_keeps_immediate_delete_semantics(tmp_path):
    async def scenario():
        store = Store()
        log = AOF(tmp_path / "negative.aof", sync_policy="always")
        log.load(store)
        store.attach_aof(log)
        try:
            await store.set(b"key", b"value")
            assert await execute_command(
                [b"EXPIRE", b"key", b"-" + b"9" * 10000], store
            ) == Integer(1)
            assert await store.get_for_command(b"key") is None
        finally:
            await log.close_async()

    asyncio.run(scenario())


def test_socket_reset_is_a_connection_failure_not_an_unhandled_task_error():
    class ResetReader:
        async def read(self, count):
            raise ConnectionResetError("Peer reset")

    class ResetWriter:
        closed = False

        def close(self):
            self.closed = True

        async def wait_closed(self):
            raise ConnectionResetError("Peer reset during cleanup")

    writer = ResetWriter()
    asyncio.run(handle_client(ResetReader(), writer, Store()))
    assert writer.closed


@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM])
def test_standalone_signal_drains_and_exits_with_live_client(tmp_path, stop_signal):
    path = tmp_path / "signal.aof"
    process = subprocess.Popen(
        [
            sys.executable,
            "-B",
            str(Path(__file__).resolve().parents[1] / "main.py"),
            "--sync",
            "always",
            "--aof",
            str(path),
            "--port",
            "0",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert select.select([process.stdout], [], [], 5)[0]
        line = process.stdout.readline().decode()
        assert line.startswith("Server started at 127.0.0.1:")
        port = int(line.rsplit(":", 1)[1])
        assert port > 0
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            client.sendall(encode_command([b"SET", b"signal", b"confirmed"]))
            assert client.recv(128) == b"+OK\r\n"
            process.send_signal(stop_signal)
            assert process.wait(timeout=5) == 0
        _, error = process.communicate(timeout=2)
        assert error == b"", error.decode()
        store, replay = Store(), AOF(path)
        replay.load(store)
        assert store.get(b"signal") == b"confirmed"
        replay.close()
    finally:
        if process.poll() is None:
            process.kill()  # Cleanup only for a failed test's isolated process.
            process.wait()
        process.stdout.close()
        process.stderr.close()


def test_tcp_bad_expiry_then_valid_write_uses_same_healthy_connection(tmp_path):
    async def scenario():
        store = Store()
        log = AOF(tmp_path / "tcp.aof", sync_policy="always")
        log.load(store)
        store.attach_aof(log)
        handlers = set()

        async def handler(reader, writer):
            task = asyncio.current_task()
            handlers.add(task)
            try:
                await handle_client(reader, writer, store)
            finally:
                handlers.discard(task)

        async with await asyncio.start_server(handler, "127.0.0.1", 0) as server:
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", server.sockets[0].getsockname()[1]
            )
            try:
                for command, expected in (
                    ([b"SET", b"key", b"before"], b"+OK\r\n"),
                    (
                        [b"EXPIRE", b"key", b"9" * 10000],
                        b"-ERR invalid expire time\r\n",
                    ),
                    ([b"SET", b"key", b"after"], b"+OK\r\n"),
                ):
                    writer.write(encode_command(command))
                    await writer.drain()
                    assert await asyncio.wait_for(reader.readline(), 2) == expected
                assert not log.failed and store.get(b"key") == b"after"
            finally:
                writer.close()
                await writer.wait_closed()
        if handlers:
            await asyncio.wait_for(asyncio.gather(*handlers), 2)
        await log.close_async()

    asyncio.run(scenario())
