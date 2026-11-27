import asyncio
import os
from pathlib import Path
from aof_lock import AOFFileLock

from resp import RESPParser, NEED_MORE_DATA, ProtocolError


VALID_SYNC_POLICIES = {"always", "everysec", "no"}


class AOFError(Exception):
    'The AOF is corrupt or cannot be recovered.'


def encode_command(command: list[bytes]) -> bytes:
    'Encode a command as a RESP array of binary-safe bulk strings.'
    if not command:
        raise ValueError("Cannot encode an empty command")

    result = bytearray()

    result.extend(b"*" + str(len(command)).encode("ascii") + b"\r\n")

    for argument in command:
        if not isinstance(argument, bytes):
            raise TypeError("Command arguments must be bytes")

        result.extend(b"$" + str(len(argument)).encode("ascii") + b"\r\n")
        result.extend(argument)
        result.extend(b"\r\n")

    return bytes(result)


class AOF:
    def __init__(
        self,
        path,
        sync_policy="everysec",
        on_failure=None,
    ):
        if sync_policy not in VALID_SYNC_POLICIES:
            raise ValueError(
                f"Invalid AOF sync policy: {sync_policy}"
            )

        self.path = Path(path).resolve()
        self._path_lock = None
        self._file = None

        
        self.sync_policy = sync_policy

        
        self._dirty = False
        self._fsync_count = 0

        
        self._failed = False
        self._failure_reason = None
        self._on_failure = on_failure
        self._failure_notified = False

        # File operations are serialized after acquiring the Store lock.
        # Never acquire Store from an AOF operation.
        self._lock = asyncio.Lock()

    def load(self, store) -> None:
        """Own the stable path lock until close, including startup repair."""
        if self._file is not None or self._path_lock is not None:
            raise RuntimeError("AOF has already been loaded")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock = AOFFileLock(self.path).acquire()
        self._path_lock = lock
        try:
            self._load_locked(store)
        except BaseException:
            lock.close()
            self._path_lock = None
            raise

    def _load_locked(self, store) -> None:
        'Replay complete records, truncate an incomplete final record, then open the AOF for append.'
        if self._file is not None:
            raise RuntimeError("AOF has already been loaded")

        self.path.parent.mkdir(parents=True, exist_ok=True)

        last_good_offset = 0
        total_bytes_read = 0

        if self.path.exists():
            parser = RESPParser()

            with self.path.open("rb") as file:
                while True:
                    data = file.read(4096)

                    if data == b"":
                        break

                    total_bytes_read += len(data)
                    parser.feed(data)

                    while True:
                        try:
                            command = parser.next_command()

                        except ProtocolError as exc:
                            raise AOFError(
                                "Invalid RESP data in AOF"
                            ) from exc

                        if command is NEED_MORE_DATA:
                            break

                        self._apply_command(command, store)

                        # Only fully parsed records advance the recoverable prefix.
                        last_good_offset = (
                            total_bytes_read - len(parser.buffer)
                        )

            
            # Truncate a partial tail before accepting another append.
            if parser.buffer:
                with self.path.open("r+b") as file:
                    file.truncate(last_good_offset)

        
        self._file = self.path.open("ab")

    def _apply_command(self, command: list[bytes], store) -> None:
        'Apply one parsed internal effect record, without rechecking its original client condition.'

        name = command[0].upper()

        if name == b"SET":
            if len(command) != 3:
                raise AOFError("Invalid SET record")

            store.restore_set(command[1], command[2])

        elif name == b"SETAT":
            if len(command) != 4 or not command[3].isdigit() or len(command[3]) > 20:
                raise AOFError("Invalid SETAT record")
            store.restore_set(command[1], command[2])
            store.restore_expiration(command[1], int(command[3]))

        elif name == b"DEL":
            if len(command) != 2:
                raise AOFError("Invalid DEL record")

            store.restore_delete(command[1])

        elif name == b"EXPIREAT":
            if len(command) != 3:
                raise AOFError("Invalid EXPIREAT record")

            deadline_bytes = command[2]

            if not deadline_bytes.isdigit():
                raise AOFError("Invalid expiration timestamp")

            deadline_unix_ms = int(deadline_bytes)

            store.restore_expiration(
                command[1],
                deadline_unix_ms
            )

        else:
            raise AOFError(
                f"Unsupported AOF operation: {name!r}"
            )

    def _append_sync(self, command: list[bytes]):
        'Append and flush a complete record in the worker thread.'
        self._ensure_healthy()

        if self._file is None:
            raise RuntimeError("AOF is not open")

        data = encode_command(command)

        
        self._dirty = True

        try:
            written = self._file.write(data)

            if written != len(data):
                raise OSError("Incomplete AOF write")

            self._file.flush()

        except OSError as exc:
            self._mark_failed(exc)
            raise AOFError("Failed to append to AOF") from exc

        # Under always, every acknowledged write waits for fsync.
        if self.sync_policy == "always":
            self._sync_sync()

    def _sync_sync(self):
        'Synchronize pending writes in the worker thread.'
        self._ensure_healthy()

        if self._file is None:
            raise RuntimeError("AOF is not open")

        if not self._dirty:
            return False

        try:
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError as exc:
            self._mark_failed(exc)
            raise AOFError("Failed to sync AOF") from exc

        self._fsync_count += 1
        self._dirty = False
        return True

    async def _run_file_operation(self, operation, *args):
        'Serialize offloaded file operations. Cancellation does not stop a worker thread, so await its completion before releasing the AOF lock.'
        async with self._lock:
            worker = asyncio.create_task(
                asyncio.to_thread(operation, *args)
            )

            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                try:
                    await asyncio.shield(worker)
                except Exception:
                    
                    # Preserve cancellation; finally delivers failure notification on the event loop.
                    pass
                raise
            finally:
                self._notify_failure()

    async def append_async(self, command: list[bytes]):
        return await self._run_file_operation(
            self._append_sync,
            command,
        )

    async def sync_async(self):
        return await self._run_file_operation(self._sync_sync)

    def _ensure_healthy(self):
        if self._failed:
            raise AOFError(
                "AOF is unavailable after a previous I/O failure."
            ) from self._failure_reason

    def _mark_failed(self, exc):
        if self._failed:
            return

        self._failed = True
        self._failure_reason = exc

    def _notify_failure(self):
        'Invoke the failure callback only from the event-loop thread.'
        if (
            self._failed
            and not self._failure_notified
            and self._on_failure is not None
        ):
            self._failure_notified = True
            self._on_failure(self._failure_reason)

    @property
    def failed(self):
        return self._failed


    @property
    def fsync_count(self):
        return self._fsync_count

    def _close_sync(self):
        file, self._file = self._file, None
        try:
            if file is not None:
                file.close()
        except OSError as exc:
            self._mark_failed(exc)
            raise AOFError("Failed to close AOF") from exc
        finally:
            if self._path_lock is not None:
                self._path_lock.close()
                self._path_lock = None

    async def close_async(self):
        return await self._run_file_operation(self._close_sync)

    def close(self):
        'Close synchronously only during startup or isolated tests; a running service must use close_async().'
        self._close_sync()
        self._notify_failure()
