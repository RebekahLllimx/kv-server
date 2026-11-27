import asyncio
import time
from collections.abc import Callable


class Store:
    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time
    ):
        self._data: dict[bytes, bytes] = {}
        self._expires_at: dict[bytes, float] = {}

        # Runtime expiry uses a clock unaffected by wall-clock adjustments.
        self._clock = clock

        # Absolute Unix deadlines retain elapsed downtime across restart.
        self._wall_clock = wall_clock

        # Attach persistence only after recovery has populated the store.
        self._aof = None

        # Lock order is Store, then AOF; condition checks and writes share this lock.
        self._lock = asyncio.Lock()

    def attach_aof(self, aof) -> None:
        'Attach the AOF after startup replay has completed.'
        if self._aof is not None:
            raise RuntimeError("AOF is already attached")

        self._aof = aof

    async def _append_aof(self, command: list[bytes]) -> None:
        'Append the operation before publishing its in-memory effect.'
        if self._aof is not None:
            await self._aof.append_async(command)

    def _is_expired(self, key: bytes) -> bool:
        expires_at = self._expires_at.get(key)

        if expires_at is None:
            return False

        return self._clock() >= expires_at

    def _remove_expired_unlocked(self, key: bytes) -> bool:
        if not self._is_expired(key):
            return False

        self._data.pop(key, None)
        self._expires_at.pop(key, None)
        return True

    async def _write_unlocked(self, key, value, seconds=None):
        deadline = None
        if seconds is None:
            record = [b"SET", key, value]
        else:
            deadline = self._clock() + seconds
            unix_ms = int((self._wall_clock() + seconds) * 1000)
            record = [b"SETAT", key, value, str(unix_ms).encode("ascii")]
        await self._append_aof(record)
        self._data[key] = value
        if deadline is None:
            self._expires_at.pop(key, None)
        else:
            self._expires_at[key] = deadline

    async def set(self, key: bytes, value: bytes, *, seconds=None, condition=None):
        if seconds is not None and (type(seconds) is not int or not 1 <= seconds <= 2147483647):
            raise ValueError("Invalid SET expiry")
        if condition not in (None, "NX", "XX"):
            raise ValueError("Invalid SET condition")
        async with self._lock:
            exists = self.get(key) is not None
            if (condition == "NX" and exists) or (condition == "XX" and not exists):
                return False
            await self._write_unlocked(key, value, seconds)
            return True

    async def compare_set(self, key, expected, value, *, seconds=None):
        if not isinstance(expected, bytes):
            raise ValueError("Expected value must be bytes")
        if seconds is not None and (type(seconds) is not int or not 1 <= seconds <= 2147483647):
            raise ValueError("Invalid CAS expiry")
        async with self._lock:
            if self.get(key) != expected:
                return 0
            await self._write_unlocked(key, value, seconds)
            return 1

    async def compare_delete(self, key, expected):
        if not isinstance(expected, bytes):
            raise ValueError("Expected value must be bytes")
        async with self._lock:
            if self.get(key) != expected:
                return 0
            await self._append_aof([b"DEL", key])
            self._data.pop(key, None)
            self._expires_at.pop(key, None)
            return 1

    def get(self, key: bytes) -> bytes | None:
        if self._is_expired(key):
            return None

        return self._data.get(key)

    async def delete(self, key: bytes) -> int:
        async with self._lock:
            self._remove_expired_unlocked(key)

            if key not in self._data:
                return 0

            # A failed append must not publish an unlogged deletion.
            await self._append_aof([b"DEL", key])

            del self._data[key]
            self._expires_at.pop(key, None)

            return 1

    async def expire(self, key: bytes, seconds: int) -> int:
        async with self._lock:
            self._remove_expired_unlocked(key)

            if key not in self._data:
                return 0

            if seconds <= 0:
                # Nonpositive expiry is an immediate deletion recorded as DEL.
                await self._append_aof([b"DEL", key])

                self._data.pop(key, None)
                self._expires_at.pop(key, None)

                return 1

            
            monotonic_deadline = self._clock() + seconds

            if self._aof is not None:
                # Persist an absolute deadline rather than a fresh TTL.
                unix_deadline_ms = int(
                    (self._wall_clock() + seconds) * 1000
                )

                await self._append_aof([
                    b"EXPIREAT",
                    key,
                    str(unix_deadline_ms).encode("ascii")
                ])

            self._expires_at[key] = monotonic_deadline

            return 1

    def restore_set(self, key: bytes, value: bytes) -> None:
        'Apply a replay effect during single-threaded startup only.'
        self._data[key] = value
        self._expires_at.pop(key, None)

    def restore_delete(self, key: bytes) -> None:
        'Apply a replay effect during single-threaded startup only.'
        self._data.pop(key, None)
        self._expires_at.pop(key, None)

    def restore_expiration(
        self,
        key: bytes,
        deadline_unix_ms: int
    ) -> None:
        "Convert a persisted Unix deadline into this process's monotonic clock during replay."
        if key not in self._data:
            return

        remaining = (
            deadline_unix_ms / 1000.0 - self._wall_clock()
        )

        if remaining <= 0:
            self._data.pop(key, None)
            self._expires_at.pop(key, None)
            return

        self._expires_at[key] = self._clock() + remaining

    def ttl(self, key: bytes) -> int:
        if self._is_expired(key):
            return -2

        if key not in self._data:
            return -2

        expires_at = self._expires_at.get(key)

        if expires_at is None:
            return -1

        remaining = expires_at - self._clock()

        return int(remaining)

    async def purge_expired(self) -> int:
        async with self._lock:
            now = self._clock()

            expired_keys = [
                key
                for key, expires_at in self._expires_at.items()
                if expires_at <= now
            ]

            for key in expired_keys:
                self._data.pop(key, None)
                self._expires_at.pop(key, None)

            return len(expired_keys)
