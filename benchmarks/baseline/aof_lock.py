"""Stable advisory lock shared by AOF writers and offline maintenance (Unix)."""
import fcntl
import os
from pathlib import Path


class AOFBusyError(RuntimeError):
    pass


class AOFFileLock:
    def __init__(self, path):
        self.path = Path(str(Path(path).resolve()) + ".lock")
        self.fd = None

    def acquire(self):
        if self.fd is not None:
            raise RuntimeError("Lock already acquired")
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise AOFBusyError("AOF is in use; stop its writer before maintenance") from None
        except BaseException:
            os.close(fd)
            raise
        self.fd = fd
        return self

    def close(self):
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)
        # Never unlink: every process must lock the same sidecar inode.

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_):
        self.close()
