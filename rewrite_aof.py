"""Offline AOF rewrite. Preview by default; --apply retains one rollback backup."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import time

from aof import AOF, AOFError, encode_command
from aof_lock import AOFFileLock
from resp import RESPParser, NEED_MORE_DATA, ProtocolError


class RewriteError(RuntimeError):
    pass


class InstalledDurabilityError(RewriteError):
    """Rename happened, but directory durability was not confirmed."""


class ReplayState:
    """Persistent value/deadline pairs at a frozen Unix-millisecond boundary."""
    def __init__(self, now_ms):
        self.now_ms, self.entries = now_ms, {}

    def restore_set(self, key, value):
        self.entries[key] = (value, None)

    def restore_delete(self, key):
        self.entries.pop(key, None)

    def restore_expiration(self, key, deadline):
        if key not in self.entries:
            return
        if deadline <= self.now_ms:
            self.restore_delete(key)
        else:
            self.entries[key] = (self.entries[key][0], deadline)


def replay(path, now_ms):
    parser, state, digest, count = RESPParser(), ReplayState(now_ms), hashlib.sha256(), 0
    validator = AOF(path)  # use existing record validation; never load/modify this path
    with path.open("rb") as stream:
        while True:
            data = stream.read(65536)
            if not data:
                break
            digest.update(data)
            parser.feed(data)
            while True:
                try:
                    command = parser.next_command()
                except ProtocolError as error:
                    raise RewriteError("Malformed AOF; original left unchanged") from error
                if command is NEED_MORE_DATA:
                    break
                try:
                    validator._apply_command(command, state)
                except (AOFError, ValueError, OverflowError) as error:
                    raise RewriteError("Invalid AOF record; original left unchanged") from error
                count += 1
    if parser.buffer:
        raise RewriteError("Incomplete AOF tail; rewrite refuses to repair the source")
    return state.entries, digest.hexdigest(), count


def records(entries):
    for key, (value, deadline) in sorted(entries.items()):
        yield ([b"SET", key, value] if deadline is None else
               [b"SETAT", key, value, str(deadline).encode("ascii")])


def fingerprint(path):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1:
        raise RewriteError("Use a regular AOF without symlinks or hard-link aliases")
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def backup_source(path, backup, expected_digest):
    fd, name = tempfile.mkstemp(prefix=path.name + ".backup-tmp-", dir=path.parent)
    temp = Path(name)
    try:
        digest = hashlib.sha256()
        with os.fdopen(fd, "wb") as target, path.open("rb") as source:
            for data in iter(lambda: source.read(65536), b""):
                digest.update(data)
                target.write(data)
            target.flush()
            os.fsync(target.fileno())
        if digest.hexdigest() != expected_digest:
            raise RewriteError("Source changed while copying backup")
        os.link(temp, backup)  # exclusive publication; never overwrite a backup
    finally:
        temp.unlink(missing_ok=True)
    sync_directory(path.parent)


def rewrite(path, *, apply=False, now_ms=None):
    path = Path(path).absolute()
    fingerprint(path)  # also rejects a final-component symlink before resolving
    path = path.resolve()
    cutoff = int(time.time() * 1000) if now_ms is None else now_ms
    if type(cutoff) is not int or cutoff < 0:
        raise ValueError("Use a nonnegative Unix-millisecond cutoff")
    backup = path.with_name(path.name + ".rewrite-backup")
    with AOFFileLock(path):
        original = fingerprint(path)
        entries, old_digest, count = replay(path, cutoff)
        if fingerprint(path) != original:
            raise RewriteError("Source changed during preview")
        size = sum(len(encode_command(c)) for c in records(entries))
        report = {"status": "preview", "cutoff_unix_ms": cutoff,
                  "old_bytes": original[2], "new_bytes": size,
                  "bytes_saved": original[2] - size, "old_records": count,
                  "live_keys_at_cutoff": len(entries), "old_sha256": old_digest,
                  "absolute_expiries_preserved": True, "backup": None}
        if not apply:
            return report
        if backup.exists() or backup.is_symlink():
            raise RewriteError("Rollback backup exists; archive or remove it explicitly before another rewrite")
        fd, name = tempfile.mkstemp(prefix=path.name + ".rewrite-tmp-", dir=path.parent)
        temp = Path(name)
        try:
            with os.fdopen(fd, "wb") as stream:
                os.fchmod(stream.fileno(), stat.S_IMODE(path.stat().st_mode))
                for command in records(entries):
                    stream.write(encode_command(command))
                stream.flush()
                os.fsync(stream.fileno())
            recovered, new_digest, new_count = replay(temp, cutoff)
            if recovered != entries or new_count != len(entries):
                raise RewriteError("Rewritten state verification failed")
            if fingerprint(path) != original:
                raise RewriteError("Source changed before backup")
            backup_source(path, backup, old_digest)
            if fingerprint(path) != original:
                raise RewriteError("Source changed before replacement")
            os.replace(temp, path)
            try:
                sync_directory(path.parent)
            except OSError as error:
                raise InstalledDurabilityError(
                    "New AOF installed but directory sync failed; stop and inspect AOF plus .rewrite-backup"
                ) from error
            return {**report, "status": "rewritten", "new_sha256": new_digest,
                    "backup": backup.name, "backup_sha256": old_digest}
        finally:
            temp.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="AOF of a stopped local server")
    parser.add_argument("--apply", action="store_true", help="Verify, retain backup, then replace the AOF")
    args = parser.parse_args(argv)
    try:
        print(json.dumps(rewrite(args.path, apply=args.apply), indent=2))
        return 0
    except InstalledDurabilityError as error:
        print(json.dumps({"status": "installed_durability_uncertain", "error": str(error)}))
        return 2
    except (OSError, RuntimeError, ValueError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
