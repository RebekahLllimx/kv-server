import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from aof import AOF, AOFError, encode_command
from aof_lock import AOFBusyError, AOFFileLock
from store import Store
import rewrite_aof as rewrite


def log(tmp_path, commands):
    path = tmp_path / "history.aof"
    path.write_bytes(b"".join(encode_command(c) for c in commands))
    return path


def restored(path, now=1000.0):
    store = Store(clock=lambda: 0.0, wall_clock=lambda: now)
    aof = AOF(path); aof.load(store); aof.close()
    return store


def test_preview_and_apply_preserve_binary_values_absolute_expiry_and_restart(tmp_path):
    path = log(tmp_path, [
        [b"SET", b"a", b"old"], [b"SET", b"a", b"new\x00\r\n"],
        [b"SET", b"gone", b"x"], [b"DEL", b"gone"],
        [b"SETAT", b"expired", b"x", b"999000"],
        [b"SET", b"ttl", b"x"], [b"EXPIREAT", b"ttl", b"1005000"],
        [b"SETAT", b"reset", b"x", b"1005000"], [b"SET", b"reset", b"permanent"],
    ])
    original = path.read_bytes()
    preview = rewrite.rewrite(path, now_ms=1000000)
    assert path.read_bytes() == original
    assert preview['live_keys_at_cutoff'] == 3
    assert not path.with_name(path.name+'.rewrite-backup').exists()
    result = rewrite.rewrite(path, apply=True, now_ms=1000000)
    assert result['new_bytes'] < result['old_bytes']
    assert (tmp_path / result['backup']).read_bytes() == original
    assert result['backup_sha256'] == hashlib.sha256(original).hexdigest()
    s = restored(path, 1002.0)
    assert s.get(b'a') == b'new\x00\r\n'
    assert s.get(b'gone') is None and s.get(b'expired') is None
    assert s.ttl(b'ttl') == 3 and s.ttl(b'reset') == -1
    assert restored(path, 1006.0).get(b'ttl') is None
    # A subsequent real append lands in the replacement file, not the old inode.
    aof = AOF(path); s = Store(); aof.load(s); s.attach_aof(aof)
    asyncio.run(s.set(b'after', b'rewrite')); aof.close()
    assert restored(path).get(b'after') == b'rewrite'
    assert (tmp_path / result['backup']).read_bytes() == original


def test_empty_and_all_expired_logs(tmp_path):
    for name, commands in [('empty', []), ('expired', [[b'SETAT', b'x', b'y', b'1000']])]:
        folder=tmp_path/name;folder.mkdir()
        path=log(folder, commands)
        result=rewrite.rewrite(path, apply=True, now_ms=1000)
        assert result['live_keys_at_cutoff']==0 and path.read_bytes()==b''
        assert restored(path).get(b'x') is None


@pytest.mark.parametrize('suffix', [b'*3\r\n$3\r\nSET\r\n', b'not-resp\r\n', encode_command([b'GET',b'x']), encode_command([b'SETAT',b'x',b'y',b'-1'])])
def test_corruption_or_partial_tail_refused_without_repair(tmp_path,suffix):
    path=log(tmp_path,[[b'SET',b'x',b'y']]);path.write_bytes(path.read_bytes()+suffix)
    original=path.read_bytes()
    with pytest.raises(rewrite.RewriteError):rewrite.rewrite(path,apply=True)
    assert path.read_bytes()==original
    assert not list(tmp_path.glob('*.rewrite-backup'))


def test_active_writer_excludes_rewrite_and_second_writer_across_process(tmp_path):
    path=log(tmp_path,[[b'SET',b'x',b'y']]); original=path.read_bytes()
    aof=AOF(path);aof.load(Store())
    try:
        with pytest.raises(AOFBusyError):rewrite.rewrite(path,apply=True)
        other=AOF(path)
        with pytest.raises(AOFBusyError):other.load(Store())
        result=subprocess.run([sys.executable,str(Path(rewrite.__file__)),str(path),'--apply'],capture_output=True,text=True)
        assert result.returncode==1 and 'in use' in json.loads(result.stdout)['error']
    finally:aof.close()
    assert path.read_bytes()==original
    rewrite.rewrite(path,apply=True)


def test_maintenance_lock_excludes_server_and_survives_replacement(tmp_path):
    path=log(tmp_path,[[b'SET',b'x',b'y']])
    with AOFFileLock(path):
        with pytest.raises(AOFBusyError):AOF(path).load(Store())
        replacement=tmp_path/'replacement';replacement.write_bytes(path.read_bytes());replacement.replace(path)
        with pytest.raises(AOFBusyError):AOF(path).load(Store())
    aof=AOF(path);aof.load(Store());aof.close()


def test_load_failure_releases_path_lock(tmp_path):
    path=log(tmp_path,[[b'GET',b'x']])
    with pytest.raises(AOFError):AOF(path).load(Store())
    path.write_bytes(b'')
    aof=AOF(path);aof.load(Store());aof.close()


def test_backup_never_overwritten_and_second_apply_requires_decision(tmp_path):
    path=log(tmp_path,[[b'SET',b'x',b'y']]);rewrite.rewrite(path,apply=True)
    backup=path.with_name(path.name+'.rewrite-backup');original=backup.read_bytes()
    with pytest.raises(rewrite.RewriteError,match='backup exists'):rewrite.rewrite(path,apply=True)
    assert backup.read_bytes()==original


@pytest.mark.parametrize('failure', ['file_sync','backup_copy','replace','verification','backup_directory_sync'])
def test_prepublication_failures_leave_original_usable(tmp_path,monkeypatch,failure):
    path=log(tmp_path,[[b'SET',b'x',b'1'],[b'SET',b'x',b'2']]);original=path.read_bytes()
    def fail(*a,**k):raise OSError('injected failure')
    if failure=='file_sync':monkeypatch.setattr(rewrite.os,'fsync',fail)
    elif failure=='backup_copy':monkeypatch.setattr(rewrite,'backup_source',fail)
    elif failure=='replace':monkeypatch.setattr(rewrite.os,'replace',fail)
    elif failure=='backup_directory_sync':monkeypatch.setattr(rewrite,'sync_directory',fail)
    else:
        replay=rewrite.replay
        def mismatch(p,now):
            data,digest,count=replay(p,now)
            if '.rewrite-tmp-' in p.name:data={}
            return data,digest,count
        monkeypatch.setattr(rewrite,'replay',mismatch)
    with pytest.raises((OSError,rewrite.RewriteError)):rewrite.rewrite(path,apply=True)
    assert path.read_bytes()==original
    assert restored(path).get(b'x')==b'2'
    assert not list(tmp_path.glob('*.rewrite-tmp-*'))


def test_post_rename_sync_failure_distinguishes_installed_state(tmp_path,monkeypatch):
    path=log(tmp_path,[[b'SET',b'x',b'1'],[b'SET',b'x',b'2']]);original=path.read_bytes()
    sync=rewrite.sync_directory;calls=[]
    def second_fails(p):
        calls.append(p)
        if len(calls)==2:raise OSError('directory sync failed')
        sync(p)
    monkeypatch.setattr(rewrite,'sync_directory',second_fails)
    with pytest.raises(rewrite.InstalledDurabilityError):rewrite.rewrite(path,apply=True)
    assert restored(path).get(b'x')==b'2'
    assert path.with_name(path.name+'.rewrite-backup').read_bytes()==original


def test_source_change_detected_before_replacement(tmp_path,monkeypatch):
    path=log(tmp_path,[[b'SET',b'x',b'1']]);replay=rewrite.replay
    def concurrent(p,now):
        result=replay(p,now)
        if p==path:p.write_bytes(p.read_bytes()+encode_command([b'SET',b'x',b'2']))
        return result
    monkeypatch.setattr(rewrite,'replay',concurrent)
    with pytest.raises(rewrite.RewriteError,match='changed'):rewrite.rewrite(path,apply=True)
    assert restored(path).get(b'x')==b'2'


def test_symbolic_and_hard_link_aliases_rejected(tmp_path):
    path=log(tmp_path,[]);sym=tmp_path/'alias';sym.symlink_to(path)
    with pytest.raises(rewrite.RewriteError):rewrite.rewrite(sym,apply=True)
    hard=tmp_path/'hard';hard.hardlink_to(path)
    with pytest.raises(rewrite.RewriteError):rewrite.rewrite(path,apply=True)


def test_cli_preview_and_apply(tmp_path):
    path=log(tmp_path,[[b'SET',b'x',b'1'],[b'SET',b'x',b'2']]);original=path.read_bytes()
    command=[sys.executable,str(Path(rewrite.__file__)),str(path)]
    preview=subprocess.run(command,capture_output=True,text=True,check=True)
    assert json.loads(preview.stdout)['status']=='preview' and path.read_bytes()==original
    applied=subprocess.run(command+['--apply'],capture_output=True,text=True,check=True)
    assert json.loads(applied.stdout)['status']=='rewritten'

@pytest.mark.parametrize('boundary', ['before_replace', 'after_replace'])
def test_process_exit_around_publication_recovers_and_releases_lock(tmp_path,boundary):
    path=log(tmp_path,[[b'SET',b'x',b'old'],[b'SET',b'x',b'new']]);original=path.read_bytes()
    code="""
import os,sys
from pathlib import Path
import rewrite_aof as r
real=r.os.replace
def interrupted(src,dst):
    if sys.argv[2]=='after_replace':real(src,dst)
    os._exit(73)
r.os.replace=interrupted
r.rewrite(Path(sys.argv[1]),apply=True)
"""
    result=subprocess.run([sys.executable,'-c',code,str(path),boundary],capture_output=True)
    assert result.returncode==73
    assert (path.read_bytes()==original)==(boundary=='before_replace')
    assert path.with_name(path.name+'.rewrite-backup').read_bytes()==original
    assert restored(path).get(b'x')==b'new'
    assert rewrite.rewrite(path)['status']=='preview'
