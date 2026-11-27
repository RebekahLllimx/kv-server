import asyncio
import pytest

from aof import AOF, AOFError, encode_command
from commands import execute_command
from response import SimpleString, NullBulkString, Integer, Error
from store import Store


class Clock:
    def __init__(self, now=1000): self.now = now
    def __call__(self): return self.now


def test_set_conditions_expiry_and_empty_value():
    async def scenario():
        clock = Clock()
        s = Store(clock=clock)
        assert await execute_command([b"SET",b"k",b"",b"EX",b"5",b"NX"],s) == SimpleString(b"OK")
        assert await execute_command([b"SET",b"k",b"other",b"NX"],s) == NullBulkString()
        assert s.get(b"k") == b"" and s.ttl(b"k") == 5
        assert await execute_command([b"SET",b"missing",b"v",b"XX"],s) == NullBulkString()
        clock.now += 5
        assert await execute_command([b"SET",b"k",b"new",b"NX",b"EX",b"7"],s) == SimpleString(b"OK")
        assert await s.set(b"k",b"persistent",condition="XX")
        assert s.ttl(b"k") == -1
    asyncio.run(scenario())


@pytest.mark.parametrize("tail", [[b"EX"],[b"EX",b"0"],[b"EX",b"-1"],[b"EX",b"2147483648"],
    [b"NX",b"XX"],[b"NX",b"NX"],[b"EX",b"2",b"EX",b"3"],[b"PX",b"5"],[b"EX",b"+5"]])
def test_bad_options_do_not_write(tail):
    s=Store()
    assert isinstance(asyncio.run(execute_command([b"SET",b"k",b"v"]+tail,s)),Error)
    assert s.get(b"k") is None


def test_compare_commands_and_ttl_policy():
    async def scenario():
        clock=Clock(); s=Store(clock=clock)
        await s.set(b"k",b"token-a",seconds=10)
        assert await execute_command([b"CAS",b"k",b"wrong",b"new"],s) == Integer(0)
        assert s.ttl(b"k") == 10
        assert await execute_command([b"CAS",b"k",b"token-a",b"token-b",b"EX",b"3"],s) == Integer(1)
        assert s.ttl(b"k") == 3
        assert await execute_command([b"CAD",b"k",b"token-a"],s) == Integer(0)
        assert await execute_command([b"CAD",b"k",b"token-b"],s) == Integer(1)
        assert await s.compare_set(b"k",b"",b"x") == 0
        await s.set(b"k",b"",seconds=1)
        assert await s.compare_set(b"k",b"",b"x") == 1
        assert s.ttl(b"k") == -1
        await s.set(b"k",b"x",seconds=1); clock.now+=1
        assert await s.compare_delete(b"k",b"x") == 0
        assert await s.compare_set(b"k",b"x",b"y") == 0
        for cmd in ([b"CAS",b"k",b"x"],[b"CAS",b"k",b"x",b"y",b"NX"],[b"CAD",b"k"],[b"CAD",b"k",b"x",b"EX",b"2"]):
            assert isinstance(await execute_command(cmd,s),Error)
    asyncio.run(scenario())


def test_concurrent_create_and_compare_have_one_winner():
    async def scenario():
        s=Store()
        created=await asyncio.gather(*(s.set(b"k",str(i).encode(),condition="NX",seconds=30) for i in range(30)))
        assert sum(created)==1
        expected=s.get(b"k")
        changed=await asyncio.gather(*(s.compare_set(b"k",expected,b"winner-"+str(i).encode()) for i in range(30)))
        assert sum(changed)==1
    asyncio.run(scenario())


def test_failed_aof_leaves_value_and_expiry_unchanged():
    class Failed:
        async def append_async(self, record): raise OSError("injected")
    async def scenario():
        s=Store(clock=Clock()); await s.set(b"k",b"old",seconds=20); s.attach_aof(Failed())
        for action in (lambda:s.set(b"k",b"new",seconds=2), lambda:s.compare_set(b"k",b"old",b"new"), lambda:s.compare_delete(b"k",b"old")):
            with pytest.raises(OSError): await action()
            assert s.get(b"k")==b"old" and s.ttl(b"k")==20
        assert not await s.set(b"k",b"ignored",condition="NX")
    asyncio.run(scenario())


def test_aof_atomic_expiry_recovery_and_downtime(tmp_path):
    async def scenario():
        wall=Clock(); mono=Clock(100); path=tmp_path/'new.aof'
        s=Store(clock=mono,wall_clock=wall); log=AOF(path,sync_policy="always"); log.load(s); s.attach_aof(log)
        await s.set(b"k",b"old",seconds=10)
        await s.compare_set(b"k",b"old",b"new",seconds=8)
        before=path.stat().st_size
        assert not await s.set(b"k",b"ignored",condition="NX")
        assert not await s.compare_delete(b"k",b"old")
        assert path.stat().st_size==before
        await log.close_async()
        wall.now+=3
        recovered=Store(clock=Clock(500),wall_clock=wall); second=AOF(path); second.load(recovered)
        assert recovered.get(b"k")==b"new" and recovered.ttl(b"k")==5
        second.close(); wall.now+=6
        expired=Store(clock=Clock(900),wall_clock=wall); third=AOF(path); third.load(expired)
        assert expired.get(b"k") is None; third.close()
    asyncio.run(scenario())


def test_torn_atomic_record_does_not_restore_persistent_value(tmp_path):
    path=tmp_path/'torn.aof'
    original=encode_command([b"SET",b"k",b"old"])
    path.write_bytes(original+encode_command([b"SETAT",b"k",b"new",b"9999999999999"])[:-2])
    s=Store(); log=AOF(path); log.load(s)
    assert s.get(b"k")==b"old" and path.read_bytes()==original
    log.close()
    path.write_bytes(encode_command([b"SETAT",b"k",b"new",b"bad"]))
    with pytest.raises(AOFError): AOF(path).load(Store())


def test_real_tcp_clients_compete_and_release_by_token():
    from server import handle_client

    async def scenario():
        store=Store()
        async with await asyncio.start_server(lambda r,w: handle_client(r,w,store),"127.0.0.1",0) as server:
            port=server.sockets[0].getsockname()[1]
            async def send(command):
                reader,writer=await asyncio.open_connection("127.0.0.1",port)
                try:
                    writer.write(encode_command(command)); await writer.drain()
                    return await asyncio.wait_for(reader.readline(),2)
                finally:
                    writer.close(); await writer.wait_closed()
            responses=await asyncio.gather(*(send([b"SET",b"job",str(i).encode(),b"NX",b"EX",b"30"]) for i in range(10)))
            assert responses.count(b"+OK\r\n")==1
            assert responses.count(b"$-1\r\n")==9
            token=store.get(b"job")
            assert await send([b"CAS",b"job",token,b"next",b"EX",b"30"])==b":1\r\n"
            assert await send([b"CAD",b"job",token])==b":0\r\n"
            assert await send([b"CAD",b"job",b"next"])==b":1\r\n"
    asyncio.run(scenario())
