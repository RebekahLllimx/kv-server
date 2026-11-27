import asyncio
import pytest

from main import aof_sync_loop


def test_aof_sync_loop_calls_sync():
    async def scenario():
        class FakeAOF:
            def __init__(self):
                self.calls = 0
                self.called = asyncio.Event()

            async def sync_async(self):
                self.calls += 1
                self.called.set()

        aof = FakeAOF()

        task = asyncio.create_task(
            aof_sync_loop(aof, interval=0.01)
        )

        try:
            await asyncio.wait_for(
                aof.called.wait(),
                timeout=1.0,
            )
            assert aof.calls == 1

        finally:
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(scenario())


def test_aof_sync_loop_propagates_error():
    async def scenario():
        class BrokenAOF:
            async def sync_async(self):
                raise OSError("simulated disk failure")

        with pytest.raises(OSError):
            await aof_sync_loop(
                BrokenAOF(),
                interval=0.01,
            )

    asyncio.run(scenario())
