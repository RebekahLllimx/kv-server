import asyncio

from server import handle_client
from store import Store
from aof import AOF, AOFError


AOF_SYNC_POLICY = "everysec"
AOF_PATH = "data/append.aof"


async def expiration_loop(
    store: Store,
    interval_seconds: float = 1.0
):
    while True:
        await asyncio.sleep(interval_seconds)

        removed_count = await store.purge_expired()

        if removed_count > 0:
            print(f"Active expiration removed {removed_count} keys")


async def main():
    
    aof_failure_event = asyncio.Event()
    aof_failure_reason = {"error": None}

    # AOF callbacks run on the event loop, so Event.set is safe here.
    def notify_aof_failure(exc):
        if aof_failure_reason["error"] is None:
            aof_failure_reason["error"] = exc

        aof_failure_event.set()

    
    aof = AOF(
        AOF_PATH,
        sync_policy=AOF_SYNC_POLICY,
        on_failure=notify_aof_failure,
    )

    
    store = Store()

    # Replay before attaching persistence to avoid logging replay effects.
    aof.load(store)
    store.attach_aof(aof)

    
    client_tasks = set()

    try:
        async def tracked_client_handler(reader, writer):
            task = asyncio.current_task()
            client_tasks.add(task)

            try:
                await handle_client(reader, writer, store)
            finally:
                client_tasks.discard(task)

        server = await asyncio.start_server(
            tracked_client_handler,
            "127.0.0.1",
            6379
        )

        print("Server started at 127.0.0.1:6379")

        async with server:
            await run_service(
                server,
                store,
                aof,
                aof_failure_event,
                aof_failure_reason,
                client_tasks,
            )

    finally:
        # Respect the selected durability policy during shutdown.
        if not aof.failed and aof.sync_policy != "no":
            try:
                await aof.sync_async()
            except AOFError:
                pass

        await aof.close_async()


async def aof_sync_loop(aof: AOF, interval: float = 1.0):
    while True:
        await asyncio.sleep(interval)
        await aof.sync_async()


async def run_service(
    server,
    store,
    aof,
    aof_failure_event,
    aof_failure_reason,
    client_tasks=None,
):
    if client_tasks is None:
        client_tasks = set()

    service_tasks = [
        asyncio.create_task(
            server.serve_forever(),
            name="tcp-server",
        ),
        asyncio.create_task(
            expiration_loop(store),
            name="expiration-loop",
        ),
    ]

    # Only everysec schedules periodic synchronization.
    if aof.sync_policy == "everysec":
        service_tasks.append(
            asyncio.create_task(
                aof_sync_loop(aof),
                name="aof-sync-loop",
            )
        )

    failure_task = asyncio.create_task(
        aof_failure_event.wait(),
        name="aof-failure-watch",
    )

    all_tasks = service_tasks + [failure_task]

    try:
        done, _ = await asyncio.wait(
            all_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        
        if failure_task in done:
            cause = aof_failure_reason["error"]
            raise AOFError(
                "AOF failed; stopping service."
            ) from cause

        
        for task in done:
            task.result()

        raise RuntimeError(
            "A service task stopped unexpectedly."
        )

    finally:
        # Close the listener before handling existing client tasks.
        server.close()
        await server.wait_closed()

        for task in all_tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(
            *all_tasks,
            return_exceptions=True,
        )

        remaining_clients = [
            task for task in client_tasks
            if task is not asyncio.current_task() and not task.done()
        ]

        for task in remaining_clients:
            task.cancel()

        if remaining_clients:
            await asyncio.gather(
                *remaining_clients,
                return_exceptions=True,
            )


if __name__ == "__main__":
    asyncio.run(main())
