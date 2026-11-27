from commands import execute_command
from commit import CommitBusy, CommitClosed, CommitFailed
from resp import NEED_MORE_DATA, ProtocolError, RESPParser
from response import Error, encode_response


async def handle_client(reader, writer, store):
    parser = RESPParser()

    try:
        while True:
            data = await reader.read(4096)

            if data == b"":
                break

            parser.feed(data)

            while True:
                try:
                    command = parser.next_command()

                except ProtocolError:
                    result = Error(b"ERR protocol error")
                    writer.write(encode_response(result))
                    await writer.drain()
                    return

                if command is NEED_MORE_DATA:
                    break

                try:
                    result = await execute_command(command, store)
                except CommitBusy:
                    result = Error(
                        b"BUSY commit capacity unavailable; request not admitted"
                    )
                except CommitClosed:
                    result = Error(b"UNAVAILABLE request not executed")
                except CommitFailed:
                    result = Error(
                        b"UNAVAILABLE in-flight outcome unknown; reconnect after recovery"
                    )

                response_bytes = encode_response(result)

                writer.write(response_bytes)
                await writer.drain()
                if isinstance(result, Error) and result.value.startswith(
                    b"UNAVAILABLE"
                ):
                    return

    except ConnectionError:
        # An admitted commit has its own owner; a lost socket only detaches
        # this response path. It must not cancel or retry the mutation.
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass
