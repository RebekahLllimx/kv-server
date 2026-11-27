from resp import RESPParser, NEED_MORE_DATA, ProtocolError
from commands import execute_command
from response import Error, encode_response
from store import Store


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

                result = await execute_command(command, store)

                response_bytes = encode_response(result)

                writer.write(response_bytes)
                await writer.drain()

    finally:
        writer.close()
        await writer.wait_closed()
