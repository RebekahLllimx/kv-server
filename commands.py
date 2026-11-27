from response import (
    BulkString,
    Error,
    Integer,
    NullBulkString,
    SimpleString,
)
from store import Store


async def execute_command(command: list[bytes], store: Store):

    if not command:
        return Error(b"ERR empty command")

    command_name = command[0].upper()

    # PING
    if command_name == b"PING":
        if len(command) != 1:
            return Error(b"ERR wrong number of arguments for PING")

        return SimpleString(b"PONG")

    # SET key value [EX seconds] [NX|XX]
    elif command_name == b"SET":
        if len(command) < 3:
            return Error(b"ERR wrong number of arguments for SET")

        key = command[1]
        value = command[2]

        try:
            seconds, condition = parse_options(command[3:], conditional=True)
        except ValueError:
            return Error(b"ERR invalid SET options")
        changed = await store.set(key, value, seconds=seconds, condition=condition)
        if not changed:
            return NullBulkString()

        return SimpleString(b"OK")

    elif command_name == b"CAS":
        if len(command) < 4:
            return Error(b"ERR wrong number of arguments for CAS")
        try:
            seconds, _ = parse_options(command[4:])
        except ValueError:
            return Error(b"ERR invalid CAS options")
        return Integer(
            await store.compare_set(command[1], command[2], command[3], seconds=seconds)
        )

    elif command_name == b"CAD":
        if len(command) != 3:
            return Error(b"ERR wrong number of arguments for CAD")
        return Integer(await store.compare_delete(command[1], command[2]))

    # GET key
    elif command_name == b"GET":
        if len(command) != 2:
            return Error(b"ERR wrong number of arguments for GET")

        key = command[1]
        value = await store.get_for_command(key)

        if value is None:
            return NullBulkString()

        return BulkString(value)

    # DEL key
    elif command_name == b"DEL":
        if len(command) != 2:
            return Error(b"ERR wrong number of arguments for DEL")

        key = command[1]
        deleted_count = await store.delete(key)

        return Integer(deleted_count)

    # EXPIRE key seconds
    elif command_name == b"EXPIRE":
        if len(command) != 3:
            return Error(b"ERR wrong number of arguments for EXPIRE")

        key = command[1]
        seconds_bytes = command[2]

        if seconds_bytes.startswith(b"-"):
            digits = seconds_bytes[1:]
        else:
            digits = seconds_bytes

        if not digits or not digits.isdigit():
            return Error(b"ERR invalid expire time")

        if seconds_bytes.startswith(b"-"):
            seconds = -1  # Every nonpositive value has the same DEL effect.
        else:
            significant = digits.lstrip(b"0")
            if len(significant) > 20:
                return Error(b"ERR invalid expire time")
            seconds = int(significant or b"0")

        try:
            result = await store.expire(key, seconds)
        except ValueError:
            return Error(b"ERR invalid expire time")

        return Integer(result)

    # TTL key
    elif command_name == b"TTL":
        if len(command) != 2:
            return Error(b"ERR wrong number of arguments for TTL")

        key = command[1]
        remaining = await store.ttl_for_command(key)

        return Integer(remaining)

    # Unknown command
    else:
        return Error(b"ERR unknown command")


def parse_options(options, conditional=False):
    seconds, condition, index = None, None, 0
    while index < len(options):
        name = options[index].upper()
        if name == b"EX" and seconds is None and index + 1 < len(options):
            raw = options[index + 1]
            if not raw.isdigit() or len(raw) > 10:
                raise ValueError("Invalid expiry")
            seconds = int(raw)
            if not 1 <= seconds <= 2147483647:
                raise ValueError("Invalid expiry")
            index += 2
        elif conditional and name in (b"NX", b"XX") and condition is None:
            condition = name.decode("ascii")
            index += 1
        else:
            raise ValueError("Unknown or repeated option")
    return seconds, condition
