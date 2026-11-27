class NeedMoreData(Exception):
    'The current frame is incomplete; wait for more bytes.'


class ProtocolError(Exception):
    'The input violates the supported RESP request contract.'


NEED_MORE_DATA = object()

MAX_HEADER_LENGTH = 64

def _read_crlf_line(buffer: bytearray, pos: int) -> tuple[bytes, int]:
    'Read one CRLF-terminated header at pos, returning its bytes and the next offset. Raise NeedMoreData for a partial header and ProtocolError above the length limit.'

    end = buffer.find(b"\r\n", pos)

    if end == -1:
        if len(buffer) - pos > MAX_HEADER_LENGTH:
            raise ProtocolError("RESP header too long")

        raise NeedMoreData

    if end - pos > MAX_HEADER_LENGTH:
        raise ProtocolError("RESP header too long")

    line = bytes(buffer[pos:end])

    next_pos = end + 2

    return line, next_pos

MAX_BULK_LENGTH = 1024 * 1024  # 1 MiB


def _parse_bulk_string(
    buffer: bytearray,
    pos: int
) -> tuple[bytes, int]:
    
    header, body_start = _read_crlf_line(buffer, pos)

    
    if not header.startswith(b"$"):
        raise ProtocolError("Expected Bulk String")

    
    length_bytes = header[1:]

    if not length_bytes or not length_bytes.isdigit():
        raise ProtocolError("Invalid Bulk String length")

    length = int(length_bytes)

    if length > MAX_BULK_LENGTH:
        raise ProtocolError("Bulk String too large")

    
    body_end = body_start + length

    
    if len(buffer) < body_end + 2:
        raise NeedMoreData

    
    if buffer[body_end:body_end + 2] != b"\r\n":
        raise ProtocolError("Missing Bulk String terminator")

    
    body = bytes(buffer[body_start:body_end])
    next_pos = body_end + 2

    return body, next_pos

MAX_ARRAY_LENGTH = 64


def _parse_array(
    buffer: bytearray,
    pos: int
) -> tuple[list[bytes], int]:

    
    header, elements_start = _read_crlf_line(buffer, pos)

    
    if not header.startswith(b"*"):
        raise ProtocolError("Expected Array")

    
    count_bytes = header[1:]

    if not count_bytes or not count_bytes.isdigit():
        raise ProtocolError("Invalid Array length")

    count = int(count_bytes)

    if count < 1 or count > MAX_ARRAY_LENGTH:
        raise ProtocolError("Invalid Array length")

    
    elements = []
    current_pos = elements_start

    for _ in range(count):
        element, current_pos = _parse_bulk_string(
            buffer,
            current_pos
        )
        elements.append(element)

    return elements, current_pos


class RESPParser:
    def __init__(self):
        self.buffer = bytearray()

    def feed(self, data:bytes):
        self.buffer.extend(data)

    def next_command(self):
        try:
            command, next_pos = _parse_array(self.buffer, 0)
        except NeedMoreData:
            return NEED_MORE_DATA
        del self.buffer[:next_pos]
        return command