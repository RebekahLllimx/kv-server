import pytest

from resp import (
    _read_crlf_line,
    NeedMoreData,
    ProtocolError
)


def test_read_first_line():
    buffer = bytearray(
        b"*3\r\n$3\r\nSET\r\n"
    )

    line, pos = _read_crlf_line(buffer, 0)

    assert line == b"*3"
    assert pos == 4


def test_read_second_line():
    buffer = bytearray(
        b"*3\r\n$3\r\nSET\r\n"
    )

    _, pos = _read_crlf_line(buffer, 0)

    line, pos = _read_crlf_line(buffer, pos)

    assert line == b"$3"
    assert pos == 8


def test_incomplete_line():
    buffer = bytearray(b"$5\r\nAli")

    with pytest.raises(NeedMoreData):
        _read_crlf_line(buffer, 4)


def test_header_too_long():
    buffer = bytearray(b"A" * 65)

    with pytest.raises(ProtocolError):
        _read_crlf_line(buffer, 0)

from resp import _parse_bulk_string


def test_parse_bulk_string():
    buffer = bytearray(b"$5\r\nA\r\nBC\r\n")

    body, next_pos = _parse_bulk_string(buffer, 0)

    assert body == b"A\r\nBC"
    assert next_pos == 11


def test_parse_empty_bulk_string():
    buffer = bytearray(b"$0\r\n\r\n")

    body, next_pos = _parse_bulk_string(buffer, 0)

    assert body == b""
    assert next_pos == 6


def test_parse_incomplete_bulk_string():
    buffer = bytearray(b"$5\r\nABC")

    with pytest.raises(NeedMoreData):
        _parse_bulk_string(buffer, 0)


def test_parse_invalid_terminator():
    buffer = bytearray(b"$3\r\nABCXX")

    with pytest.raises(ProtocolError):
        _parse_bulk_string(buffer, 0)


def test_parse_invalid_bulk_string_header():
    buffer = bytearray(b"*3\r\n")

    with pytest.raises(ProtocolError):
        _parse_bulk_string(buffer, 0)


def test_parse_oversized_bulk_string():
    buffer = bytearray(b"$1048577\r\n")

    with pytest.raises(ProtocolError):
        _parse_bulk_string(buffer, 0)

from resp import _parse_array
def test_parse_array():
    buffer = bytearray(
        b"*3\r\n"
        b"$3\r\nSET\r\n"
        b"$4\r\nname\r\n"
        b"$5\r\nAlice\r\n"
    )

    result, next_pos = _parse_array(buffer, 0)

    assert result == [
        b"SET",
        b"name",
        b"Alice"
    ]

    assert next_pos == len(buffer)


def test_parse_array_with_following_data():
    buffer = bytearray(
        b"*1\r\n"
        b"$4\r\nPING\r\n"
        b"*1\r\n"
        b"$4\r\nPONG\r\n"
    )

    result, next_pos = _parse_array(buffer, 0)

    assert result == [b"PING"]
    assert next_pos == 14

    
    result2, next_pos2 = _parse_array(buffer, next_pos)

    assert result2 == [b"PONG"]
    assert next_pos2 == len(buffer)


def test_parse_incomplete_array():
    buffer = bytearray(
        b"*2\r\n"
        b"$3\r\nSET\r\n"
        b"$4\r\nna"
    )

    with pytest.raises(NeedMoreData):
        _parse_array(buffer, 0)


def test_parse_invalid_array_header():
    buffer = bytearray(b"$3\r\nSET\r\n")

    with pytest.raises(ProtocolError):
        _parse_array(buffer, 0)


def test_parse_empty_array():
    buffer = bytearray(b"*0\r\n")

    with pytest.raises(ProtocolError):
        _parse_array(buffer, 0)


def test_parse_oversized_array():
    buffer = bytearray(b"*65\r\n")

    with pytest.raises(ProtocolError):
        _parse_array(buffer, 0)


from resp import RESPParser, NEED_MORE_DATA


def test_parser_complete_ping_command():
    parser = RESPParser()
    parser.feed(b"*1\r\n$4\r\nPING\r\n")

    command = parser.next_command()

    assert command == [b"PING"]


def test_parser_incomplete_command():
    parser = RESPParser()
    parser.feed(b"*2\r\n$3\r\nSET\r\n$4\r\nna")

    assert parser.next_command() is NEED_MORE_DATA


def test_parser_complete_after_more_data():
    parser = RESPParser()
    parser.feed(b"*3\r\n$3\r\nSET\r\n$4\r\nna")

    assert parser.next_command() is NEED_MORE_DATA

    parser.feed(b"me\r\n$5\r\nAlice\r\n")

    assert parser.next_command() == [b"SET", b"name", b"Alice"]


def test_parser_two_complete_commands():
    parser = RESPParser()
    parser.feed(
        b"*1\r\n$4\r\nPING\r\n"
        b"*1\r\n$4\r\nPONG\r\n"
    )

    assert parser.next_command() == [b"PING"]
    assert parser.next_command() == [b"PONG"]


def test_parser_complete_then_incomplete():
    parser = RESPParser()
    parser.feed(
        b"*1\r\n$4\r\nPING\r\n"
        b"*2\r\n$3\r\nSET\r\n$4\r\nna"
    )

    assert parser.next_command() == [b"PING"]
    assert parser.next_command() is NEED_MORE_DATA