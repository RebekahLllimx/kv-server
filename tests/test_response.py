
from response import (
    SimpleString,
    BulkString,
    Error,
    NullBulkString,
    encode_response,
    Integer,
)


def test_encode_simple_string():
    assert encode_response(SimpleString(b"PONG")) == b"+PONG\r\n"


def test_encode_bulk_string():
    assert encode_response(BulkString(b"Alice")) == b"$5\r\nAlice\r\n"


def test_encode_empty_bulk_string():
    assert encode_response(BulkString(b"")) == b"$0\r\n\r\n"


def test_encode_null_bulk_string():
    assert encode_response(NullBulkString()) == b"$-1\r\n"


def test_encode_error():
    assert encode_response(Error(b"ERR unknown command")) == (
        b"-ERR unknown command\r\n"
    )


def test_encode_binary_bulk_string():
    value = b"a\r\nb"

    assert encode_response(BulkString(value)) == (
        b"$4\r\na\r\nb\r\n"
    )


def test_encode_integer_one():
    assert encode_response(Integer(1)) == b":1\r\n"


def test_encode_integer_zero():
    assert encode_response(Integer(0)) == b":0\r\n"

def test_encode_negative_integer():
    assert encode_response(Integer(-2)) == b":-2\r\n"


def test_encode_positive_integer():
    assert encode_response(Integer(10)) == b":10\r\n"