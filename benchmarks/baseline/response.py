
from dataclasses import dataclass


@dataclass(frozen=True)
class SimpleString:
    value: bytes


@dataclass(frozen=True)
class BulkString:
    value: bytes


@dataclass(frozen=True)
class Error:
    value: bytes


@dataclass(frozen=True)
class NullBulkString:
    pass


@dataclass(frozen=True)
class Integer:
    value: int


def encode_response(result) -> bytes:
    if isinstance(result, SimpleString):
        return b"+" + result.value + b"\r\n"

    if isinstance(result, BulkString):
        length = len(result.value)

        return (
            b"$"
            + str(length).encode("ascii")
            + b"\r\n"
            + result.value
            + b"\r\n"
        )

    if isinstance(result, Error):
        return b"-" + result.value + b"\r\n"

    if isinstance(result, NullBulkString):
        return b"$-1\r\n"

    if isinstance(result, Integer):
        return b":" + str(result.value).encode("ascii") + b"\r\n"

    raise TypeError("Unsupported response type")
