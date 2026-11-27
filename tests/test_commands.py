
import asyncio

from store import Store

from commands import execute_command

from response import (
    SimpleString,
    BulkString,
    NullBulkString,
    Error,
    Integer,
)


def run(coroutine):
    return asyncio.run(coroutine)


def test_ping():
    store = Store()

    result = run(execute_command([b"PING"], store))

    assert result == SimpleString(b"PONG")


def test_set():
    store = Store()

    result = run(execute_command([b"SET", b"name", b"Alice"], store))

    assert store.get(b"name") == b"Alice"
    assert result == SimpleString(b"OK")


def test_get():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(execute_command([b"GET", b"name"], store))

    assert result == BulkString(b"Alice")


def test_get_missing_key():
    store = Store()

    result = run(execute_command([b"GET", b"name"], store))

    assert result == NullBulkString()


def test_get_empty_value():
    store = Store()
    run(store.set(b"empty", b""))

    result = run(execute_command([b"GET", b"empty"], store))

    assert result == BulkString(b"")


def test_set_then_get():
    store = Store()

    run(execute_command([b"SET", b"name", b"Alice"], store))
    result = run(execute_command([b"GET", b"name"], store))

    assert result == BulkString(b"Alice")


def test_del_existing_key():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(execute_command([b"DEL", b"name"], store))

    assert result == Integer(1)
    assert store.get(b"name") is None


def test_del_missing_key():
    store = Store()

    result = run(execute_command([b"DEL", b"name"], store))

    assert result == Integer(0)


def test_wrong_arguments():
    store = Store()

    result = run(execute_command([b"SET", b"name"], store))

    assert isinstance(result, Error)


def test_unknown_command():
    store = Store()

    result = run(execute_command([b"HELLO"], store))

    assert isinstance(result, Error)



def test_expire_command():
    store = Store(clock=lambda: 1000.0)
    run(store.set(b"name", b"Alice"))

    result = run(execute_command([b"EXPIRE", b"name", b"10"], store))

    assert result == Integer(1)


def test_expire_missing_key():
    store = Store()

    result = run(execute_command([b"EXPIRE", b"name", b"10"], store))

    assert result == Integer(0)


def test_ttl_command():
    store = Store(clock=lambda: 1000.0)
    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 10))

    result = run(execute_command([b"TTL", b"name"], store))

    assert result == Integer(10)


def test_ttl_missing_key():
    store = Store()

    result = run(execute_command([b"TTL", b"name"], store))

    assert result == Integer(-2)


def test_ttl_without_expiration():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(execute_command([b"TTL", b"name"], store))

    assert result == Integer(-1)


def test_expire_invalid_argument():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(execute_command([b"EXPIRE", b"name", b"ten"], store))

    assert isinstance(result, Error)
    assert store.get(b"name") == b"Alice"


def test_expire_zero():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(execute_command([b"EXPIRE", b"name", b"0"], store))

    assert result == Integer(1)
    assert store.get(b"name") is None


def test_wrong_ttl_arguments():
    store = Store()

    result = run(execute_command([b"TTL"], store))

    assert isinstance(result, Error)
