
import asyncio

from store import Store


def run(coroutine):
    return asyncio.run(coroutine)


class FakeClock:
    def __init__(self, initial_time=1000.0):
        self.current_time = initial_time

    def __call__(self):
        return self.current_time

    def advance(self, seconds):
        self.current_time += seconds


def test_set_and_get():
    store = Store()

    run(store.set(b"name", b"Alice"))

    assert store.get(b"name") == b"Alice"


def test_get_missing_key():
    store = Store()

    assert store.get(b"name") is None


def test_get_empty_value():
    store = Store()

    run(store.set(b"empty", b""))

    assert store.get(b"empty") == b""


def test_set_overwrite():
    store = Store()

    run(store.set(b"name", b"Alice"))
    run(store.set(b"name", b"Bob"))

    assert store.get(b"name") == b"Bob"


def test_delete_existing_key():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(store.delete(b"name"))

    assert result == 1
    assert store.get(b"name") is None


def test_delete_missing_key():
    store = Store()

    result = run(store.delete(b"name"))

    assert result == 0


def test_expire_existing_key():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))

    result = run(store.expire(b"name", 10))

    assert result == 1
    assert store.ttl(b"name") == 10


def test_expiration_after_time_passes():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 10))

    clock.advance(10)

    assert store.get(b"name") is None
    assert store.ttl(b"name") == -2


def test_ttl_decreases():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 10))

    clock.advance(3)

    assert store.ttl(b"name") == 7


def test_ttl_without_expiration():
    store = Store()
    run(store.set(b"name", b"Alice"))

    assert store.ttl(b"name") == -1


def test_ttl_missing_key():
    store = Store()

    assert store.ttl(b"name") == -2


def test_expire_missing_key():
    store = Store()

    assert run(store.expire(b"name", 10)) == 0


def test_expire_zero_deletes_key():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(store.expire(b"name", 0))

    assert result == 1
    assert store.get(b"name") is None


def test_negative_expire_deletes_key():
    store = Store()
    run(store.set(b"name", b"Alice"))

    result = run(store.expire(b"name", -5))

    assert result == 1
    assert store.get(b"name") is None


def test_set_clears_existing_ttl():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 10))

    clock.advance(3)

    run(store.set(b"name", b"Bob"))

    assert store.get(b"name") == b"Bob"
    assert store.ttl(b"name") == -1


def test_delete_clears_expiration():
    store = Store()
    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 10))

    assert run(store.delete(b"name")) == 1
    assert store.ttl(b"name") == -2

def test_ttl_zero_does_not_always_mean_expired():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 1))

    clock.advance(0.5)

    assert store.ttl(b"name") == 0
    assert store.get(b"name") == b"Alice"

def test_purge_expired_removes_expired_keys():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.set(b"age", b"20"))
    run(store.set(b"city", b"Beijing"))

    run(store.expire(b"name", 5))
    run(store.expire(b"age", 10))

    clock.advance(5)

    removed_count = run(store.purge_expired())

    assert removed_count == 1
    assert store.get(b"name") is None
    assert store.get(b"age") == b"20"
    assert store.get(b"city") == b"Beijing"


def test_purge_expired_removes_multiple_keys():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"a", b"1"))
    run(store.set(b"b", b"2"))
    run(store.set(b"c", b"3"))

    run(store.expire(b"a", 5))
    run(store.expire(b"b", 5))
    run(store.expire(b"c", 10))

    clock.advance(5)

    removed_count = run(store.purge_expired())

    assert removed_count == 2
    assert store.get(b"a") is None
    assert store.get(b"b") is None
    assert store.get(b"c") == b"3"


def test_purge_expired_keeps_keys_without_ttl():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.set(b"city", b"Beijing"))

    run(store.expire(b"name", 5))

    clock.advance(10)

    run(store.purge_expired())

    assert store.get(b"name") is None
    assert store.get(b"city") == b"Beijing"


def test_purge_expired_returns_zero_when_nothing_expired():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 10))

    clock.advance(5)

    assert run(store.purge_expired()) == 0


def test_purge_expired_is_idempotent():
    clock = FakeClock()
    store = Store(clock=clock)

    run(store.set(b"name", b"Alice"))
    run(store.expire(b"name", 5))

    clock.advance(5)

    assert run(store.purge_expired()) == 1
    assert run(store.purge_expired()) == 0 # test idempotence
