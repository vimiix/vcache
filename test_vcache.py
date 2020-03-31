# coding:utf-8

import pytest
from vcache import (
    Cache,
    Item,
    RedisIface,
    Option,
    OTHER_SUFFIX,
    STR_SUFFIX,
    BYTES_SUFFIX,
    NO_COMPRESSION,
    ZLIB_COMPRESSION,
    CacheMissError,
    RedisLocalCacheNoneError,
)


@pytest.fixture
def cache():
    return Cache()


class Foo:
    def __init__(self, val):
        self.val = val

    __eq__ = lambda x, o: x.val == o.val
    __ne__ = lambda x, o: x.val != o.val


def test_set_item(cache):
    s = "hello,中国"
    key = "k"
    item = Item(key, s)
    ok = cache.set(item)
    assert ok


def test_set_item_none(cache):
    key = "k"
    with pytest.raises(ValueError) as e:
        item = Item(key, None)
    err_msg = e.value.args[0]
    assert err_msg == "cache:value is None"


def test_set_none(cache):
    key = "k"
    item = Item(key, 'v')
    item.value = None
    with pytest.raises(ValueError) as e:
        cache.set(item)
    err_msg = e.value.args[0]
    assert err_msg == "cache:value is None"


def test_marshal_string(cache):
    v = "a"
    b = cache.marshal(v)
    assert b[:-1] == b"a"
    assert b[-1:] == STR_SUFFIX


def test_marshal_bytes(cache):
    v = b"a"
    b = cache.marshal(v)
    assert b[:-1] == v
    assert b[-1:] == BYTES_SUFFIX


def test_marshal_int(cache):
    v = 1
    b = cache.marshal(v)
    assert b[-1:] == OTHER_SUFFIX
    assert b[-2:-1] == NO_COMPRESSION


def test_marshal_tuple(cache):
    v = (1,)
    b = cache.marshal(v)
    assert b[-1:] == OTHER_SUFFIX
    assert b[-2:-1] == NO_COMPRESSION


def test_marshal_list(cache):
    v = [
        1,
    ]
    b = cache.marshal(v)
    assert b[-1:] == OTHER_SUFFIX
    assert b[-2:-1] == NO_COMPRESSION


def test_marshal_dict(cache):
    v = {"a": 1}
    b = cache.marshal(v)
    assert b[-1:] == OTHER_SUFFIX
    assert b[-2:-1] == NO_COMPRESSION


def test_marshal_obj(cache):
    v = Foo(1)
    b = cache.marshal(v)
    assert b[-1:] == OTHER_SUFFIX
    assert b[-2:-1] == NO_COMPRESSION


def test_marshal_long_value(cache):
    v = [1] * 1000
    b = cache.marshal(v)
    assert b[-1:] == OTHER_SUFFIX
    assert b[-2:-1] == ZLIB_COMPRESSION


def test_get_bytes(cache):
    v = b"\x00"
    item = Item("k", v)
    cache.set(item)
    r = cache.get("k")
    assert r == v


def test_get_string(cache):
    v = "sss"
    item = Item("k", v)
    cache.set(item)
    r = cache.get("k")
    assert r == v


def test_get_int(cache):
    v = 1
    item = Item("k", v)
    cache.set(item)
    r = cache.get("k")
    assert r == v


def test_get_tuple(cache):
    v = (1,)
    item = Item("k", v)
    cache.set(item)
    r = cache.get("k")
    assert r == v


def test_get_list(cache):
    v = [1]
    item = Item("k", v)
    cache.set(item)
    r = cache.get("k")
    assert r == v


def test_get_obj(cache):
    v = Foo(1)
    item = Item("k", v)
    cache.set(item)
    r = cache.get("k")
    assert r == v


def test_get_by_wrong_key(cache):
    with pytest.raises(CacheMissError) as e:
        cache.get("k")
    err_msg = e.value.args[0]
    assert err_msg == "cache: key is missing"


def test_get_by_skipping_local_cache(cache):
    item = Item("k", "v")
    cache.set(item)
    with pytest.raises(CacheMissError) as e:
        cache.get("foo", skip_local_cache=True)
    err_msg = e.value.args[0]
    assert err_msg == "cache: key is missing"


def test_redis_interface_error(cache):
    bad_redis = RedisIface()
    opt = Option(redis=bad_redis, local_cache=None)
    cache.opt = opt
    item = Item("k", "v")
    with pytest.raises(NotImplementedError) as e:
        cache.set(item)


def test_redis_local_cache_none_error(cache):
    cache.opt.local_cache = None
    item = Item("k", "v")
    with pytest.raises(RedisLocalCacheNoneError) as e:
        cache.set(item)
    err_msg = e.value.args[0]
    assert err_msg == "cache: both Redis and LocalCache are None"


def test_cache_once(cache):
    item1 = Item("k", "v1")
    item2 = Item("k", "v2")
    cache.set(item1)
    cache.once(item2)
    assert item2.value == "v1"

