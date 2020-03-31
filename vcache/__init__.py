# coding: utf-8

import pickle
import zlib
import time
from datetime import datetime
from threading import RLock
from cacheout import Cache as LocalCache

LOCAL_CACHE_MAX_SIZE = 256
COMPRESSION_THRESHOLD = 64
ONE_MINUTE = 60
ONE_HOUR = ONE_MINUTE * 60
NO_COMPRESSION = b"\x00"
ZLIB_COMPRESSION = b"\x01"
BYTES_SUFFIX = b"\x00"
STR_SUFFIX = b"\x01"
OTHER_SUFFIX = b"\x02"


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


class CacheMissError(Exception):
    err = "cache: key is missing"

    def __init__(self):
        super(CacheMissError, self).__init__(self.err)


class RedisLocalCacheNoneError(Exception):
    err = "cache: both Redis and LocalCache are None"

    def __init__(self):
        super(RedisLocalCacheNoneError, self).__init__(self.err)


class RedisIfaceError(Exception):
    pass


class UnkownCompressionError(Exception):
    pass


class RedisIface:
    def set(self, key, value, expiration):
        raise NotImplementedError(
            "cache:not implement 'set' method in RedisIface sub class"
        )

    def setxx(self, key, value, expiration):
        raise NotImplementedError(
            "cache:not implement 'setxx' method in RedisIface sub class"
        )

    def setnx(self, key, value, expiration):
        raise NotImplementedError(
            "cache:not implement 'setnx' method in RedisIface sub class"
        )

    def get(self, key):
        raise NotImplementedError(
            "cache:not implement 'get' method in RedisIface sub class"
        )

    def delete(self, *key):
        raise NotImplementedError(
            "cache:not implement 'delete' method in RedisIface sub class"
        )


class Item:
    def __init__(
        self,
        key,
        value,
        ttl=ONE_HOUR,
        do_func=None,
        if_exists=False,
        if_not_exists=False,
        skip_local_cache=False,
    ):
        self.key = key
        if value is None:
            raise ValueError("cache:value is None")
        self._value = value

        # ttl is the cache expiration time.
        # Default ttl is 1 hour.
        self._ttl = ttl

        # returns value to be cached.
        self.do = do_func

        # if_exists only sets the key if it already exist.
        self.if_exists = if_exists

        # if_not_exists only sets the key if it does not already exist.
        self.if_not_exists = if_not_exists

        # skip_local_cache skips local cache as if it is not set.
        self.skip_local_cache = skip_local_cache

    def get_value(self):
        ret = None
        if self.do:
            ret = self.do(self)
        elif self._value:
            ret = self._value
        return ret

    def set_value(self, v):
        self._value = v

    value = property(get_value, set_value)

    def get_ttl(self):
        if self._ttl < 0:
            return 0
        elif self._ttl < 1:
            return ONE_HOUR
        return self._ttl

    def set_ttl(self, ttl):
        self._ttl = ttl

    ttl = property(get_ttl, set_ttl)


class Option:
    def __init__(
        self, redis=None, local_cache=None, local_cache_ttl=0, stats_enabled=False
    ):
        self.redis = redis
        self.local_cache = (
            local_cache if local_cache else LocalCache(maxsize=LOCAL_CACHE_MAX_SIZE)
        )
        if local_cache_ttl < 0:
            self.local_cache_ttl = 0
        elif local_cache_ttl == 0:
            self.local_cache_ttl = ONE_MINUTE
        else:
            self.local_cache_ttl = local_cache_ttl

        self.stats_enabled = stats_enabled


class Cache:
    def __init__(self, opt=None, hits=0, misses=0):
        self.opt = opt if opt else Option()
        self.hits = hits
        self.misses = misses
        self._lock = RLock()

    def set(self, item):
        if item.value is None:
            raise ValueError("cache:value is None")

        b = self.marshal(item.value)
        if self.opt.local_cache is not None:
            self.local_set(item.key, b)

        if self.opt.redis is None:
            if self.opt.local_cache is None:
                raise RedisLocalCacheNoneError
            return True

        set_func = "set"
        if item.if_exists:
            set_func = "setxx"
        if item.if_not_exists:
            set_func = "setnx"

        try:
            getattr(self.opt.redis, set_func)(item.key, b, item.ttl)
            return True
        except NotImplementedError:
            raise
        except Exception as e:
            raise RedisIfaceError("cache: redis '%s' error. %s" % (set_func, str(e)))

    def get_skipping_local_cache(self, key):
        return self.get(key, True)

    def get(self, key, skip_local_cache=False):
        b = self._get_bytes(key, skip_local_cache)
        return self.unmarshal(b)

    # Once gets the item.Value for the given item.Key from the cache or
    # executes, caches, and returns the results of the given item.Func,
    # making sure that only one execution is in-flight for a given item.Key
    # at a time. If a duplicate comes in, the duplicate caller waits for the
    # original to complete and receives the same results.
    def once(self, item):
        b, cached = self.set_get_item_bytes_once(item)
        if not b:
            return None

        try:
            item.value = self.unmarshal(b)
        except:
            if cached:
                self.delete(item.key)
                return self.once(item)

    def delete(self, key):
        if self.opt.local_cache is not None:
            self.opt.local_cache.delete(key)

        if self.opt.redis is None:
            if self.opt.local_cache is None:
                raise RedisLocalCacheNoneError
            return True

        r = self.opt.redis.delete(key)
        if r == 0:
            raise CacheMissError
        return True

    def set_get_item_bytes_once(self, item):
        if self.opt.local_cache is not None:
            b = self.local_get(item.key)
            return b, True

        def do_func():
            b = self._get_bytes(item.key, item.skip_local_cache)
            if b is not None:
                return b, True
            b = self.set(item)
            return b, False

        try:
            with self._lock:
                return do_func()
        except:
            return None, False

    def _get_bytes(self, key, skip_local_cache=False):
        if not skip_local_cache and self.opt.local_cache is not None:
            b = self.local_get(key)
            if b is not None:
                return b

        if self.opt.redis is None:
            if self.opt.local_cache is None:
                raise RedisLocalCacheNoneError
            raise CacheMissError

        try:
            b = self.opt.redis.get(key)
        except Exception as e:
            if self.opt.stats_enabled:
                with self._lock:
                    self.misses += 1
            raise RedisIfaceError("cache: redis 'get' error. %s" % str(e))

        if b is None:
            with self._lock:
                    self.misses += 1
            return None

        if self.opt.stats_enabled:
            with self._lock:
                self.hits += 1

        if not skip_local_cache and self.opt.local_cache is not None:
            self.local_set(key, b)
        return b

    def marshal(self, value):
        if value is None:
            return None
        
        value_type = type(value)
        if value_type is bytes:
            return value + BYTES_SUFFIX
        elif value_type is str:
            return bytes(value, encoding="utf-8") + STR_SUFFIX

        b = pickle.dumps(value)
        if len(b) < COMPRESSION_THRESHOLD:
            b = b + NO_COMPRESSION + OTHER_SUFFIX
            return b

        b = zlib.compress(b)
        b = b + ZLIB_COMPRESSION + OTHER_SUFFIX
        return b

    def unmarshal(self, b):
        if b is None or len(b) == 0:
            return None

        type_suffix = b[-1:]
        b = b[:-1]
        if type_suffix == BYTES_SUFFIX:
            return b
        elif type_suffix == STR_SUFFIX:
            return b.decode(encoding="utf-8")

        if len(b) == 0:
            return None

        compression = b[-1:]
        b = b[:-1]
        if compression == NO_COMPRESSION:
            pass
        elif compression == ZLIB_COMPRESSION:
            b = zlib.decompress(b)
        else:
            raise UnkownCompressionError("uknownn compression method:", compression)

        return pickle.loads(b)

    def local_set(self, key, b):
        if self.opt.local_cache_ttl > 0:
            b += encode_time(datetime.now())
        self.opt.local_cache.add(key, b)

    def local_get(self, key):
        b = self.opt.local_cache.get(key)
        if b is None:
            return None
        if len(b) == 0 or self.opt.local_cache_ttl == 0:
            return b
        if len(b) < 4:
            raise Exception("not reached")

        dt = decode_time(b[-4:])
        now = datetime.now()
        if (now - dt).seconds > self.opt.local_cache_ttl:
            self.opt.local_cache.delete(key)
            return None
        return b[:-4]

    def stats(self):
        if not self.opt.stats_enabled:
            return None
        with self._lock:
            return AttrDict(hits=self.hits, misses=self.misses)


# ------ utils functions
EPOCH = int(datetime.timestamp(datetime(2020, 1, 1)))


def encode_time(dt):
    secs = int(datetime.timestamp(dt)) - EPOCH
    return secs.to_bytes(4, byteorder="little", signed=True)


def decode_time(b):
    secs = int.from_bytes(b, byteorder="little", signed=True)
    return datetime.fromtimestamp(EPOCH + secs)


