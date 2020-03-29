# cache-py

阅读 [`go-redis/cache`](https://github.com/go-redis/cache) 的代码后，为了理解原代码，遂用Python 实现了一个版本。

*（用 py3.7 编写，其他版本不确定支持，自行测试）*

## Feature

- 支持缓存 Python的所有内置对象以及自定义类实例

## Usage

### without redis

```python
>>> value = "Hello, World, Hello 中国"
>>> key = "key"
>>> item = Item(key, value)
>>> cache = Cache()
>>> cache.set(item)
True
>>> cache.get(key)
'Hello, World, Hello 中国'
>>>
```

### use redis

```python
>>> from redis import Redis
>>> from cache import Cache, Item
>>> r = Redis(host='localhost', port=6379, db=0)
>>> from cache import Cache, Item, Option
>>> opt = Option(redis=r, stats_enabled=True)
>>> cache = Cache(opt=opt)
>>> value = "Hello, World, Hello 中国"
>>> key = "key"
>>> item = Item(key, value)
>>> cache.set(item)
True
>>> cache.get(key)
'Hello, World, Hello 中国'
>>> cache.get(key, skip_local_cache=True)
'Hello, World, Hello 中国'
```

## Test

```shell
pip install -r req-test.txt
python -m pytest test_cache.py -v
```

## Link

[https://github.com/go-redis/cache](https://github.com/go-redis/cache)
