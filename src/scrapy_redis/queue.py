from typing import Optional, AnyStr, Callable, Union

from scrapy import Request
from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy import Spider

from redis import Redis
from rediscluster import RedisCluster

from . import picklecompat


class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server: Union[Redis, RedisCluster],
                 spider: Spider,
                 key: AnyStr,
                 serializer: Optional[Callable[[dict], str]] = None):
        """Initialize per-spider redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        spider : Spider
            Scrapy spider instance.
        key: str
            Redis key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError("serializer does not implement 'loads' function: %r"
                            % serializer)
        if not hasattr(serializer, 'dumps'):
            raise TypeError("serializer '%s' does not implement 'dumps' function: %r"
                            % serializer)

        self.server = server
        self.spider = spider
        self.key = key % {'spider': spider.name}
        self.serializer = serializer

    def _encode_request(self, request: Request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request: AnyStr):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request: Request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout: Optional[Union[int, float]] = 0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self) -> None:
        """Clear queue/stack"""
        self.server.delete(self.key)


class FifoQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self) -> int:
        """Return the length of the queue"""
        return self.server.llen(self.key)

    def push(self, request: Request) -> int:
        """Push a request"""
        self.server.lpush(self.key, self._encode_request(request))

    def pop(self, timeout: Optional[int] = 0) -> Request:
        """Pop a request"""
        if timeout > 0:
            data = self.server.brpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.rpop(self.key)
        if data:
            return self._decode_request(data)


class PriorityQueue(Base):
    """Per-spider priority queue abstraction using redis' sorted set"""

    def __len__(self) -> int:
        """Return the length of the queue"""
        return self.server.zcard(self.key)

    def push(self, request: Request) -> None:
        """Push a request"""
        data = self._encode_request(request)
        score = -request.priority
        # We don't use zadd method as the order of arguments change depending on
        # whether the class is Redis or StrictRedis, and the option of using
        # kwargs only accepts strings, not bytes.
        self.server.execute_command('ZADD', self.key, score, data)

    def pop(self, timeout: Optional[Union[int, float]] = 0) -> Request:
        """
        Pop a request
        timeout not support in this queue class
        """
        # use atomic range/remove using lua script
        lua_script = """
                    local result = redis.call('zrange', KEYS[1], 0, 0)
                    local element = result[1]
                    if element then
                        redis.call('zremrangebyrank', KEYS[1], 0, 0)
                        return element
                    else
                        return nil
                    end
                    """
        script = self.server.register_script(lua_script)
        result = script(keys=[self.key])
        if result:
            return self._decode_request(result)


class LifoQueue(Base):
    """Per-spider LIFO queue."""

    def __len__(self) -> int:
        """Return the length of the stack"""
        return self.server.llen(self.key)

    def push(self, request: Request) -> None:
        """Push a request"""
        self.server.lpush(self.key, self._encode_request(request))

    def pop(self, timeout: Optional[Union[int, float]] = 0) -> Request:
        """Pop a request"""
        if timeout > 0:
            data = self.server.blpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.lpop(self.key)

        if data:
            return self._decode_request(data)


# TODO: Deprecate the use of these names.
SpiderQueue = FifoQueue
SpiderStack = LifoQueue
SpiderPriorityQueue = PriorityQueue
