import json
from collections.abc import Iterable
import time
from typing import Union, Optional, Generator, NoReturn

from scrapy import signals, FormRequest, Request
from scrapy.exceptions import DontCloseSpider
from scrapy.settings import Settings
from scrapy.crawler import Crawler
from scrapy.spiders import Spider, CrawlSpider
from redis import Redis

from . import connection, defaults
from .utils import bytes_to_str


class RedisMixin(object):
    """Mixin class to implement reading urls from a redis queue."""
    redis_key: str = None
    redis_batch_size: int = None
    redis_encoding: str = None

    # Redis client placeholder.
    server: Redis = None

    # Idle start time
    spider_idle_start_time = int(time.time())
    max_idle_time = None

    def start_requests(self) -> Generator[Request, None, None]:
        """Returns a batch of start requests from redis."""
        return self.next_requests()

    def setup_redis(self, crawler: Optional[Crawler] = None):
        """Setup redis connection and idle signal.

        This should be called after the spider has set its crawler object.
        """
        if self.server is not None:
            return

        if crawler is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings: Settings = crawler.settings

        if self.redis_key is None:
            self.redis_key = settings.get(
                'REDIS_START_URLS_KEY', defaults.START_URLS_KEY,
            )

        self.redis_key = self.redis_key % {'name': self.name}

        if not self.redis_key.strip():
            raise ValueError("redis_key must not be empty")

        if self.redis_batch_size is None:
            # TODO: Deprecate this setting (REDIS_START_URLS_BATCH_SIZE).
            self.redis_batch_size = settings.getint(
                'REDIS_START_URLS_BATCH_SIZE',
                settings.getint('CONCURRENT_REQUESTS'),
            )

        try:
            self.redis_batch_size = int(self.redis_batch_size)
        except (TypeError, ValueError):
            raise ValueError("redis_batch_size must be an integer")

        if self.redis_encoding is None:
            self.redis_encoding = settings.get('REDIS_ENCODING', defaults.REDIS_ENCODING)

        self.logger.info("Reading start URLs from redis key '%(redis_key)s' "
                         "(batch size: %(redis_batch_size)s, encoding: %(redis_encoding)s)",
                         self.__dict__)

        self.server = connection.from_settings(crawler.settings)

        if settings.getbool('REDIS_START_URLS_AS_SET', defaults.START_URLS_AS_SET):
            self.fetch_data = self.server.spop
            self.count_size = self.server.scard
        elif settings.getbool('REDIS_START_URLS_AS_ZSET', defaults.START_URLS_AS_ZSET):
            self.fetch_data = self.pop_priority_queue
            self.count_size = self.server.zcard
        else:
            self.fetch_data = self.pop_list_queue
            self.count_size = self.server.llen

        if self.max_idle_time is None:
            self.max_idle_time = settings.getint(
                "MAX_IDLE_TIME_BEFORE_CLOSE",
                defaults.MAX_IDLE_TIME
            )

        try:
            self.max_idle_time = int(self.max_idle_time)
        except (TypeError, ValueError):
            raise ValueError("max_idle_time must be an integer")

        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from redis queue
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def pop_list_queue(self, redis_key: str, batch_size: int) -> list:
        with self.server.pipeline() as pipe:
            pipe.lrange(redis_key, 0, batch_size - 1)
            pipe.ltrim(redis_key, batch_size, -1)
            datas, _ = pipe.execute()
        return datas

    def pop_priority_queue(self, redis_key: str, batch_size: int) -> list:
        with self.server.pipeline() as pipe:
            pipe.zrevrange(redis_key, 0, batch_size - 1)
            pipe.zremrangebyrank(redis_key, -batch_size, -1)
            datas, _ = pipe.execute()
        return datas

    def next_requests(self) -> Generator[Request, None, None]:
        """Returns a request to be scheduled or none."""
        # XXX: Do we need to use a timeout here?
        found = 0
        datas = self.fetch_data(self.redis_key, self.redis_batch_size)
        for data in datas:
            reqs = self.make_request_from_data(data)
            if isinstance(reqs, Iterable):
                for req in reqs:
                    yield req
                    # XXX: should be here?
                    found += 1
                    self.logger.info(f'start req url:{req.url}')
            elif reqs:
                yield reqs
                found += 1
            else:
                self.logger.debug("Request not made from data: %r", data)

        if found:
            self.logger.debug("Read %s requests from '%s'", found, self.redis_key)

    def make_request_from_data(self, data: Union[str, bytes]) -> Request:
        """Returns a Request instance from data coming from Redis.

        Overriding this function to support the 'json' requested ``data`` that contains
        `url` ,`meta` and other optional parameters. `meta` is a nested json which contains sub-data.

        Along with:
        After accessing the data, sending the FormRequest with `url`, `meta` and addition `formdata`

        For example:
        {"url": "https://exaple.com", "meta": {'job-id':'123xsd', 'start-date':'dd/mm/yy'}, "url_cookie_key":"fertxsas" }

        this data can be accessed from 'scrapy.spider' through response.
        'response.url', 'response.meta', 'response.url_cookie_key'

        Parameters
        ----------
        data : bytes
            Message from redis.

        """
        # url = bytes_to_str(data, self.redis_encoding)
        formatted_data = bytes_to_str(data, self.redis_encoding)

        # change to json array
        parameter = json.loads(formatted_data)
        url = parameter['url']
        del parameter['url']
        metadata = {}
        try:
            metadata = parameter['meta']
            del parameter['meta']
        except:
            pass

        return FormRequest(url, dont_filter=True, formdata=parameter, meta=metadata)

    def schedule_next_requests(self) -> None:
        """Schedules a request if available"""
        # TODO: While there is capacity, schedule a batch of redis requests.
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self) -> NoReturn:
        """
        Schedules a request if available, otherwise waits.
        or close spider when waiting seconds > MAX_IDLE_TIME_BEFORE_CLOSE.
        MAX_IDLE_TIME_BEFORE_CLOSE will not affect SCHEDULER_IDLE_BEFORE_CLOSE.
        """
        if self.server is not None and self.count_size(self.redis_key) > 0:
            self.spider_idle_start_time = int(time.time())
        self.schedule_next_requests()

        idle_time = int(time.time()) - self.spider_idle_start_time
        if self.max_idle_time != 0 and idle_time >= self.max_idle_time:
            return
        raise DontCloseSpider


class RedisSpider(RedisMixin, Spider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: False)
        Use SET operations to retrieve messages from the redis queue. If False,
        the messages are retrieve using the LPOP command.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs) -> "RedisSpider":
        obj = super().from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj


class RedisCrawlSpider(RedisMixin, CrawlSpider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: True)
        Use SET operations to retrieve messages from the redis queue.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    @classmethod
    def from_crawler(cls, crawler: Crawler, *args, **kwargs) -> "RedisCrawlSpider":
        obj = super().from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj
