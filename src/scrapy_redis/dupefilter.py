import logging
import time
from typing import Optional, AnyStr, Type

from scrapy import Request, Spider
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint
from scrapy.utils.misc import load_object
from scrapy.settings import Settings
from scrapy.crawler import Crawler

from redis import Redis
from pyfilters import RedisBloomFilter, MMH3HashMap, BaseHash

from . import defaults
from .connection import from_settings  # todo 全部改为from_settings

logger = logging.getLogger(__name__)


class RedisDupeFilter(BaseDupeFilter):
    """Redis-based request duplicates filter.

    This class can also be used with default Scrapy's scheduler.

    """

    logger = logger

    def __init__(self, server: Redis, key: AnyStr, debug: Optional[bool] = False):
        """Initialize the duplicates filter.

        Parameters
        ----------
        server : redis.StrictRedis
            The redis server instance.
        key : str
            Redis key Where to store fingerprints.
        debug : bool, optional
            Whether to log filtered requests.

        """
        self.server = server
        self.key = key
        self.debug = debug
        self.logdupes = True

    @classmethod
    def from_settings(cls, settings: Settings) -> "RedisDupeFilter":
        """Returns an instance from given settings.

        This uses by default the key ``dupefilter:<timestamp>``. When using the
        ``scrapy_redis.scheduler.Scheduler`` class, this method is not used as
        it needs to pass the spider name in the key.

        Parameters
        ----------
        settings : scrapy.settings.Settings

        Returns
        -------
        RFPDupeFilter
            A RFPDupeFilter instance.


        """
        server = from_settings(settings)
        # XXX: This creates one-time key. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        # TODO: Use SCRAPY_JOB env as default and fallback to timestamp.
        key = defaults.DUPEFILTER_KEY % {'timestamp': int(time.time())}
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(server, key=key, debug=debug)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "RedisDupeFilter":
        """Returns instance from crawler.

        Parameters
        ----------
        crawler : scrapy.crawler.Crawler

        Returns
        -------
        RFPDupeFilter
            Instance of RFPDupeFilter.

        """
        return cls.from_settings(crawler.settings)

    def request_seen(self, request: Request) -> bool:
        """Returns True if request was already seen.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        bool

        """
        fp = self.request_fingerprint(request)
        # This returns the number of values added, zero if already exists.
        added = self.server.sadd(self.key, fp)
        return added == 0

    def request_fingerprint(self, request: Request) -> AnyStr:
        """Returns a fingerprint for a given request.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        str

        """
        return request_fingerprint(request)

    @classmethod
    def from_spider(cls, spider: Spider) -> "RedisDupeFilter":
        settings = spider.settings
        server = from_settings(settings)
        dupefilter_key = settings.get("SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY)
        key = dupefilter_key % {'spider': spider.name}
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(server, key=key, debug=debug)

    def close(self, reason: Optional[str] = '') -> None:
        """Delete data on close. Called by Scrapy's scheduler.

        Parameters
        ----------
        reason : str, optional

        """
        self.clear()

    def clear(self) -> None:
        """Clears fingerprints data."""
        self.server.delete(self.key)

    def log(self, request: Request, spider: Spider) -> None:
        """Logs given request.

        Parameters
        ----------
        request : scrapy.http.Request
        spider : scrapy.spiders.Spider

        """
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False


class RedisBloomDupeFilter(RedisDupeFilter):
    """
    Redis-based request duplicates filter. Using RedisBloomFilter instead of set which saves
    a lot of memory.

    This class can also be used with default Scrapy's scheduler.
    settinss:
    [SCHEDULER_]DUPEFILTER_KEY:  when cluster ``scrapy_redis.scheduler.Scheduler``,SCHEDULER_DUPEFILTER_KEY is used
    DUPEFILTER_DEBUG
    BLOOMFILTER_CAPACITY
    BLOOMFILTER_ERROR_RATE
    BLOOMFILTER_HASH_CLS
    """

    def __init__(self, server,
                 key: AnyStr,
                 capacity: int,
                 error_rate: float,
                 hash_cls: Optional[Type[BaseHash]] = MMH3HashMap,
                 bloom_filter_cls=RedisBloomFilter,
                 debug: Optional[bool] = False):
        super().__init__(server, key, debug=debug)
        self.bf = bloom_filter_cls(server, key, capacity, error_rate, hash_cls)

    @classmethod
    def from_settings(cls, settings: Settings) -> "RedisBloomFilterDupeFilter":
        """
        单机模式会用到
        :param settings:
        :return:
        """
        server = from_settings(settings)
        key = settings.get('DUPEFILTER_KEY', defaults.DUPEFILTER_KEY % {'timestamp': int(time.time())})
        debug = settings.getbool('DUPEFILTER_DEBUG', defaults.DUPEFILTER_DEBUG)
        capacity = settings.getint('BLOOMFILTER_CAPACITY', defaults.BLOOMFILTER_CAPACITY)
        error_rate = settings.getfloat('BLOOMFILTER_ERROR_RATE', defaults.BLOOMFILTER_ERROR_RATE)
        hash_cls = settings.get('BLOOMFILTER_HASH_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(hash_cls, str):
            hash_cls = load_object(hash_cls)
        bloom_filter_cls = settings.get('BLOOMFILTER_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(bloom_filter_cls, str):
            bloom_filter_cls = load_object(bloom_filter_cls)
        return cls(server, key, capacity, error_rate, hash_cls, bloom_filter_cls, debug=debug)

    @classmethod
    def from_spider(cls, spider: Spider) -> "RedisBloomFilterDupeFilter":
        """
        cluster mod
        :param spider:
        :return:
        """
        settings = spider.settings
        server = from_settings(settings)
        dupefilter_key = settings.get("SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY)
        key = dupefilter_key % {'spider': spider.name}
        debug = settings.getbool('DUPEFILTER_DEBUG')

        capacity = settings.getint('BLOOMFILTER_CAPACITY', defaults.BLOOMFILTER_CAPACITY)
        error_rate = settings.getfloat('BLOOMFILTER_ERROR_RATE', defaults.BLOOMFILTER_ERROR_RATE)
        hash_cls = settings.get('BLOOMFILTER_HASH_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(hash_cls, str):
            hash_cls = load_object(hash_cls)
        bloom_filter_cls = settings.get('BLOOMFILTER_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(bloom_filter_cls, str):
            bloom_filter_cls = load_object(bloom_filter_cls)
        return cls(server, key, capacity, error_rate, hash_cls, bloom_filter_cls, debug=debug)

    def request_seen(self, request: Request) -> bool:
        """Returns True if request was already seen.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        bool

        """
        fp = self.request_fingerprint(request)
        # This returns if the fp is added, False if already exists.
        added = self.bf.add(fp)
        return added

    def clear(self) -> None:
        """Clears fingerprints data."""
        self.bf.clear()


'''
class LockedRedisDupeFilter(RedisDupeFilter):  # todo 需要修改
    """
    Add lock when deduplication, which reduce the performance,
    however, it ensures the correctness of data

    去重时，先加锁，会降低性能，但是可以保证数据正确性
    """

    def __init__(self, server,
                 key: AnyStr,
                 capacity: int,
                 error_rate: float,
                 lock_key: str,
                 lock_num: int,
                 lock_timeout: int,
                 hash_cls: Optional[Type[BaseHash]] = MMH3HashMap,
                 bloom_filter_cls=RedisBloomFilter,
                 debug: Optional[bool] = False):
        super().__init__(server, key, debug=debug)
        self.bf = bloom_filter_cls(server, key, capacity, error_rate, hash_cls)
        self.lock_key = lock_key
        self.lock_num = lock_num
        self.lock_timeout = lock_timeout
        self.locks = list()
        # 初始化 N 把锁，最多 4096，缓解多个 scrapy 实例抢一个锁带来的性能下降问题
        for i in range(0, int('f' * self.lock_value_split_num, 16) + 1):
            self.locks.append(self.server.lock(lock_key + str(i), lock_timeout))

    @classmethod
    def from_settings(cls, settings: Settings):
        """单机模式"""
        server = from_settings(settings)
        key = settings.get('DUPEFILTER_KEY', defaults.DUPEFILTER_KEY % {'timestamp': int(time.time())})
        debug = settings.getbool('DUPEFILTER_DEBUG', defaults.DUPEFILTER_DEBUG)
        capacity = settings.getint('BLOOMFILTER_CAPACITY', defaults.BLOOMFILTER_CAPACITY)
        error_rate = settings.getfloat('BLOOMFILTER_ERROR_RATE', defaults.BLOOMFILTER_ERROR_RATE)
        hash_cls = settings.get('BLOOMFILTER_HASH_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(hash_cls, str):
            hash_cls = load_object(hash_cls)
        bloom_filter_cls = settings.get('BLOOMFILTER_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(bloom_filter_cls, str):
            bloom_filter_cls = load_object(bloom_filter_cls)
        # bit = settings.getint('BLOOMFILTER_BIT', defaults.BLOOMFILTER_BIT)
        # hash_number = settings.getint('BLOOMFILTER_HASH_NUMBER', defaults.BLOOMFILTER_HASH_NUMBER)
        # block_num = settings.getint('BLOOMFILTER_BLOCK_NUM', defaults.BLOOMFILTER_BLOCK_NUM)
        lock_key = settings.get('DUPEFILTER_LOCK_KEY', defaults.DUPEFILTER_LOCK_KEY)
        lock_num = settings.getint('DUPEFILTER_LOCK_NUM', defaults.DUPEFILTER_LOCK_NUM)
        lock_timeout = settings.getint('DUPEFILTER_LOCK_TIMEOUT', defaults.DUPEFILTER_LOCK_TIMEOUT)
        return cls(
            server=server, key=key, debug=debug,
            capacity=capacity, hash_cls=hash_cls, bloom_filter_cls=bloom_filter_cls, error_rate=error_rate,
            lock_key=lock_key, lock_num=lock_num, lock_timeout=lock_timeout
        )

    @classmethod
    def from_spider(cls, spider: Spider) -> "LockedRedisDupeFilter":
        """cluster mod"""
        settings = spider.settings
        server = from_settings(settings)
        dupefilter_key = settings.get("SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY)
        key = dupefilter_key % {'spider': spider.name}
        debug = settings.getbool('DUPEFILTER_DEBUG', defaults.DUPEFILTER_DEBUG)
        capacity = settings.getint('BLOOMFILTER_CAPACITY', defaults.BLOOMFILTER_CAPACITY)
        error_rate = settings.getfloat('BLOOMFILTER_ERROR_RATE', defaults.BLOOMFILTER_ERROR_RATE)
        hash_cls = settings.get('BLOOMFILTER_HASH_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(hash_cls, str):
            hash_cls = load_object(hash_cls)
        bloom_filter_cls = settings.get('BLOOMFILTER_CLS', defaults.BLOOMFILTER_HASH_CLS)
        if isinstance(bloom_filter_cls, str):
            bloom_filter_cls = load_object(bloom_filter_cls)
        lock_key = settings.get('DUPEFILTER_LOCK_KEY', defaults.DUPEFILTER_LOCK_KEY)
        lock_num = settings.getint('DUPEFILTER_LOCK_NUM', defaults.DUPEFILTER_LOCK_NUM)
        lock_timeout = settings.getint('DUPEFILTER_LOCK_TIMEOUT', defaults.DUPEFILTER_LOCK_TIMEOUT)
        return cls(
            server=server, key=key, debug=debug,
            capacity=capacity, hash_cls=hash_cls, bloom_filter_cls=bloom_filter_cls, error_rate=error_rate,
            lock_key=lock_key, lock_num=lock_num, lock_timeout=lock_timeout
        )

    def request_seen(self, request):
        fp: str = self.request_fingerprint(request)
        # 根据 request 生成的 sha1 选择相应的锁
        # lock = self.lock[int(fp[0:self.lock_value_split_num], 16)]
        lock = self.locks[int(fp, 16) % self.lock_num]
        while True:
            if lock.acquire(blocking=False):
                if fp in self.bf:
                    lock.release()
                    return True
                self.bf.add(fp)
                lock.release()
                return False


class ListLockRFPDupeFilter(LockedRedisDupeFilter):  # todo 需要修改  包括defaults.py
    def __init__(self, rules_list, key_list, bit_list, hash_number_list, block_num_list, **kwargs):
        self.rules_list = rules_list
        self.key_list = key_list
        self.bit_list = bit_list
        self.hash_number_list = hash_number_list
        self.block_num_list = block_num_list
        super().__init__(**kwargs)
        self.bf_list = RedisBloomFilter(self.server, key_list, bit_list, hash_number_list, block_num_list)

    @classmethod
    def from_settings(cls, settings):
        server = from_settings(settings)
        key = settings.get('DUPEFILTER_KEY', defaults.DUPEFILTER_KEY)
        debug = settings.getbool('DUPEFILTER_DEBUG', defaults.DUPEFILTER_DEBUG)
        bit = settings.getint('BLOOMFILTER_BIT', defaults.BLOOMFILTER_BIT)
        hash_number = settings.getint('BLOOMFILTER_HASH_NUMBER', defaults.BLOOMFILTER_HASH_NUMBER)
        block_num = settings.getint('BLOOMFILTER_BLOCK_NUM', defaults.BLOOMFILTER_BLOCK_NUM)
        lock_key = settings.get('DUPEFILTER_LOCK_KEY', defaults.DUPEFILTER_LOCK_KEY)
        lock_num = settings.getint('DUPEFILTER_LOCK_NUM', defaults.DUPEFILTER_LOCK_NUM)
        lock_timeout = settings.getint('DUPEFILTER_LOCK_TIMEOUT', defaults.DUPEFILTER_LOCK_TIMEOUT)

        rules_list = settings.get('DUPEFILTER_RULES_LIST', defaults.DUPEFILTER_RULES_LIST)
        key_list = settings.get('DUPEFILTER_KEY_LIST', defaults.DUPEFILTER_KEY_LIST)
        bit_list = settings.getint('BLOOMFILTER_BIT_LIST', defaults.BLOOMFILTER_BIT_LIST)
        hash_number_list = settings.getint('BLOOMFILTER_HASH_NUMBER_LIST', defaults.BLOOMFILTER_HASH_NUMBER_LIST)
        block_num_list = settings.getint('BLOOMFILTER_BLOCK_NUM_LIST', defaults.BLOOMFILTER_BLOCK_NUM_LIST)

        return cls(
            server=server, key=key, debug=debug, bit=bit, hash_number=hash_number,
            block_num=block_num, lock_key=lock_key, lock_num=lock_num, lock_timeout=lock_timeout,
            rules_list=rules_list, key_list=key_list, bit_list=bit_list,
            hash_number_list=hash_number_list, block_num_list=block_num_list
        )

    def request_seen(self, request):
        """
        列表页去重时不需要加锁
        """
        fp = self.request_fingerprint(request)

        for rule in self.rules_list:
            if re.search(rule, request.url, re.I):
                if self.bf_list.exists(fp):
                    return True
                self.bf_list.insert(fp)
                return False
        else:
            # 根据 request 生成的 sha1 选择相应的锁
            lock = self.lock[int(fp[0:self.lock_value_split_num], 16)]
            while 1:
                if lock.acquire(blocking=False):
                    if self.bf.exists(fp):
                        lock.release()
                        return True
                    self.bf.insert(fp)
                    lock.release()
                    return False
'''
