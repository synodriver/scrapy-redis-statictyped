from typing import Optional, AnyStr, Union

from scrapy.statscollectors import StatsCollector
from scrapy.crawler import Crawler
from scrapy import Spider
from .connection import from_settings as redis_from_settings
from .defaults import STATS_KEY, SCHEDULER_PERSIST


class RedisStatsCollector(StatsCollector):
    """
    Stats Collector based on Redis
    """

    def __init__(self, crawler: Crawler, spider: Optional[Spider] = None):
        super().__init__(crawler)
        self.server = redis_from_settings(crawler.settings)
        self.spider = spider
        self.spider_name = spider.name if spider else crawler.spidercls.name
        self.stats_key = crawler.settings.get('STATS_KEY', STATS_KEY)
        self.persist = crawler.settings.get(
            'SCHEDULER_PERSIST', SCHEDULER_PERSIST)

    def _get_key(self, spider: Optional[Spider] = None) -> AnyStr:
        """Return the hash name of stats"""
        if spider:
            return self.stats_key % {'spider': spider.name}
        if self.spider:
            return self.stats_key % {'spider': self.spider.name}
        return self.stats_key % {'spider': self.spider_name or 'scrapy'}

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "RedisStatsCollector":
        return cls(crawler)

    def get_value(self, key: AnyStr, default: Optional[int] = None, spider: Optional[Spider] = None) -> Optional[int]:
        """Return the value of hash stats"""
        if self.server.hexists(self._get_key(spider), key):
            return int(self.server.hget(self._get_key(spider), key))
        else:
            return default

    def get_stats(self, spider: Optional[Spider] = None) -> dict:
        """Return the all of the values of hash stats"""
        return self.server.hgetall(self._get_key(spider))

    def set_value(self, key: AnyStr, value: Union[AnyStr, int], spider: Optional[Spider] = None):
        """Set the value according to hash key of stats"""
        self.server.hset(self._get_key(spider), key, str(value))

    def set_stats(self, stats: dict, spider: Optional[Spider] = None):
        """Set all the hash stats"""
        self.server.hmset(self._get_key(spider), stats)

    def inc_value(self, key: AnyStr, count: Optional[int] = 1, start: Optional[int] = 0,
                  spider: Optional[Spider] = None):
        """Set increment of value according to key"""
        if not self.server.hexists(self._get_key(spider), key):
            self.set_value(key, start)
        self.server.hincrby(self._get_key(spider), key, count)

    def max_value(self, key: AnyStr, value: int, spider: Optional[Spider] = None):
        """Set max value between current and new value"""
        self.set_value(key, max(self.get_value(key, value), value))

    def min_value(self, key: AnyStr, value: int, spider: Optional[Spider] = None):
        """Set min value between current and new value"""
        self.set_value(key, min(self.get_value(key, value), value))

    def clear_stats(self, spider: Optional[Spider] = None):
        """Clarn all the hash stats"""
        self.server.delete(self._get_key(spider))

    def open_spider(self, spider: Spider):
        """Set spider to self"""
        if spider:
            self.spider = spider

    def close_spider(self, spider: Spider, reason: AnyStr):
        """Clear spider and clear stats"""
        self.spider = None
        if not self.persist:
            self.clear_stats(spider)
