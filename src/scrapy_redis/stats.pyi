from scrapy import Spider as Spider
from scrapy.crawler import Crawler as Crawler
from scrapy.statscollectors import StatsCollector
from typing import Any, AnyStr, Optional, Union

class RedisStatsCollector(StatsCollector):
    server: Any = ...
    spider: Any = ...
    spider_name: Any = ...
    stats_key: Any = ...
    persist: Any = ...
    def __init__(self, crawler: Crawler, spider: Optional[Spider]=...) -> None: ...
    @classmethod
    def from_crawler(cls: Any, crawler: Crawler) -> RedisStatsCollector: ...
    def get_value(self, key: AnyStr, default: Optional[int]=..., spider: Optional[Spider]=...) -> Optional[int]: ...
    def get_stats(self, spider: Optional[Spider]=...) -> dict: ...
    def set_value(self, key: AnyStr, value: Union[AnyStr, int], spider: Optional[Spider]=...) -> Any: ...
    def set_stats(self, stats: dict, spider: Optional[Spider]=...) -> Any: ...
    def inc_value(self, key: AnyStr, count: Optional[int]=..., start: Optional[int]=..., spider: Optional[Spider]=...) -> Any: ...
    def max_value(self, key: AnyStr, value: int, spider: Optional[Spider]=...) -> Any: ...
    def min_value(self, key: AnyStr, value: int, spider: Optional[Spider]=...) -> Any: ...
    def clear_stats(self, spider: Optional[Spider]=...) -> Any: ...
    def open_spider(self, spider: Spider) -> Any: ...
    def close_spider(self, spider: Spider, reason: AnyStr) -> Any: ...
