from typing import Union, Optional, Callable

from scrapy import Spider, Item
from scrapy.settings import Settings
from scrapy.crawler import Crawler
from scrapy.utils.misc import load_object
from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet.threads import deferToThread

from redis import Redis

from . import connection, defaults  # type: ignore

default_serialize = ScrapyJSONEncoder().encode


class RedisPipeline(object):
    """Pushes serialized item into a redis list/queue

    Settings
    --------
    REDIS_ITEMS_KEY : str
        Redis key where to store items.
    REDIS_ITEMS_SERIALIZER : str
        Object path to serializer function.

    """

    def __init__(self, server: Redis,
                 key: Optional[str] = defaults.PIPELINE_KEY,
                 serialize_func: Callable[[dict], str] = default_serialize):
        """Initialize pipeline.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        key : str
            Redis key where to store items.
        serialize_func : callable
            Items serializer function.

        """
        self.server = server
        self.key = key
        self.serialize = serialize_func

    @classmethod
    def from_settings(cls, settings: Settings) -> "RedisPipeline":
        params = {
            'server': connection.from_settings(settings),
        }
        if settings.get('REDIS_ITEMS_KEY'):
            params['key'] = settings['REDIS_ITEMS_KEY']
        if settings.get('REDIS_ITEMS_SERIALIZER'):
            params['serialize_func'] = load_object(
                settings['REDIS_ITEMS_SERIALIZER']
            )

        return cls(**params)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "RedisPipeline":
        return cls.from_settings(crawler.settings)

    def process_item(self, item: Union[Item, dict], spider: Spider) -> Union[Item, dict]:
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item: Union[Item, dict], spider: Spider) -> Union[Item, dict]:
        key = self.item_key(item, spider)
        data = self.serialize(item)
        self.server.rpush(key, data)
        return item

    def item_key(self, item: Union[Item, dict], spider: Spider) -> str:
        """Returns redis key based on given spider.

        Override this function to use a different key depending on the item
        and/or spider.

        """
        return self.key % {'spider': spider.name}
