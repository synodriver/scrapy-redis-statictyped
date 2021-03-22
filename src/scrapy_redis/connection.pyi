from redis import Redis
from scrapy.settings import Settings as Settings
from typing import Any, Dict

SETTINGS_PARAMS_MAP: Dict[str, str]

def get_redis_from_settings(settings: Settings) -> Redis: ...
from_settings = get_redis_from_settings

def get_redis(**kwargs: Any) -> Redis: ...
