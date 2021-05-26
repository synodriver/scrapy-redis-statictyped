import redis
from redis.sentinel import Sentinel
import rediscluster
from pyfilters import MMH3HashMap, RedisBloomFilter

# For standalone use.
DUPEFILTER_KEY = 'dupefilter:%(timestamp)s'
DUPEFILTER_DEBUG = False

PIPELINE_KEY = '%(spider)s:items'

STATS_KEY = '%(spider)s:stats'

REDIS_CLS = redis.StrictRedis
REDIS_CLUSTER_CLS = rediscluster.RedisCluster
REDIS_SENTINEL_CLS = Sentinel
REDIS_ENCODING = 'utf-8'
# Sane connection defaults.
REDIS_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'encoding': REDIS_ENCODING,
}
# redis集群默认参数
REDIS_CLUSTER_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'password': None,
    'encoding': REDIS_ENCODING,
}
# redis哨兵
REDIS_SENTINEL_PARAMS = {
    'service_name': 'my_sentinel',
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'password': None,
    'encoding': REDIS_ENCODING,
}
# cluster mod
SCHEDULER_QUEUE_KEY = '%(spider)s:requests'
SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.PriorityQueue'
SCHEDULER_DUPEFILTER_KEY = '%(spider)s:dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'scrapy_redis.dupefilter.RedisDupeFilter'

SCHEDULER_PERSIST = False

START_URLS_KEY = '%(name)s:start_urls'
START_URLS_AS_SET = False
START_URLS_AS_ZSET = False

BLOOMFILTER_CAPACITY = 100000
BLOOMFILTER_ERROR_RATE = 0.0001
BLOOMFILTER_HASH_CLS = MMH3HashMap
BLOOMFILTER_CLS = RedisBloomFilter
