# beyond scrapy-redis：
- rediscluster redissential support

##  extra settings
- REDIS_CLUSTER_PARAMS: Dict # overwritten by follow
- REDIS_CLUSTER_CLS: Union[str,Type]
- REDIS_CLUSTER_URL
- REDIS_CLUSTER_NODES
- REDIS_CLUSTER_PASSWORD


- REDIS_SENTINEL_PARAMS: Dict # overwritten by follow
- REDIS_SENTINEL_CLS: Union[str,Type]
- REDIS_SENTINEL_NODES
- REDIS_SENTINEL_SERVICE_NAME
- REDIS_SENTINEL_PASSWORD

- BLOOMFILTER_CAPACITY
- BLOOMFILTER_ERROR_RATE
- BLOOMFILTER_HASH_CLS
- BLOOMFILTER_CLS
