import sys
from typing import Dict

import six

from scrapy.utils.misc import load_object
from scrapy.settings import Settings
from redis import Redis

from . import defaults

# Shortcut maps 'setting name' -> 'parmater name'.
SETTINGS_PARAMS_MAP: Dict[str, str] = {
    'REDIS_URL': 'url',
    'REDIS_HOST': 'host',
    'REDIS_PORT': 'port',
    'REDIS_DB': 'db',
    'REDIS_ENCODING': 'encoding',
}

# 集群设置
REDIS_CLUSTER_SETTINGS_PARAMS_MAP = {
    'REDIS_CLUSTER_CLS': 'redis_cluster_cls',
    'REDIS_CLUSTER_URL': 'url',
    'REDIS_CLUSTER_NODES': 'startup_nodes',
    'REDIS_CLUSTER_PASSWORD': 'password',
    'REDIS_ENCODING': 'encoding',
}

# 哨兵连接配置
REDIS_SENTINEL_SETTINGS_PARAMS_MAP = {
    'REDIS_SENTINEL_CLS': 'redis_sentinel_cls',
    'REDIS_SENTINEL_NODES': 'sentinel_nodes',
    'REDIS_SENTINEL_SERVICE_NAME': 'service_name',
    'REDIS_SENTINEL_PASSWORD': 'password',
    'REDIS_ENCODING': 'encoding',
}

if sys.version_info > (3,):
    SETTINGS_PARAMS_MAP['REDIS_DECODE_RESPONSES'] = 'decode_responses'


def get_redis_from_settings(settings: Settings) -> Redis:
    """Returns a redis client instance from given Scrapy settings object.

    This function uses ``get_client`` to instantiate the client and uses
    ``defaults.REDIS_PARAMS`` global as defaults values for the parameters. You
    can override them using the ``REDIS_PARAMS`` setting.

    Parameters
    ----------
    settings : Settings
        A scrapy settings object. See the supported settings below.

    Returns
    -------
    server
        Redis client instance.

    Other Parameters
    ----------------
    REDIS_URL : str, optional
        Server connection URL.
    REDIS_HOST : str, optional
        Server host.
    REDIS_PORT : str, optional
        Server port.
    REDIS_DB : int, optional
        Server database
    REDIS_ENCODING : str, optional
        Data encoding.
    REDIS_PARAMS : dict, optional
        Additional client parameters.

    Python 3 Only
    ----------------
    REDIS_DECODE_RESPONSES : bool, optional
        Sets the `decode_responses` kwarg in Redis cls ctor

    """
    params = defaults.REDIS_PARAMS.copy()
    params.update(settings.getdict('REDIS_PARAMS'))
    # XXX: Deprecate REDIS_* settings.
    for source, dest in SETTINGS_PARAMS_MAP.items():
        val = settings.get(source)
        if val:
            params[dest] = val

    # Allow ``redis_cls`` to be a path to a class.
    if isinstance(params.get('redis_cls'), six.string_types):
        params['redis_cls'] = load_object(params['redis_cls'])

    return get_redis(**params)


def get_redis(**kwargs) -> Redis:
    """Returns a redis client instance.

    Parameters
    ----------
    redis_cls : class, optional
        Defaults to ``redis.StrictRedis``.
    url : str, optional
        If given, ``redis_cls.from_url`` is used to instantiate the class.
    **kwargs
        Extra parameters to be passed to the ``redis_cls`` class.

    Returns
    -------
    server
        Redis client instance.

    """
    redis_cls = kwargs.pop('redis_cls', defaults.REDIS_CLS)
    url = kwargs.pop('url', None)
    if url:
        return redis_cls.from_url(url, **kwargs)
    else:
        return redis_cls(**kwargs)


def get_redis_cluster_from_settings(settings: Settings):
    params = defaults.REDIS_CLUSTER_PARAMS.copy()
    params.update(settings.getdict('REDIS_CLUSTER_PARAMS'))
    # XXX: Deprecate REDIS_CLUSTER* settings.
    for setting_name, name in REDIS_CLUSTER_SETTINGS_PARAMS_MAP.items():
        val = settings.get(setting_name)
        if val:
            params[name] = val

    # Allow ``redis_cluster_cls`` to be a path to a class.
    if isinstance(params.get('redis_cluster_cls'), six.string_types):
        params['redis_cluster_cls'] = load_object(params['redis_cluster_cls'])

    return get_redis_cluster(**params)


def get_redis_cluster(**kwargs):
    """
    返回一个 redis 集群实例
    """
    redis_cluster_cls = kwargs.pop('redis_cluster_cls', defaults.REDIS_CLUSTER_CLS)
    url = kwargs.pop('url', None)
    # redis cluster 只有 db0，不支持 db 参数
    try:
        kwargs.pop('db')
    except KeyError:
        pass
    if url:
        try:
            kwargs.pop('startup_nodes')
        except KeyError:
            pass
        return redis_cluster_cls.from_url(url, **kwargs)
    else:
        return redis_cluster_cls(**kwargs)


def get_redis_sentinel_from_settings(settings: Settings):
    params = defaults.REDIS_SENTINEL_PARAMS.copy()
    params.update(settings.getdict('REDIS_SENTINEL_PARAMS'))
    # XXX: Deprecate REDIS_CLUSTER* settings.
    for setting_name, name in REDIS_SENTINEL_SETTINGS_PARAMS_MAP.items():
        val = settings.get(setting_name)
        if val:
            params[name] = val

    # Allow ``redis_cluster_cls`` to be a path to a class.
    if isinstance(params.get('redis_sentinel_cls'), six.string_types):
        params['redis_sentinel_cls'] = load_object(params['redis_sentinel_cls'])

    return get_redis_sentinel(**params)


def get_redis_sentinel(**kwargs):
    """
    返回一个 redis sentinel实例
    """
    redis_sentinel_cls = kwargs.pop('redis_sentinel_cls', defaults.REDIS_SENTINEL_CLS)
    sentinel_nodes = kwargs.pop('sentinel_nodes')
    service_name = kwargs.pop('service_name')
    redis_sentinel_conn = redis_sentinel_cls(sentinel_nodes, **kwargs)
    # return [redis_sentinel_conn.master_for(service_name), redis_sentinel_conn.slave_for(service_name)]
    return redis_sentinel_conn.master_for(service_name)


# Backwards compatible alias.
def from_settings(settings: Settings):
    """
    根据settings中的配置来决定返回不同 redis 实例，优先级： 集群 > 哨兵 > 单机
    """
    if "REDIS_CLUSTER_NODES" in settings or 'REDIS_CLUSTER_URL' in settings:
        return get_redis_cluster_from_settings(settings)
    elif "REDIS_SENTINEL_NODES" in settings:
        return get_redis_sentinel_from_settings(settings)
    return get_redis_from_settings(settings)
