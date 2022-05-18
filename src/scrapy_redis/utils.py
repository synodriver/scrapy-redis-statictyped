import six


def bytes_to_str(s, encoding: str = 'utf-8'):
    """Returns a str if a bytes object is given."""
    return s.decode(encoding) if six.PY3 and isinstance(s, bytes) else s
