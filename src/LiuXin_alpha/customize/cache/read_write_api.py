"""
Decorates used in the cache to declare if the function is a read function, a write function, or both.
"""


def api(f):
    f.is_cache_api = True
    return f


def read_api(f):
    f = api(f)
    f.is_read_api = True
    return f


def write_api(f):
    f = api(f)
    f.is_read_api = False
    return f
