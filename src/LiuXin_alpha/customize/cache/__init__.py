"""
Base classes to implement a database cache object.

Caches are used to store the database - or sections of it - in memory for faster access.
They have several components - hence why it's broken down into a module
"""

from LiuXin_alpha.customize.cache.base_cache import BaseCache
from LiuXin_alpha.customize.cache.read_write_api import api, read_api, write_api

__all__ = ["BaseCache", "api", "read_api", "write_api"]
