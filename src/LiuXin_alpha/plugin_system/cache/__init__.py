"""
Expose the supported cache compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/plugin_system/test_plugin_base.py
"""

from LiuXin_alpha.plugin_system.cache.base_cache import BaseCache
from LiuXin_alpha.plugin_system.cache.read_write_api import api, read_api, write_api

__all__ = ["BaseCache", "api", "read_api", "write_api"]
