"""
Global preferences for web metadata-source plugins.

These defaults mirror the calibre/LiuXin behavior while remaining safe to
import in partially-ported environments.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from LiuXin_alpha.utils.config.config_tools import JSONConfig

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


MSPREFS_DEFAULTS: dict[str, Any] = {
    "txt_comments": False,
    "ignore_fields": [],
    "user_default_ignore_fields": [],
    "max_tags": 20,
    "wait_after_first_identify_result": 30,  # seconds
    "wait_after_first_cover_result": 60,  # seconds
    "swap_author_names": False,
    "fewer_tags": True,
    "find_first_edition_date": False,
    "append_comments": False,
    "tag_map_rules": (),
    "author_map_rules": (),
    "publisher_map_rules": (),
    "series_map_rules": (),
    "id_link_rules": {},
    "keep_dups": False,
    # Google covers are often high-resolution but poor quality (scans/errors).
    # Keep them lower priority unless nothing better is found.
    "cover_priorities": {
        "Google": 2,
        "Google Images": 2,
        "Big Book Search": 2,
    },
}


def _apply_defaults(config: JSONConfig) -> JSONConfig:
    for key, value in MSPREFS_DEFAULTS.items():
        config.defaults[key] = deepcopy(value)
    return config


def create_msprefs() -> JSONConfig:
    """
    Build and return the global web-source preferences config object.
    """
    return _apply_defaults(JSONConfig("metadata_sources/global.json"))


msprefs = create_msprefs()


__all__ = [
    "MSPREFS_DEFAULTS",
    "create_msprefs",
    "msprefs",
]
