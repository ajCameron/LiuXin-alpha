"""
Create web-source preferences with stable defaults for metadata and cover coordination.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise prefs with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_prefs.py
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
    """
    Perform the prefs apply defaults operation with explicit ordering and failure behavior.

    Example:
        Exercise  apply defaults with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_prefs.py


    :param config: Value supplied for config.
    :return: None.
    """
    for key, value in MSPREFS_DEFAULTS.items():
        config.defaults[key] = deepcopy(value)
    return config


def create_msprefs() -> JSONConfig:
    """
    Create a fresh web-source preference object populated with project defaults.

    Example:
        Exercise create msprefs with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_prefs.py


    :return: The normalized provider value, metadata result or collection described
        above.
    """
    return _apply_defaults(JSONConfig("metadata_sources/global.json"))


msprefs = create_msprefs()


__all__ = [
    "MSPREFS_DEFAULTS",
    "create_msprefs",
    "msprefs",
]
