"""Compatibility forwarding module for the canonical WEMI API home.

This keeps family-module imports stable at the ``wemi_containers_api`` root
while the actual implementation lives in a more structured subpackage.
"""

from __future__ import annotations

import importlib

_target_module = importlib.import_module("LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.metadata_additional_containers_api.resources_containers")

__all__ = list(getattr(_target_module, "__all__", []))

globals().update({name: getattr(_target_module, name) for name in __all__})
