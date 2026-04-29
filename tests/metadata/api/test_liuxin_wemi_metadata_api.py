from __future__ import annotations

from LiuXin_alpha.metadata.api import (
    LiuXinWEMIAPI,
    LiuXinWEMIMetadataAPI,
    WemiLevel,
)
from LiuXin_alpha.metadata.api import __all__ as metadata_api_all


def test_liuxin_wemi_metadata_api_is_exported_from_metadata_api_root() -> None:
    assert "LiuXinWEMIMetadataAPI" in metadata_api_all
    assert "LiuXinWEMIAPI" in metadata_api_all
    assert LiuXinWEMIAPI is LiuXinWEMIMetadataAPI


def test_wemi_level_alias_allows_item_centered_stack_names() -> None:
    level: WemiLevel = "item"
    assert level == "item"
