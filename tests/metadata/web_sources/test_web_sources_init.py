from __future__ import annotations

import pytest


def test_web_sources_init_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources as web_sources

    assert web_sources is not None


def test_web_sources_known_module_list_is_deterministic() -> None:
    from LiuXin_alpha.metadata.web_sources import iter_known_web_source_modules

    names_1 = iter_known_web_source_modules()
    names_2 = iter_known_web_source_modules()

    assert names_1 == names_2
    assert isinstance(names_1, tuple)
    assert "amazon" in names_1
    assert "google" in names_1
    assert "openlibrary" in names_1


def test_web_sources_import_web_source_module_validates_name() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    with pytest.raises(ValueError, match="module_name"):
        import_web_source_module("")


def test_web_sources_import_web_source_module_reports_missing_port_cleanly() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    with pytest.raises(ModuleNotFoundError):
        import_web_source_module("amazon")
