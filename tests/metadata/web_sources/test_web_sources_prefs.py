"""
Verify web-source preference defaults and isolation.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources prefs through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_prefs.py
"""
from __future__ import annotations


def test_web_sources_prefs_import_smoke() -> None:
    """
    Verify web sources prefs import smoke.

    Example:
        Exercise test web sources prefs import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_prefs.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.prefs as prefs

    assert prefs is not None


def test_web_sources_prefs_defaults_are_registered() -> None:
    """
    Verify web sources prefs defaults remain registered.

    Example:
        Exercise test web sources prefs defaults are registered through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_prefs.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.prefs import MSPREFS_DEFAULTS, msprefs

    for key, expected in MSPREFS_DEFAULTS.items():
        assert key in msprefs.defaults
        assert msprefs.defaults[key] == expected


def test_web_sources_prefs_create_msprefs_has_same_default_shape() -> None:
    """
    Verify web sources prefs create msprefs has same default shape.

    Example:
        Exercise test web sources prefs create msprefs has same default shape through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_prefs.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.prefs import MSPREFS_DEFAULTS, create_msprefs

    cfg = create_msprefs()
    assert set(cfg.defaults) >= set(MSPREFS_DEFAULTS)
    assert cfg.defaults["max_tags"] == 20
    assert isinstance(cfg.defaults["id_link_rules"], dict)
