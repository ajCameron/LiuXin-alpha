from __future__ import annotations


def test_web_sources_prefs_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.prefs as prefs

    assert prefs is not None


def test_web_sources_prefs_defaults_are_registered() -> None:
    from LiuXin_alpha.metadata.web_sources.prefs import MSPREFS_DEFAULTS, msprefs

    for key, expected in MSPREFS_DEFAULTS.items():
        assert key in msprefs.defaults
        assert msprefs.defaults[key] == expected


def test_web_sources_prefs_create_msprefs_has_same_default_shape() -> None:
    from LiuXin_alpha.metadata.web_sources.prefs import MSPREFS_DEFAULTS, create_msprefs

    cfg = create_msprefs()
    assert set(cfg.defaults) >= set(MSPREFS_DEFAULTS)
    assert cfg.defaults["max_tags"] == 20
    assert isinstance(cfg.defaults["id_link_rules"], dict)
