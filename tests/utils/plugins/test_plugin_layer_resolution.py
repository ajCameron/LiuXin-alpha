from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest


class FakeWandImage:
    def __init__(self, *args, **kwargs):
        pass

    def make_blob(self, *args, **kwargs) -> bytes:
        return b"ok"

    def close(self) -> None:
        pass


def _install_fake_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    wand_mod = ModuleType("wand")
    wand_image_mod = ModuleType("wand.image")
    wand_image_mod.Image = FakeWandImage  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "wand", wand_mod)
    monkeypatch.setitem(sys.modules, "wand.image", wand_image_mod)


def _uninstall_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "wand", raising=False)
    monkeypatch.delitem(sys.modules, "wand.image", raising=False)


def test_prefers_alpha_when_wand_works(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LIUXIN_PLUGIN_CACHE_PATH", str(tmp_path / "plugin_selection.json"))
    _install_fake_wand(monkeypatch)

    # Make sure beta doesn't accidentally probe as available in this environment
    import shutil
    monkeypatch.setattr(shutil, "which", lambda *_a, **_k: None)

    from LiuXin_alpha.utils.plugins import plugins

    # Reset memoized loads between tests (singleton survives across tests)
    plugins._loaded.clear()  # type: ignore[attr-defined]

    mod, err = plugins["magick"]
    assert mod is not None
    assert mod.__name__.endswith(".fallbacks.fallback_alpha.magick")
    assert err is None or "Loaded fallback" in err

    mod2, _ = plugins["imageops"]
    assert mod2 is not None
    assert mod2.__name__.endswith(".fallbacks.fallback_alpha.imageops")


def test_falls_back_to_beta_when_no_wand_but_cli_available(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LIUXIN_PLUGIN_CACHE_PATH", str(tmp_path / "plugin_selection.json"))
    _uninstall_wand(monkeypatch)

    import shutil
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/magick" if name in ("magick", "convert", "identify") else None)

    import subprocess
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: type("CP", (), {"returncode": 0, "stdout": b"", "stderr": b""})())

    from LiuXin_alpha.utils.plugins import plugins

    # Reset memoized loads between tests (singleton survives across tests)
    plugins._loaded.clear()  # type: ignore[attr-defined]

    mod, _ = plugins["magick"]
    assert mod is not None
    assert mod.__name__.endswith(".fallbacks.fallback_beta.magick")

    mod2, _ = plugins["imageops"]
    assert mod2 is not None
    assert mod2.__name__.endswith(".fallbacks.fallback_beta.imageops")
