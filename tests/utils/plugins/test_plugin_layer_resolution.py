"""
Provide test plugin layer resolution utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test plugin layer resolution through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest


class FakeWandImage:
    """
    Provide the FakeWandImage utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FakeWandImage through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    def __init__(self, *args, **kwargs):
        """
        Initialize and validate the FakeWandImage state.

        Example:
            Exercise FakeWandImage.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        pass

    def make_blob(self, *args, **kwargs) -> bytes:
        """
        Perform the make blob utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.make blob through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return b"ok"

    def close(self) -> None:
        """
        Forward the close operation while preserving adapter ownership rules.

        Example:
            Exercise FakeWandImage.close through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


def _install_fake_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the install fake wand utility operation under explicit compatibility rules.

    Example:
        Exercise  install fake wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    wand_mod = ModuleType("wand")
    wand_image_mod = ModuleType("wand.image")
    wand_image_mod.Image = FakeWandImage  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "wand", wand_mod)
    monkeypatch.setitem(sys.modules, "wand.image", wand_image_mod)


def _uninstall_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the uninstall wand utility operation under explicit compatibility rules.

    Example:
        Exercise  uninstall wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.delitem(sys.modules, "wand", raising=False)
    monkeypatch.delitem(sys.modules, "wand.image", raising=False)


def test_prefers_alpha_when_wand_works(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Perform the test prefers alpha when wand works utility operation under explicit compatibility rules.

    Example:
        Exercise test prefers alpha when wand works through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test falls back to beta when no wand but cli available utility operation under explicit compatibility rules.

    Example:
        Exercise test falls back to beta when no wand but cli available through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
