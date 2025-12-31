from __future__ import annotations

import importlib


def test_which_os_flags_change_with_platform(monkeypatch) -> None:
    # Reload the module after patching sys.platform, since flags are computed at import-time.
    import LiuXin_alpha.utils.which_os as mod

    monkeypatch.setattr(mod.sys, "platform", "win32")
    importlib.reload(mod)
    assert mod.iswindows is True
    assert mod.isosx is False

    monkeypatch.setattr(mod.sys, "platform", "darwin")
    importlib.reload(mod)
    assert mod.isosx is True
    assert mod.iswindows is False
