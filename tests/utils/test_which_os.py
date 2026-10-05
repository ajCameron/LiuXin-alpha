"""
Provide test which os utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test which os through a consuming regression::

        python -m pytest -q tests/utils/test_which_os.py
"""
from __future__ import annotations

import importlib


def test_which_os_flags_change_with_platform(monkeypatch) -> None:
    # Reload the module after patching sys.platform, since flags are computed at import-time.
    """
    Perform the test which os flags change with platform utility operation under explicit compatibility rules.

    Example:
        Exercise test which os flags change with platform through a consuming regression::

            python -m pytest -q tests/utils/test_which_os.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.utils.which_os as mod

    monkeypatch.setattr(mod.sys, "platform", "win32")
    importlib.reload(mod)
    assert mod.iswindows is True
    assert mod.isosx is False

    monkeypatch.setattr(mod.sys, "platform", "darwin")
    importlib.reload(mod)
    assert mod.isosx is True
    assert mod.iswindows is False
