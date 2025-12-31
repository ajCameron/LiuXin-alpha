from __future__ import annotations

import io
import sys
import types


def _install_clint_stubs() -> None:
    """terminal.py depends on `clint`, which isn't a strict runtime dependency.

    For unit testing, we provide minimal stubs so the module can import.
    """
    clint = types.ModuleType("clint")
    textui = types.ModuleType("clint.textui")
    colored = types.SimpleNamespace(green=lambda s: s)

    def puts(s: str) -> None:
        # Behaves like clint.textui.puts: print without extra formatting.
        sys.stdout.write(str(s) + "\n")

    textui.colored = colored  # type: ignore[attr-defined]
    textui.puts = puts  # type: ignore[attr-defined]
    packages = types.ModuleType("clint.packages")
    six = types.ModuleType("clint.packages.six")
    six.text_type = str  # type: ignore[attr-defined]

    sys.modules.setdefault("clint", clint)
    sys.modules.setdefault("clint.textui", textui)
    sys.modules.setdefault("clint.textui.colored", textui)  # accessed via from ... import colored
    sys.modules.setdefault("clint.packages", packages)
    sys.modules.setdefault("clint.packages.six", six)


def test_ansi_stream_strips_escape_sequences_when_not_tty(monkeypatch) -> None:
    _install_clint_stubs()

    import importlib

    # Import after installing stubs.
    import LiuXin_alpha.utils.terminal as term

    importlib.reload(term)

    buf = io.StringIO()

    class _Stream(io.StringIO):
        def isatty(self) -> bool:  # noqa: D401 - simple override
            return False

    raw = _Stream()
    s = term.ANSIStream(raw)
    colored_text = term.colored("hi", fg="red")
    s.write(colored_text)
    assert raw.getvalue() == "hi"  # escape sequences stripped


def test_ansi_stream_passthrough_when_tty(monkeypatch) -> None:
    _install_clint_stubs()

    import importlib
    import LiuXin_alpha.utils.terminal as term

    importlib.reload(term)

    class _Stream(io.StringIO):
        def isatty(self) -> bool:
            return True

    raw = _Stream()
    s = term.ANSIStream(raw)
    colored_text = term.colored("hi", fg="red")
    s.write(colored_text)
    assert "hi" in raw.getvalue()
    # Contains an escape prefix
    assert "\x1b" in raw.getvalue() or "\033" in raw.getvalue()