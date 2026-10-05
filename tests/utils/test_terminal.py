"""
Provide test terminal utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test terminal through a consuming regression::

        python -m pytest -q tests/utils/test_terminal.py
"""
from __future__ import annotations

import io
import sys
import types


def _install_clint_stubs() -> None:
    """
    terminal.py depends on `clint`, which isn't a strict runtime dependency.

    Example:
        Exercise  install clint stubs through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    clint = types.ModuleType("clint")
    textui = types.ModuleType("clint.textui")
    colored = types.SimpleNamespace(green=lambda s: s)

    def puts(s: str) -> None:
        # Behaves like clint.textui.puts: print without extra formatting.
        """
        Perform the puts utility operation under explicit compatibility rules.

        Example:
            Exercise  install clint stubs.puts through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param s: Value supplied for s under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
    """
    Perform the test ansi stream strips escape sequences when not tty utility operation under explicit compatibility rules.

    Example:
        Exercise test ansi stream strips escape sequences when not tty through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    _install_clint_stubs()

    import importlib

    # Import after installing stubs.
    import LiuXin_alpha.utils.terminal as term

    importlib.reload(term)

    buf = io.StringIO()

    class _Stream(io.StringIO):
        """
        Provide the Stream utility contract with explicit state and cleanup behavior.

        Example:
            Exercise test ansi stream strips escape sequences when not tty. Stream through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py
        """
        def isatty(self) -> bool:  # noqa: D401 - simple override
            """
            Perform the isatty utility operation under explicit compatibility rules.

            Example:
                Exercise test ansi stream strips escape sequences when not tty. Stream.isatty through a consuming regression::

                    python -m pytest -q tests/utils/test_terminal.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return False

    raw = _Stream()
    s = term.ANSIStream(raw)
    colored_text = term.colored("hi", fg="red")
    s.write(colored_text)
    assert raw.getvalue() == "hi"  # escape sequences stripped


def test_ansi_stream_passthrough_when_tty(monkeypatch) -> None:
    """
    Perform the test ansi stream passthrough when tty utility operation under explicit compatibility rules.

    Example:
        Exercise test ansi stream passthrough when tty through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    _install_clint_stubs()

    import importlib
    import LiuXin_alpha.utils.terminal as term

    importlib.reload(term)

    class _Stream(io.StringIO):
        """
        Provide the Stream utility contract with explicit state and cleanup behavior.

        Example:
            Exercise test ansi stream passthrough when tty. Stream through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py
        """
        def isatty(self) -> bool:
            """
            Perform the isatty utility operation under explicit compatibility rules.

            Example:
                Exercise test ansi stream passthrough when tty. Stream.isatty through a consuming regression::

                    python -m pytest -q tests/utils/test_terminal.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return True

    raw = _Stream()
    s = term.ANSIStream(raw)
    colored_text = term.colored("hi", fg="red")
    s.write(colored_text)
    assert "hi" in raw.getvalue()
    # Contains an escape prefix
    assert "\x1b" in raw.getvalue() or "\033" in raw.getvalue()
