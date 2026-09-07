"""Portable browser cursor and completion values, plus the default history location."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class RowRecord(Protocol):
    """Column membership and access shared by Core rows and legacy row objects."""

    def __contains__(self, column: str, /) -> bool: ...

    def __getitem__(self, column: str, /) -> object: ...


class ReadlineBackend(Protocol):
    """History and input inspection used when an optional readline backend exists."""

    def add_history(self, line: str, /) -> None: ...

    def read_history_file(self, filename: str, /) -> None: ...

    def write_history_file(self, filename: str, /) -> None: ...

    def get_line_buffer(self) -> str: ...

    def get_endidx(self) -> int: ...


def default_history_file_path() -> Path:
    """
    Return the default on-disk command history path for the terminal browser.

    Preference order:
    - ``$XDG_STATE_HOME/liuxin_alpha/terminal_history``
    - ``~/.local/state/liuxin_alpha/terminal_history``
    """
    xdg_state_home = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg_state_home:
        base = Path(xdg_state_home).expanduser()
    else:
        base = Path.home() / ".local" / "state"
    return base / "liuxin_alpha" / "terminal_history"


@dataclass
class BrowseWindow:
    """Stores the current browsing table and paging cursor."""

    table: str
    limit: int
    offset: int = 0


@dataclass(frozen=True)
class CommandCompletion:
    """One token-completion result for the current input buffer."""

    token_start: int
    token_end: int
    prefix: str
    candidates: tuple[str, ...]
