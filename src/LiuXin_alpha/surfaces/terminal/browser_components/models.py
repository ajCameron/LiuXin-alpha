"""
Define portable row/readline capabilities and terminal cursor/completion values.

These contracts do not import the browser composition root. History path
selection returns a location only; session ownership controls directory creation
and best-effort persistence.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class RowRecord(Protocol):
    """
    Describe the column lookup shared by Core rows and legacy row objects.

    Consumers can check for a column before reading it without requiring a full
    mapping interface or assuming a particular database row implementation.

    Example:
        >>> row: RowRecord = {'work_id': 7}
        >>> 'work_id' in row
        True
    """

    def __contains__(self, column: str, /) -> bool:
        """
        Report whether this row exposes the requested column.

        Example:
            >>> 'title' in {'title': 'A shared library'}
            True


        :param column: Exact column key understood by the row implementation.
        :return: Whether the column is available for indexed access.
        """
        ...

    def __getitem__(self, column: str, /) -> object:
        """
        Read one column without coercing its stored value to display text.

        Example:
            >>> {'work_id': 7}['work_id']
            7


        :param column: Exact key of the column to retrieve.
        :return: The column value, retaining its backend-provided Python type.
        :raises LookupError: If the row implementation cannot resolve the key.
        """
        ...


class ReadlineBackend(Protocol):
    """
    Describe the optional readline operations needed by browser sessions.

    Editing configuration is probed separately because implementations differ.
    Session owners catch history I/O failures instead of making readline or a
    writable history directory a prerequisite for browsing.

    Example:
        >>> backend.add_history('help')  # doctest: +SKIP
    """

    def add_history(self, line: str, /) -> None:
        """
        Append one entered command to in-memory input history.

        Example:
            >>> backend.add_history('show works')  # doctest: +SKIP


        :param line: Command text without the input stream's trailing newline.
        :return: ``None`` after recording the command.
        """
        ...

    def read_history_file(self, filename: str, /) -> None:
        """
        Load commands from the backend's persistent history format.

        Example:
            >>> backend.read_history_file('terminal_history')  # doctest: +SKIP


        :param filename: Filesystem path to the history file to read.
        :return: ``None`` after loading history into the backend.
        :raises OSError: If the history file cannot be read.
        """
        ...

    def write_history_file(self, filename: str, /) -> None:
        """
        Persist the backend's current command history to a file.

        Example:
            >>> backend.write_history_file('terminal_history')  # doctest: +SKIP


        :param filename: Destination path; its parent is prepared by the session owner.
        :return: ``None`` after writing the history file.
        :raises OSError: If the destination cannot be written.
        """
        ...

    def get_line_buffer(self) -> str:
        """
        Return the complete command currently being edited.

        Example:
            >>> buffer = backend.get_line_buffer()  # doctest: +SKIP


        :return: Current editing-buffer text, including text after the cursor.
        """
        ...

    def get_endidx(self) -> int:
        """
        Locate the end of the text involved in the active completion request.

        Example:
            >>> cursor = backend.get_endidx()  # doctest: +SKIP


        :return: Backend-reported completion end index in the current line buffer.
        """
        ...


def default_history_file_path() -> Path:
    """
    Return the default on-disk command history path for the terminal browser.

    Preference order:

    - ``$XDG_STATE_HOME/liuxin_alpha/terminal_history``
    - ``~/.local/state/liuxin_alpha/terminal_history``

    The environment override is trimmed and user-home markers are expanded.
    No directories or files are created by this lookup.

    Example:
        >>> default_history_file_path().name
        'terminal_history'


    :return: History file path beneath the selected user-state directory.
    """
    xdg_state_home = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg_state_home:
        base = Path(xdg_state_home).expanduser()
    else:
        base = Path.home() / ".local" / "state"
    return base / "liuxin_alpha" / "terminal_history"


@dataclass
class BrowseWindow:
    """
    Store the active table and mutable paging position for terminal browsing.

    Example:
        >>> BrowseWindow('works', limit=20).offset
        0

    :ivar table: Resolved table token whose rows are being browsed.
    :ivar limit: Requested number of rows in a page.
    :ivar offset: Zero-based starting row position for the current page.
    """

    table: str
    limit: int
    offset: int = 0


@dataclass(frozen=True)
class CommandCompletion:
    """
    Describe candidate replacements for one token in an editing buffer.

    The half-open token span lets input owners replace only the completed token
    while retaining surrounding command text.

    Example:
        >>> CommandCompletion(0, 2, 'he', ('help',)).candidates
        ('help',)

    :ivar token_start: First buffer character belonging to the token.
    :ivar token_end: First character after the token's replacement span.
    :ivar prefix: Token text before the completion cursor used for matching.
    :ivar candidates: Ordered full candidate tokens, rather than suffixes alone.
    """

    token_start: int
    token_end: int
    prefix: str
    candidates: tuple[str, ...]
