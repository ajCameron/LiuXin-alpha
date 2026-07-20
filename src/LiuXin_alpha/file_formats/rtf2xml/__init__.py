from __future__ import annotations

import typing as _typing
def open_for_read(path: _typing.Any) -> _typing.Any:
    """
    Open a path for reading.

    :param path:
    :return:
    """
    return open(path, encoding="utf-8", errors="replace")


def open_for_write(path: _typing.Any, append: bool = False) -> _typing.Any:
    mode = "a" if append else "w"
    return open(path, mode, encoding="utf-8", errors="replace", newline="")
