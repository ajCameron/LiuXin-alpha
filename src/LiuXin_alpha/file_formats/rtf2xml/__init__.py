"""
Expose the supported rtf2xml compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import annotations

import typing as _typing
def open_for_read(path: _typing.Any) -> _typing.Any:
    """
    Open a path for reading.

    Example:
        Exercise open for read through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return open(path, encoding="utf-8", errors="replace")


def open_for_write(path: _typing.Any, append: bool = False) -> _typing.Any:
    """
    Perform the open for write operation under explicit file-format and conversion rules.

    Example:
        Exercise open for write through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param append: Value supplied for append under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    mode = "a" if append else "w"
    return open(path, mode, encoding="utf-8", errors="replace", newline="")
