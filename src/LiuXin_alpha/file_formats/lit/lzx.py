"""
Decode LZX-compressed blocks used by Microsoft LIT containers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lzx through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

"""
LZX compression/decompression wrapper.
"""

from LiuXin_alpha.utils.plugins import plugins

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"


_lzx, _error = plugins["lzx"]
if _lzx is None:
    raise RuntimeError("Failed to load the lzx plugin: %s" % _error)

__all__ = ["Compressor", "Decompressor", "LZXError"]

LZXError = _lzx.LZXError
Compressor = _lzx.Compressor


class Decompressor(object):
    """
    Provide the decompressor contract for validated ebook processing.

    Example:
        Exercise Decompressor through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
    """
    def __init__(self: _typing.Self, wbits: _typing.Any) -> None:
        """
        Initialize and validate the decompressor state.

        Example:
            Exercise Decompressor.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :param wbits: Value supplied for wbits under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.wbits = wbits
        self.blocksize = 1 << wbits
        _lzx.init(wbits)

    def decompress(self: _typing.Self, data: _typing.Any, outlen: _typing.Any) -> _typing.Any:
        """
        Decode the supplied format payload and return its uncompressed bytes.

        Example:
            Exercise Decompressor.decompress through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param outlen: Value supplied for outlen under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return _lzx.decompress(data, outlen)

    def reset(self: _typing.Self) -> _typing.Any:
        """
        Perform the reset operation under explicit file-format and conversion rules.

        Example:
            Exercise Decompressor.reset through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return _lzx.reset()
