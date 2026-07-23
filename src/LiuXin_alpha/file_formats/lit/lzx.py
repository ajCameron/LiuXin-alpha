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
    def __init__(self: _typing.Self, wbits: _typing.Any) -> None:
        self.wbits = wbits
        self.blocksize = 1 << wbits
        _lzx.init(wbits)

    def decompress(self: _typing.Self, data: _typing.Any, outlen: _typing.Any) -> _typing.Any:
        return _lzx.decompress(data, outlen)

    def reset(self: _typing.Self) -> _typing.Any:
        return _lzx.reset()
