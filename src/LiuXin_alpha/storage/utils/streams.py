"""Provide dependency-free binary stream adapters shared across storage layers.

These helpers do not know about configured Stores, raw drivers, catalogue values,
or ownership policy. Callers remain responsible for closing streams they create.
"""

from __future__ import annotations

import io

from typing import BinaryIO


def bytes_stream(data: bytes) -> BinaryIO:
    """
    Wrap an in-memory payload in a fresh binary stream.

    Example:
        >>> bytes_stream(b"book").read()
        b'book'


    :param data: Immutable in-memory payload accepted by ``io.BytesIO``.
    :return: New seekable stream positioned at the beginning of the payload.
    """

    return io.BytesIO(data)


__all__ = ["bytes_stream"]
