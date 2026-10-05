#! /usr/bin/env python
# coding: utf-8

"""
Read DjVu structure, text and metadata through the retained document interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise djvu through a consuming regression::

        python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

# this code is based on:
# Lizardtech DjVu Reference
# DjVu v3
# November 2005

import sys
import struct
from collections.abc import Callable
from typing import BinaryIO, cast

from LiuXin_alpha.utils.plugins import plugins

__license__ = "GPL v3"
__copyright__ = "2011, Anthon van der Neut <A.van.der.Neut@ruamel.eu>"


class DjvuChunk(object):
    """
    Provide the djvuchunk contract for validated ebook processing.

    Example:
        Exercise DjvuChunk through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
    """
    def __init__(
        self: _typing.Self,
        buf: bytes,
        start: int,
        end: int,
        align: bool = True,
        bigendian: bool = True,
        inclheader: bool = False,
        verbose: int = 0,
    ) -> None:

        # Load the DJVU BZZ decompressor (compiled extension or pure-Python fallback).
        """
        Initialize and validate the djvuchunk state.

        Example:
            Exercise DjvuChunk.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


        :param buf: Value supplied for buf under the utility contract.
        :param start: Value supplied for start under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :param align: Value supplied for align under the utility contract.
        :param bigendian: Value supplied for bigendian under the utility contract.
        :param inclheader: Value supplied for inclheader under the utility contract.
        :param verbose: Value supplied for verbose under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        speedup, err_msg = plugins["bzzdec"]
        if speedup is None:
            raise RuntimeError("Failed to load bzzdec plugin: %s" % err_msg)
        if not hasattr(speedup, "decompress"):
            raise RuntimeError("bzzdec plugin is missing required 'decompress' callable")
        self.speedup: object = speedup

        self.subtype: bytes | None = None
        self._subchunks: list[DjvuChunk] = []
        self.buf = buf
        pos = start + 4
        self.type = buf[start:pos]
        self.align = align  # whether to align to word (2-byte) boundaries
        self.headersize = 0 if inclheader else 8
        if bigendian:
            self.strflag = b">"
        else:
            self.strflag = b"<"
        oldpos, pos = pos, pos + 4
        self.size = struct.unpack(self.strflag + b"L", buf[oldpos:pos])[0]
        self.dataend = pos + self.size - (8 if inclheader else 0)
        if self.type == b"FORM":
            oldpos, pos = pos, pos + 4
            # print oldpos, pos
            self.subtype = buf[oldpos:pos]
            # self.headersize += 4
        self.datastart = pos
        if verbose > 0:
            print("found", self.type, self.subtype, pos, self.size)
        if self.type in b"FORM".split():
            if verbose > 0:
                print("processing substuff %d %d (%x)" % (pos, self.dataend, self.dataend))
            numchunks = 0
            while pos < self.dataend:
                x = DjvuChunk(buf, pos, start + self.size, verbose=verbose)
                numchunks += 1
                self._subchunks.append(x)
                newpos = pos + x.size + x.headersize + (1 if (x.size % 2) else 0)
                if verbose > 0:
                    print("newpos %d %d (%x, %x) %d" % (newpos, self.dataend, newpos, self.dataend, x.headersize))
                pos = newpos
            if verbose > 0:
                print("                  end of chunk %d (%x)" % (pos, pos))

    def dump(
        self: _typing.Self,
        verbose: int = 0,
        indent: int = 1,
        out: BinaryIO | None = None,
        txtout: BinaryIO | None = None,
        maxlevel: int = 100,
    ) -> None:
        """
        Perform the dump operation under explicit file-format and conversion rules.

        Example:
            Exercise DjvuChunk.dump through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


        :param verbose: Value supplied for verbose under the utility contract.
        :param indent: Value supplied for indent under the utility contract.
        :param out: Value supplied for out under the utility contract.
        :param txtout: Value supplied for txtout under the utility contract.
        :param maxlevel: Value supplied for maxlevel under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if out:
            out.write(b"  " * indent)
            out.write(b"%s%s [%d]\n" % (self.type, b":" + self.subtype if self.subtype else b"", self.size))
        if txtout and self.type == b"TXTz":
            decompress = cast(
                Callable[[bytes], bytes],
                getattr(self.speedup, "decompress"),
            )
            txtout.write(decompress(self.buf[self.datastart : self.dataend]))
            txtout.write(b"\037")
        if txtout and self.type == b"TXTa":
            res = self.buf[self.datastart : self.dataend]
            if len(res) < 3:
                raise ValueError("TXTa block missing length header")
            l = int.from_bytes(res[:3], byteorder="big")
            if verbose > 0 and out:
                out.write(f"{l}\n".encode("ascii"))
            txtout.write(res[3 : 3 + l])
            txtout.write(b"\037")
        if indent >= maxlevel:
            return
        for schunk in self._subchunks:
            schunk.dump(verbose=verbose, indent=indent + 1, out=out, txtout=txtout)


class DJVUFile(object):
    """
    Provide the djvufile contract for validated ebook processing.

    Example:
        Exercise DJVUFile through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
    """
    def __init__(
        self: _typing.Self,
        instream: BinaryIO,
        verbose: int = 0,
    ) -> None:
        """
        Initialize and validate the djvufile state.

        Example:
            Exercise DJVUFile.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


        :param instream: Value supplied for instream under the utility contract.
        :param verbose: Value supplied for verbose under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.instream = instream
        buf = self.instream.read(4)
        assert buf == b"AT&T"
        buf = self.instream.read()
        self.dc = DjvuChunk(buf, 0, len(buf), verbose=verbose)

    def get_text(
        self: _typing.Self,
        outfile: BinaryIO | None = None,
    ) -> None:
        """
        Return text under the format's safety and compatibility rules.

        Example:
            Exercise DJVUFile.get text through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


        :param outfile: Value supplied for outfile under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.dc.dump(txtout=outfile)

    def dump(
        self: _typing.Self,
        outfile: BinaryIO | None = None,
        maxlevel: int = 0,
    ) -> None:
        """
        Perform the dump operation under explicit file-format and conversion rules.

        Example:
            Exercise DJVUFile.dump through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


        :param outfile: Value supplied for outfile under the utility contract.
        :param maxlevel: Value supplied for maxlevel under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.dc.dump(out=outfile, maxlevel=maxlevel)


def main() -> None:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with open(sys.argv[-1], "rb") as in_file:
        f = DJVUFile(in_file)
        f.get_text(sys.stdout.buffer)


if __name__ == "__main__":
    main()
