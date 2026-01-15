#!/usr/bin/env  python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import print_function

from struct import pack

from LiuXin.utils.libraries.KindleUnpack.lib.mobi_uncompress import PalmdocReader
from LiuXin.utils.logger import default_log
from LiuXin.utils.libraries.mobi_python.mobi.lz77 import uncompress_lz77
from LiuXin.utils.plugins import plugins

# Py2/Py3
from LiuXin.utils.lx_libraries.liuxin_six import memory_range
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

# Tries to load the plugin from the plugins store - if this fails falls back to the slower pure python method
c_import = False
if plugins.plugin_okay("cPalmdoc"):
    cPalmdoc, cPalmdoc_err = plugins["cPalmdoc"]
    c_import = True
else:
    err_str = default_log.info("cPalmdoc couldn't be loaded - falling back to pure Python implementation.")


def decompress_doc(data):
    """
    Takes a stream of data. Decompresses it. Either using an algorithm in cPalmdoc, or using a Python implementation if
    that isn't available.
    :param data: The data string to be decompressed
    :return decompressed_data:
    """
    if not c_import:
        return uncompress_lz77(data)
    else:
        return cPalmdoc.decompress(data)


def compress_doc(data):
    if not data:
        return ""
    if not c_import:
        return py_compress_doc(data)
    else:
        return cPalmdoc.compress(data)


def test():
    tests = [
        "abc\x03\x04\x05\x06ms",  # Test binary writing
        "a b c \xfed ",  # Test encoding of spaces
        "0123456789axyz2bxyz2cdfgfo9iuyerh",
        "0123456789asd0123456789asd|yyzzxxffhhjjkk",
        (
            "ciewacnaq eiu743 r787q 0w%  ; sa fd\xef\ffdxosac wocjp acoiecowei "
            "owaic jociowapjcivcjpoivjporeivjpoavca; p9aw8743y6r74%$^$^%8 "
        ),
    ]

    for test in tests:
        print("Test:", repr(test))
        print("\tTesting compression...")
        if cPalmdoc:
            print("cPalmdoc has loaded.")
        good = py_compress_doc(test)
        x = compress_doc(test)
        print("\t\tgood:", repr(good))
        print("\t\tx   :", repr(x))
        assert x == good
        print("\tTesting decompression...")
        print("\t\t", repr(decompress_doc(x)))
        assert decompress_doc(x) == test
        print()


def py_compress_doc(data):
    """
    Python implementation of the lz77 algorithm.
    This will be incredibly slow.
    :param data:
    :return:
    """
    out = six_cStringIO()
    i = 0
    ldata = len(data)
    while i < ldata:
        if i > 10 and (ldata - i) > 10:
            chunk = ""
            match = -1
            for j in memory_range(10, 2, -1):
                chunk = data[i : i + j]
                try:
                    match = data.rindex(chunk, 0, i)
                except ValueError:
                    continue
                if (i - match) <= 2047:
                    break
                match = -1
            if match >= 0:
                n = len(chunk)
                m = i - match
                code = 0x8000 + ((m << 3) & 0x3FF8) + (n - 3)
                out.write(pack(">H", code))
                i += n
                continue
        ch = data[i]
        och = ord(ch)
        i += 1
        if ch == " " and (i + 1) < ldata:
            onch = ord(data[i])
            if onch >= 0x40 and onch < 0x80:
                out.write(pack(">B", onch ^ 0x80))
                i += 1
                continue
        if och == 0 or (och > 8 and och < 0x80):
            out.write(ch)
        else:
            j = i
            binseq = [ch]
            while j < ldata and len(binseq) < 8:
                ch = data[j]
                och = ord(ch)
                if och == 0 or (och > 8 and och < 0x80):
                    break
                binseq.append(ch)
                j += 1
            out.write(pack(">B", len(binseq)))
            out.write("".join(binseq))
            i += len(binseq) - 1
    return out.getvalue()
