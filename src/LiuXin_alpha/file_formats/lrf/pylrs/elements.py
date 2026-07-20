""" elements.py -- replacements and helpers for ElementTree """
from __future__ import annotations

import typing as _typing

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class ElementWriter(object):
    def __init__(
        self: _typing.Self,
        e: _typing.Any,
        header: bool = False,
        sourceEncoding: str = "ascii",
        spaceBeforeClose: bool = True,
        outputEncodingName: str = "UTF-16",
    ) -> None:
        self.header = header
        self.e = e
        self.sourceEncoding = sourceEncoding
        self.spaceBeforeClose = spaceBeforeClose
        self.outputEncodingName = outputEncodingName

    def _encodeCdata(self: _typing.Self, rawText: _typing.Any) -> _typing.Any:
        if isinstance(rawText, (bytes, bytearray, memoryview)):
            rawText = bytes(rawText).decode(self.sourceEncoding, "replace")
        elif not isinstance(rawText, str):
            rawText = six_unicode(rawText)

        text = rawText.replace("&", "&amp;")
        text = text.replace("<", "&lt;")
        text = text.replace(">", "&gt;")
        return text

    def _writeAttribute(self: _typing.Self, f: _typing.Any, name: _typing.Any, value: _typing.Any) -> None:
        f.write(' %s="' % six_unicode(name))
        if not isinstance(value, six_string_types):
            value = six_unicode(value)
        value = self._encodeCdata(value)
        value = value.replace('"', "&quot;")
        f.write(value)
        f.write('"')

    def _writeText(self: _typing.Self, f: _typing.Any, rawText: _typing.Any) -> None:
        text = self._encodeCdata(rawText)
        f.write(text)

    def _write(self: _typing.Self, f: _typing.Any, e: _typing.Any) -> None:
        f.write("<" + six_unicode(e.tag))

        attributes = sorted(e.items())
        for name, value in attributes:
            self._writeAttribute(f, name, value)

        if e.text is not None or len(e) > 0:
            f.write(">")

            if e.text:
                self._writeText(f, e.text)

            for e2 in e:
                self._write(f, e2)

            f.write("</%s>" % e.tag)
        else:
            if self.spaceBeforeClose:
                f.write(" ")
            f.write("/>")

        if e.tail is not None:
            self._writeText(f, e.tail)

    def toString(self: _typing.Self) -> _typing.Any:
        class x:
            pass

        buffer = []
        x.write = buffer.append
        self.write(x)
        return "".join(buffer)

    def write(self: _typing.Self, f: _typing.Any) -> None:
        if self.header:
            f.write('<?xml version="1.0" encoding="%s"?>\n' % self.outputEncodingName)

        self._write(f, self.e)
