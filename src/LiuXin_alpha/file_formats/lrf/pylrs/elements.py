"""
Define reusable XML element descriptors for LRS document construction.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise elements through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
"""
from __future__ import annotations

import typing as _typing

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class ElementWriter(object):
    """
    Provide the elementwriter contract for validated ebook processing.

    Example:
        Exercise ElementWriter through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(
        self: _typing.Self,
        e: _typing.Any,
        header: bool = False,
        sourceEncoding: str = "ascii",
        spaceBeforeClose: bool = True,
        outputEncodingName: str = "UTF-16",
    ) -> None:
        """
        Initialize and validate the elementwriter state.

        Example:
            Exercise ElementWriter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param e: Value supplied for e under the utility contract.
        :param header: Value supplied for header under the utility contract.
        :param sourceEncoding: Value supplied for sourceEncoding under the utility contract.
        :param spaceBeforeClose: Value supplied for spaceBeforeClose under the utility
            contract.
        :param outputEncodingName: Value supplied for outputEncodingName under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.header = header
        self.e = e
        self.sourceEncoding = sourceEncoding
        self.spaceBeforeClose = spaceBeforeClose
        self.outputEncodingName = outputEncodingName

    def _encodeCdata(self: _typing.Self, rawText: _typing.Any) -> _typing.Any:
        """
        Perform the encodeCdata operation under explicit file-format and conversion rules.

        Example:
            Exercise ElementWriter. encodeCdata through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param rawText: Value supplied for rawText under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(rawText, (bytes, bytearray, memoryview)):
            rawText = bytes(rawText).decode(self.sourceEncoding, "replace")
        elif not isinstance(rawText, str):
            rawText = six_unicode(rawText)

        text = rawText.replace("&", "&amp;")
        text = text.replace("<", "&lt;")
        text = text.replace(">", "&gt;")
        return text

    def _writeAttribute(self: _typing.Self, f: _typing.Any, name: _typing.Any, value: _typing.Any) -> None:
        """
        Perform the writeAttribute operation under explicit file-format and conversion rules.

        Example:
            Exercise ElementWriter. writeAttribute through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param f: Value supplied for f under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write(' %s="' % six_unicode(name))
        if not isinstance(value, six_string_types):
            value = six_unicode(value)
        value = self._encodeCdata(value)
        value = value.replace('"', "&quot;")
        f.write(value)
        f.write('"')

    def _writeText(self: _typing.Self, f: _typing.Any, rawText: _typing.Any) -> None:
        """
        Perform the writeText operation under explicit file-format and conversion rules.

        Example:
            Exercise ElementWriter. writeText through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param f: Value supplied for f under the utility contract.
        :param rawText: Value supplied for rawText under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        text = self._encodeCdata(rawText)
        f.write(text)

    def _write(self: _typing.Self, f: _typing.Any, e: _typing.Any) -> None:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise ElementWriter. write through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param f: Value supplied for f under the utility contract.
        :param e: Value supplied for e under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the toString operation under explicit file-format and conversion rules.

        Example:
            Exercise ElementWriter.toString through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        class x:
            """
            Provide the x contract for validated ebook processing.

            Example:
                Exercise ElementWriter.toString.x through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
            """
            pass

        buffer = []
        x.write = buffer.append
        self.write(x)
        return "".join(buffer)

    def write(self: _typing.Self, f: _typing.Any) -> None:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise ElementWriter.write through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.header:
            f.write('<?xml version="1.0" encoding="%s"?>\n' % self.outputEncodingName)

        self._write(f, self.e)
