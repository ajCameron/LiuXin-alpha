#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Convert LIT content into the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lit input through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations, with_statement

import typing as _typing

from LiuXin_alpha.customize.conversion import InputFormatPlugin

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LITInput(InputFormatPlugin):

    """
    Convert litinput sources into the normalized OEB pipeline model.

    Example:
        Exercise LITInput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "LIT Input"
    author = "Marshall T. Vandegrift"
    description = "Convert LIT files to HTML"
    file_types = {"lit"}

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:

        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise LITInput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.conversion.plumber import create_oebbook
        from LiuXin_alpha.file_formats.lit.reader import LitReader

        self.log = log
        return create_oebbook(log, stream, options, reader=LitReader)

    def postprocess_book(self: _typing.Self, oeb: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:

        """
        Perform the postprocess book operation under explicit file-format and conversion rules.

        Example:
            Exercise LITInput.postprocess book through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from LiuXin_alpha.file_formats.oeb.base import XHTML, XHTML_NS, XPath

        for item in oeb.spine:
            root = item.data
            if not hasattr(root, "xpath"):
                continue
            for bad in ("metadata", "guide"):
                metadata = XPath("//h:" + bad)(root)
                if metadata:
                    for x in metadata:
                        x.getparent().remove(x)
            body = XPath("//h:body")(root)
            if body:
                body = body[0]
                if len(body) == 1 and body[0].tag == XHTML("pre"):
                    pre = body[0]
                    import copy

                    from LiuXin_alpha.file_formats.txt.processor import (
                        convert_basic,
                        separate_paragraphs_single_line,
                    )
                    from LiuXin_alpha.utils.libraries.calibre_chardet import (
                        xml_to_unicode,
                    )
                    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

                    html = separate_paragraphs_single_line(pre.text)
                    html = convert_basic(html).replace("<html>", '<html xmlns="%s">' % XHTML_NS)
                    html = xml_to_unicode(html, strip_encoding_pats=True, resolve_entities=True)[0]
                    root = etree.fromstring(html)
                    body = XPath("//h:body")(root)
                    pre.tag = XHTML("div")
                    pre.text = ""
                    for elem in body:
                        ne = copy.deepcopy(elem)
                        pre.append(ne)
