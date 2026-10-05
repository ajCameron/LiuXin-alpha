"""
Convert RTF input into normalized OEB content and resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise input through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

from lxml import etree

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


class InlineClass(etree.XSLTExtension):

    """
    Provide the inlineclass contract for validated ebook processing.

    Example:
        Exercise InlineClass through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py
    """
    FMTS = ("italics", "bold", "strike-through", "small-caps")

    def __init__(self: _typing.Self, log: _typing.Any) -> None:
        """
        Initialize and validate the inlineclass state.

        Example:
            Exercise InlineClass.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        etree.XSLTExtension.__init__(self)
        self.log = log
        self.font_sizes = []
        self.colors = []

    def execute(self: _typing.Self, context: _typing.Any, self_node: _typing.Any, input_node: _typing.Any, output_parent: _typing.Any) -> None:
        """
        Perform the execute operation under explicit file-format and conversion rules.

        Example:
            Exercise InlineClass.execute through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_modernized.py


        :param context: Value supplied for context under the utility contract.
        :param self_node: Value supplied for self node under the utility contract.
        :param input_node: Value supplied for input node under the utility contract.
        :param output_parent: Value supplied for output parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        classes = ["none"]
        for x in self.FMTS:
            if input_node.get(x, None) == "true":
                classes.append(x)

        # underlined is special
        if input_node.get("underlined", "false") != "false":
            classes.append("underlined")
        fs = input_node.get("font-size", False)
        if fs:
            if fs not in self.font_sizes:
                self.font_sizes.append(fs)
            classes.append("fs%d" % self.font_sizes.index(fs))
        fc = input_node.get("font-color", False)
        if fc:
            if fc not in self.colors:
                self.colors.append(fc)
            classes.append("col%d" % self.colors.index(fc))

        output_parent.text = " ".join(classes)
