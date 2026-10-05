"""
Render fenced and indented code through optional syntax highlighting.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise codehilite through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
CodeHilite Extension for Python-Markdown
========================================

Adds code/syntax highlighting to standard Python-Markdown code blocks.

Copyright 2006-2008 [Waylan Limberg](http://achinghead.com/).

Project website: <http://packages.python.org/Markdown/extensions/code_hilite.html>
Contact: markdown@freewisdom.org

License: BSD (see ../LICENSE.md for details)

Dependencies:
* [Python 2.3+](http://python.org/)
* [Markdown 2.0+](http://packages.python.org/Markdown/)
* [Pygments](http://pygments.org/)

"""

from . import Extension
from ..treeprocessors import Treeprocessor
import warnings

try:
    from pygments import highlight
    from pygments.lexers import get_lexer_by_name, guess_lexer, TextLexer
    from pygments.formatters import HtmlFormatter

    pygments = True
except ImportError:
    pygments = False


# ------------------ The Main CodeHilite Class ----------------------
class CodeHilite(object):
    """
    Determine language of source code, and pass it into the pygments hilighter.

    Example:
        Exercise CodeHilite through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(
        self: _typing.Self,
        src: _typing.Any = None,
        linenums: _typing.Any = None,
        guess_lang: bool = True,
        css_class: str = "codehilite",
        lang: _typing.Any = None,
        style: str = "default",
        noclasses: bool = False,
        tab_length: int = 4,
    ) -> None:
        """
        Initialize and validate the codehilite state.

        Example:
            Exercise CodeHilite.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param src: Value supplied for src under the utility contract.
        :param linenums: Value supplied for linenums under the utility contract.
        :param guess_lang: Value supplied for guess lang under the utility contract.
        :param css_class: Value supplied for css class under the utility contract.
        :param lang: Value supplied for lang under the utility contract.
        :param style: Value supplied for style under the utility contract.
        :param noclasses: Value supplied for noclasses under the utility contract.
        :param tab_length: Value supplied for tab length under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.src = src
        self.lang = lang
        self.linenums = linenums
        self.guess_lang = guess_lang
        self.css_class = css_class
        self.style = style
        self.noclasses = noclasses
        self.tab_length = tab_length

    def hilite(self: _typing.Self) -> _typing.Any:
        """
        Pass code to the [Pygments](http://pygments.pocoo.org/) highliter with optional line numbers. The output should then be styled with css to your liking. No styles are applied by default - only styling hooks (i.e.: <span class="k">).

        Example:
            Exercise CodeHilite.hilite through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        self.src = self.src.strip("\n")

        if self.lang is None:
            self._getLang()

        if pygments:
            try:
                lexer = get_lexer_by_name(self.lang)
            except ValueError:
                try:
                    if self.guess_lang:
                        lexer = guess_lexer(self.src)
                    else:
                        lexer = TextLexer()
                except ValueError:
                    lexer = TextLexer()
            formatter = HtmlFormatter(
                linenos=self.linenums,
                cssclass=self.css_class,
                style=self.style,
                noclasses=self.noclasses,
            )
            return highlight(self.src, lexer, formatter)
        else:
            # just escape and build markup usable by JS highlighting libs
            txt = self.src.replace("&", "&amp;")
            txt = txt.replace("<", "&lt;")
            txt = txt.replace(">", "&gt;")
            txt = txt.replace('"', "&quot;")
            classes = []
            if self.lang:
                classes.append("language-%s" % self.lang)
            if self.linenums:
                classes.append("linenums")
            class_str = ""
            if classes:
                class_str = ' class="%s"' % " ".join(classes)
            return '<pre class="%s"><code%s>%s</code></pre>\n' % (
                self.css_class,
                class_str,
                txt,
            )

    def _getLang(self: _typing.Self) -> None:
        """
        Determines language of a code block from shebang line and whether said line should be removed or left in place. If the sheband line contains a path (even a single /) then it is assumed to be a real shebang line and left alone. However, if no path is given (e.i.: #!python or :::python) then it is assumed to be a mock shebang for language identifitation of a code fragment and removed from the code block prior to processing for code highlighting. When a mock shebang (e.i: #!python) is found, line numbering is turned on. When colons are found in place of a shebang (e.i.: :::python), line numbering is left in the current state - off by default.

        Example:
            Exercise CodeHilite. getLang through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        import re

        # split text into lines
        lines = self.src.split("\n")
        # pull first line to examine
        fl = lines.pop(0)

        c = re.compile(
            r"""
            (?:(?:^::+)|(?P<shebang>^[#]!))	# Shebang or 2 or more colons.
            (?P<path>(?:/\w+)*[/ ])?        # Zero or 1 path
            (?P<lang>[\w+-]*)               # The language
            """,
            re.VERBOSE,
        )
        # search first line for shebang
        m = c.search(fl)
        if m:
            # we have a match
            try:
                self.lang = m.group("lang").lower()
            except IndexError:
                self.lang = None
            if m.group("path"):
                # path exists - restore first line
                lines.insert(0, fl)
            if self.linenums is None and m.group("shebang"):
                # Overridable and Shebang exists - use line numbers
                self.linenums = True
        else:
            # No match
            lines.insert(0, fl)

        self.src = "\n".join(lines).strip("\n")


# ------------------ The Markdown Extension -------------------------------
class HiliteTreeprocessor(Treeprocessor):
    """
    Hilight source code in code blocks.

    Example:
        Exercise HiliteTreeprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def run(self: _typing.Self, root: _typing.Any) -> None:
        """
        Find code blocks and store in htmlStash.

        Example:
            Exercise HiliteTreeprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        blocks = root.iter("pre")
        for block in blocks:
            children = list(block)
            if len(children) == 1 and children[0].tag == "code":
                code = CodeHilite(
                    children[0].text,
                    linenums=self.config["linenums"],
                    guess_lang=self.config["guess_lang"],
                    css_class=self.config["css_class"],
                    style=self.config["pygments_style"],
                    noclasses=self.config["noclasses"],
                    tab_length=self.markdown.tab_length,
                )
                placeholder = self.markdown.htmlStash.store(code.hilite(), safe=True)
                # Clear codeblock in etree instance
                block.clear()
                # Change to p element which will later
                # be removed when inserting raw html
                block.tag = "p"
                block.text = placeholder


class CodeHiliteExtension(Extension):
    """
    Add source code hilighting to markdown codeblocks.

    Example:
        Exercise CodeHiliteExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, configs: _typing.Any) -> None:
        # define default configs
        """
        Initialize and validate the codehiliteextension state.

        Example:
            Exercise CodeHiliteExtension.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param configs: Value supplied for configs under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.config = {
            "linenums": [None, "Use lines numbers. True=yes, False=no, None=auto"],
            "force_linenos": [
                False,
                "Depreciated! Use 'linenums' instead. Force line numbers - Default: False",
            ],
            "guess_lang": [True, "Automatic language detection - Default: True"],
            "css_class": [
                "codehilite",
                "Set class name for wrapper <div> - Default: codehilite",
            ],
            "pygments_style": [
                "default",
                "Pygments HTML Formatter Style (Colorscheme) - Default: default",
            ],
            "noclasses": [
                False,
                "Use inline styles instead of CSS classes - Default false",
            ],
        }

        # Override defaults with user settings
        for key, value in configs:
            # convert strings to booleans
            if value == "True":
                value = True
            if value == "False":
                value = False
            if value == "None":
                value = None

            if key == "force_linenos":
                warnings.warn(
                    'The "force_linenos" config setting'
                    " to the CodeHilite extension is deprecrecated."
                    ' Use "linenums" instead.',
                    PendingDeprecationWarning,
                )
                if value:
                    # Carry 'force_linenos' over to new 'linenos'.
                    self.setConfig("linenums", True)

            self.setConfig(key, value)

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Add HilitePostprocessor to Markdown instance.

        Example:
            Exercise CodeHiliteExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        hiliter = HiliteTreeprocessor(md)
        hiliter.config = self.getConfigs()
        md.treeprocessors.add("hilite", hiliter, "<inline")

        md.registerExtension(self)


def makeExtension(configs: _typing.Any = None) -> _typing.Any:
    """
    Perform the makeExtension operation under explicit file-format and conversion rules.

    Example:
        Exercise makeExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param configs: Value supplied for configs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return CodeHiliteExtension(configs=configs or {})
