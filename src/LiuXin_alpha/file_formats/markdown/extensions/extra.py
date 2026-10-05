"""
Register the retained bundle of common Markdown extensions.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise extra through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
Python-Markdown Extra Extension
===============================

A compilation of various Python-Markdown extensions that imitates
[PHP Markdown Extra](http://michelf.com/projects/php-markdown/extra/).

Note that each of the individual extensions still need to be available
on your PYTHONPATH. This extension simply wraps them all up as a 
convenience so that only one extension needs to be listed when
initiating Markdown. See the documentation for each individual
extension for specifics about that extension.

In the event that one or more of the supported extensions are not 
available for import, Markdown will issue a warning and simply continue 
without that extension. 

There may be additional extensions that are distributed with 
Python-Markdown that are not included here in Extra. Those extensions
are not part of PHP Markdown Extra, and therefore, not part of
Python-Markdown Extra. If you really would like Extra to include
additional extensions, we suggest creating your own clone of Extra
under a differant name. You could also edit the `extensions` global 
variable defined below, but be aware that such changes may be lost 
when you upgrade to any future version of Python-Markdown.

"""

from . import Extension

extensions = [
    "smart_strong",
    "fenced_code",
    "footnotes",
    "attr_list",
    "def_list",
    "tables",
    "abbr",
]


class ExtraExtension(Extension):
    """
    Add various extensions to Markdown class.

    Example:
        Exercise ExtraExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Register extension instances.

        Example:
            Exercise ExtraExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        md.registerExtensions(extensions, self.config)
        if not md.safeMode:
            # Turn on processing of markdown text within raw html
            md.preprocessors["html_block"].markdown_in_raw = True


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
    return ExtraExtension(configs=dict(configs or {}))
