"""
Expose the supported extensions compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import annotations

import typing as _typing

"""
Extensions
-----------------------------------------------------------------------------
"""


class Extension(object):
    """
    Base class for extensions to subclass.

    Example:
        Exercise Extension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, configs: _typing.Any = None) -> None:
        """
        Create an instance of an Extention.

        Example:
            Exercise Extension.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param configs: Value supplied for configs under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.config = configs or {}

    def getConfig(self: _typing.Self, key: _typing.Any, default: str = "") -> _typing.Any:
        """
        Return a setting for the given key or an empty string.

        Example:
            Exercise Extension.getConfig through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if key in self.config:
            return self.config[key][0]
        else:
            return default

    def getConfigs(self: _typing.Self) -> _typing.Any:
        """
        Return all configs settings as a dict.

        Example:
            Exercise Extension.getConfigs through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return dict([(key, self.getConfig(key)) for key in self.config.keys()])

    def getConfigInfo(self: _typing.Self) -> _typing.Any:
        """
        Return all config descriptions as a list of tuples.

        Example:
            Exercise Extension.getConfigInfo through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [(key, self.config[key][1]) for key in self.config.keys()]

    def setConfig(self: _typing.Self, key: _typing.Any, value: _typing.Any) -> None:
        """
        Set a config setting for `key` with the given `value`.

        Example:
            Exercise Extension.setConfig through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.config[key][0] = value

    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Add the various proccesors and patterns to the Markdown Instance.

        Example:
            Exercise Extension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError(
            'Extension "%s.%s" must define an "extendMarkdown"'
            "method." % (self.__class__.__module__, self.__class__.__name__)
        )
