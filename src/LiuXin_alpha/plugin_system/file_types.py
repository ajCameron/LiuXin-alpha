"""Passive file-hook and metadata-I/O contracts for loadable plugins.

These contracts describe streams and lifecycle hooks, without discovering
providers or importing format implementations, persistence, or application UI.
"""

from __future__ import annotations

import pathlib
from typing import Any, BinaryIO

from LiuXin_alpha.constants import CALIBRE_NUMERIC_VERSION as calibre_numeric_version
from LiuXin_alpha.plugin_system.base import Plugin
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


class FileTypePlugin(Plugin):  # {{{
    """
    A plugin transforms a particular set of file types.

    Example:
        Exercise FileTypePlugin through a consuming regression::

            python -m pytest -q tests/plugin_system/test_plugin_base.py
    """

    #: Set of file types for which this plugin should be run. For example: ``{'lit', 'mobi', 'prc'}``
    file_types: set[str] = set()

    #: If True, this plugin is run when books are added to the database
    on_import = False

    #: If True, this plugin is run after books are added to the database
    on_postimport = False

    #: If True, this plugin is run just before a conversion
    on_preprocess = False

    #: If True, this plugin is run after conversion on the final file produced by the conversion output plugin.
    on_postprocess = False

    plugin_type = _("File plugin_type")

    # Todo: This wants to be an abc
    def run(self, path_to_ebook: str) -> str:
        """
        Run the plugin. Must be implemented in subclasses.

        Example:
            Exercise FileTypePlugin.run through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param path_to_ebook: Value supplied for path to ebook under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Default implementation does nothing
        return path_to_ebook

    def postimport(self, book_id, book_format, db):
        """
        Called post import, i.e., after the book file has been added to the database.

        Example:
            Exercise FileTypePlugin.postimport through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param book_format: Value supplied for book format under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass  # Default implementation does nothing


class _MetadataReaderPlugin:
    """
    We want both LiuXin and calibre metadata readers to have a common interface - but not a common class heirachy.

    Example:
        Exercise  MetadataReaderPlugin through a consuming regression::

            python -m pytest -q tests/plugin_system/test_plugin_base.py
    """

    # Set of file types for which this plugin should be run. For example: ``set(['lit', 'mobi', 'prc'])``
    file_types = frozenset([])

    # Basic measure of run cost
    inplace_run_cost = "high"

    # What platforms does this plugin work on?
    supported_platforms = ["windows", "osx", "linux"]

    # Default numeric version tuple
    version = calibre_numeric_version

    author = "Kovid Goyal"

    # Displayable plugin type
    plugin_type = _("Metadata reader")

    # Todo: Work out the signature from where these are called
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Startup the plugin.

        Example:
            Exercise  MetadataReaderPlugin.  init   through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(*args, **kwargs)

        self.quick = False

    # Todo: Pull the calibre metadata object out and gen an API for it
    def get_metadata(self, stream: BinaryIO, ftype: str):
        """
        Return metadata for the file represented by stream (a file like object that supports reading).

        Example:
            Exercise  MetadataReaderPlugin.get metadata through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param ftype: Value supplied for ftype under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_metadata_inplace(self, file_path: pathlib.Path | str, ftype: str):
        """
        Returns metadata for the file represented by the file path.

        Example:
            Exercise  MetadataReaderPlugin.get metadata inplace through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param ftype: Value supplied for ftype under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Tries to open the file as a stream and use the get_metadata method on it
        with open(file_path, "rb") as md_file_stream:
            return self.get_metadata(stream=md_file_stream, ftype=ftype)


class MetadataReaderPlugin(Plugin, _MetadataReaderPlugin):  # {{{
    """
    A plugin that implements reading metadata from a set of file types.

    Example:
        Exercise MetadataReaderPlugin through a consuming regression::

            python -m pytest -q tests/plugin_system/test_plugin_base.py
    """

    def __init__(
        self, plugin_path: str | pathlib.Path | None, *args: Any, **kwargs: Any
    ) -> None:
        """
        Initialize shared plugin state and metadata-reader flags.

        Example:
            Exercise MetadataReaderPlugin.  init   through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param plugin_path: Value supplied for plugin path under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(plugin_path, *args, **kwargs)
        self.quick = False


class MetadataWriterPlugin(Plugin):
    """
    A plugin that implements writing metadata to files in a certain set of file types.

    Example:
        Exercise MetadataWriterPlugin through a consuming regression::

            python -m pytest -q tests/plugin_system/test_plugin_base.py
    """

    #: Set of file types for which this plugin should be run
    #: For example: ``set(['lit', 'mobi', 'prc'])``
    file_types = set([])

    supported_platforms = ["windows", "osx", "linux"]

    version = calibre_numeric_version

    author = "Kovid Goyal"

    plugin_type = _("Metadata writer")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Startup the plugin.

        Example:
            Exercise MetadataWriterPlugin.  init   through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(*args, **kwargs)

        self.apply_null = False

    def set_metadata(self, stream: BinaryIO, mi, type: str) -> None:
        """
        Set metadata for the file represented by stream (a file like object that supports reading).

        Example:
            Exercise MetadataWriterPlugin.set metadata through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param mi: Metadata object exposed to the template function.
        :param type: Value supplied for type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def set_metadata_inplace(
        self, file_path: str | pathlib.Path, mi, type: str
    ) -> None:
        """
        Set metadata for the file pointed to by the file path.

        Example:
            Exercise MetadataWriterPlugin.set metadata inplace through a consuming regression::

                python -m pytest -q tests/plugin_system/test_plugin_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param type: Value supplied for type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError
