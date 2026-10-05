"""
Dispatch metadata extraction through registered reader plugins while retaining legacy loader helpers.

Extensions are normalized by the registry, adapters expose the historic uppercase
fields, and callers keep ownership of supplied streams.

Example:
    >>> filter_plugin_sources(['__init__.py', 'epub.py'])
    ['epub.py']
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from LiuXin_alpha.metadata.file_sources.registry import (
    get_metadata_reader_plugins,
    get_metadata_reader_registry_revision,
    known_metadata_file_types,
    normalize_file_type,
    register_metadata_reader_plugin,
    unregister_metadata_reader_plugin,
)
from LiuXin_alpha.utils.logging import default_log

__folder__ = os.path.realpath(os.path.dirname(__file__))

# Backwards-compatible globals kept for callers that inspect this module.
valid_plugins: list["MetaDataReaderPlugin"] = []
valid_file_formats: set[str] = set()
_loaded_registry_revision = -1


class InvalidMetadataExtractor(Exception):
    """
    Report that no registered metadata reader accepts the requested extension.

    Example:
        >>> str(InvalidMetadataExtractor('missing reader'))
        'missing reader'
    """
    pass


@dataclass
class MetaDataReaderPlugin:
    """
    Adapt a registered metadata-reader class to the legacy loader surface.

    The adapter derives names, paths, supported formats and run cost from class
    attributes and creates a fresh plugin instance for each extraction.

    Example:
        >>> class Reader:
        ...     file_types = {'epub'}
        ...     inplace_run_cost = 'low'
        ...     def __init__(self, _): pass
        ...     def get_metadata(self, stream, ftype): return ftype
        >>> adapter = MetaDataReaderPlugin(Reader)
        >>> adapter.VALID_FOR
        ['EPUB']
    """

    plugin_cls: type

    @property
    def module_name(self) -> str:
        """
        Return the wrapped reader class name used in diagnostics.

        Example:
            >>> class Reader:
            ...     file_types = {'epub'}
            ...     inplace_run_cost = 'low'
            ...     def __init__(self, _): pass
            ...     def get_metadata(self, stream, ftype): return ftype
            >>> adapter = MetaDataReaderPlugin(Reader)
            >>> adapter.module_name
            'Reader'


        :return: Reader class name.
        """
        return self.plugin_cls.__name__

    @property
    def file_path(self) -> str:
        """
        Return a source-style path derived from the wrapped class module.

        Example:
            >>> class Reader:
            ...     file_types = {'epub'}
            ...     inplace_run_cost = 'low'
            ...     def __init__(self, _): pass
            ...     def get_metadata(self, stream, ftype): return ftype
            >>> adapter = MetaDataReaderPlugin(Reader)
            >>> adapter.file_path.endswith('.py')
            True


        :return: Slash-separated module path ending in .py.
        """
        module = self.plugin_cls.__module__.replace(".", "/")
        return f"{module}.py"

    @property
    def VALID_FOR(self) -> list[str]:
        """
        Return supported file types uppercased for legacy callers.

        A new list is derived from file_types on each access.

        Example:
            >>> class Reader:
            ...     file_types = {'epub'}
            ...     inplace_run_cost = 'low'
            ...     def __init__(self, _): pass
            ...     def get_metadata(self, stream, ftype): return ftype
            >>> adapter = MetaDataReaderPlugin(Reader)
            >>> adapter.VALID_FOR
            ['EPUB']


        :return: New list of uppercase file types.
        """
        return [x.upper() for x in getattr(self.plugin_cls, "file_types", [])]

    @property
    def PRIORITY_FOR(self) -> list[str]:
        # Legacy loaders expected this field; file types were commonly reused.
        """
        Return the legacy priority formats, which mirror VALID_FOR.

        Example:
            >>> class Reader:
            ...     file_types = {'epub'}
            ...     inplace_run_cost = 'low'
            ...     def __init__(self, _): pass
            ...     def get_metadata(self, stream, ftype): return ftype
            >>> adapter = MetaDataReaderPlugin(Reader)
            >>> adapter.PRIORITY_FOR
            ['EPUB']


        :return: New list of uppercase priority formats.
        """
        return self.VALID_FOR

    @property
    def RUN_COST(self) -> list[str]:
        """
        Return the legacy one-element uppercase run-cost list.

        Readers without inplace_run_cost default to HIGH.

        Example:
            >>> class Reader:
            ...     file_types = {'epub'}
            ...     inplace_run_cost = 'low'
            ...     def __init__(self, _): pass
            ...     def get_metadata(self, stream, ftype): return ftype
            >>> adapter = MetaDataReaderPlugin(Reader)
            >>> adapter.RUN_COST
            ['LOW']


        :return: One-element run-cost list.
        """
        cost = str(getattr(self.plugin_cls, "inplace_run_cost", "high")).upper()
        return [cost]

    def get_metadata(self, target_object, force_type: str | None = None):
        """
        Run the wrapped reader against a path or readable stream.

        force_type is forwarded as the reader's file type and no extension inference occurs
        in this adapter method.

        Example:
            >>> class Reader:
            ...     file_types = {'epub'}
            ...     inplace_run_cost = 'low'
            ...     def __init__(self, _): pass
            ...     def get_metadata(self, stream, ftype): return ftype
            >>> adapter = MetaDataReaderPlugin(Reader)
            >>> import io
            >>> adapter.get_metadata(io.BytesIO(b'data'), 'epub')
            'epub'


        :param target_object: Filesystem path or readable binary stream.
        :param force_type: File type passed to the reader.
        :return: Metadata returned by the wrapped reader.
        """
        return _run_metadata_reader(self.plugin_cls, target_object, ftype=force_type)


def _normalize_ext(raw_ext: str | None) -> str:
    """
    Normalize a raw extension through the central metadata-reader registry.

    Leading dots and case are handled by the registry policy.

    Example:
        >>> _normalize_ext('.EPUB')
        'epub'


    :param raw_ext: Raw extension or None.
    :return: Normalized lowercase file type, or an empty string.
    """
    return normalize_file_type(raw_ext)


def _target_path_hint(target_object) -> str | None:
    """
    Return a usable path hint from a path-like object, string or named stream.

    Unnamed and non-string stream names yield None.

    Example:
        >>> import io
        >>> stream = io.BytesIO()
        >>> stream.name = 'book.epub'
        >>> _target_path_hint(stream)
        'book.epub'


    :param target_object: Potential path or named stream.
    :return: Path text usable for extension inference, or None.
    """
    if isinstance(target_object, os.PathLike):
        return os.fspath(target_object)
    if isinstance(target_object, str):
        return target_object
    name = getattr(target_object, "name", None)
    if isinstance(name, str) and name:
        return name
    return None


def _resolve_extension(target_object, force_type: str | bool | None = None) -> str:
    """
    Resolve a normalized extension from an explicit override or target path hint.

    A truthy override wins; when neither source provides an extension, ValueError asks
    the caller to pass force_type.

    Example:
        >>> _resolve_extension('BOOK.EPUB')
        'epub'


    :param target_object: Path, named stream or other extraction target.
    :param force_type: Optional explicit file type override.
    :return: Normalized lowercase extension.
    """
    if force_type:
        return _normalize_ext(str(force_type))

    source_path = _target_path_hint(target_object)
    dotted_ext = os.path.splitext(source_path or "")[1]
    ext = dotted_ext[1:] if dotted_ext.startswith(".") else dotted_ext
    ext = _normalize_ext(ext)
    if not ext:
        raise ValueError("Could not infer extension for metadata extraction. Pass force_type to override.")
    return ext


def _is_path_like(target_object) -> bool:
    """
    Return whether the target is a string, bytes path or os.PathLike object.

    Example:
        >>> from pathlib import Path
        >>> _is_path_like(Path('book.epub'))
        True


    :param target_object: Candidate extraction target.
    :return: True for accepted filesystem-path representations.
    """
    return isinstance(target_object, (str, bytes, os.PathLike))


def _run_metadata_reader(plugin_cls: type, target_object, *, ftype: str):
    """
    Instantiate a reader and run it against a path or readable stream.

    Path targets prefer get_metadata_inplace when available; other paths are opened in
    binary mode. Unsupported targets raise TypeError.

    Example:
        >>> class Reader:
        ...     file_types = {'epub'}
        ...     inplace_run_cost = 'low'
        ...     def __init__(self, _): pass
        ...     def get_metadata(self, stream, ftype): return ftype
        >>> adapter = MetaDataReaderPlugin(Reader)
        >>> import io
        >>> _run_metadata_reader(Reader, io.BytesIO(b'data'), ftype='epub')
        'epub'


    :param plugin_cls: Reader plugin class instantiated with None.
    :param target_object: Filesystem path or readable stream.
    :param ftype: Normalized file type forwarded to the reader.
    :return: Metadata returned by the reader.
    """
    plugin = plugin_cls(None)
    if _is_path_like(target_object):
        path = os.fspath(target_object)
        if hasattr(plugin, "get_metadata_inplace"):
            return plugin.get_metadata_inplace(path, ftype)
        with open(path, "rb") as stream:
            return plugin.get_metadata(stream=stream, ftype=ftype)

    if hasattr(target_object, "read"):
        return plugin.get_metadata(stream=target_object, ftype=ftype)

    raise TypeError("target_object must be a filesystem path or a readable binary stream.")


def sort_plugins_by_run_cost(plugins):
    """
    Return adapters ordered HIGH, MEDIUM, then LOW while preserving order within a cost.

    Any other run-cost token raises AssertionError with plugin diagnostics.

    Example:
        >>> class Reader:
        ...     file_types = {'epub'}
        ...     inplace_run_cost = 'low'
        ...     def __init__(self, _): pass
        ...     def get_metadata(self, stream, ftype): return ftype
        >>> adapter = MetaDataReaderPlugin(Reader)
        >>> sort_plugins_by_run_cost([adapter])[0] is adapter
        True


    :param plugins: Iterable of metadata-reader adapters.
    :return: New list of sorted adapters.
    """
    run_cost_dict = {"HIGH": 1, "MEDIUM": 2, "LOW": 3}

    sortable_index = []
    for plugin in plugins:
        plugin_cost = plugin.RUN_COST[0]
        if plugin_cost not in run_cost_dict:
            raise AssertionError(
                "Unrecognized run cost detected\n"
                f"Plugin name: {plugin.module_name}\n"
                f"Given RUN_COST: {plugin.RUN_COST!r}"
            )
        sortable_index.append((plugin, run_cost_dict[plugin_cost]))

    sortable_index.sort(key=lambda x: x[1])
    return [item[0] for item in sortable_index]


def load_plugins():
    """
    Refresh compatibility adapters and uppercase format names from the registered reader classes.

    The public list and set are mutated in place, and the loaded registry revision is
    updated.

    Example:
        >>> load_plugins()
        >>> isinstance(valid_plugins, list) and isinstance(valid_file_formats, set)
        True


    :return: None.
    """
    global _loaded_registry_revision
    valid_plugins[:] = [MetaDataReaderPlugin(cls) for cls in get_metadata_reader_plugins()]
    valid_file_formats.clear()
    for plugin in valid_plugins:
        valid_file_formats.update(plugin.VALID_FOR)
    _loaded_registry_revision = get_metadata_reader_registry_revision()


def get_plugins_for_extension(ext: str):
    """
    Return registered adapters that accept a normalized extension, sorted by run cost.

    The compatibility cache is refreshed when empty or stale.

    Example:
        >>> all('EPUB' in plugin.VALID_FOR for plugin in get_plugins_for_extension('.epub'))
        True


    :param ext: Raw file extension to normalize.
    :return: New sorted list of matching adapters.
    """
    ext = _normalize_ext(ext).upper()
    if not valid_plugins or _loaded_registry_revision != get_metadata_reader_registry_revision():
        load_plugins()
    plugins = [plugin for plugin in valid_plugins if ext in plugin.VALID_FOR]
    return sort_plugins_by_run_cost(plugins)


def filter_plugin_sources(plugin_sources_names):
    """
    Materialize source names and remove the package initializer and compiled Python files.

    Other names and their original order are retained.

    Example:
        >>> filter_plugin_sources(iter(['__init__.py', 'epub.py', 'old.pyc']))
        ['epub.py']


    :param plugin_sources_names: Iterable of source names.
    :return: Filtered source-name list.
    """
    plugin_sources_names = list(plugin_sources_names)
    plugin_sources_names = [name for name in plugin_sources_names if name != "__init__.py"]
    plugin_sources_names = [name for name in plugin_sources_names if not name.endswith(".pyc")]
    return plugin_sources_names


def get_metadata(target_object, force_type: str | bool = False):
    """
    Read metadata with registered readers for an inferred or forced extension.

    Readers run by cost until one returns non-None. Missing readers raise
    InvalidMetadataExtractor; if every attempted result is an exception, RuntimeError is
    chained from the last failure.

    Example:
        Exercise dispatch, fallback and diagnostics with pytest::

            python -m pytest -q tests/metadata/file_sources/test_dispatcher_modernized.py


    :param target_object: Filesystem path or readable binary stream.
    :param force_type: Optional explicit type; a truthy value overrides inference.
    :return: First non-None metadata result, or None when all readers decline.
    """
    ext = _resolve_extension(target_object, force_type=force_type)
    plugins = get_plugins_for_extension(ext)
    if not plugins:
        raise InvalidMetadataExtractor(f"No metadata reader plugin is registered for extension: {ext!r}")

    errors: list[tuple[str, Exception]] = []
    for plugin in plugins:
        try:
            md = plugin.get_metadata(target_object, force_type=ext)
            if md is not None:
                return md
        except Exception as err:
            errors.append((plugin.module_name, err))
            default_log.log_exception(
                "Error while running metadata extractor plugin.",
                err,
                "DEBUG",
                ("plugin_name", plugin.module_name),
                ("plugin_path", plugin.file_path),
                ("extension", ext),
            )

    if errors:
        plugin_names = [name for name, _ in errors]
        raise RuntimeError(
            "Metadata extraction failed for extension %r. Tried plugins: %s"
            % (ext, ", ".join(plugin_names))
        ) from errors[-1][1]
    return None


__all__ = [
    "InvalidMetadataExtractor",
    "MetaDataReaderPlugin",
    "filter_plugin_sources",
    "get_metadata",
    "get_plugins_for_extension",
    "known_metadata_file_types",
    "load_plugins",
    "register_metadata_reader_plugin",
    "sort_plugins_by_run_cost",
    "unregister_metadata_reader_plugin",
]
