"""Select and run file plugins without importing application composition.

Standalone format operations lazily initialize bundled and installed file plugins.
Application hosts may bind their already initialized instances with ``bind_plugins``;
profiles, settings, hooks, and metadata flags then operate on the same instances.
"""

from __future__ import annotations

import functools
import os
import shutil
import sys
import traceback
import typing as _typing
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from threading import RLock
from typing import Any, BinaryIO

from LiuXin_alpha.file_formats.conversion.plugin import (
    InputFormatPlugin,
    OutputFormatPlugin,
)
from LiuXin_alpha.file_formats.conversion.profiles import InputProfile, OutputProfile
from LiuXin_alpha.plugin_system.base import Plugin
from LiuXin_alpha.plugin_system.configuration import config, is_disabled
from LiuXin_alpha.plugin_system.configuration import (
    customize_plugin as customize_plugin,
)
from LiuXin_alpha.plugin_system.configuration import (
    plugin_customization as plugin_customization,
)
from LiuXin_alpha.plugin_system.file_types import (
    FileTypePlugin,
    MetadataReaderPlugin,
    MetadataWriterPlugin,
)
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

basestring = str
_initialized_plugins: list[Plugin] | None = None
_initialization_lock = RLock()


@contextmanager
def initialization_scope() -> Iterator[None]:
    """Serialize host and standalone startup, allowing same-thread callbacks."""
    with _initialization_lock:
        yield


def replace_plugins(builder: Callable[[], Iterable[Plugin]]) -> tuple[Plugin, ...]:
    """Build and bind a fleet, restoring the previous binding if startup aborts.

    Reentrant format lookups see an empty fleet while building. Other threads
    wait for publication instead of starting a second standalone registry.
    """
    with _initialization_lock:
        previous = _initialized_plugins
        bind_plugins(())
        try:
            plugins = tuple(builder())
            bind_plugins(plugins)
            return plugins
        except BaseException:
            if previous is None:
                reset_plugins()
            else:
                bind_plugins(previous)
            raise


def _ensure_plugins() -> None:
    """Initialize the standalone file-plugin fleet on first use."""
    with _initialization_lock:
        if _initialized_plugins is not None:
            return
        from LiuXin_alpha.file_formats.plugin_defaults import build_plugins

        replace_plugins(build_plugins)


def bind_plugins(plugins: Iterable[Plugin]) -> None:
    """Bind host-owned plugin instances and rebuild file-hook/metadata indexes."""
    global _initialized_plugins
    with _initialization_lock:
        _initialized_plugins = list(plugins)
        reread_filetype_plugins()
        reread_metadata_plugins()


def reset_plugins() -> None:
    """Discard the bound fleet so the next operation reloads persisted settings."""
    global _initialized_plugins
    with _initialization_lock:
        _initialized_plugins = None
        _on_import.clear()
        _on_postimport.clear()
        _on_preprocess.clear()
        _on_postprocess.clear()
        _metadata_readers.clear()
        _metadata_writers.clear()


def initialized_plugins() -> Iterator[Plugin]:
    """Yield the active file-plugin instances in their established order."""
    with _initialization_lock:
        _ensure_plugins()
        plugins = tuple(_initialized_plugins or ())
    yield from plugins


_on_import: dict[str, list[Plugin]] = {}


_on_postimport: dict[str, list[Plugin]] = {}


_on_preprocess: dict[str, list[Plugin]] = {}


_on_postprocess: dict[str, list[Plugin]] = {}


def reread_filetype_plugins() -> None:
    """
    Reload the filetype plugins.

    Example:
        Exercise reread filetype plugins through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    _ensure_plugins()
    default_log.info("Starting reread_filetype_plugins")

    global _on_import
    global _on_postimport
    global _on_preprocess
    global _on_postprocess
    _on_import = {}
    _on_postimport = {}
    _on_preprocess = {}
    _on_postprocess = {}

    for plugin in initialized_plugins():
        if isinstance(plugin, FileTypePlugin):
            for ft in plugin.file_types:
                if plugin.on_import:
                    if ft not in _on_import.keys():  # used to use "has_ket"
                        _on_import[ft] = []
                    _on_import[ft].append(plugin)

                if plugin.on_postimport:
                    if ft not in _on_postimport.keys():
                        _on_postimport[ft] = []
                    _on_postimport[ft].append(plugin)

                if plugin.on_preprocess:
                    if ft not in _on_preprocess.keys():
                        _on_preprocess[ft] = []
                    _on_preprocess[ft].append(plugin)

                if plugin.on_postprocess:
                    if ft not in _on_postprocess.keys():
                        _on_postprocess[ft] = []
                    _on_postprocess[ft].append(plugin)

    default_log.info("Finishing reread_filetype_plugins")


def _run_filetype_plugins(
    path_to_file: str, ft: str = None, occasion: str = "preprocess"
) -> str:
    """
    INTERNAL USE - runs filetype plugins on a given file.

    Example:
        Exercise  run filetype plugins through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param path_to_file: Value supplied for path to file under the utility contract.
    :param ft: Value supplied for ft under the utility contract.
    :param occasion: Value supplied for occasion under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    _ensure_plugins()
    occasion_plugins = {
        "import": _on_import,
        "preprocess": _on_preprocess,
        "postprocess": _on_postprocess,
    }[occasion]

    customization = config["plugin_customization"]

    if ft is None:
        ft = os.path.splitext(path_to_file)[-1].lower().replace(".", "")
    nfp = path_to_file
    for plugin in occasion_plugins.get(ft, []):
        if is_disabled(plugin):
            continue

        plugin.site_customization = customization.get(plugin.name, "")
        # Some file type plugins out there override the output streams with buggy implementations
        oo, oe = sys.stdout, sys.stderr

        with plugin:
            try:
                nfp = plugin.run(path_to_file)
                if not nfp:
                    nfp = path_to_file
            except BaseException:
                print(
                    f"Running file type plugin {plugin.name} failed with traceback:",
                    file=oe,
                )
                traceback.print_exc(file=oe)

        sys.stdout, sys.stderr = oo, oe

    def normalized_path(value: str) -> str:
        """Compare hook outputs using the historical platform-normalized path."""
        return os.path.normpath(os.path.normcase(value))

    if occasion == "postprocess" and normalized_path(nfp) != normalized_path(
        path_to_file
    ):
        shutil.copyfile(nfp, path_to_file)
        nfp = path_to_file
    return nfp


run_plugins_on_import = functools.partial(_run_filetype_plugins, occasion="import")


run_plugins_on_preprocess = functools.partial(
    _run_filetype_plugins, occasion="preprocess"
)


run_plugins_on_postprocess = functools.partial(
    _run_filetype_plugins, occasion="postprocess"
)


def run_plugins_on_postimport(db: Any, book_id: int, fmt: str) -> None:
    """
    Runs all the postimport plugins available on the target book.

    Example:
        Exercise run plugins on postimport through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param db: Value supplied for db under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :param fmt: Date, number or template format specification.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    _ensure_plugins()
    customization = config["plugin_customization"]
    fmt = fmt.lower()
    for plugin in _on_postimport.get(fmt, []):
        if is_disabled(plugin):
            continue
        plugin.site_customization = customization.get(plugin.name, "")
        with plugin:
            try:
                plugin.postimport(book_id, fmt, db)
            except BaseException:
                print(f"Running file type plugin {plugin.name} failed with traceback:")
                traceback.print_exc()


run_plugins_on_postadd = run_plugins_on_postimport


def input_profiles() -> Iterator[InputProfile]:
    """
    Yield all input profiles.

    Example:
        Exercise input profiles through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: An iterator yielding the normalized values described above.
    """

    for plugin in initialized_plugins():
        if isinstance(plugin, InputProfile):
            yield plugin


def output_profiles() -> Iterator[OutputProfile]:
    """
    Yield all the output profiles.

    Example:
        Exercise output profiles through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, OutputProfile):
            yield plugin


_metadata_readers: dict[str, list[MetadataReaderPlugin]] = {}


_metadata_writers: dict[str, list[MetadataWriterPlugin]] = {}


def reread_metadata_plugins() -> None:
    """
    Read the metadata IO plugins.

    Example:
        Exercise reread metadata plugins through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    _ensure_plugins()

    default_log.info("About to start reread_metadata_plugins")

    global _metadata_readers
    global _metadata_writers
    _metadata_readers = {}
    _metadata_writers = {}
    for plugin in initialized_plugins():
        if isinstance(plugin, MetadataReaderPlugin):
            for ft in plugin.file_types:
                if ft not in _metadata_readers.keys():
                    _metadata_readers[ft] = []
                _metadata_readers[ft].append(plugin)
        elif isinstance(plugin, MetadataWriterPlugin):
            for ft in plugin.file_types:
                if ft not in _metadata_writers.keys():
                    _metadata_writers[ft] = []
                _metadata_writers[ft].append(plugin)

    default_log.info("Finishing reread_metadata_plugins")


def metadata_readers() -> set[MetadataReaderPlugin]:
    """
    Return a set of all the metadata reader plugins.

    Example:
        Exercise metadata readers through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    _ensure_plugins()
    ans = set([])
    for plugins in _metadata_readers.values():
        for plugin in plugins:
            ans.add(plugin)
    return ans


def metadata_writers() -> set[MetadataWriterPlugin]:
    """
    Return a set of the metadata writer plugins.

    Example:
        Exercise metadata writers through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    _ensure_plugins()
    ans = set([])
    for plugins in _metadata_writers.values():
        for plugin in plugins:
            ans.add(plugin)
    return ans


class QuickMetadata:
    """
    Context manager which turns the quick metadata option on and off.

    Example:
        Exercise QuickMetadata through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the quickmetadata state.

        Example:
            Exercise QuickMetadata.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; validated state is stored on the receiving object.
        """
        self.quick = False

    def __enter__(self: _typing.Self) -> None:
        """
        Implement the conversion resource's enter lifecycle operation.

        Example:
            Exercise QuickMetadata.  enter   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.quick = True

    def __exit__(self: _typing.Self, *args: Any) -> None:
        """
        Implement the conversion resource's exit lifecycle operation.

        Example:
            Exercise QuickMetadata.  exit   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.quick = False


quick_metadata = QuickMetadata()


class ApplyNullMetadata:
    """
    Context manager which turns the apply null metadata option on and off.

    Example:
        Exercise ApplyNullMetadata through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the applynullmetadata state.

        Example:
            Exercise ApplyNullMetadata.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; validated state is stored on the receiving object.
        """
        self.apply_null = False

    def __enter__(self: _typing.Self) -> None:
        """
        Implement the conversion resource's enter lifecycle operation.

        Example:
            Exercise ApplyNullMetadata.  enter   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.apply_null = True

    def __exit__(self: _typing.Self, *args: _typing.Any) -> None:
        """
        Implement the conversion resource's exit lifecycle operation.

        Example:
            Exercise ApplyNullMetadata.  exit   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.apply_null = False


apply_null_metadata = ApplyNullMetadata()


class ForceIdentifiers:
    """
    Context manager which turns the force_identifiers option on and off.

    Example:
        Exercise ForceIdentifiers through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the forceidentifiers state.

        Example:
            Exercise ForceIdentifiers.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; validated state is stored on the receiving object.
        """
        self.force_identifiers = False

    def __enter__(self: _typing.Self) -> None:
        """
        Implement the conversion resource's enter lifecycle operation.

        Example:
            Exercise ForceIdentifiers.  enter   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.force_identifiers = True

    def __exit__(self: _typing.Self, *args: Any) -> None:
        """
        Implement the conversion resource's exit lifecycle operation.

        Example:
            Exercise ForceIdentifiers.  exit   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.force_identifiers = False


force_identifiers = ForceIdentifiers()


def get_file_type_metadata(
    stream: BinaryIO | str | os.PathLike, ftype: str, calibre: bool = False
) -> _typing.Any:
    """
    Get metadata from a stream of ftype.

    Example:
        Exercise get file type metadata through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param ftype: Value supplied for ftype under the utility contract.
    :param calibre: Value supplied for calibre under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    _ensure_plugins()
    from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
        CalibreLikeLiuXinBookMetaData as MetaInformation,
    )

    # Ensure that the method returns a mi object - come what may.
    mi = MetaInformation(None, None)

    # If the method has been given a string instead of a stream checks to see if that file exists and then opens it as a
    # stream - if it's been fed a stream then proceed
    if isinstance(stream, basestring) or isinstance(stream, os.PathLike):
        if os.path.exists(stream):
            info_str = (
                "get_file_type_metadata passed a file path in place of a stream.\n"
            )
            info_str += "location exists - opening for reading.\n"
            info_str += "stream: " + six_unicode(stream)
            default_log.info(info_str)
            stream = open(stream)
        else:
            info_str = (
                "get_file_type_metadata passed a file path in place of a stream.\n"
            )
            info_str += "location doesn't exist - aborting.\n"
            info_str += "stream: " + six_unicode(stream)
            default_log.info(info_str)
            return mi

    ftype = ftype.lower().strip()
    if ftype in _metadata_readers.keys():
        for plugin in _metadata_readers[ftype]:
            if not is_disabled(plugin):
                with plugin:
                    try:
                        plugin.quick = quick_metadata.quick
                        if hasattr(stream, "seek"):
                            stream.seek(0)
                        mi = plugin.get_metadata(stream, ftype.lower().strip())
                        break
                    except BaseException:
                        traceback.print_exc()
                        continue

    if calibre:
        return mi.to_calibre()
    return mi


def set_file_type_metadata(
    stream: BinaryIO,
    mi: _typing.Any,
    ftype: str,
    report_error: Callable[[Any, str, str], None] | None = None,
) -> None:
    """
    Write MetaData into the file of the given type.

    Example:
        Exercise set file type metadata through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param mi: Metadata object exposed to the template function.
    :param ftype: Value supplied for ftype under the utility contract.
    :param report_error: Value supplied for report error under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    _ensure_plugins()
    ftype = ftype.lower().strip()

    # There should only be one plugin to write metadata to each file type
    if ftype in _metadata_writers.keys():
        for plugin in _metadata_writers[ftype]:
            if not is_disabled(plugin):
                with plugin:
                    try:
                        plugin.apply_null = apply_null_metadata.apply_null
                        plugin.force_identifiers = force_identifiers.force_identifiers
                        plugin.set_metadata(stream, mi, ftype.lower().strip())
                        break
                    except BaseException:
                        if report_error is None:
                            from LiuXin_alpha.utils.logging import prints

                            prints(
                                "Failed to set metadata for the",
                                ftype.upper(),
                                "format of:",
                                getattr(mi, "title", ""),
                                file=sys.stderr,
                            )
                            traceback.print_exc()
                        else:
                            report_error(mi, ftype, traceback.format_exc())


def can_set_metadata(ftype: str) -> bool:
    """
    Can metadata be set for this particular file type?.

    Example:
        Exercise can set metadata through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param ftype: Value supplied for ftype under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """

    _ensure_plugins()
    ftype = ftype.lower().strip()

    for plugin in _metadata_writers.get(ftype, ()):
        if not is_disabled(plugin):
            return True

    return False


def input_format_plugins() -> Iterator[InputFormatPlugin]:
    """
    Iterator for the input format plugins

    Example:
        Exercise input format plugins through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, InputFormatPlugin):
            yield plugin


def plugin_for_input_format(fmt: str) -> Plugin | None:
    """
    Get the plugin associated with the given input format.

    Example:
        Exercise plugin for input format through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    customization = config["plugin_customization"]
    for plugin in input_format_plugins():
        if fmt.lower() in plugin.file_types:
            plugin.site_customization = customization.get(plugin.name, None)
            return plugin


def all_input_formats() -> set[str]:
    """
    Returns a set of all the formats' calibre can read - disabled or not.

    Example:
        Exercise all input formats through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    formats = set([])
    for plugin in input_format_plugins():
        for fmt in plugin.file_types:
            formats.add(fmt)
    return formats


def available_input_formats() -> set[str]:
    """
    Returns a set of the currently available input formats.

    Example:
        Exercise available input formats through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    formats = set([])
    for plugin in input_format_plugins():
        if not is_disabled(plugin):
            for fmt in plugin.file_types:
                formats.add(fmt)
    formats.add("zip"), formats.add("rar")
    return formats


def output_format_plugins() -> Iterator[OutputFormatPlugin]:
    """
    An iterator over all the output format plugins.

    Example:
        Exercise output format plugins through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: An iterator yielding the normalized values described above.
    """
    for plugin in initialized_plugins():
        if isinstance(plugin, OutputFormatPlugin):
            yield plugin


def plugin_for_output_format(fmt: str) -> Plugin | None:
    """
    Returns a output format plugin for the requested format.

    Example:
        Exercise plugin for output format through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    customization = config["plugin_customization"]
    for plugin in output_format_plugins():
        if fmt.lower() == plugin.file_type:
            plugin.site_customization = customization.get(plugin.name, None)
            return plugin


def available_output_formats() -> set[str]:
    """
    Returns a set of all available output formats.

    Example:
        Exercise available output formats through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    formats = set([])
    for plugin in output_format_plugins():
        if not is_disabled(plugin):
            formats.add(plugin.file_type)
    return formats


def run_import_plugins(path_or_stream: str | os.PathLike[str] | BinaryIO, fmt: str) -> str:
    """Run file-import hooks for a path or a binary stream.

    Streams are rewound and copied to a persistent temporary file with the
    supplied format extension; the returned path belongs to the caller. Path
    inputs are passed to the hooks directly. This does not start application plugins.
    """
    from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

    fmt = fmt.lower()
    if hasattr(path_or_stream, "seek"):
        path_or_stream.seek(0)
        pt = PersistentTemporaryFile("_import_plugin." + fmt)
        shutil.copyfileobj(path_or_stream, pt, 1024**2)
        pt.close()
        path = pt.name
    else:
        path = path_or_stream
    return run_plugins_on_import(path)
