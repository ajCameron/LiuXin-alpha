"""
Register built-in file-format metadata writers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise metadata writers through a consuming regression::

        python -m pytest -q tests/plugins/test_plugin_base.py
"""

import contextlib
import functools
import io
import os
from collections.abc import Callable
from typing import Any, BinaryIO

from LiuXin_alpha.plugins.file_types import MetadataWriterPlugin

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.metadata.file_sources.subject_fields import reads_subjects

plugins: list[type[MetadataWriterPlugin]] = []


def write_through_copy(stream: Any, write: Callable[[BinaryIO], None]) -> None:
    """
    Run a writer on a copy of a stream in memory, and change the stream only when it succeeds.

    A result that only adds to the end of the file (an incremental update) is appended.
    If putting the result into the stream fails, the original bytes are put back
    before the error is raised. Something that is not a seekable stream, such as a
    path, is given to the writer as it is.

    Example:
        >>> book = io.BytesIO(b"old")
        >>> def broken(copy):
        ...     copy.seek(0); copy.truncate(); raise ValueError("full disk")
        >>> try:
        ...     write_through_copy(book, broken)
        ... except ValueError:
        ...     pass
        >>> book.getvalue()
        b'old'
        >>> write_through_copy(book, lambda copy: copy.write(b"new"))
        >>> book.getvalue()
        b'new'


    :param stream: The book: a readable, writable, seekable binary stream.
    :param write: The writer, given the copy.
    :return: None.
    """
    if not all(callable(getattr(stream, name, None)) for name in ("read", "write", "seek", "truncate")):
        write(stream)
        return
    stream.seek(0)
    original = stream.read()
    copy = io.BytesIO(original)
    write(copy)
    result = copy.getvalue()
    if result != original:
        start = len(original) if result.startswith(original) else 0
        try:
            stream.seek(start)
            stream.truncate()
            stream.write(result[start:])
            stream.flush()
            with contextlib.suppress(AttributeError, OSError, io.UnsupportedOperation):
                os.fsync(stream.fileno())
        except BaseException:
            with contextlib.suppress(Exception):
                stream.seek(start)
                stream.truncate()
                stream.write(original[start:])
                stream.flush()
            raise
    stream.seek(0)


class BuiltinMetadataWriter(MetadataWriterPlugin):
    """
    A built-in writer that behaves the same called directly as under the runtime.

    Each subclass's ``set_metadata`` is wrapped. The wrapper:

    - gives a writer for a format with one field for tags and subjects the subjects
      among the tags, unless it ``writes_subjects`` (the runtime does the same for
      every writer plugin);
    - runs the writer on a copy of the stream in memory, and changes the stream only
      when the writer succeeds, unless the writer ``protects_stream`` itself.

    Example:
        Exercise the wrapped writers through their regression module::

            python -m pytest -q tests/metadata/file_sources/test_builtin_writers_called_directly.py
    """

    #: Whether the writer leaves the stream as it was when it fails, by itself.
    protects_stream = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Wrap a subclass's own ``set_metadata``.

        Example:
            Exercise the wrapping through its regression module::

                python -m pytest -q tests/metadata/file_sources/test_builtin_writers_called_directly.py


        :param kwargs: Passed on to the parent class.
        :return: None.
        """
        super().__init_subclass__(**kwargs)
        write = cls.__dict__.get("set_metadata")
        if write is not None:
            cls.set_metadata = _guarded(write)


def _guarded(write: Callable[..., None]) -> Callable[..., None]:
    """
    Wrap a writer's ``set_metadata`` as :class:`BuiltinMetadataWriter` describes.

    Example:
        Exercise the wrapping through its regression module::

            python -m pytest -q tests/metadata/file_sources/test_builtin_writers_called_directly.py


    :param write: The writer's own ``set_metadata``.
    :return: The wrapped method.
    """

    @functools.wraps(write)
    def set_metadata(self: Any, stream: Any, mi: Any, type: str) -> None:
        """
        Write the record into the book, as :class:`BuiltinMetadataWriter` describes.

        Example:
            Exercise the wrapping through its regression module::

                python -m pytest -q tests/metadata/file_sources/test_builtin_writers_called_directly.py


        :param self: The writer plugin.
        :param stream: The book: a readable, writable, seekable binary stream.
        :param mi: The record to write.
        :param type: The format.
        :return: None.
        """
        from LiuXin_alpha.metadata.book.base import subjects_among_tags

        record = mi if getattr(self, "writes_subjects", False) else subjects_among_tags(mi)
        if getattr(self, "protects_stream", False):
            write(self, stream, record, type)
            return
        write_through_copy(stream, lambda copy: write(self, copy, record, type))

    return set_metadata

# Note: Always open a stream as rb+ to allow read-write before passing into one of these classes

try:
    from LiuXin_alpha.metadata.file_sources.docx import set_metadata as docx_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.metadata.file_sources.docx - DOCXMetadataWriter cannot be "
        "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("DocXMetadataWriter components imported successfully")

    # Capitalization to match the metadata reader
    class DocXMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the docxmetadatawriter contract for validated ebook processing.

        Example:
            Exercise DocXMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set DOCX metadata"
        file_types = frozenset(["docx"])
        description = _("Set metadata in %s files") % "DOCX"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise DocXMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            docx_set_metadata(stream, mi)

    plugins += [DocXMetadataWriter]


# Todo: Merge epub_old into epub
try:
    from LiuXin_alpha.metadata.file_sources.epub import set_metadata as epub_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.metadata.file_sources.epub - EPUBMetadataWriter cannot be "
        "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("EPUBMetadataWriter components imported successfully")

    class EPUBMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the epubmetadatawriter contract for validated ebook processing.

        Example:
            Exercise EPUBMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set EPUB metadata"
        file_types = frozenset(["epub"])
        description = _("Set metadata in %s files") % "EPUB"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise EPUBMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            epub_set_metadata(stream, mi, apply_null=self.apply_null)

    plugins += [EPUBMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.fb2 import set_metadata as fb2_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.metadata.fb2 - FB2MetadataWriter cannot be " "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("FB2MetadataWriter components imported successfully")

    class FB2MetadataWriter(BuiltinMetadataWriter):

        """
        Provide the fb2metadatawriter contract for validated ebook processing.

        Example:
            Exercise FB2MetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set FB2 metadata"
        file_types = frozenset(["fb2", "fbz"])
        description = _("Set metadata in %s files") % "FB2"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise FB2MetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            fb2_set_metadata(stream, mi, apply_null=self.apply_null)

    plugins += [FB2MetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.extz import set_metadata as extz_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.metadata.extz - FB2MetadataWriter cannot be "
        "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("HTMLZMetadataWriter components imported successfully")

    class HTMLZMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the htmlzmetadatawriter contract for validated ebook processing.

        Example:
            Exercise HTMLZMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set HTMLZ metadata"
        file_types = frozenset(["htmlz"])
        description = _("Set metadata from %s files") % "HTMLZ"
        author = "John Schember"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise HTMLZMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            extz_set_metadata(stream, mi)

    plugins += [HTMLZMetadataWriter]


# Todo: Move into metadata.file_sources
try:
    from LiuXin_alpha.file_formats.lrf.meta import set_metadata as lrf_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.lrf.meta - LRFMetadataWriter cannot be " "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("LRFMetadataWriter components imported successfully")

    class LRFMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the lrfmetadatawriter contract for validated ebook processing.

        Example:
            Exercise LRFMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set LRF metadata"
        file_types = frozenset(["lrf"])
        description = _("Set metadata in %s files") % "LRF"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise LRFMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            lrf_set_metadata(stream, mi)

    plugins += [LRFMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.mobi import set_metadata as mobi_set_metadata
except RuntimeError as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.metadata.file_sources.mobi_old - MOBIMetadataWriter "
        "cannot be initialized - RuntimeError"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.metadata.file_sources.mobi_old - MOBIMetadataWriter "
        "cannot be initialized"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("MOBIMetadataWriter components imported successfully")

    class MOBIMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the mobimetadatawriter contract for validated ebook processing.

        Example:
            Exercise MOBIMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set MOBI metadata"
        file_types = frozenset(["mobi", "prc", "azw", "azw3", "azw4"])
        description = _("Set metadata in %s files") % "MOBI"
        author = "Marshall T. Vandegrift"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise MOBIMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            mobi_set_metadata(stream, mi)

    plugins += [MOBIMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.odt import set_metadata as odt_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.metadata.file_sources.odt - ODTMetadataWriter cannot be initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("ODTMetadataWriter components imported successfully")

    class ODTMetadataWriter(BuiltinMetadataWriter):

        """
        Write metadata into an ODT's ``meta.xml``, as calibre's ODT writer does.

        Example:
            >>> ODTMetadataWriter.name, sorted(ODTMetadataWriter.file_types)
            ('Set ODT metadata', ['odt'])
        """
        name = "Set ODT metadata"
        file_types = frozenset(["odt"])

        @property
        def writes_subjects(self) -> bool:
            """
            Write the subjects into the document's Subject, unless calibre's reading is asked for.

            Example:
                >>> ODTMetadataWriter(None).writes_subjects in (True, False)
                True


            :return: True when the ``read_subject_fields_as`` preference reads the Subject as subjects.
            """
            return reads_subjects()
        description = _("Set metadata in %s files") % "ODT"

        def set_metadata(self, stream, mi, type):
            """
            Replace the book's ``meta.xml``, leaving every other member of the package as it was.

            Example:
                Every field, and what the file keeps, is checked in
                ``tests/metadata/file_sources/test_odt_metadata_writer.py``; it runs
                :func:`LiuXin_alpha.metadata.file_sources.odt.set_metadata`, whose
                example writes a small ODT.


            :param stream: The ODT, open for reading and writing; it is rewritten in place.
            :param mi: The metadata to write.
            :param type: The file type, ``odt``.
            :return: None.
            """
            odt_set_metadata(stream, mi)

    plugins += [ODTMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.pdb import set_metadata as set_pdb_metadata
except RuntimeError as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.metadata.file_sources.pdb - PDBMetadataWriter "
        "cannot be initialized - RuntimeError"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.metadata.file_sources.pdb - PDBMetadataWriter " "cannot be initialized"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("PDBMetadataWriter components imported successfully")

    class PDBMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the pdbmetadatawriter contract for validated ebook processing.

        Example:
            Exercise PDBMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set PDB metadata"
        file_types = {"pdb"}
        description = _("Set metadata from %s files") % "PDB"
        author = "John Schember"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise PDBMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            set_pdb_metadata(stream, mi)

    plugins += [PDBMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.pdf import set_metadata as pdf_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.metadata.pdf - PDFMetadataWriter cannot be " "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("PDFMetadataWriter components imported successfully")

    class PDFMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the pdfmetadatawriter contract for validated ebook processing.

        Example:
            Exercise PDFMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set PDF metadata"
        file_types = frozenset(["pdf"])
        description = _("Set metadata in %s files") % "PDF"
        # The PDF writer puts the original bytes back itself, and appends an update
        # to the stream where it can; a copy would only cost memory.
        protects_stream = True

        @property
        def writes_subjects(self) -> bool:
            """
            Write the subjects into the Info ``/Subject`` (the comments into the packet), unless calibre's reading is asked for.

            Example:
                >>> PDFMetadataWriter(None).writes_subjects in (True, False)
                True


            :return: True when the ``read_subject_fields_as`` preference reads the Subject as subjects.
            """
            return reads_subjects()
        author = "Kovid Goyal"

        def set_metadata(self, stream, mi, type):
            """
            The PDF stream must be opened in mode rb+ before metadata can be written.

            Example:
                Exercise PDFMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pdf_set_metadata(stream, mi)

    plugins += [PDFMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.rtf import set_metadata as rtf_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.metadata.rt - RTFMetadataWriter cannot be " "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("RTFMetadataWriter components imported successfully")

    class RTFMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the rtfmetadatawriter contract for validated ebook processing.

        Example:
            Exercise RTFMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set RTF metadata"
        file_types = frozenset(["rtf"])
        description = _("Set metadata in %s files") % "RTF"

        @property
        def writes_subjects(self) -> bool:
            """
            Write the subjects into ``\\subject`` (the comments into ``\\doccomm``), unless calibre's reading is asked for.

            Example:
                >>> RTFMetadataWriter(None).writes_subjects in (True, False)
                True


            :return: True when the ``read_subject_fields_as`` preference reads the Subject as subjects.
            """
            return reads_subjects()

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise RTFMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            rtf_set_metadata(stream, mi)

    plugins += [RTFMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.topaz import set_metadata as topaz_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.metadata.topaz - TOPAZMetadataWriter cannot be "
        "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("TOPAZMetadataWriter components imported successfully")

    class TOPAZMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the topazmetadatawriter contract for validated ebook processing.

        Example:
            Exercise TOPAZMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set TOPAZ metadata"
        file_types = frozenset(["tpz", "azw1"])
        description = _("Set metadata in %s files") % "TOPAZ"
        author = "Greg Riker"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise TOPAZMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            topaz_set_metadata(stream, mi)

    plugins += [TOPAZMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.txtz import set_metadata as txtz_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.metadata.extz - TXTZMetadataWriter cannot be "
        "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("TXTZMetadataWriter components imported successfully")

    class TXTZMetadataWriter(BuiltinMetadataWriter):

        """
        Provide the txtzmetadatawriter contract for validated ebook processing.

        Example:
            Exercise TXTZMetadataWriter through a consuming regression::

                python -m pytest -q tests/plugins/test_plugin_base.py
        """
        name = "Set TXTZ metadata"
        file_types = frozenset(["txtz"])
        description = _("Set metadata from %s files") % "TXTZ"
        author = "John Schember"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise TXTZMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/plugins/test_plugin_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param mi: Metadata object exposed to the template function.
            :param type: Value supplied for type under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            txtz_set_metadata(stream, mi)

    plugins += [TXTZMetadataWriter]


def get_metadata_set_plugins() -> list[type[MetadataWriterPlugin]]:
    """
    Returns all the loaded, builtin, MetadataWwriterPlugins.

    Example:
        Exercise get metadata set plugins through a consuming regression::

            python -m pytest -q tests/plugins/test_plugin_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return plugins
