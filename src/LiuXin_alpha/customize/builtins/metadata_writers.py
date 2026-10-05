"""
Register built-in file-format metadata writers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise metadata writers through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""

from LiuXin_alpha.customize import MetadataWriterPlugin

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

plugins: list[type[MetadataWriterPlugin]] = []

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
    class DocXMetadataWriter(MetadataWriterPlugin):

        """
        Provide the docxmetadatawriter contract for validated ebook processing.

        Example:
            Exercise DocXMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Set DOCX metadata"
        file_types = frozenset(["docx"])
        description = _("Set metadata in %s files") % "DOCX"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise DocXMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class EPUBMetadataWriter(MetadataWriterPlugin):

        """
        Provide the epubmetadatawriter contract for validated ebook processing.

        Example:
            Exercise EPUBMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Set EPUB metadata"
        file_types = frozenset(["epub"])
        description = _("Set metadata in %s files") % "EPUB"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise EPUBMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class FB2MetadataWriter(MetadataWriterPlugin):

        """
        Provide the fb2metadatawriter contract for validated ebook processing.

        Example:
            Exercise FB2MetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Set FB2 metadata"
        file_types = frozenset(["fb2", "fbz"])
        description = _("Set metadata in %s files") % "FB2"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise FB2MetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class HTMLZMetadataWriter(MetadataWriterPlugin):

        """
        Provide the htmlzmetadatawriter contract for validated ebook processing.

        Example:
            Exercise HTMLZMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
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

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class LRFMetadataWriter(MetadataWriterPlugin):

        """
        Provide the lrfmetadatawriter contract for validated ebook processing.

        Example:
            Exercise LRFMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Set LRF metadata"
        file_types = frozenset(["lrf"])
        description = _("Set metadata in %s files") % "LRF"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise LRFMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class MOBIMetadataWriter(MetadataWriterPlugin):

        """
        Provide the mobimetadatawriter contract for validated ebook processing.

        Example:
            Exercise MOBIMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
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

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class PDBMetadataWriter(MetadataWriterPlugin):

        """
        Provide the pdbmetadatawriter contract for validated ebook processing.

        Example:
            Exercise PDBMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
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

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class PDFMetadataWriter(MetadataWriterPlugin):

        """
        Provide the pdfmetadatawriter contract for validated ebook processing.

        Example:
            Exercise PDFMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Set PDF metadata"
        file_types = frozenset(["pdf"])
        description = _("Set metadata in %s files") % "PDF"
        author = "Kovid Goyal"

        def set_metadata(self, stream, mi, type):
            """
            The PDF stream must be opened in mode rb+ before metadata can be written.

            Example:
                Exercise PDFMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class RTFMetadataWriter(MetadataWriterPlugin):

        """
        Provide the rtfmetadatawriter contract for validated ebook processing.

        Example:
            Exercise RTFMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Set RTF metadata"
        file_types = frozenset(["rtf"])
        description = _("Set metadata in %s files") % "RTF"

        def set_metadata(self, stream, mi, type):
            """
            Update document metadata while preserving unrelated package state.

            Example:
                Exercise RTFMetadataWriter.set metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class TOPAZMetadataWriter(MetadataWriterPlugin):

        """
        Provide the topazmetadatawriter contract for validated ebook processing.

        Example:
            Exercise TOPAZMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
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

                    python -m pytest -q tests/customize/test_customize_base.py


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

    class TXTZMetadataWriter(MetadataWriterPlugin):

        """
        Provide the txtzmetadatawriter contract for validated ebook processing.

        Example:
            Exercise TXTZMetadataWriter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
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

                    python -m pytest -q tests/customize/test_customize_base.py


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

            python -m pytest -q tests/customize/test_customize_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return plugins
