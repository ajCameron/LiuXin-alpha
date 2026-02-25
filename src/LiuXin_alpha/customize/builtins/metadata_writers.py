"""
Meatdata Writers are responsible for writing metadata back to ebook files.

The builtin ones bundled with LiuXin are imported here.
"""

from LiuXin_alpha.customize import MetadataWriterPlugin

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logger import default_log

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

        name = "Set DOCX metadata"
        file_types = frozenset(["docx"])
        description = _("Set metadata in %s files") % "DOCX"

        def set_metadata(self, stream, mi, type):
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

        name = "Set EPUB metadata"
        file_types = frozenset(["epub"])
        description = _("Set metadata in %s files") % "EPUB"

        def set_metadata(self, stream, mi, type):
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

        name = "Set FB2 metadata"
        file_types = frozenset(["fb2"])
        description = _("Set metadata in %s files") % "FB2"

        def set_metadata(self, stream, mi, type):
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

        name = "Set HTMLZ metadata"
        file_types = frozenset(["htmlz"])
        description = _("Set metadata from %s files") % "HTMLZ"
        author = "John Schember"

        def set_metadata(self, stream, mi, type):
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

        name = "Set LRF metadata"
        file_types = frozenset(["lrf"])
        description = _("Set metadata in %s files") % "LRF"

        def set_metadata(self, stream, mi, type):
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

        name = "Set MOBI metadata"
        file_types = frozenset(["mobi", "prc", "azw", "azw3", "azw4"])
        description = _("Set metadata in %s files") % "MOBI"
        author = "Marshall T. Vandegrift"

        def set_metadata(self, stream, mi, type):
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

        name = "Set PDB metadata"
        file_types = {"pdb"}
        description = _("Set metadata from %s files") % "PDB"
        author = "John Schember"

        def set_metadata(self, stream, mi, type):
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

        name = "Set PDF metadata"
        file_types = frozenset(["pdf"])
        description = _("Set metadata in %s files") % "PDF"
        author = "Kovid Goyal"

        def set_metadata(self, stream, mi, type):
            """
            The PDF stream must be opened in mode rb+ before metadata can be written.
            :param stream:
            :type stream: A PDF file stream - which must be opened as a rb+ stream
            :param mi:
            :type mi: calibreMetaInformation object - cannot upgrade to LiuXin MetaData due to C++ module dependency
            :param type: The type of file - currently not used
            :return:
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

        name = "Set RTF metadata"
        file_types = frozenset(["rtf"])
        description = _("Set metadata in %s files") % "RTF"

        def set_metadata(self, stream, mi, type):
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

        name = "Set TOPAZ metadata"
        file_types = frozenset(["tpz", "azw1"])
        description = _("Set metadata in %s files") % "TOPAZ"
        author = "Greg Riker"

        def set_metadata(self, stream, mi, type):
            topaz_set_metadata(stream, mi)

    plugins += [TOPAZMetadataWriter]


try:
    from LiuXin_alpha.metadata.file_sources.extz import set_metadata as extz_set_metadata
except Exception as e:
    debug_str = (
        "Cannot import set_metadata from LiuXin.file_formats.metadata.extz - TXTZMetadataWriter cannot be "
        "initialized."
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("TXTZMetadataWriter components imported successfully")

    class TXTZMetadataWriter(MetadataWriterPlugin):

        name = "Set TXTZ metadata"
        file_types = frozenset(["txtz"])
        description = _("Set metadata from %s files") % "TXTZ"
        author = "John Schember"

        def set_metadata(self, stream, mi, type):
            extz_set_metadata(stream, mi)

    plugins += [TXTZMetadataWriter]


def get_metadata_set_plugins() -> list[type[MetadataWriterPlugin]]:
    """
    Returns all the loaded, builtin, MetadataWwriterPlugins.

    :return:
    """
    return plugins
