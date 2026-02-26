"""
Metadata readers are tools to read metadata from an ebook file.

The builtin ones bundled with LiuXin are imported here.
"""


import os
import functools

from LiuXin_alpha.customize import MetadataReaderPlugin

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

file_type_plugins: list[type[MetadataReaderPlugin]] = []


# Todo: Make sure finalize is being called from every method
try:
    from LiuXin_alpha.utils.decompression.unrar import extract_first_alphabetically as comic_extract_first
    from LiuXin_alpha.utils.libunzip import extract_member as comic_extract_member
    from LiuXin_alpha.metadata.file_sources.archive import get_comic_metadata
except ImportError as e:
    info_str = "Unable to define ComicMetadataReader - required functions cannot be imported"
    default_log.log_exception(info_str, e, "INFO")
else:
    default_log.info("ComicMetadataReader components imported successfully")

    class ComicMetadataReader(MetadataReaderPlugin):

        name = "Read comic metadata"
        file_types = frozenset(["cbr", "cbz"])
        description = _("Extract cover from comic files")

        def customization_help(self, gui=False):
            return (
                "Read series number from volume or issue number. Default is volume, set this to issue to use "
                "issue number instead."
            )

        def get_metadata(self, stream, ftype):
            if hasattr(stream, "seek") and hasattr(stream, "tell"):
                pos = stream.tell()
                id_ = stream.read(3)
                stream.seek(pos)
                if id_ == b"Rar":
                    ftype = "cbr"
                elif id_.startswith(b"PK"):
                    ftype = "cbz"
            if ftype == "cbr":
                from LiuXin_alpha.utils.decompression.unrar import (
                    extract_first_alphabetically as extract_first,
                )
            else:
                from LiuXin_alpha.utils.libunzip import extract_member

                extract_first = functools.partial(extract_member, sort_alphabetically=True)
            from LiuXin_alpha.metadata.metadata import MetaInformation

            ret = extract_first(stream)
            mi = MetaInformation(None, None)
            stream.seek(0)
            if ftype in {"cbr", "cbz"}:
                series_index = self.site_customization
                if series_index not in {"volume", "issue"}:
                    series_index = "volume"
                try:
                    mi.smart_update(get_comic_metadata(stream, ftype, series_index=series_index))
                except:
                    pass
            if ret is not None:
                path, data = ret
                ext = os.path.splitext(path)[1][1:]
                mi.cover_data = (ext.lower(), data)
            return mi

    file_type_plugins += [ComicMetadataReader]


try:
    from LiuXin_alpha.file_formats.chm.metadata import get_metadata as chm_get_metadata
except ImportError as e:
    info_str = "Unable to import get_metadata from LiuXin.metadata.file_sources.chm"
    default_log.log_exception(info_str, e, "INFO")
else:
    default_log.info("CHMMetadataReader components imported successfully")

    class CHMMetadataReader(MetadataReaderPlugin):

        name = "Read CHM metadata"
        file_types = frozenset(["chm"])
        description = _("Read metadata from %s files") % "CHM"

        def get_metadata(self, stream, ftype):
            return chm_get_metadata(stream)

    file_type_plugins += [CHMMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata as docx_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize DocXMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("DocXMetadataReader components imported successfully")

    class DocXMetadataReader(MetadataReaderPlugin):

        name = "Read DOCX metadata"
        file_types = frozenset(["docx"])
        description = _("Read metadata from %s files") % "DOCX"

        def get_metadata(self, stream, ftype):
            return docx_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            return docx_get_metadata(file_path)

    file_type_plugins += [DocXMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata_inplace
    from LiuXin_alpha.metadata.file_sources.epub import get_metadata as epub_get_metadata
    from LiuXin_alpha.metadata.file_sources.epub import (
        get_quick_metadata as epub_quick_get_metadata,
    )
except Exception as e:
    debug_str = (
        "Unable to initialize EPUBMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.epub_old"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("EPUBMetadataReader components imported successfully")

    class EPUBMetadataReader(MetadataReaderPlugin):

        name = "Read EPUB metadata"
        file_types = frozenset(["epub"])
        description = _("Read metadata from %s files") % "EPUB"

        def get_metadata(self, stream, ftype):

            if self.quick:
                return epub_quick_get_metadata(stream)
            return epub_get_metadata(stream, calibre_metadata=False)

        def get_metadata_inplace(self, file_path, ftype):
            from LiuXin_alpha.metadata.file_sources.epub import get_metadata_inplace

            return get_metadata_inplace(file_path)

    file_type_plugins += [EPUBMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata as fb2_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize FB2MetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.fb2"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("FB2MetadataReader components imported successfully")

    class FB2MetadataReader(MetadataReaderPlugin):

        name = "Read FB2 metadata"
        file_types = frozenset(["fb2"])
        description = _("Read metadata from %s files") % "FB2"

        def get_metadata(self, stream, ftype):
            return fb2_get_metadata(stream)

    file_type_plugins += [FB2MetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.html import get_metadata as html_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize HTMLMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("HTMLMetadataReader components imported successfully")

    class HTMLMetadataReader(MetadataReaderPlugin):

        name = "Read HTML metadata"
        file_types = frozenset(["html"])
        description = _("Read metadata from %s files") % "HTML"

        def get_metadata(self, stream, ftype):
            return html_get_metadata(stream)

    file_type_plugins += [HTMLMetadataReader]

try:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata as extz_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize EXTZMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("HTMLZMetadataReader components imported successfully")

    class HTMLZMetadataReader(MetadataReaderPlugin):

        name = "Read HTMLZ metadata"
        file_types = frozenset(["htmlz"])
        description = _("Read metadata from %s files") % "HTMLZ"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            return extz_get_metadata(stream).finalize()

    file_type_plugins += [HTMLZMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata as imp_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize IMPMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("IMPMetadataReader components imported successfully")

    class IMPMetadataReader(MetadataReaderPlugin):

        name = "Read IMP metadata"
        file_types = frozenset(["imp"])
        description = _("Read metadata from %s files") % "IMP"
        author = "Ashish Kulkarni"

        def get_metadata(self, stream, ftype):
            return imp_get_metadata(stream)

    file_type_plugins += [IMPMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.lit import get_metadata as lit_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize LITMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("LITMetadataReader components imported successfully")

    class LITMetadataReader(MetadataReaderPlugin):

        name = "Read LIT metadata"
        file_types = frozenset(["lit"])
        description = _("Read metadata from %s files") % "LIT"

        def get_metadata(self, stream, ftype):
            return lit_get_metadata(stream)

    file_type_plugins += [LITMetadataReader]


try:
    from LiuXin_alpha.file_formats.lrf.meta import get_metadata as lrf_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize LRFMetadataReader - necessary functions couldn't be imported from "
        "LiuXfile_formats.lrf.metatml"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("LRFMetadataReader components imported successfully")

    class LRFMetadataReader(MetadataReaderPlugin):

        name = "Read LRF metadata"
        file_types = frozenset(["lrf"])
        description = _("Read metadata from %s files") % "LRF"

        def get_metadata(self, stream, ftype):
            # Check this actually works
            return lrf_get_metadata(stream, calibre_md=False)

        def get_metadata_inplace(self, file_path, ftype):
            with open(file_path, "rb") as lrf_file_stream:
                return lrf_get_metadata(lrf_file_stream, calibre_md=False)

    file_type_plugins += [LRFMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.lrx import get_metadata as lrx_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize LRXMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("LRXMetadataReader components imported successfully")

    class LRXMetadataReader(MetadataReaderPlugin):

        name = "Read LRX metadata"
        file_types = frozenset(["lrx"])
        description = _("Read metadata from %s files") % "LRX"

        def get_metadata(self, stream, ftype):
            return lrx_get_metadata(stream)

    file_type_plugins += [LRXMetadataReader]

# Todo: Make sure that all file extensions are added to the constants
try:
    from LiuXin_alpha.metadata.file_sources.mobi import get_metadata as mobi_get_metadata
    from LiuXin_alpha.metadata.file_sources.mobi import (
        get_metadata_inplace as mobi_get_metadata_inplace,
    )
except Exception as e:
    debug_str = (
        "Unable to initialize MOBIMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("MOBIMetadataReader components imported successfully")

    class MOBIMetadataReader(MetadataReaderPlugin):

        name = "Read MOBI metadata"
        file_types = frozenset(["mobi", "prc", "azw", "azw3", "azw4", "pobi"])
        description = _("Read metadata from %s files") % "MOBI"

        def get_metadata(self, stream, ftype):
            return mobi_get_metadata(stream).finalize()

        def get_metadata_inplace(self, file_path, ftype):
            return mobi_get_metadata_inplace(file_path).finalize()

    file_type_plugins += [MOBIMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.odt import get_metadata as odt_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize ODTMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("ODTMetadataReader components imported successfully")

    class ODTMetadataReader(MetadataReaderPlugin):

        name = "Read ODT metadata"
        file_types = frozenset(["odt"])
        description = _("Read metadata from %s files") % "ODT"

        def get_metadata(self, stream, ftype, **kwargs):
            return odt_get_metadata(stream, **kwargs)

        def get_metadata_inplace(self, file_path, ftype):
            with open(file_path, "rb") as odt_file_stream:
                return odt_get_metadata(odt_file_stream)

    file_type_plugins += [ODTMetadataReader]

try:
    from LiuXin_alpha.file_formats.opf.opf2 import OPF
except Exception as e:
    debug_str = (
        "Unable to initialize OPFMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("OPFMetadataReader components imported successfully")

    class OPFMetadataReader(MetadataReaderPlugin):

        name = "Read OPF metadata"
        file_types = frozenset(["opf"])
        description = _("Read metadata from %s files") % "OPF"

        def get_metadata(self, stream, ftype):
            return OPF(stream, os.getcwdu()).to_book_metadata()

    file_type_plugins += [OPFMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata as pdb_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize PDBXMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("PDBMetadataReader components imported successfully")

    class PDBMetadataReader(MetadataReaderPlugin):

        name = "Read PDB metadata"
        file_types = frozenset(["pdb", "updb"])
        description = _("Read metadata from %s files") % "PDB"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            return pdb_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            return pdb_get_metadata(file_path)

    file_type_plugins += [PDBMetadataReader]

try:
    from LiuXin_alpha.metadata.file_sources.pdf import get_metadata as pdf_get_metadata
    from LiuXin_alpha.metadata.file_sources.pdf import (
        get_metadata_inplace as pdf_get_metadata_inplace,
    )
    from LiuXin_alpha.metadata.file_sources.pdf import (
        get_quick_metadata as pdf_get_quick_metadata,
    )
except Exception as e:
    debug_str = (
        "Unable to initialize PDFMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.pdf"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("PDFMetadataReader components imported successfully")

    class PDFMetadataReader(MetadataReaderPlugin):

        name = "Read PDF metadata"
        file_types = frozenset(["pdf"])
        description = _("Read metadata from %s files") % "PDF"

        def get_metadata(self, stream, ftype):
            if self.quick:
                return pdf_get_quick_metadata(stream).finalize()
            return pdf_get_metadata(stream).finalize()

        def get_metadata_inplace(self, file_path, ftype):
            if self.quick:
                return pdf_get_metadata_inplace(file_path).finalize()
            return pdf_get_metadata_inplace(file_path).finalize()

    file_type_plugins += [PDFMetadataReader]

# Todo: Add a call to finalize everywhere
try:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata as pml_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize PDBXMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("PMLMetadataReader components imported successfully")

    class PMLMetadataReader(MetadataReaderPlugin):

        name = "Read PML metadata"
        file_types = frozenset(["pml", "pmlz"])
        description = _("Read metadata from %s files") % "PML"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            return pml_get_metadata(stream).finalize()

    file_type_plugins += [PMLMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.rar import get_metadata as rar_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize RARXMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("RARMetadataReader components imported successfully")

    class RARMetadataReader(MetadataReaderPlugin):

        name = "Read RAR metadata"
        file_types = frozenset(["rar"])
        description = _("Read metadata from ebooks in RAR archives")

        def get_metadata(self, stream, ftype):
            return rar_get_metadata(stream)

    file_type_plugins += [RARMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata as rb_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize RBMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("RBMetadataReader components imported successfully")

    class RBMetadataReader(MetadataReaderPlugin):

        name = "Read RB metadata"
        file_types = frozenset(["rb"])
        description = _("Read metadata from %s files") % "RB"
        author = "Ashish Kulkarni"

        def get_metadata(self, stream, ftype):
            return rb_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            return rb_get_metadata(file_path)

    file_type_plugins += [RBMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata as rtf_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize RBMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("RTFMetadataReader components imported successfully")

    class RTFMetadataReader(MetadataReaderPlugin):

        name = "Read RTF metadata"
        file_types = frozenset(["rtf"])
        description = _("Read metadata from %s files") % "RTF"

        def get_metadata(self, stream, ftype):
            return rtf_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            return rtf_get_metadata(file_path)

    file_type_plugins += [RTFMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.snb import get_metadata as snb_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize SNBMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("SNBMetadataReader components imported successfully")

    class SNBMetadataReader(MetadataReaderPlugin):

        name = "Read SNB metadata"
        file_types = frozenset(["snb"])
        description = _("Read metadata from %s files") % "SNB"
        author = "Li Fanxi"

        def get_metadata(self, stream, ftype):
            return snb_get_metadata(stream).finalize()

    file_type_plugins += [SNBMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.topaz import get_metadata as topaz_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize TOPAZMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("TOPAZMetadataReader components imported successfully")

    class TOPAZMetadataReader(MetadataReaderPlugin):

        name = "Read Topaz metadata"
        file_types = frozenset(["tpz", "azw1"])
        description = _("Read metadata from %s files") % "MOBI"

        def get_metadata(self, stream, ftype):
            return topaz_get_metadata(stream)

    file_type_plugins += [TOPAZMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata as txt_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize TXTMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("TXTMetadataReader components imported successfully")

    class TXTMetadataReader(MetadataReaderPlugin):

        name = "Read TXT metadata"
        file_types = frozenset(["txt"])
        description = _("Read metadata from %s files") % "TXT"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            return txt_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            return txt_get_metadata(file_path)

    file_type_plugins += [TXTMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.extz import get_metadata as extz_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize TXTZMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("TXTZMetadataReader components imported successfully")

    class TXTZMetadataReader(MetadataReaderPlugin):

        name = "Read TXTZ metadata"
        file_types = frozenset(["txtz"])
        description = _("Read metadata from %s files") % "TXTZ"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            return extz_get_metadata(stream)

    file_type_plugins += [TXTZMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.zip import get_metadata as zip_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize ZipMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.zip"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("ZipMetadataReader components imported successfully")

    class ZipMetadataReader(MetadataReaderPlugin):

        name = "Read ZIP metadata"
        file_types = frozenset(["zip", "oebzip"])
        description = _("Read metadata from ebooks in ZIP archives")

        def get_metadata(self, stream, ftype):
            return zip_get_metadata(stream)

    file_type_plugins += [ZipMetadataReader]


def get_metadata_reader_plugins() -> list[type[MetadataReaderPlugin]]:
    """
    Get all the Metadata Reader plugins which have successfully loaded.

    :return:
    """
    return file_type_plugins
