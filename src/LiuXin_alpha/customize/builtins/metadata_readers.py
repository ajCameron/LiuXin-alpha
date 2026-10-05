"""
Register built-in file-format metadata readers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise metadata readers through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""
from LiuXin_alpha.customize import MetadataReaderPlugin

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

file_type_plugins: list[type[MetadataReaderPlugin]] = []


# Todo: Make sure finalize is being called from every method
try:
    from LiuXin_alpha.metadata.file_sources.comic import get_metadata as comic_get_metadata
except ImportError as e:
    info_str = "Unable to define ComicMetadataReader - required functions cannot be imported"
    default_log.log_exception(info_str, e, "INFO")
else:
    default_log.info("ComicMetadataReader components imported successfully")

    class ComicMetadataReader(MetadataReaderPlugin):

        """
        Parse comicmetadatareader data into normalized ebook structures.

        Example:
            Exercise ComicMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read comic metadata"
        file_types = frozenset(["cbr", "cbz"])
        description = _("Extract cover from comic files")

        def customization_help(self, gui=False):
            """
            Perform the customization help operation under explicit file-format and conversion rules.

            Example:
                Exercise ComicMetadataReader.customization help through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param gui: Value supplied for gui under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return (
                "Read series number from volume or issue number. Default is volume, set this to issue to use "
                "issue number instead."
            )

        def get_metadata(self, stream, ftype, **kwargs):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise ComicMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            series_index = self.site_customization
            if series_index not in {"volume", "issue"}:
                series_index = "volume"
            return comic_get_metadata(stream, ftype=ftype, series_index=series_index, **kwargs)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise ComicMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            series_index = self.site_customization
            if series_index not in {"volume", "issue"}:
                series_index = "volume"
            return comic_get_metadata(file_path, ftype=ftype, series_index=series_index)

    file_type_plugins += [ComicMetadataReader]


try:
    from LiuXin_alpha.file_formats.chm.metadata import get_metadata as chm_get_metadata
except ImportError as e:
    info_str = "Unable to import get_metadata from LiuXin.metadata.file_sources.chm"
    default_log.log_exception(info_str, e, "INFO")
else:
    default_log.info("CHMMetadataReader components imported successfully")

    class CHMMetadataReader(MetadataReaderPlugin):

        """
        Parse chmmetadatareader data into normalized ebook structures.

        Example:
            Exercise CHMMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read CHM metadata"
        file_types = frozenset(["chm"])
        description = _("Read metadata from %s files") % "CHM"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise CHMMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse docxmetadatareader data into normalized ebook structures.

        Example:
            Exercise DocXMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read DOCX metadata"
        file_types = frozenset(["docx"])
        description = _("Read metadata from %s files") % "DOCX"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise DocXMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return docx_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise DocXMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse epubmetadatareader data into normalized ebook structures.

        Example:
            Exercise EPUBMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read EPUB metadata"
        file_types = frozenset(["epub"])
        description = _("Read metadata from %s files") % "EPUB"

        def get_metadata(self, stream, ftype):

            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise EPUBMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.quick:
                return epub_quick_get_metadata(stream)
            return epub_get_metadata(stream, calibre_metadata=False)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise EPUBMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse fb2metadatareader data into normalized ebook structures.

        Example:
            Exercise FB2MetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read FB2 metadata"
        file_types = frozenset(["fb2", "fbz"])
        description = _("Read metadata from %s files") % "FB2"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise FB2MetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse htmlmetadatareader data into normalized ebook structures.

        Example:
            Exercise HTMLMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read HTML metadata"
        file_types = frozenset(["html"])
        description = _("Read metadata from %s files") % "HTML"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise HTMLMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse htmlzmetadatareader data into normalized ebook structures.

        Example:
            Exercise HTMLZMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read HTMLZ metadata"
        file_types = frozenset(["htmlz"])
        description = _("Read metadata from %s files") % "HTMLZ"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise HTMLZMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse impmetadatareader data into normalized ebook structures.

        Example:
            Exercise IMPMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read IMP metadata"
        file_types = frozenset(["imp"])
        description = _("Read metadata from %s files") % "IMP"
        author = "Ashish Kulkarni"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise IMPMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse litmetadatareader data into normalized ebook structures.

        Example:
            Exercise LITMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read LIT metadata"
        file_types = frozenset(["lit"])
        description = _("Read metadata from %s files") % "LIT"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise LITMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return lit_get_metadata(stream)

    file_type_plugins += [LITMetadataReader]


try:
    from LiuXin_alpha.metadata.file_sources.lrf import get_metadata as lrf_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize LRFMetadataReader - necessary functions couldn't be imported from "
        "LiuXin_alpha.metadata.file_sources.lrf"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("LRFMetadataReader components imported successfully")

    class LRFMetadataReader(MetadataReaderPlugin):

        """
        Parse lrfmetadatareader data into normalized ebook structures.

        Example:
            Exercise LRFMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read LRF metadata"
        file_types = frozenset(["lrf"])
        description = _("Read metadata from %s files") % "LRF"

        def get_metadata(self, stream, ftype, **kwargs):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise LRFMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return lrf_get_metadata(stream, calibre_md=False, **kwargs)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise LRFMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return lrf_get_metadata(file_path, calibre_md=False)

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

        """
        Parse lrxmetadatareader data into normalized ebook structures.

        Example:
            Exercise LRXMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read LRX metadata"
        file_types = frozenset(["lrx"])
        description = _("Read metadata from %s files") % "LRX"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise LRXMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse mobimetadatareader data into normalized ebook structures.

        Example:
            Exercise MOBIMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read MOBI metadata"
        file_types = frozenset(["mobi", "prc", "azw", "azw3", "azw4", "pobi"])
        description = _("Read metadata from %s files") % "MOBI"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise MOBIMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            md = mobi_get_metadata(stream)
            return md.finalize() if hasattr(md, "finalize") else md

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise MOBIMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            md = mobi_get_metadata_inplace(file_path)
            return md.finalize() if hasattr(md, "finalize") else md

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

        """
        Parse odtmetadatareader data into normalized ebook structures.

        Example:
            Exercise ODTMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read ODT metadata"
        file_types = frozenset(["odt"])
        description = _("Read metadata from %s files") % "ODT"

        def get_metadata(self, stream, ftype, **kwargs):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise ODTMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return odt_get_metadata(stream, **kwargs)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise ODTMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            with open(file_path, "rb") as odt_file_stream:
                return odt_get_metadata(odt_file_stream)

    file_type_plugins += [ODTMetadataReader]

try:
    from LiuXin_alpha.metadata.file_sources.opf import get_metadata as opf_get_metadata
except Exception as e:
    debug_str = (
        "Unable to initialize OPFMetadataReader - necessary functions couldn't be imported from "
        "LiuXin.metadata.file_sources.html"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("OPFMetadataReader components imported successfully")

    class OPFMetadataReader(MetadataReaderPlugin):

        """
        Parse opfmetadatareader data into normalized ebook structures.

        Example:
            Exercise OPFMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read OPF metadata"
        file_types = frozenset(["opf"])
        description = _("Read metadata from %s files") % "OPF"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise OPFMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return opf_get_metadata(stream, calibre=True)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise OPFMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return opf_get_metadata(file_path, calibre=True)

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

        """
        Parse pdbmetadatareader data into normalized ebook structures.

        Example:
            Exercise PDBMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read PDB metadata"
        file_types = frozenset(["pdb", "updb"])
        description = _("Read metadata from %s files") % "PDB"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise PDBMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return pdb_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise PDBMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse pdfmetadatareader data into normalized ebook structures.

        Example:
            Exercise PDFMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read PDF metadata"
        file_types = frozenset(["pdf"])
        description = _("Read metadata from %s files") % "PDF"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise PDFMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.quick:
                return pdf_get_quick_metadata(stream).finalize()
            return pdf_get_metadata(stream).finalize()

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise PDFMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse pmlmetadatareader data into normalized ebook structures.

        Example:
            Exercise PMLMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read PML metadata"
        file_types = frozenset(["pml", "pmlz"])
        description = _("Read metadata from %s files") % "PML"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise PMLMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse rarmetadatareader data into normalized ebook structures.

        Example:
            Exercise RARMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read RAR metadata"
        file_types = frozenset(["rar"])
        description = _("Read metadata from ebooks in RAR archives")

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise RARMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse rbmetadatareader data into normalized ebook structures.

        Example:
            Exercise RBMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read RB metadata"
        file_types = frozenset(["rb"])
        description = _("Read metadata from %s files") % "RB"
        author = "Ashish Kulkarni"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise RBMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return rb_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise RBMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse rtfmetadatareader data into normalized ebook structures.

        Example:
            Exercise RTFMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read RTF metadata"
        file_types = frozenset(["rtf"])
        description = _("Read metadata from %s files") % "RTF"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise RTFMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return rtf_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise RTFMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse snbmetadatareader data into normalized ebook structures.

        Example:
            Exercise SNBMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read SNB metadata"
        file_types = frozenset(["snb"])
        description = _("Read metadata from %s files") % "SNB"
        author = "Li Fanxi"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise SNBMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return snb_get_metadata(stream)

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

        """
        Parse topazmetadatareader data into normalized ebook structures.

        Example:
            Exercise TOPAZMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read Topaz metadata"
        file_types = frozenset(["tpz", "azw1"])
        description = _("Read metadata from %s files") % "MOBI"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise TOPAZMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse txtmetadatareader data into normalized ebook structures.

        Example:
            Exercise TXTMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read TXT metadata"
        file_types = frozenset(["txt"])
        description = _("Read metadata from %s files") % "TXT"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise TXTMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return txt_get_metadata(stream)

        def get_metadata_inplace(self, file_path, ftype):
            """
            Return metadata inplace under the format's safety and compatibility rules.

            Example:
                Exercise TXTMetadataReader.get metadata inplace through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param file_path: Value supplied for file path under the utility contract.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse txtzmetadatareader data into normalized ebook structures.

        Example:
            Exercise TXTZMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read TXTZ metadata"
        file_types = frozenset(["txtz"])
        description = _("Read metadata from %s files") % "TXTZ"
        author = "John Schember"

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise TXTZMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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

        """
        Parse zipmetadatareader data into normalized ebook structures.

        Example:
            Exercise ZipMetadataReader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py
        """
        name = "Read ZIP metadata"
        file_types = frozenset(["zip", "oebzip"])
        description = _("Read metadata from ebooks in ZIP archives")

        def get_metadata(self, stream, ftype):
            """
            Return normalized metadata parsed from the supplied document.

            Example:
                Exercise ZipMetadataReader.get metadata through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param ftype: Value supplied for ftype under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return zip_get_metadata(stream)

    file_type_plugins += [ZipMetadataReader]


def get_metadata_reader_plugins() -> list[type[MetadataReaderPlugin]]:
    """
    Get all the Metadata Reader plugins which have successfully loaded.

    Example:
        Exercise get metadata reader plugins through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return file_type_plugins
