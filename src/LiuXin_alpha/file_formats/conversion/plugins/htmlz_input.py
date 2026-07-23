# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)
from LiuXin_alpha.file_formats.conversion.report import ensure_conversion_report
from LiuXin_alpha.file_formats.archive_preflight import (
    normalized_zip_member_name,
    validate_zip_member_infos,
)

from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.calibre import guess_type
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL 3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class HTMLZInput(InputFormatPlugin):

    name = "HTLZ Input"
    author = "John Schember"
    description = "Convert HTML files to HTML"
    file_types = {"htmlz"}
    max_archive_members = 4096
    max_member_uncompressed_size = 256 * 1024 * 1024
    max_total_uncompressed_size = 512 * 1024 * 1024
    max_compression_ratio = 1000
    min_compression_ratio_check_size = 1024 * 1024

    def _warn(self: _typing.Self, log: _typing.Any, message: _typing.Any) -> None:
        warn = getattr(log, "warning", None) or getattr(log, "warn", None)
        if warn is not None:
            warn(message)

    def _warn_optional_enrichment_loss(self: _typing.Self, log: _typing.Any, options: _typing.Any, code: _typing.Any, message: _typing.Any, details: _typing.Any = None) -> None:
        self._warn(log, message)
        report = ensure_conversion_report(options)
        report.add_warning(message)
        report.add_loss_event(
            phase="htmlz-input",
            code=code,
            message=message,
            count=1,
            source_format="htmlz",
            target_format="oeb",
            edge_name="htmlz-to-oeb",
            details=details or {},
        )

    def _safe_cover_path(self: _typing.Self, basedir: _typing.Any, cover_path: _typing.Any) -> _typing.Any:
        if not cover_path:
            return None
        cover_path = str(cover_path).replace("\\", "/")
        first_part = cover_path.split("/", 1)[0]
        if cover_path.startswith("/") or (
            len(first_part) == 2 and first_part[1] == ":"
        ):
            return None

        base = os.path.abspath(basedir)
        candidate = os.path.abspath(os.path.join(base, cover_path))
        try:
            if os.path.commonpath([base, candidate]) != base:
                return None
        except ValueError:
            return None
        return candidate

    def normalized_archive_member_name(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        return normalized_zip_member_name(
            name,
            member_label="HTMLZ archive",
            error_type=ValueError,
        )

    def validate_container_members(self: _typing.Self, stream: _typing.Any) -> None:
        from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

        stream.seek(0)
        try:
            zf = ZipFile(stream, "r")
        except Exception as err:
            stream.seek(0)
            raise ValueError("HTMLZ appears to be invalid ZIP file") from err

        try:
            names = validate_zip_member_infos(
                zf.infolist(),
                container_label="HTMLZ file",
                member_label="HTMLZ archive",
                error_type=ValueError,
                max_archive_members=self.max_archive_members,
                max_member_uncompressed_size=self.max_member_uncompressed_size,
                max_total_uncompressed_size=self.max_total_uncompressed_size,
                max_compression_ratio=self.max_compression_ratio,
                min_compression_ratio_check_size=self.min_compression_ratio_check_size,
            )

            has_top_level_html = False
            for normalized_name, original_name in names.items():
                is_dir = original_name.endswith("/")
                if (
                    not is_dir
                    and "/" not in normalized_name
                    and os.path.splitext(normalized_name)[1].lower()
                    in (".html", ".xhtml", ".htm")
                ):
                    has_top_level_html = True

            if not has_top_level_html:
                raise ValueError(_("No top level HTML file found."))
        finally:
            zf.close()
            stream.seek(0)

    def warn_preflight_rejection(self: _typing.Self, stream: _typing.Any, log: _typing.Any, error: _typing.Any) -> None:
        path = getattr(stream, "name", "stream")
        self._warn(log, "HTMLZ preflight rejected %s: %s" % (path, error))

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Takes an htmlz file as input and outputs an OEB.
        :param stream: The html file as a stream to convert
        :param options:
        :param file_ext:
        :param log:
        :param accelerators:
        :return:
        """
        from LiuXin_alpha.file_formats.chardet import xml_to_unicode
        from LiuXin_alpha.file_formats.opf.opf2 import OPF
        from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

        self.log = log
        try:
            self.validate_container_members(stream)
        except ValueError as err:
            self.warn_preflight_rejection(stream, log, err)
            raise

        work_root = choose_conversion_workdir("_htmlz_input")
        with CurrentDir(work_root):
            top_levels = []

            # Extract content from zip archive.
            zf = ZipFile(stream)
            zf.extractall()

            # Find the HTML file in the archive. It needs to be top level.
            index = ""
            multiple_html = False
            # Get a list of all top level files in the archive.
            for x in os.listdir("."):
                if os.path.isfile(x):
                    top_levels.append(x)

            # Try to find an index. file.
            for x in top_levels:
                if x.lower() in ("index.html", "index.xhtml", "index.htm"):
                    index = x
                    break

            # Look for multiple HTML files in the archive. We look at the
            # top level files only as only they matter in HTMLZ.
            for x in top_levels:
                if os.path.splitext(x)[1].lower() in (".html", ".xhtml", ".htm"):
                    # Set index to the first HTML file found if it's not
                    # called index.
                    if not index:
                        index = x
                    else:
                        multiple_html = True

            # Warn the user if there multiple HTML file in the archive. HTMLZ
            # supports a single HTML file. A conversion with a multiple HTML file
            # HTMLZ archive probably won't turn out as the user expects. With
            # Multiple HTML files ZIP input should be used in place of HTMLZ.
            if multiple_html:
                log.warn(_("Multiple HTML files found in the archive. Only %s will be used.") % index)

            if index:
                with open(index, "rb") as tf:
                    html = tf.read()
            else:
                raise Exception(_("No top level HTML file found."))

            if not html:
                raise Exception(_("Top level HTML file %s is empty") % index)

            # Encoding
            if options.input_encoding:
                ienc = options.input_encoding
            else:
                ienc = xml_to_unicode(html[:4096])[-1] or "utf-8"
            html = html.decode(ienc, "replace")

            # Run the HTML through the html processing plugin.
            from LiuXin_alpha.customize.ui import plugin_for_input_format

            html_input = plugin_for_input_format("html")
            for opt in html_input.options:
                setattr(options, opt.option.name, opt.recommended_value)
            options.input_encoding = "utf-8"
            base = os.getcwd()
            fname = os.path.join(base, "index.html")
            c = 0
            while os.path.exists(fname):
                c += 1
                fname = os.path.join(base, "index%d.html" % c)
            htmlfile = open(fname, "wb")
            with htmlfile:
                htmlfile.write(html.encode("utf-8"))
            odi = options.debug_pipeline
            options.debug_pipeline = None
            # Generate oeb from html conversion.
            with open(htmlfile.name, "rb") as bin_html_file:
                oeb = html_input.convert(bin_html_file, options, "html", log, {})
            options.debug_pipeline = odi
            os.remove(htmlfile.name)

            # Set metadata from file.
            from LiuXin_alpha.customize.ui import get_file_type_metadata
            from LiuXin_alpha.file_formats.oeb.transforms.metadata import (
                meta_info_to_oeb_metadata,
            )

            stream.seek(0)
            mi = get_file_type_metadata(stream, file_ext)
            if hasattr(mi, "to_calibre"):
                mi = mi.to_calibre()
            meta_info_to_oeb_metadata(mi, oeb.metadata, log)

            # Get the cover path from the OPF.
            cover_path = None
            opf = None
            for x in top_levels:
                if os.path.splitext(x)[1].lower() == ".opf":
                    opf = x
                    break
            if opf:
                try:
                    opf_obj = OPF(opf, basedir=os.getcwd())
                    cover_path = opf_obj.raster_cover or opf_obj.cover
                except Exception as err:
                    self._warn_optional_enrichment_loss(
                        log,
                        options,
                        "optional-opf-enrichment-failed",
                        _("Could not read HTMLZ metadata file %s: %s") % (opf, err),
                        {"opf_member": opf, "reason": str(err)},
                    )
            # Set the cover.
            if cover_path:
                cover_file = self._safe_cover_path(os.getcwd(), cover_path)
                if cover_file is None:
                    self._warn_optional_enrichment_loss(
                        log,
                        options,
                        "optional-cover-unsafe-path",
                        _("Ignoring unsafe HTMLZ cover path: %s") % cover_path,
                        {"cover_path": cover_path},
                    )
                elif not os.path.isfile(cover_file):
                    self._warn_optional_enrichment_loss(
                        log,
                        options,
                        "optional-cover-missing",
                        _("HTMLZ cover file %s was not found") % cover_path,
                        {"cover_path": cover_path},
                    )
                else:
                    with open(cover_file, "rb") as cf:
                        cdata = cf.read()
                    cover_name = os.path.basename(cover_path)
                    item_id, href = oeb.manifest.generate("cover", cover_name)
                    oeb.manifest.add(item_id, href, guess_type(cover_name)[0], data=cdata)
                    oeb.guide.add("cover", "Cover", href)

            return oeb
