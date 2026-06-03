# -*- coding: utf-8 -*-

import os
import shutil

from LiuXin_alpha.customize.conversion import OutputFormatPlugin, OptionRecommendation
from LiuXin_alpha.file_formats.conversion.report import (
    ConversionLossSample,
    ensure_conversion_report,
)

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory, TemporaryFile

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

NEWLINE_TYPES = ["system", "unix", "old_mac", "windows"]


def _report_context(output_plugin, input_plugin, opts):
    edge = getattr(opts, "conversion_edge", None)
    input_format = getattr(input_plugin, "file_type", None) or "oeb"
    source_format = getattr(edge, "source_format", None) or input_format
    target_format = getattr(edge, "target_format", None) or output_plugin.file_type
    edge_name = getattr(edge, "name", None) or "%s-to-%s" % (source_format, target_format)
    return ensure_conversion_report(
        opts,
        source_format=source_format,
        target_format=target_format,
        edge_name=edge_name,
    )


def _unencodable_character_counts(text, encoding):
    counts = {}
    for char in text:
        try:
            char.encode(encoding, "strict")
        except UnicodeEncodeError:
            counts[char] = counts.get(char, 0) + 1
    return counts


def _report_output_encoding_replacements(report, unsupported_counts, output_encoding):
    if not unsupported_counts:
        return
    replacement_count = sum(unsupported_counts.values())
    sample_chars = list(unsupported_counts.keys())[:8]
    report.add_loss_event(
        phase="txt-output",
        code="output-encoding-character-replacement",
        message=(
            "TXT output encoded with %s replaced %d unsupported character%s with '?'."
            % (output_encoding, replacement_count, "" if replacement_count == 1 else "s")
        ),
        count=replacement_count,
        samples=[ConversionLossSample.from_text(char) for char in sample_chars],
        details={
            "encoding": output_encoding,
            "replacement": "?",
            "unique_characters": len(unsupported_counts),
        },
    )


class TXTOutput(OutputFormatPlugin):

    name = "TXT Output"
    author = "John Schember"
    file_type = "txt"

    options = {
        OptionRecommendation(
            name="newline",
            recommended_value="system",
            level=OptionRecommendation.LOW,
            short_switch="n",
            choices=NEWLINE_TYPES,
            option_help=_(
                "Type of newline to use. Options are %s. Default is 'system'. "
                "Use 'old_mac' for compatibility with Mac OS 9 and earlier. "
                "For Mac OS X use 'unix'. 'system' will default to the newline "
                "type used by this OS."
            )
            % sorted(NEWLINE_TYPES),
        ),
        OptionRecommendation(
            name="txt_output_encoding",
            recommended_value="utf-8",
            level=OptionRecommendation.LOW,
            option_help=_("Specify the character encoding of the output document. The default is utf-8."),
        ),
        OptionRecommendation(
            name="inline_toc",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_("Add Table of Contents to beginning of the book."),
        ),
        OptionRecommendation(
            name="max_line_length",
            recommended_value=0,
            level=OptionRecommendation.LOW,
            option_help=_(
                "The maximum number of characters per line. This splits on "
                "the first space before the specified value. If no space is found "
                "the line will be broken at the space after and will exceed the "
                "specified value. Also, there is a minimum of 25 characters. "
                "Use 0 to disable line splitting."
            ),
        ),
        OptionRecommendation(
            name="force_max_line_length",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Force splitting on the max-line-length value when no space "
                "is present. Also allows max-line-length to be below the minimum"
            ),
        ),
        OptionRecommendation(
            name="txt_output_formatting",
            recommended_value="plain",
            choices=["plain", "markdown", "textile"],
            option_help=_(
                "Formatting used within the document.\n"
                "* plain: Produce plain text.\n"
                "* markdown: Produce Markdown formatted text.\n"
                "* textile: Produce Textile formatted text."
            ),
        ),
        OptionRecommendation(
            name="keep_links",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Do not remove links within the document. This is only "
                "useful when paired with a txt-output-formatting option that "
                "is not none because links are always removed with plain text output."
            ),
        ),
        OptionRecommendation(
            name="keep_image_references",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Do not remove image references within the document. This is only "
                "useful when paired with a txt-output-formatting option that "
                "is not none because links are always removed with plain text output."
            ),
        ),
        OptionRecommendation(
            name="keep_color",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Do not remove font color from output. This is only useful when "
                "txt-output-formatting is set to textile. Textile is the only "
                "formatting that supports setting font color. If this option is "
                "not specified font color will not be set and default to the "
                "color displayed by the reader (generally this is black)."
            ),
        ),
    }

    def convert(self, oeb_book, output_path, input_plugin, opts, log):
        from LiuXin_alpha.file_formats.txt.newlines import specified_newlines, TxtNewlines
        from LiuXin_alpha.file_formats.txt.txtml import TXTMLizer
        from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars

        self.conversion_report = _report_context(self, input_plugin, opts)

        if opts.txt_output_formatting.lower() == "markdown":
            from LiuXin_alpha.file_formats.txt.markdownml import MarkdownMLizer

            self.writer = MarkdownMLizer(log)
        elif opts.txt_output_formatting.lower() == "textile":
            from LiuXin_alpha.file_formats.txt.textileml import TextileMLizer

            self.writer = TextileMLizer(log)
        else:
            self.writer = TXTMLizer(log)

        txt = self.writer.extract_content(oeb_book, opts)
        txt = clean_ascii_chars(txt)

        log.debug("\tReplacing newlines with selected type...")
        newline_opt = getattr(opts, "newline", "system")
        txt = specified_newlines(TxtNewlines(newline_opt).newline, txt)

        close = False
        if not hasattr(output_path, "write"):
            close = True
            if not os.path.exists(os.path.dirname(output_path)) and os.path.dirname(output_path) != "":
                os.makedirs(os.path.dirname(output_path))
            out_stream = open(output_path, "wb")
        else:
            out_stream = output_path

        if hasattr(out_stream, "seek"):
            out_stream.seek(0)
        if hasattr(out_stream, "truncate"):
            out_stream.truncate()
        output_encoding = getattr(opts, "txt_output_encoding", "utf-8") or "utf-8"
        unsupported_counts = _unencodable_character_counts(txt, output_encoding)
        _report_output_encoding_replacements(self.conversion_report, unsupported_counts, output_encoding)
        out_stream.write(txt.encode(output_encoding, "replace"))

        if close:
            out_stream.close()


class TXTZOutput(TXTOutput):

    name = "TXTZ Output"
    author = "John Schember"
    file_type = "txtz"

    def convert(self, oeb_book, output_path, input_plugin, opts, log):

        from lxml import etree
        from LiuXin_alpha.file_formats.oeb.base import OEB_IMAGES
        from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

        with TemporaryDirectory("_txtz_output") as tdir:
            # TXT
            txt_name = "index.txt"
            if opts.txt_output_formatting.lower() == "textile":
                txt_name = "index.text"
            with TemporaryFile(txt_name) as tf:
                TXTOutput.convert(self, oeb_book, tf, input_plugin, opts, log)
                shutil.copy(tf, os.path.join(tdir, txt_name))

            # Images
            for item in oeb_book.manifest:
                if item.media_type in OEB_IMAGES:
                    if hasattr(self.writer, "images"):
                        path = os.path.join(tdir, "images")
                        if item.href in self.writer.images:
                            href = self.writer.images[item.href]
                        else:
                            continue
                    else:
                        path = os.path.join(tdir, os.path.dirname(item.href))
                        href = os.path.basename(item.href)
                    if not os.path.exists(path):
                        os.makedirs(path)
                    with open(os.path.join(path, href), "wb") as imgf:
                        imgf.write(item.data)

            # Metadata
            with open(os.path.join(tdir, "metadata.opf"), "wb") as mdataf:
                mdataf.write(etree.tostring(oeb_book.metadata.to_opf1()))

            txtz = ZipFile(output_path, "w")
            txtz.add_dir(tdir)
