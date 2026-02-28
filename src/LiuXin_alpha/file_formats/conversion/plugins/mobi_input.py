from __future__ import with_statement

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryDirectory

__license__ = "GPL 3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class MOBIInput(InputFormatPlugin):

    name = "MOBI Input"
    author = "Kovid Goyal"
    description = "Convert MOBI files (.mobi, .prc, .azw) to HTML"
    file_types = {"mobi", "prc", "azw", "azw3", "pobi"}

    def convert(self, stream, options, file_ext, log, accelerators):
        if accelerators is None:
            accelerators = {}
        self.is_kf8 = False
        self.mobi_is_joint = False
        workdir = PersistentTemporaryDirectory("_mobi_input")

        from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader
        from lxml import html

        parse_cache = {}
        with CurrentDir(workdir):
            try:
                mr = MobiReader(stream, log, options.input_encoding, options.debug_pipeline)
                if mr.kf8_type is None:
                    mr.extract_content(workdir, parse_cache)

            except:
                mr = MobiReader(
                    stream,
                    log,
                    options.input_encoding,
                    options.debug_pipeline,
                    try_extra_data_fix=True,
                )
                if mr.kf8_type is None:
                    mr.extract_content(workdir, parse_cache)

            if mr.kf8_type is not None:
                log("Found KF8 MOBI of type %r" % mr.kf8_type)
                if mr.kf8_type == "joint":
                    self.mobi_is_joint = True
                from LiuXin_alpha.file_formats.mobi.reader.mobi8 import Mobi8Reader

                mr = Mobi8Reader(mr, log)
                opf = os.path.abspath(mr())
                self.encrypted_fonts = mr.encrypted_fonts
                self.is_kf8 = True
                return opf

            raw = parse_cache.pop("calibre_raw_mobi_markup", False)
            if raw:
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                with open(os.path.join(workdir, "debug-raw.html"), "wb") as debug_raw_html_file:
                    debug_raw_html_file.write(raw)
            from LiuXin_alpha.file_formats.oeb.base import close_self_closing_tags

            for f, root in parse_cache.items():
                raw = html.tostring(root, encoding="utf-8", method="xml", include_meta_content_type=False)
                raw = close_self_closing_tags(raw)
                target = f if os.path.isabs(f) else os.path.join(workdir, f)
                with open(target, "wb") as q:
                    q.write(raw)
                    accelerators["pagebreaks"] = '//h:div[@class="mbp_pagebreak"]'
            return mr.created_opf_path if os.path.isabs(mr.created_opf_path) else os.path.join(workdir, mr.created_opf_path)
