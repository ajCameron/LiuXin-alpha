# -*- coding: utf-8 -*-

import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin

from LiuXin_alpha.utils.storage.local.filenames import ascii_filename
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

__license__ = "GPL 3"
__copyright__ = "2010, Li Fanxi <lifanxi@freemindworld.com>"
__docformat__ = "restructuredtext en"


HTML_TEMPLATE = (
    '<html><head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"/><title>%s</title>'
    "</head><body>\n%s\n</body></html>"
)


def html_encode(s):
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
        .replace("\n", "<br/>")
        .replace(" ", "&nbsp;")
    )


class SNBInput(InputFormatPlugin):

    name = "SNB Input"
    author = "Li Fanxi"
    description = "Convert SNB files to OEB"
    file_types = {"snb"}

    options = set([])

    def convert(self, stream, options, file_ext, log, accelerators):
        import uuid
        from lxml import etree

        from LiuXin_alpha.file_formats.oeb.base import DirContainer
        from LiuXin_alpha.file_formats.snb.snbfile import SNBFile

        log.debug("Parsing SNB file...")
        snb_file = SNBFile()
        try:
            snb_file.Parse(stream)
        except:
            raise ValueError("Invalid SNB file")
        if not snb_file.IsValid():
            log.debug("Invaild SNB file")
            raise ValueError("Invalid SNB file")
        log.debug("Handle meta data ...")
        from LiuXin_alpha.file_formats.conversion.plumber import create_oebbook

        oeb = create_oebbook(log, None, options, encoding=options.input_encoding, populate=False)
        meta = snb_file.GetFileStream("snbf/book.snbf")
        if meta is not None:
            meta = etree.fromstring(meta)
            l = {
                "title": ".//head/name",
                "creator": ".//head/author",
                "language": ".//head/language",
                "generator": ".//head/generator",
                "publisher": ".//head/publisher",
                "cover": ".//head/cover",
            }
            d = {}
            for item in l:
                node = meta.find(l[item])
                if node is not None:
                    d[item] = node.text if node.text is not None else ""
                else:
                    d[item] = ""

            oeb.metadata.add("title", d["title"])
            oeb.metadata.add("creator", d["creator"], attrib={"role": "aut"})
            oeb.metadata.add("language", d["language"].lower().replace("_", "-"))
            oeb.metadata.add("generator", d["generator"])
            oeb.metadata.add("publisher", d["publisher"])
            if d["cover"] != "":
                oeb.guide.add("cover", "Cover", d["cover"])

        book_id = str(uuid.uuid4())
        oeb.metadata.add("identifier", book_id, id="uuid_id", scheme="uuid")
        for ident in oeb.metadata.identifier:
            if "id" in ident.attrib:
                oeb.uid = oeb.metadata.identifier[0]
                break

        with TemporaryDirectory("_snb2oeb", keep=True) as tdir:

            log.debug("Process TOC ...")
            toc = snb_file.GetFileStream("snbf/toc.snbf")
            oeb.container = DirContainer(tdir, log)
            if toc is not None:
                toc = etree.fromstring(toc)
                i = 1
                for ch in toc.find(".//body"):
                    chapter_name = ch.text
                    chapter_src = ch.get("src")
                    fname = "ch_%d.htm" % i
                    data = snb_file.GetFileStream("snbc/" + chapter_src)
                    if data is None:
                        continue
                    snbc = etree.fromstring(data)
                    output_file = open(os.path.join(tdir, fname), "wb")
                    lines = []
                    for line in snbc.find(".//body"):
                        if line.tag == "text":
                            lines.append("<p>%s</p>" % html_encode(line.text))
                        elif line.tag == "img":
                            lines.append('<p><img src="%s" /></p>' % html_encode(line.text))
                    output_file.write((HTML_TEMPLATE % (chapter_name, "\n".join(lines))).encode("utf-8", "replace"))
                    output_file.close()
                    oeb.toc.add(ch.text, fname)
                    ch_id, href = oeb.manifest.generate(id="html", href=ascii_filename(fname))
                    item = oeb.manifest.add(ch_id, href, "text/html")
                    item.html_input_href = fname
                    oeb.spine.add(item, True)
                    i += 1

                image_files = snb_file.OutputImageFiles(tdir)
                for f, m in image_files:
                    image_id, href = oeb.manifest.generate(id="image", href=ascii_filename(f))
                    item = oeb.manifest.add(image_id, href, m)
                    item.html_input_href = f

        # Todo: Note that you need to delete the folder containing this when you're done with it
        return oeb
