from __future__ import with_statement

import os
import re
import shutil

from functools import partial
from os.path import dirname, abspath, relpath as _relpath, exists, basename

from LiuXin.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin.utils.calibre import CurrentDir
from LiuXin.utils.localization import trans as _
from LiuXin.utils.ptempfiles import PersistentTemporaryDirectory
from LiuXin.utils.resources import P

__license__ = "GPL 3"
__copyright__ = "2010, Fabian Grassl <fg@jusmeum.de>"
__docformat__ = "restructuredtext en"


def relpath(*args):
    return _relpath(*args).replace(os.sep, "/")


class HTMLOutput(OutputFormatPlugin):

    name = "HTML Output"
    author = "Fabian Grassl"
    file_type = "zip"

    options = {
        OptionRecommendation(
            name="template_css",
            option_help=_("CSS file used for the output instead of the default file"),
        ),
        OptionRecommendation(
            name="template_html_index",
            option_help=_("Template used for generation of the html index file instead of the " "default file"),
        ),
        OptionRecommendation(
            name="template_html",
            option_help=_(
                "Template used for the generation of the html contents of the book " "instead of the default file"
            ),
        ),
        OptionRecommendation(
            name="extract_to",
            option_help=_(
                "Extract the contents of the generated ZIP file to the "
                "specified directory. WARNING: The contents of the directory "
                "will be deleted."
            ),
        ),
    }

    recommendations = {("pretty_print", True, OptionRecommendation.HIGH)}

    def generate_toc(self, oeb_book, ref_url, output_dir):
        """
        Generate table of contents
        :param oeb_book:
        :param ref_url:
        :param output_dir:
        :return:
        """
        from lxml import etree
        from urllib import unquote

        from LiuXin.file_formats.oeb.base import element

        with CurrentDir(output_dir):

            def build_node(current_node, parent=None):
                if parent is None:
                    parent = etree.Element("ul")
                elif len(current_node.nodes):
                    parent = element(parent, "ul")
                for node in current_node.nodes:
                    point = element(parent, "li")
                    href = relpath(abspath(unquote(node.href)), dirname(ref_url))
                    link = element(point, "a", href=href)
                    title = node.title
                    if title:
                        title = re.sub(r"\s+", " ", title)
                    link.text = title
                    build_node(node, point)
                return parent

            wrap = etree.Element("div")
            wrap.append(build_node(oeb_book.toc))
            return wrap

    def generate_html_toc(self, oeb_book, ref_url, output_dir):
        from lxml import etree

        root = self.generate_toc(oeb_book, ref_url, output_dir)
        return etree.tostring(root, pretty_print=True, encoding="utf-8", xml_declaration=False)

    def convert(self, oeb_book, output_path, input_plugin, opts, log):
        """
        Takes an OEB book and converts it to an HTML file.
        :param oeb_book:
        :param output_path:
        :param input_plugin:
        :param opts:
        :param log:
        :return:
        """
        from urllib import unquote

        from lxml import etree
        from LiuXin.utils.liuxin_templite import Templite

        from LiuXin.file_formats.html.meta import EasyMeta
        from LiuXin.utils import calibre_zipfile

        # read template files
        if opts.template_html_index is not None:
            template_html_index_data = open(opts.template_html_index, "rb").read()
        else:
            template_html_index_data = P("templates/html_export_default_index.tmpl", data=True)

        if opts.template_html is not None:
            template_html_data = open(opts.template_html, "rb").read()
        else:
            template_html_data = P("templates/html_export_default.tmpl", data=True)

        if opts.template_css is not None:
            template_css_data = open(opts.template_css, "rb").read()
        else:
            template_css_data = P("templates/html_export_default.css", data=True)

        template_html_index_data = template_html_index_data.decode("utf-8")
        template_html_data = template_html_data.decode("utf-8")
        template_css_data = template_css_data.decode("utf-8")

        self.log = log
        self.opts = opts
        meta = EasyMeta(oeb_book.metadata)

        tempdir = os.path.realpath(PersistentTemporaryDirectory())
        output_file = os.path.join(tempdir, basename(re.sub(r"\.zip", "", output_path) + ".html"))
        output_dir = re.sub(r"\.html", "", output_file) + "_files"

        if not exists(output_dir):
            os.makedirs(output_dir)

        css_path = output_dir + os.sep + "calibreHtmlOutBasicCss.css"
        with open(css_path, "wb") as f:
            f.write(template_css_data.encode("utf-8"))

        with open(output_file, "wb") as f:
            html_toc = self.generate_html_toc(oeb_book, output_file, output_dir)
            templite = Templite(template_html_index_data)
            next_link = oeb_book.spine[0].href
            next_link = relpath(output_dir + os.sep + next_link, dirname(output_file))
            css_link = relpath(abspath(css_path), dirname(output_file))
            toc_url = relpath(output_file, dirname(output_file))
            t = templite.render(
                has_toc=bool(oeb_book.toc.count()),
                toc=html_toc,
                meta=meta,
                nextLink=next_link,
                tocUrl=toc_url,
                cssLink=css_link,
                firstContentPageLink=next_link,
            )
            f.write(t)

        with CurrentDir(output_dir):
            for item in oeb_book.manifest:
                path = abspath(unquote(item.href))
                item_dir = dirname(path)
                if not exists(item_dir):
                    os.makedirs(item_dir)
                if item.spine_position is not None:
                    with open(path, "wb") as f:
                        pass
                else:
                    with open(path, "wb") as f:
                        f.write(str(item))
                    item.unload_data_from_memory(memory=path)

            for item in oeb_book.spine:
                path = abspath(unquote(item.href))
                item_dir = dirname(path)
                root = item.data.getroottree()

                # get & clean HTML <HEAD>-data
                head = root.xpath("//h:head", namespaces={"h": "http://www.w3.org/1999/xhtml"})[0]
                head_content = etree.tostring(head, pretty_print=True, encoding="utf-8")
                head_content = re.sub(r"\<\/?head.*\>", "", head_content)
                head_content = re.sub(re.compile(r"\<style.*\/style\>", re.M | re.S), "", head_content)
                head_content = re.sub(r"<(title)([^>]*)/>", r"<\1\2></\1>", head_content)

                # get & clean HTML <BODY>-data
                body = root.xpath("//h:body", namespaces={"h": "http://www.w3.org/1999/xhtml"})[0]
                ebook_content = etree.tostring(body, pretty_print=True, encoding="utf-8")
                ebook_content = re.sub(r"\<\/?body.*\>", "", ebook_content)
                ebook_content = re.sub(r"<(div|a|span)([^>]*)/>", r"<\1\2></\1>", ebook_content)

                # generate link to next page
                if item.spine_position + 1 < len(oeb_book.spine):
                    next_link = oeb_book.spine[item.spine_position + 1].href
                    next_link = relpath(abspath(next_link), item_dir)
                else:
                    next_link = None

                # generate link to previous page
                if item.spine_position > 0:
                    prev_link = oeb_book.spine[item.spine_position - 1].href
                    prev_link = relpath(abspath(prev_link), item_dir)
                else:
                    prev_link = None

                css_link = relpath(abspath(css_path), item_dir)
                toc_url = relpath(output_file, item_dir)
                first_content_page_link = oeb_book.spine[0].href

                # render template
                templite = Templite(template_html_data)

                toc = partial(
                    self.generate_html_toc,
                    oeb_book=oeb_book,
                    ref_url=path,
                    output_dir=output_dir,
                )

                t = templite.render(
                    ebookContent=ebook_content,
                    prevLink=prev_link,
                    nextLink=next_link,
                    has_toc=bool(oeb_book.toc.count()),
                    toc=toc,
                    tocUrl=toc_url,
                    head_content=head_content,
                    meta=meta,
                    cssLink=css_link,
                    firstContentPageLink=first_content_page_link,
                )

                # write html to file
                with open(path, "wb") as f:
                    f.write(t)
                item.unload_data_from_memory(memory=path)

        zfile = calibre_zipfile.ZipFile(output_path, "w")
        zfile.add_dir(output_dir, basename(output_dir))
        zfile.write(output_file, basename(output_file), calibre_zipfile.ZIP_DEFLATED)

        if opts.extract_to:
            if os.path.exists(opts.extract_to):
                shutil.rmtree(opts.extract_to)
            os.makedirs(opts.extract_to)
            zfile.extractall(opts.extract_to)
            self.log("Zip file extracted to", opts.extract_to)

        zfile.close()

        # cleanup temp dir
        shutil.rmtree(tempdir)
