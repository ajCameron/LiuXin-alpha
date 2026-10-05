"""
Convert CHM archives into normalized conversion input.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise chm input through a consuming regression::

        python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
"""

from __future__ import annotations

import os
import typing as _typing
from urllib.parse import unquote_to_bytes

from LiuXin_alpha.customize.conversion import InputFormatPlugin
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>, and Alex Bramley <a.bramley at gmail.com>."


class CHMInput(InputFormatPlugin):
    """
    Convert chminput sources into the normalized OEB pipeline model.

    Example:
        Exercise CHMInput through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
    """
    name = "CHM Input"
    author = "Kovid Goyal and Alex Bramley"
    description = "Convert CHM files to OEB"
    file_types = {"chm"}

    def _chmtohtml(self: _typing.Self, output_dir: _typing.Any, chm_path: _typing.Any, no_images: _typing.Any, log: _typing.Any, debug_dump: bool = False) -> _typing.Any:
        """
        Perform the chmtohtml operation under explicit file-format and conversion rules.

        Example:
            Exercise CHMInput. chmtohtml through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param output_dir: Value supplied for output dir under the utility contract.
        :param chm_path: Value supplied for chm path under the utility contract.
        :param no_images: Value supplied for no images under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param debug_dump: Value supplied for debug dump under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.chm.reader import CHMReader

        log.debug("Opening CHM file")
        rdr = CHMReader(chm_path, log, input_encoding=self.opts.input_encoding)
        log.debug(f"Extracting CHM to {output_dir}")
        rdr.extract_content(output_dir, debug_dump=debug_dump)
        self._chm_reader = rdr
        return rdr.hhc_path

    def _stream_to_path(self: _typing.Self, stream: _typing.Any, tdir: _typing.Any) -> _typing.Any:
        """
        Perform the stream to path operation under explicit file-format and conversion rules.

        Example:
            Exercise CHMInput. stream to path through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param tdir: Value supplied for tdir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        stream_name = getattr(stream, "name", None)
        if stream_name and os.path.exists(stream_name):
            return stream_name

        temp_input = os.path.join(tdir, "input.chm")
        with open(temp_input, "wb") as out:
            out.write(stream.read())
        return temp_input

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Convert a CHM stream into an OEBBook.

        Example:
            Exercise CHMInput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.customize.ui import plugin_for_input_format
        from LiuXin_alpha.file_formats.chm.metadata import get_metadata_from_reader
        from LiuXin_alpha.metadata.utils import calibreMetaInformation

        self.opts = options

        log.debug("Processing CHM...")
        with TemporaryDirectory("_chm2oeb") as tdir:
            tdir = os.fspath(tdir)

            html_input = plugin_for_input_format("html")
            if html_input is None:
                raise RuntimeError("No input plugin registered for 'html'")
            for opt in getattr(html_input, "options", ()):  # pragma: no branch
                opt_obj = getattr(opt, "option", None)
                opt_name = getattr(opt_obj, "name", None)
                if opt_name and not hasattr(options, opt_name):
                    setattr(options, opt_name, getattr(opt, "recommended_value", None))

            chm_name = self._stream_to_path(stream, tdir)
            try:
                stream.close()
            except Exception:
                pass

            debug_dump = False
            odi = getattr(options, "debug_pipeline", None)
            if odi:
                debug_dump = os.path.join(odi, "input")

            mainname = self._chmtohtml(tdir, chm_name, no_images=False, log=log, debug_dump=debug_dump)
            mainpath = os.path.join(tdir, mainname)

            try:
                metadata = get_metadata_from_reader(self._chm_reader, calibre=True)
            except Exception:
                log.exception("Failed to read CHM metadata, using filename fallback")
                metadata = calibreMetaInformation(os.path.basename(chm_name), [_("Unknown")])

            encoding = self._chm_reader.get_encoding() or options.input_encoding or "cp1252"

            options.debug_pipeline = None
            options.input_encoding = "utf-8"
            try:
                uenc = encoding
                if os.path.abspath(mainpath) in self._chm_reader.re_encoded_files:
                    uenc = "utf-8"

                htmlpath, toc = self._create_html_root(mainpath, log, uenc)
                oeb = self._create_oebbook_html(htmlpath, tdir, options, log, metadata)
            finally:
                self._chm_reader.CloseCHM()
                options.debug_pipeline = odi

            if toc.count() > 1:
                oeb.toc = self.parse_html_toc(oeb.spine[0])
                oeb.manifest.remove(oeb.spine[0])
                oeb.auto_generated_toc = False

        return oeb

    def parse_html_toc(self: _typing.Self, item: _typing.Any) -> _typing.Any:
        """
        Parse an HTML document into an OEB TOC tree.

        Example:
            Exercise CHMInput.parse html toc through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.base import TOC, XPath

        dx = XPath("./h:div")
        ax = XPath("./h:a[1]")

        def do_node(parent: _typing.Any, div: _typing.Any) -> None:
            """
            Perform the do node operation under explicit file-format and conversion rules.

            Example:
                Exercise CHMInput.parse html toc.do node through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :param parent: Value supplied for parent under the utility contract.
            :param div: Value supplied for div under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            for child in dx(div):
                links = ax(child)
                if not links:
                    continue
                a = links[0]
                c = parent.add(a.text, a.attrib.get("href", ""))
                do_node(c, child)

        toc = TOC()
        roots = XPath("//h:div[1]")(item.data)
        if roots:
            do_node(toc, roots[0])
        return toc

    def _create_oebbook_html(self: _typing.Self, htmlpath: _typing.Any, basedir: _typing.Any, opts: _typing.Any, log: _typing.Any, mi: _typing.Any) -> _typing.Any:
        """
        Use HTMLInput plugin to generate an OEBBook.

        Example:
            Exercise CHMInput. create oebbook html through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param htmlpath: Value supplied for htmlpath under the utility contract.
        :param basedir: Value supplied for basedir under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.conversion.plugins.html_input import HTMLInput

        opts.breadth_first = True
        if hasattr(opts, "max_levels"):
            opts.max_levels = max(getattr(opts, "max_levels", 5), 30)
        if hasattr(opts, "correct_case_mismatches"):
            opts.correct_case_mismatches = True

        htmlinput = HTMLInput(None)
        oeb = htmlinput.create_oebbook(htmlpath, basedir, opts, log, mi)
        return oeb

    def _create_html_root(self: _typing.Self, hhcpath: _typing.Any, log: _typing.Any, encoding: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Create html root under the format's safety and compatibility rules.

        Example:
            Exercise CHMInput. create html root through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param hhcpath: Value supplied for hhcpath under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from lxml import html

        from LiuXin_alpha.file_formats.oeb.base import TOC, urlquote
        from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode

        try:
            hhcdata = self._read_file(hhcpath)
        except FileNotFoundError:
            log.warning("No HHC file found in CHM, using default topic")
            fallback = os.path.join(os.path.dirname(hhcpath), self._chm_reader.relpath_to_first_html_file())
            return fallback, TOC()

        hhcdata = hhcdata.decode(encoding, errors="replace")
        hhcdata = xml_to_unicode(hhcdata, verbose=True, strip_encoding_pats=True, resolve_entities=True)[0]
        hhcroot = html.fromstring(hhcdata)
        toc = self._process_nodes(hhcroot)

        log.debug(f"Found {toc.count()} section nodes")
        htmlpath = os.path.splitext(hhcpath)[0] + ".html"
        base = os.path.dirname(os.path.abspath(htmlpath))

        def unquote(text: _typing.Any) -> _typing.Any:
            """
            Perform the unquote operation under explicit file-format and conversion rules.

            Example:
                Exercise CHMInput. create html root.unquote through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :param text: Text parsed, normalized or rendered.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            raw = text if isinstance(text, bytes) else text.encode("utf-8")
            return unquote_to_bytes(raw).decode("utf-8", errors="replace")

        def unquote_path(path: _typing.Any) -> tuple[_typing.Any, ...]:
            """
            Perform the unquote path operation under explicit file-format and conversion rules.

            Example:
                Exercise CHMInput. create html root.unquote path through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :param path: Filesystem path read, written, normalized or validated by the
                operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            raw, frag = (path.split("#", 1) + [""])[:2]
            if frag:
                frag = "#" + frag
            decoded = unquote(raw)
            if not os.path.exists(os.path.join(base, raw)) and os.path.exists(os.path.join(base, decoded)):
                raw = decoded
            return raw, frag

        def donode(item: _typing.Any, parent: _typing.Any, base_dir: _typing.Any, subpath: _typing.Any) -> None:
            """
            Perform the donode operation under explicit file-format and conversion rules.

            Example:
                Exercise CHMInput. create html root.donode through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :param item: Value supplied for item under the utility contract.
            :param parent: Value supplied for parent under the utility contract.
            :param base_dir: Value supplied for base dir under the utility contract.
            :param subpath: Value supplied for subpath under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            for child in item:
                title = child.title
                if not title:
                    continue

                raw, frag = unquote_path(child.href or "")
                rsrcname = os.path.basename(raw)
                rsrcpath = os.path.join(subpath, rsrcname)
                if not os.path.exists(os.path.join(base_dir, rsrcpath)) and os.path.exists(os.path.join(base_dir, raw)):
                    rsrcpath = raw

                if "%" not in rsrcpath:
                    rsrcpath = urlquote(rsrcpath)
                if not raw:
                    rsrcpath = ""

                c = DIV(A(title, href=rsrcpath + frag))
                donode(child, c, base_dir, subpath)
                parent.append(c)

        with open(htmlpath, "wb") as f:
            if toc.count() > 1:
                from lxml.html.builder import BODY, DIV, HTML, A

                path0 = toc[0].href
                path0 = unquote_path(path0)[0]
                subpath = os.path.dirname(path0)
                base_dir = os.path.dirname(f.name)
                root = DIV()
                donode(toc, root, base_dir, subpath)
                raw = html.tostring(HTML(BODY(root)), encoding="utf-8", pretty_print=True)
                f.write(raw)
            else:
                if isinstance(hhcdata, str):
                    hhcdata = hhcdata.encode("utf-8")
                f.write(hhcdata)
        return htmlpath, toc

    def _read_file(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        """
        Read file under the format's safety and compatibility rules.

        Example:
            Exercise CHMInput. read file through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with open(name, "rb") as f:
            return f.read()

    def add_node(self: _typing.Self, node: _typing.Any, toc: _typing.Any, ancestor_map: _typing.Any) -> None:
        """
        Perform the add node operation under explicit file-format and conversion rules.

        Example:
            Exercise CHMInput.add node through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param node: Value supplied for node under the utility contract.
        :param toc: Value supplied for toc under the utility contract.
        :param ancestor_map: Value supplied for ancestor map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from LiuXin_alpha.file_formats.chm.reader import match_string

        if match_string(node.attrib.get("type", ""), "text/sitemap"):
            p = node.xpath("ancestor::ul[1]/ancestor::li[1]/object[1]")
            parent = p[0] if p else None
            toc = ancestor_map.get(parent, toc)
            title = href = ""
            for param in node.xpath("./param"):
                if match_string(param.attrib.get("name", ""), "name"):
                    title = param.attrib.get("value", "")
                elif match_string(param.attrib.get("name", ""), "local"):
                    href = param.attrib.get("value", "")
            child = toc.add(title or _("Unknown"), href)
            ancestor_map[node] = child

    def _process_nodes(self: _typing.Self, root: _typing.Any) -> _typing.Any:
        """
        Perform the process nodes operation under explicit file-format and conversion rules.

        Example:
            Exercise CHMInput. process nodes through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.base import TOC

        toc = TOC()
        ancestor_map = {}
        for node in root.xpath("//object"):
            self.add_node(node, toc, ancestor_map)
        return toc
