"""CHM file decoding support."""

from __future__ import annotations

import typing as _typing

import codecs
import os
import struct

from LiuXin_alpha.constants import iswindows
from LiuXin_alpha.file_formats.chardet import xml_to_unicode
from LiuXin_alpha.file_formats.toc import TOC
from LiuXin_alpha.utils.calibre import guess_type as guess_mimetype
from LiuXin_alpha.utils.libraries.chm import CHM_ENUMERATE_NORMAL, CHM_RESOLVE_SUCCESS, CHMError, CHMFile, chm_enumerate

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>, and Alex Bramley <a.bramley at gmail.com>."


def match_string(s1: _typing.Any, s2_already_lowered: _typing.Any) -> bool:
    if s1 is not None and s2_already_lowered is not None:
        if s1.lower() == s2_already_lowered:
            return True
    return False


class CHMReader(CHMFile):
    def __init__(self: _typing.Self, input_path: _typing.Any, log: _typing.Any, input_encoding: _typing.Any = None) -> None:
        super().__init__()
        if not self.LoadCHM(input_path):
            raise CHMError(f"Unable to open CHM file {input_path!r}")

        self.log = log
        self.input_encoding = input_encoding
        self._sourcechm = input_path
        self._contents = None
        self._playorder = 0
        self._metadata = False
        self._extracted = False
        self.re_encoded_files = set()

        self.get_encodings()
        if self.home:
            self.home = self.decode_hhp_filename(self.home) or self.home
        if self.topics:
            self.topics = self.decode_hhp_filename(self.topics) or self.topics

        base = self.topics or self.home or "/"
        self.root = os.path.splitext(base.lstrip("/"))[0]
        self.hhc_path = self.root + ".hhc"

    def _log_exception(self: _typing.Self, message: _typing.Any, exception: _typing.Any = None, level: str = "INFO") -> None:
        if hasattr(self.log, "log_exception"):
            self.log.log_exception(message=message, exception=exception, level=level)
            return
        if hasattr(self.log, "exception"):
            self.log.exception(message)
            return
        if hasattr(self.log, "warn"):
            self.log.warn(message)
            return
        if hasattr(self.log, "warning"):
            self.log.warning(message)

    def relpath_to_first_html_file(self: _typing.Self) -> _typing.Any:
        data = self.GetFile("/#SYSTEM")
        pos = 4
        while pos + 4 <= len(data):
            code, length_of_data = struct.unpack_from("<HH", data, pos)
            pos += 4
            if code == 2:
                default_topic = data[pos : pos + length_of_data].rstrip(b"\0")
                break
            pos += length_of_data
        else:
            raise CHMError("No default topic found in CHM file that has no HHC ToC either")
        default_topic = self.decode_hhp_filename(b"/" + default_topic)
        return default_topic[1:]

    def decode_hhp_filename(self: _typing.Self, path: _typing.Any) -> _typing.Any:
        if isinstance(path, str):
            return path
        for enc in (self.encoding_from_system_file, self.encoding_from_lcid, "cp1252", "cp1251", "latin1", "utf-8"):
            if enc:
                try:
                    q = path.decode(enc)
                except UnicodeDecodeError:
                    continue
                res, _ = self.ResolveObject(q)
                if res == CHM_RESOLVE_SUCCESS:
                    return q
        return path.decode("latin1", errors="replace")

    def get_encodings(self: _typing.Self) -> None:
        self.encoding_from_system_file = self.encoding_from_lcid = None

        q = self.GetEncoding()
        if q:
            try:
                if isinstance(q, bytes):
                    q = q.decode("ascii")
                codecs.lookup(q)
                self.encoding_from_system_file = q
            except Exception:
                pass

        lcid = self.GetLCID()
        if lcid is not None:
            q = lcid[0]
            if q:
                try:
                    if isinstance(q, bytes):
                        q = q.decode("ascii")
                    codecs.lookup(q)
                    self.encoding_from_lcid = q
                except Exception:
                    pass

    def get_encoding(self: _typing.Self) -> bool:
        return self.encoding_from_system_file or self.encoding_from_lcid or "cp1252"

    def _parse_toc(self: _typing.Self, ul: _typing.Any, basedir: _typing.Any = os.getcwd()) -> _typing.Any:
        toc = TOC(play_order=self._playorder, base_path=basedir, text="")
        self._playorder += 1
        for li in ul("li", recursive=False):
            try:
                href = li.object("param", {"name": "Local"})[0]["value"]
                name = self._deentity(li.object("param", {"name": "Name"})[0]["value"])
            except Exception:
                continue
            if href.count("#"):
                href, frag = href.split("#", 1)
            else:
                frag = None
            toc.add_item(href, frag, name, play_order=self._playorder)
            self._playorder += 1
            if li.ul:
                child = self._parse_toc(li.ul)
                child.parent = toc
                toc.append(child)
        return toc

    def ResolveObject(self: _typing.Self, path: _typing.Any) -> _typing.Any:
        if not isinstance(path, bytes):
            path = path.encode("utf-8")
        return CHMFile.ResolveObject(self, path)

    def file_exists(self: _typing.Self, path: _typing.Any) -> bool:
        res, _ui = self.ResolveObject(path)
        return res == CHM_RESOLVE_SUCCESS

    def GetFile(self: _typing.Self, path: _typing.Any) -> _typing.Any:
        if isinstance(path, bytes):
            path = path.decode("utf-8", errors="replace")
        if not path.startswith("/"):
            path = "/" + path

        res, ui = self.ResolveObject(path)
        if res != CHM_RESOLVE_SUCCESS:
            raise CHMError(f"Unable to locate {path!r} within CHM file {self.filename!r}")

        size, data = self.RetrieveObject(ui)
        if size == 0:
            raise CHMError(f"{path!r} is zero bytes in length!")
        return data

    def get_home(self: _typing.Self) -> _typing.Any:
        return self.GetFile(self.home)

    def ExtractFiles(self: _typing.Self, output_dir: _typing.Any = os.getcwd(), debug_dump: bool = False) -> None:
        html_files = set()

        try:
            x = self.get_encoding()
            codecs.lookup(x)
            enc = x
        except Exception as e:
            enc = "cp1252"
            self._log_exception(message="Failed to get encoding from a CHM file.", exception=e, level="INFO")

        for path in self.Contents():
            fpath = path.decode(enc, errors="replace") if isinstance(path, bytes) else path
            lpath = os.path.join(output_dir, fpath)
            self._ensure_dir(lpath)

            try:
                data = self.GetFile(path)
            except Exception as e:
                self._log_exception(
                    message=f"Failed to extract {path!r} from CHM, ignoring",
                    exception=e,
                    level="WARN",
                )
                continue

            if ";" in lpath:
                lpath = lpath.split(";", 1)[0]

            try:
                with open(lpath, "wb") as f:
                    f.write(data)
                try:
                    mt = guess_mimetype(fpath)[0] or ""
                    if "html" in mt:
                        html_files.add(lpath)
                except Exception as e:
                    self._log_exception(message="Error in CHM extraction metadata phase", exception=e, level="INFO")
            except Exception as e:
                if iswindows and len(lpath) > 250:
                    if hasattr(self.log, "warn"):
                        self.log.warn(f"{path!r} filename too long, skipping")
                    elif hasattr(self.log, "warning"):
                        self.log.warning(f"{path!r} filename too long, skipping")
                    continue
                self._log_exception(message="Unable to open output path for writing", exception=e, level="CRITICAL")
                raise

        if debug_dump:
            import shutil

            shutil.copytree(output_dir, os.path.join(debug_dump, "debug_dump"))

        for lpath in html_files:
            with open(lpath, "r+b") as f:
                data = f.read()
                data = self._reformat(data, lpath)
                if isinstance(data, str):
                    data = data.encode("utf-8")
                f.seek(0)
                f.truncate()
                f.write(data)

        self._extracted = True

        relative_files = []
        for root, _dirs, files in os.walk(output_dir):
            for fname in files:
                full = os.path.join(root, fname)
                relative_files.append(os.path.relpath(full, output_dir).replace(os.sep, "/"))

        if self.hhc_path not in relative_files:
            lowered = {f.lower(): f for f in relative_files}
            match = lowered.get(self.hhc_path.lower())
            if match:
                self.hhc_path = match

        if self.hhc_path not in relative_files and relative_files:
            for f in relative_files:
                if f.rpartition(".")[-1].lower() in {"html", "htm", "xhtm", "xhtml"}:
                    self.hhc_path = f
                    break

        if self.hhc_path == ".hhc" and self.hhc_path not in relative_files:
            for f in relative_files:
                name = os.path.basename(f).lower()
                if name in ("index.htm", "index.html", "contents.htm", "contents.html"):
                    self.hhc_path = f
                    break

        if self.hhc_path not in relative_files and relative_files:
            self.hhc_path = relative_files[0]

    def _reformat(self: _typing.Self, data: _typing.Any, htmlpath: _typing.Any) -> _typing.Any:
        from lxml import html

        if self.input_encoding and isinstance(data, bytes):
            data = data.decode(self.input_encoding, errors="replace")

        try:
            normalized = xml_to_unicode(data, strip_encoding_pats=True)[0]
            root = html.fromstring(normalized)
        except Exception as e:
            self._log_exception(
                message="Unable to parse html for cleaning, leaving source as-is",
                exception=e,
                level="INFO",
            )
            return data

        for script in root.xpath("//script"):
            parent = script.getparent()
            if parent is not None:
                parent.remove(script)

        body_nodes = root.xpath("//body")
        body = body_nodes[0] if body_nodes else root

        def nav_table_candidate(table: _typing.Any) -> _typing.Any:
            try:
                alt = "".join(table.xpath(".//img[1]/@alt")).lower()
            except Exception:
                alt = ""
            return any(x in alt for x in ("prev", "next", "team"))

        top_tables = body.xpath("./table")
        if top_tables:
            if nav_table_candidate(top_tables[0]):
                body.remove(top_tables[0])
            top_tables = body.xpath("./table")
            if top_tables and nav_table_candidate(top_tables[-1]):
                body.remove(top_tables[-1])

        first_elements = body.xpath("./*")
        if first_elements and first_elements[0].tag.lower() == "br":
            body.remove(first_elements[0])

        base = os.path.dirname(htmlpath)
        for img in root.xpath("//img[@src]"):
            src = img.get("src", "")
            ipath = os.path.join(base, *src.split("/"))
            if os.path.exists(ipath):
                continue
            src = src.split(";", 1)[0]
            if not src:
                continue
            ipath = os.path.join(base, *src.split("/"))
            if not os.path.exists(ipath):
                while src.startswith("../"):
                    src = src[3:]
            img.set("src", src)

        try:
            tables = body.xpath("./table")
            if len(tables) == 1:
                rows = tables[0].xpath("./tr")
                if len(rows) == 1:
                    cells = rows[0].xpath("./td")
                    if len(cells) == 1:
                        table = tables[0]
                        td = cells[0]
                        insert_at = body.index(table)
                        text = (td.text or "").strip()
                        if text:
                            p = html.Element("p")
                            p.text = text
                            body.insert(insert_at, p)
                            insert_at += 1
                        for child in list(td):
                            td.remove(child)
                            body.insert(insert_at, child)
                            insert_at += 1
                        body.remove(table)
        except Exception:
            pass

        try:
            ans = html.tostring(root, encoding="unicode", method="html")
            self.re_encoded_files.add(os.path.abspath(htmlpath))
            return ans
        except Exception:
            return normalized

    def Contents(self: _typing.Self) -> _typing.Any:
        if self._contents is not None:
            return self._contents

        paths = []

        def get_paths(_chm: _typing.Any, ui: _typing.Any, _ctx: _typing.Any) -> None:
            path = ui.path
            if isinstance(path, bytes):
                path = path.decode("utf-8", errors="replace")
            if path and path[-1] != "/":
                paths.append(path.lstrip("/"))

        chm_enumerate(self.file, CHM_ENUMERATE_NORMAL, get_paths, None)
        self._contents = paths
        return self._contents

    def _ensure_dir(self: _typing.Self, path: _typing.Any) -> None:
        local_dir = os.path.dirname(path)
        if local_dir and not os.path.isdir(local_dir):
            os.makedirs(local_dir, exist_ok=True)

    def extract_content(self: _typing.Self, output_dir: _typing.Any = os.getcwd(), debug_dump: bool = False) -> None:
        self.ExtractFiles(output_dir=output_dir, debug_dump=debug_dump)
