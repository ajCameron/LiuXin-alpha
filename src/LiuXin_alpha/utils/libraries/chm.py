"""CHM backend shim.

This module provides a small compatibility layer over the optional ``chmlib``
plugin and exposes the subset of the old CHM API used by
``LiuXin_alpha.file_formats.chm``.
"""

from __future__ import annotations

import codecs
from typing import Any

from LiuXin_alpha.utils.plugins import plugins


class CHMError(Exception):
    pass


_chmlib_mod, _chmlib_err = plugins["chmlib"]

_REQUIRED_CHMLIB_ATTRS = (
    "chm_open",
    "chm_close",
    "chm_resolve_object",
    "chm_retrieve_object",
    "chm_enumerate",
    "CHM_RESOLVE_SUCCESS",
    "CHM_ENUMERATE_NORMAL",
)

_HAVE_CHMLIB = bool(
    _chmlib_mod is not None and all(hasattr(_chmlib_mod, attr) for attr in _REQUIRED_CHMLIB_ATTRS)
)

CHM_RESOLVE_SUCCESS = getattr(_chmlib_mod, "CHM_RESOLVE_SUCCESS", 0)
CHM_ENUMERATE_NORMAL = getattr(_chmlib_mod, "CHM_ENUMERATE_NORMAL", 0)


def _decode_best_effort(raw: Any) -> str:
    if isinstance(raw, str):
        return raw
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        return str(raw)
    data = bytes(raw)
    for enc in ("utf-8", "cp1252", "latin1"):
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode("latin1", errors="replace")


def _read_u16_le(data: bytes, offset: int) -> int:
    if offset + 2 > len(data):
        return 0
    return int.from_bytes(data[offset : offset + 2], "little", signed=False)


def _read_u32_le(data: bytes, offset: int) -> int:
    if offset + 4 > len(data):
        return 0
    value = int.from_bytes(data[offset : offset + 4], "little", signed=False)
    if value == 0xFFFFFFFF:
        return 0
    return value


def _read_c_string(data: bytes, idx: int) -> str:
    if idx < 0 or idx >= len(data):
        return ""
    end = data.find(b"\x00", idx)
    if end < 0:
        end = len(data)
    return _decode_best_effort(data[idx:end])


def _retrieve(chm_file: Any, ui: Any, start: int, length: int):
    if not _HAVE_CHMLIB:
        return 0, b""

    try:
        return _chmlib_mod.chm_retrieve_object(chm_file, ui, int(start), int(length))
    except TypeError:
        # Some wrappers expose only the 2-argument signature.
        return _chmlib_mod.chm_retrieve_object(chm_file, ui)


def chm_enumerate(chm_file, flags, callback, context):
    if not _HAVE_CHMLIB:
        msg = _chmlib_err or "chmlib backend is unavailable"
        raise CHMError(msg)
    return _chmlib_mod.chm_enumerate(chm_file, flags, callback, context)


_CHARSET_TABLE = {
    0: "iso8859_1",  # ANSI_CHARSET
    204: "cp1251",  # RUSSIAN_CHARSET
    128: "cp932",  # SHIFTJIS_CHARSET
    134: "cp936",  # GB2312_CHARSET
    129: "cp949",  # HANGUL_CHARSET
    136: "cp950",  # CHINESEBIG5_CHARSET
}

_LOCALE_TABLE = {
    0x0409: ("iso8859_1", "English_United_States", "Western Europe & US"),
    0x0809: ("iso8859_1", "English_United_Kingdom", "Western Europe & US"),
    0x0411: ("cp932", "Japanese", "Japanese"),
    0x0804: ("cp936", "Chinese_PRC", "Simplified Chinese"),
    0x0404: ("cp950", "Chinese_Taiwan", "Traditional Chinese"),
    0x0412: ("cp949", "Korean", "Korean"),
    0x0419: ("cp1251", "Russian", "Cyrillic"),
}


class CHMFile:
    """Small compatibility wrapper around low-level chmlib calls."""

    filename = ""
    file = None
    title = ""
    home = "/"
    index = None
    topics = None
    encoding = None
    lcid = None

    def __init__(self):
        self.searchable = False

    def LoadCHM(self, archive_name) -> bool:
        if not _HAVE_CHMLIB:
            return False

        if self.filename:
            self.CloseCHM()

        open_arg = archive_name.encode("utf-8") if isinstance(archive_name, str) else archive_name
        self.file = _chmlib_mod.chm_open(open_arg)
        if self.file is None:
            return False

        self.filename = archive_name
        self.GetArchiveInfo()
        return True

    def CloseCHM(self):
        if self.file is not None and _HAVE_CHMLIB:
            _chmlib_mod.chm_close(self.file)
        self.file = None
        self.filename = ""
        self.title = ""
        self.home = "/"
        self.index = None
        self.topics = None
        self.encoding = None
        self.lcid = None

    def ResolveObject(self, document):
        if self.file is None or not _HAVE_CHMLIB:
            return 1, None
        path = document.encode("utf-8") if isinstance(document, str) else document
        return _chmlib_mod.chm_resolve_object(self.file, path)

    def RetrieveObject(self, ui, start=-1, length=-1):
        if self.file is None or ui is None or not _HAVE_CHMLIB:
            return 0, b""

        if start == -1:
            start = 0
        if length == -1:
            length = int(getattr(ui, "length", 0))
        return _retrieve(self.file, ui, int(start), int(length))

    def GetArchiveInfo(self):
        self.searchable = False

        result, ui = self.ResolveObject("/#SYSTEM")
        if result != CHM_RESOLVE_SUCCESS or ui is None:
            return 0

        size, text = self.RetrieveObject(ui, start=4, length=int(getattr(ui, "length", 0)))
        if size <= 0:
            return 0

        payload = bytes(text)
        idx = 0
        while idx + 4 <= len(payload):
            code = _read_u16_le(payload, idx)
            seg_len = _read_u16_le(payload, idx + 2)
            idx += 4
            seg = payload[idx : idx + seg_len]
            idx += seg_len
            if not seg:
                continue

            value_bytes = seg[:-1] if seg.endswith(b"\x00") else seg
            value_text = _decode_best_effort(value_bytes)

            if code == 0 and value_text:
                self.topics = "/" + value_text.lstrip("/")
            elif code == 1 and value_text:
                self.index = "/" + value_text.lstrip("/")
            elif code == 2 and value_text:
                self.home = "/" + value_text.lstrip("/")
            elif code == 3:
                self.title = value_text
            elif code == 4 and len(value_bytes) >= 2:
                self.lcid = _read_u16_le(value_bytes, 0)
            elif code == 6 and value_text:
                if not self.topics:
                    cand = "/" + value_text.lstrip("/") + ".hhc"
                    res, _ = self.ResolveObject(cand)
                    if res == CHM_RESOLVE_SUCCESS:
                        self.topics = cand
                if not self.index:
                    cand = "/" + value_text.lstrip("/") + ".hhk"
                    res, _ = self.ResolveObject(cand)
                    if res == CHM_RESOLVE_SUCCESS:
                        self.index = cand
            elif code == 16:
                self.encoding = value_text

        self.GetWindowsInfo()
        return 1

    def GetWindowsInfo(self):
        result, ui = self.ResolveObject("/#WINDOWS")
        if result != CHM_RESOLVE_SUCCESS or ui is None:
            return -1

        size, header = self.RetrieveObject(ui, start=0, length=8)
        header = bytes(header)
        if size < 8 or len(header) < 8:
            return -2

        num_entries = _read_u32_le(header, 0)
        entry_size = _read_u32_le(header, 4)
        if num_entries < 1 or entry_size <= 0:
            return -3

        size, first_entry = self.RetrieveObject(ui, start=8, length=entry_size)
        first_entry = bytes(first_entry)
        if size < entry_size or len(first_entry) < entry_size:
            return -4

        toc_index = _read_u32_le(first_entry, 0x60)
        idx_index = _read_u32_le(first_entry, 0x64)
        dft_index = _read_u32_le(first_entry, 0x68)

        result, strings_ui = self.ResolveObject("/#STRINGS")
        if result != CHM_RESOLVE_SUCCESS or strings_ui is None:
            return -5

        size, strings_buf = self.RetrieveObject(strings_ui, start=0, length=int(getattr(strings_ui, "length", 0)))
        strings_buf = bytes(strings_buf)
        if size <= 0:
            return -6

        if not self.topics:
            val = _read_c_string(strings_buf, toc_index)
            if val:
                self.topics = "/" + val.lstrip("/")

        if not self.index:
            val = _read_c_string(strings_buf, idx_index)
            if val:
                self.index = "/" + val.lstrip("/")

        if dft_index:
            val = _read_c_string(strings_buf, dft_index)
            if val:
                self.home = "/" + val.lstrip("/")

        return 0

    def GetEncoding(self):
        if not self.encoding:
            return None

        raw = _decode_best_effort(self.encoding)
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) > 2:
            try:
                mapped = _CHARSET_TABLE.get(int(parts[2]))
                if mapped:
                    return mapped
            except Exception:
                pass

        # Some CHM files store an encoding-like token directly.
        if parts:
            token = parts[-1]
            try:
                codecs.lookup(token)
                return token
            except Exception:
                pass
        return None

    def GetLCID(self):
        if self.lcid in _LOCALE_TABLE:
            return _LOCALE_TABLE[self.lcid]
        return None

    def get_encoding(self):
        ans = self.GetEncoding()
        if ans is None:
            lcid = self.GetLCID()
            if lcid is not None:
                ans = lcid[0]
        if ans:
            try:
                codecs.lookup(ans)
            except Exception:
                ans = None
        return ans


__all__ = [
    "CHMError",
    "CHMFile",
    "CHM_RESOLVE_SUCCESS",
    "CHM_ENUMERATE_NORMAL",
    "chm_enumerate",
]
