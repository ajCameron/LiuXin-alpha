"""
Read/write metadata from Amazon's Topaz format.
"""

from __future__ import annotations

import io
import numbers
import os
from typing import Iterable

from LiuXin_alpha.metadata.utils import calibreMetaInformation, string_to_authors
from LiuXin_alpha.utils.calibre import force_unicode
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars

__license__ = "GPL 3"
__copyright__ = "2010, Greg Riker <griker@hotmail.com>"
__docformat__ = "restructuredtext en"

VALID_FOR = ["TPZ", "AZW1"]
PRIORITY_FOR = ["TPZ", "AZW1"]
RUN_COST = ["LOW"]


class StreamSlicer:
    """
    Byte-addressable view over a binary stream.
    """

    def __init__(self, stream, start: int = 0, stop: int | None = None):
        self._stream = stream
        self.start = start
        if stop is None:
            stream.seek(0, 2)
            stop = stream.tell()
        self.stop = stop
        self._len = max(0, stop - start)

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, key):
        stream = self._stream
        base = self.start
        if isinstance(key, numbers.Integral):
            stream.seek(base + key)
            return stream.read(1)
        if isinstance(key, slice):
            start, stop, stride = key.indices(self._len)
            if stride < 0:
                start, stop = stop, start
            size = stop - start
            if size <= 0:
                return b""
            stream.seek(base + start)
            data = stream.read(size)
            if stride != 1:
                data = data[::stride]
            return data
        raise TypeError("stream indices must be integers")

    def update(self, data_blocks: Iterable[bytes]) -> None:
        stream = self._stream
        base = self.start
        stream.seek(base)
        stream.truncate(base)
        for block in data_blocks:
            stream.write(block)

    def truncate(self, value: int) -> None:
        self._stream.truncate(value)


def _byte_as_int(one: bytes) -> int:
    if not one:
        return 0
    return one[0]


def _decode_tag(raw: bytes) -> str:
    return raw.decode("ascii", "replace")


def _decode_text(raw: bytes | None) -> str:
    if raw is None:
        return ""
    return clean_xml_chars(force_unicode(raw, "utf-8"))


def _default_metadata():
    return calibreMetaInformation("Unknown", ["Unknown"])


def _safe_seek(stream, pos: int | None) -> None:
    if pos is None or not hasattr(stream, "seek"):
        return
    try:
        stream.seek(pos)
    except Exception:
        pass


def _source_name(target_file) -> str:
    if isinstance(target_file, os.PathLike):
        return os.fspath(target_file)
    if isinstance(target_file, str):
        return target_file
    return getattr(target_file, "name", "") or ""


class MetadataUpdater:
    """
    Parse and update Topaz metadata blocks.
    """

    def __init__(self, stream):
        self.stream = stream
        self.data = StreamSlicer(stream)

        sig = self.data[:4]
        if not sig.startswith(b"TPZ"):
            raise ValueError("Not a Topaz file")
        offset = 4

        self.header_records, consumed = self.decode_vwi(self.data[offset : offset + 8])
        if consumed <= 0:
            raise ValueError("Corrupt Topaz header")
        offset += consumed
        self.topaz_headers, self.th_seq = self.get_headers(offset)

        if "metadata" not in self.topaz_headers:
            raise ValueError("Invalid Topaz format - no metadata record")
        if not self.topaz_headers["metadata"]["blocks"]:
            raise ValueError("Invalid Topaz format - metadata header has no blocks")

        md_offset = self.topaz_headers["metadata"]["blocks"][0]["offset"] + self.base
        if self.data[md_offset + 1 : md_offset + 9] != b"metadata":
            raise ValueError("Damaged Topaz metadata record")

    @staticmethod
    def decode_vwi(byts: bytes) -> tuple[int, int]:
        pos, val = 0, 0
        bb = bytearray(byts)
        done = False
        while pos < len(bb) and not done:
            b = bb[pos]
            pos += 1
            if (b & 0x80) == 0:
                done = True
            b &= 0x7F
            val = (val << 7) | b
        return val, pos

    @staticmethod
    def encode_vwi(value: int) -> bytes:
        if value < 0:
            raise ValueError("VWI cannot encode negative values")
        parts = [value & 0x7F]
        value >>= 7
        while value:
            parts.append((value & 0x7F) | 0x80)
            value >>= 7
        return bytes(reversed(parts))

    def get_headers(self, offset: int):
        topaz_headers: dict[str, dict] = {}
        th_seq: list[str] = []
        for _ in range(self.header_records):
            offset += 1  # record marker ('c')

            taglen, consumed = self.decode_vwi(self.data[offset : offset + 8])
            offset += consumed
            tag = _decode_tag(self.data[offset : offset + taglen])
            offset += taglen

            num_vals, consumed = self.decode_vwi(self.data[offset : offset + 8])
            offset += consumed
            blocks = {}
            for val in range(num_vals):
                hdr_offset, consumed = self.decode_vwi(self.data[offset : offset + 8])
                offset += consumed
                len_uncomp, consumed = self.decode_vwi(self.data[offset : offset + 8])
                offset += consumed
                len_comp, consumed = self.decode_vwi(self.data[offset : offset + 8])
                offset += consumed
                blocks[val] = {
                    "offset": hdr_offset,
                    "len_uncomp": len_uncomp,
                    "len_comp": len_comp,
                }
            topaz_headers[tag] = {"tag": tag, "blocks": blocks}
            th_seq.append(tag)

        self.eoth = self.data[offset : offset + 1]
        offset += 1
        self.base = offset
        return topaz_headers, th_seq

    def get_original_metadata(self) -> None:
        offset = self.base + self.topaz_headers["metadata"]["blocks"][0]["offset"]

        taglen, consumed = self.decode_vwi(self.data[offset : offset + 8])
        offset += consumed
        tag = _decode_tag(self.data[offset : offset + taglen])
        offset += taglen

        self.md_header = {
            "tag": tag,
            "flags": _byte_as_int(self.data[offset : offset + 1]),
        }
        offset += 1
        self.md_header["num_recs"] = _byte_as_int(self.data[offset : offset + 1])
        offset += 1

        self.metadata: dict[str, bytes] = {}
        self.md_seq: list[str] = []
        for _ in range(self.md_header["num_recs"]):
            taglen, consumed = self.decode_vwi(self.data[offset : offset + 8])
            offset += consumed
            key = _decode_tag(self.data[offset : offset + taglen])
            offset += taglen

            md_len, consumed = self.decode_vwi(self.data[offset : offset + 8])
            offset += consumed
            value = self.data[offset : offset + md_len]
            offset += md_len

            self.metadata[key] = value
            self.md_seq.append(key)

    def get_metadata(self):
        self.get_original_metadata()
        title = _decode_text(self.metadata.get("Title")) or "Unknown"

        raw_authors = _decode_text(self.metadata.get("Authors"))
        authors: list[str] = []
        if raw_authors:
            for chunk in raw_authors.split(";"):
                chunk = chunk.strip()
                if not chunk:
                    continue
                parsed = [x.strip() for x in string_to_authors(chunk) if x.strip()]
                authors.extend(parsed or [chunk])
        if not authors:
            authors = ["Unknown"]

        return calibreMetaInformation(title, authors)

    def generate_metadata_stream(self) -> bytes:
        out = bytearray()
        tag = (self.md_header.get("tag") or "metadata").encode("ascii", "replace")
        out.extend(self.encode_vwi(len(tag)))
        out.extend(tag)
        out.append(int(self.md_header.get("flags", 0)) & 0xFF)
        out.append(len(self.md_seq) & 0xFF)

        for key in self.md_seq:
            key_bytes = key.encode("ascii", "replace")
            value = self.metadata.get(key, b"")
            out.extend(self.encode_vwi(len(key_bytes)))
            out.extend(key_bytes)
            out.extend(self.encode_vwi(len(value)))
            out.extend(value)
        return bytes(out)

    def regenerate_headers(self, updated_md_len: int) -> bytes:
        original_md_len = self.topaz_headers["metadata"]["blocks"][0]["len_uncomp"]
        original_md_offset = self.topaz_headers["metadata"]["blocks"][0]["offset"]
        delta = updated_md_len - original_md_len

        out = bytearray(self.data[:5])
        for tag in self.th_seq:
            tag_bytes = tag.encode("ascii", "replace")
            out.extend(b"c")
            out.extend(self.encode_vwi(len(tag_bytes)))
            out.extend(tag_bytes)

            blocks = self.topaz_headers[tag]["blocks"]
            if blocks:
                out.extend(self.encode_vwi(len(blocks)))
                for block_index in sorted(blocks):
                    block = blocks[block_index]

                    if block["offset"] <= original_md_offset:
                        out.extend(self.encode_vwi(block["offset"]))
                    else:
                        out.extend(self.encode_vwi(block["offset"] + delta))

                    if tag == "metadata":
                        out.extend(self.encode_vwi(updated_md_len))
                    else:
                        out.extend(self.encode_vwi(block["len_uncomp"]))
                    out.extend(self.encode_vwi(block["len_comp"]))
            else:
                out.extend(self.encode_vwi(0))

        self.original_md_start = original_md_offset + self.base
        self.original_md_len = original_md_len
        return bytes(out)

    def _ensure_key(self, key: str) -> None:
        if key in self.metadata:
            return
        self.metadata[key] = b""
        self.md_seq.append(key)
        self.md_header["num_recs"] = len(self.md_seq)

    def update(self, mi) -> None:
        self.get_original_metadata()

        try:
            from LiuXin_alpha.file_formats.conversion.config import load_defaults

            prefs = load_defaults("mobi_output")
            prefer_author_sort = bool(prefs.get("prefer_author_sort", False))
        except Exception:
            prefer_author_sort = False

        title = clean_xml_chars(str(getattr(mi, "title", None) or "Unknown")) or "Unknown"
        self._ensure_key("Title")
        self.metadata["Title"] = title.encode("utf-8", "replace")

        self._ensure_key("Authors")
        if getattr(mi, "author_sort", None) and prefer_author_sort:
            author_sort = clean_xml_chars(str(mi.author_sort)).strip() or "Unknown"
            self.metadata["Authors"] = author_sort.encode("utf-8", "replace")
        else:
            authors = [
                clean_xml_chars(str(x)).strip()
                for x in (getattr(mi, "authors", None) or [])
                if clean_xml_chars(str(x)).strip()
            ]
            self.metadata["Authors"] = ("; ".join(authors or ["Unknown"])).encode("utf-8", "replace")

        updated_metadata = self.generate_metadata_stream()
        # Matches upstream Topaz metadata updater behavior.
        prefix = len("metadata") + 2
        updated_buffer_len = len(updated_metadata) - prefix

        head = self.regenerate_headers(updated_buffer_len)
        chunk1 = self.data[self.base : self.original_md_start]
        chunk2 = self.data[prefix + self.original_md_start + self.original_md_len :]

        self.stream.seek(0)
        self.stream.truncate(0)
        self.stream.write(head)
        self.stream.write(b"d")
        self.stream.write(chunk1)
        self.stream.write(updated_metadata)
        self.stream.write(chunk2)


def get_metadata(target_file):
    """
    Read Topaz metadata from a path, bytes payload, or binary stream.
    """
    source_name = _source_name(target_file)
    stream_needs_close = False
    stream = None
    pos = None

    try:
        if isinstance(target_file, os.PathLike):
            target_file = os.fspath(target_file)

        if isinstance(target_file, str):
            stream = open(target_file, "rb")
            stream_needs_close = True
        elif isinstance(target_file, (bytes, bytearray, memoryview)):
            stream = io.BytesIO(bytes(target_file))
            stream_needs_close = True
        elif hasattr(target_file, "read"):
            stream = target_file
            if hasattr(stream, "tell"):
                try:
                    pos = stream.tell()
                except Exception:
                    pos = None
            _safe_seek(stream, 0)
        else:
            raise TypeError("Topaz metadata reader expects a filesystem path or readable binary stream.")

        return MetadataUpdater(stream).get_metadata()
    except Exception as err:
        default_log.log_exception(
            "Failed to read Topaz metadata; using defaults.",
            err,
            "DEBUG",
            ("source", source_name or "<stream>"),
        )
        return _default_metadata()
    finally:
        if stream_needs_close and stream is not None:
            stream.close()
        elif stream is not None:
            _safe_seek(stream, pos)


def set_metadata(target_file, mi) -> None:
    """
    Update Topaz metadata in-place for a path or writable binary stream.
    """
    stream_needs_close = False
    stream = None
    pos = None

    try:
        if isinstance(target_file, os.PathLike):
            target_file = os.fspath(target_file)

        if isinstance(target_file, str):
            stream = open(target_file, "r+b")
            stream_needs_close = True
        elif hasattr(target_file, "read") and hasattr(target_file, "write"):
            stream = target_file
            if hasattr(stream, "tell"):
                try:
                    pos = stream.tell()
                except Exception:
                    pos = None
            _safe_seek(stream, 0)
        else:
            raise TypeError("Topaz metadata writer expects a writable binary stream or filesystem path.")

        MetadataUpdater(stream).update(mi)
    finally:
        if stream_needs_close and stream is not None:
            stream.close()
        elif stream is not None:
            _safe_seek(stream, pos)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "StreamSlicer",
    "MetadataUpdater",
    "get_metadata",
    "set_metadata",
]
