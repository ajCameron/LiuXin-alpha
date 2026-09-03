#!/usr/bin/env python3
"""Inventory ebook-shaped files from a .torrent and group likely logical books.

This script is intentionally standalone:

- Python 3 stdlib only
- no torrent client required
- no network access required

What it does:
- parses a `.torrent` file
- extracts embedded file paths and sizes
- classifies ebook-shaped files by extension
- groups likely logical books by directory + normalized stem
- emits JSON or a terminal-friendly text report
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any


EBOOK_EXTENSIONS = {
    "azw",
    "azw3",
    "cbz",
    "cbr",
    "chm",
    "djvu",
    "doc",
    "docx",
    "epub",
    "fb2",
    "htm",
    "html",
    "kfx",
    "kepub",
    "lit",
    "lrf",
    "mobi",
    "odt",
    "pdb",
    "pdf",
    "prc",
    "rb",
    "rtf",
    "snb",
    "txt",
    "xhtml",
    "zip",
}

FORMAT_PRIORITY = {
    "epub": 0,
    "kepub": 1,
    "azw3": 2,
    "mobi": 3,
    "pdf": 4,
    "txt": 5,
    "zip": 6,
    "html": 7,
    "htm": 8,
    "xhtml": 9,
}

NORMALIZE_STEM_PATTERN = re.compile(r"[\s._-]+")


@dataclass(frozen=True)
class TorrentFileRecord:
    """Normalized file entry extracted from one torrent manifest."""

    index: int
    path: str
    directory: str
    filename: str
    stem: str
    normalized_stem: str
    extension: str
    size: int
    ebook_like: bool


class BencodeDecodeError(ValueError):
    """Raised when torrent metadata is not valid bounded bencode."""


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def format_bytes(size: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(max(0, int(size)))
    unit_index = 0
    while value >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return "{} {}".format(int(value), units[unit_index])
    return "{:.1f} {}".format(value, units[unit_index])


def _decode_bencode_value(data: bytes, index: int) -> tuple[Any, int, bytes | None]:
    if index >= len(data):
        raise BencodeDecodeError("unexpected end of data")

    marker = data[index : index + 1]
    if marker == b"i":
        end = data.find(b"e", index)
        if end < 0:
            raise BencodeDecodeError("unterminated integer")
        number_text = data[index + 1 : end]
        try:
            value = int(number_text)
        except ValueError as exc:
            raise BencodeDecodeError("invalid integer {!r}".format(number_text)) from exc
        return value, end + 1, None

    if marker == b"l":
        index += 1
        items: list[Any] = []
        info_bytes: bytes | None = None
        while data[index : index + 1] != b"e":
            value, index, child_info = _decode_bencode_value(data, index)
            items.append(value)
            if info_bytes is None and child_info is not None:
                info_bytes = child_info
        return items, index + 1, info_bytes

    if marker == b"d":
        index += 1
        mapping: dict[bytes, Any] = {}
        info_bytes: bytes | None = None
        while data[index : index + 1] != b"e":
            key, index, _ = _decode_bencode_value(data, index)
            if not isinstance(key, bytes):
                raise BencodeDecodeError("dictionary key was not bytes")
            value_start = index
            value, index, child_info = _decode_bencode_value(data, index)
            mapping[key] = value
            if key == b"info":
                info_bytes = data[value_start:index]
            elif info_bytes is None and child_info is not None:
                info_bytes = child_info
        return mapping, index + 1, info_bytes

    if b"0" <= marker <= b"9":
        colon = data.find(b":", index)
        if colon < 0:
            raise BencodeDecodeError("unterminated byte-string length")
        try:
            length = int(data[index:colon])
        except ValueError as exc:
            raise BencodeDecodeError("invalid byte-string length") from exc
        start = colon + 1
        end = start + length
        if end > len(data):
            raise BencodeDecodeError("byte-string overruns input")
        return data[start:end], end, None

    raise BencodeDecodeError("unknown bencode marker {!r}".format(marker))


def decode_torrent_file_bytes(data: bytes) -> tuple[dict[bytes, Any], bytes]:
    value, index, info_bytes = _decode_bencode_value(data, 0)
    if index != len(data):
        raise BencodeDecodeError("trailing data after torrent payload")
    if not isinstance(value, dict):
        raise BencodeDecodeError("top-level torrent payload was not a dictionary")
    if info_bytes is None:
        raise BencodeDecodeError("torrent missing info dictionary")
    return value, info_bytes


def decode_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        for encoding in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                return value.decode(encoding)
            except UnicodeDecodeError:
                continue
        return value.decode("utf-8", errors="replace")
    return str(value)


def decode_path_components(entry: dict[bytes, Any]) -> list[str]:
    raw = entry.get(b"path.utf-8")
    if raw is None:
        raw = entry.get(b"path")
    if not isinstance(raw, list):
        raise BencodeDecodeError("torrent file entry missing path list")
    parts = [decode_text(part) or "" for part in raw]
    return [part for part in parts if part]


def normalize_stem(stem: str) -> str:
    text = NORMALIZE_STEM_PATTERN.sub(" ", str(stem or "").strip().casefold())
    return text.strip()


def file_record_from_path(*, index: int, path: str, size: int) -> TorrentFileRecord:
    pure = PurePosixPath(path)
    filename = pure.name
    stem = pure.stem
    extension = pure.suffix[1:].lower() if pure.suffix.startswith(".") else pure.suffix.lower()
    directory = "." if str(pure.parent) in {"", "."} else str(pure.parent)
    return TorrentFileRecord(
        index=index,
        path=str(pure),
        directory=directory,
        filename=filename,
        stem=stem,
        normalized_stem=normalize_stem(stem),
        extension=extension,
        size=int(size),
        ebook_like=extension in EBOOK_EXTENSIONS,
    )


def extract_torrent_files(torrent_dict: dict[bytes, Any]) -> list[TorrentFileRecord]:
    info = torrent_dict.get(b"info")
    if not isinstance(info, dict):
        raise BencodeDecodeError("torrent info value was not a dictionary")

    files_value = info.get(b"files")
    records: list[TorrentFileRecord] = []
    if isinstance(files_value, list):
        for index, raw_entry in enumerate(files_value, start=1):
            if not isinstance(raw_entry, dict):
                raise BencodeDecodeError("torrent file entry was not a dictionary")
            length = int(raw_entry.get(b"length") or 0)
            path = "/".join(decode_path_components(raw_entry))
            records.append(file_record_from_path(index=index, path=path, size=length))
        return records

    name = decode_text(info.get(b"name.utf-8") or info.get(b"name")) or "unnamed"
    length = int(info.get(b"length") or 0)
    return [file_record_from_path(index=1, path=name, size=length)]


def _variant_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    ext = str(item.get("extension") or "").lower()
    return (
        FORMAT_PRIORITY.get(ext, len(FORMAT_PRIORITY)),
        str(item.get("directory") or ""),
        str(item.get("filename") or ""),
        str(item.get("path") or ""),
    )


def group_ebook_files(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for item in files:
        key = (str(item.get("directory") or "."), str(item.get("normalized_stem") or ""))
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {
                "group_key": "{}:{}".format(key[0], key[1]),
                "directory": key[0],
                "stem": str(item.get("stem") or ""),
                "normalized_stem": key[1],
                "files": [],
            }
            buckets[key] = bucket
        bucket["files"].append(dict(item))

    groups: list[dict[str, Any]] = []
    for _key, bucket in sorted(buckets.items(), key=lambda item: item[0]):
        variants = sorted(list(bucket["files"]), key=_variant_sort_key)
        groups.append(
            {
                "group_key": bucket["group_key"],
                "directory": bucket["directory"],
                "stem": bucket["stem"],
                "normalized_stem": bucket["normalized_stem"],
                "variant_count": len(variants),
                "extensions": sorted(
                    {str(item.get("extension") or "") for item in variants if str(item.get("extension") or "")}
                ),
                "total_size": sum(int(item.get("size") or 0) for item in variants),
                "primary_path": str(variants[0].get("path") or "") if variants else None,
                "likely_multiformat_book": len(variants) > 1,
                "files": variants,
            }
        )
    return groups


def group_ebook_files_by_directory(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for item in files:
        directory = str(item.get("directory") or ".")
        bucket = buckets.get(directory)
        if bucket is None:
            bucket = {
                "group_key": directory,
                "directory": directory,
                "files": [],
            }
            buckets[directory] = bucket
        bucket["files"].append(dict(item))

    groups: list[dict[str, Any]] = []
    for directory, bucket in sorted(buckets.items(), key=lambda item: item[0]):
        variants = sorted(list(bucket["files"]), key=_variant_sort_key)
        normalized_stems = sorted(
            {
                str(item.get("normalized_stem") or "")
                for item in variants
                if str(item.get("normalized_stem") or "")
            }
        )
        groups.append(
            {
                "group_key": directory,
                "directory": directory,
                "file_count": len(variants),
                "stem_count": len(normalized_stems),
                "stems": normalized_stems,
                "extensions": sorted(
                    {str(item.get("extension") or "") for item in variants if str(item.get("extension") or "")}
                ),
                "total_size": sum(int(item.get("size") or 0) for item in variants),
                "primary_path": str(variants[0].get("path") or "") if variants else None,
                "files": variants,
            }
        )
    return groups


def analyze_torrent_bytes(data: bytes) -> dict[str, Any]:
    torrent_dict, info_bytes = decode_torrent_file_bytes(data)
    info = torrent_dict[b"info"]
    assert isinstance(info, dict)

    files = [record.__dict__.copy() for record in extract_torrent_files(torrent_dict)]
    ebook_files = [dict(item) for item in files if bool(item.get("ebook_like"))]
    groups = group_ebook_files(ebook_files)
    directory_groups = group_ebook_files_by_directory(ebook_files)

    announce = decode_text(torrent_dict.get(b"announce"))
    announce_list_raw = torrent_dict.get(b"announce-list")
    announce_list: list[str] = []
    if isinstance(announce_list_raw, list):
        for tier in announce_list_raw:
            if isinstance(tier, list):
                for url in tier:
                    decoded = decode_text(url)
                    if decoded:
                        announce_list.append(decoded)
            else:
                decoded = decode_text(tier)
                if decoded:
                    announce_list.append(decoded)

    name = decode_text(info.get(b"name.utf-8") or info.get(b"name")) or "unnamed"
    payload = {
        "generated_at": utc_now(),
        "torrent": {
            "name": name,
            "info_hash": hashlib.sha1(info_bytes).hexdigest(),
            "announce": announce,
            "announce_list": announce_list,
            "created_by": decode_text(torrent_dict.get(b"created by")),
            "comment": decode_text(torrent_dict.get(b"comment")),
            "creation_date": int(torrent_dict.get(b"creation date") or 0) or None,
            "piece_length": int(info.get(b"piece length") or 0) or None,
            "private": bool(int(info.get(b"private") or 0)),
            "file_count": len(files),
            "ebook_file_count": len(ebook_files),
            "group_count": len(groups),
            "directory_group_count": len(directory_groups),
            "multi_variant_group_count": sum(1 for group in groups if bool(group.get("likely_multiformat_book"))),
            "multi_stem_directory_group_count": sum(
                1 for group in directory_groups if int(group.get("stem_count") or 0) > 1
            ),
            "total_size": sum(int(item.get("size") or 0) for item in files),
        },
        "files": files,
        "ebook_files": ebook_files,
        "groups": groups,
        "directory_groups": directory_groups,
    }
    return payload


def analyze_torrent_file(path: str | Path) -> dict[str, Any]:
    data = Path(path).read_bytes()
    return analyze_torrent_bytes(data)


def render_text_report(payload: dict[str, Any]) -> str:
    torrent = dict(payload.get("torrent") or {})
    groups = list(payload.get("groups") or [])
    directory_groups = list(payload.get("directory_groups") or [])
    ebook_files = list(payload.get("ebook_files") or [])

    lines: list[str] = []
    lines.append("Torrent")
    lines.append("  Name: {}".format(torrent.get("name") or "unnamed"))
    lines.append("  Info hash: {}".format(torrent.get("info_hash") or "-"))
    lines.append("  Announce: {}".format(torrent.get("announce") or "-"))
    lines.append("  Files: {}".format(int(torrent.get("file_count") or 0)))
    lines.append("  Ebook files: {}".format(int(torrent.get("ebook_file_count") or 0)))
    lines.append("  Logical book groups: {}".format(int(torrent.get("group_count") or 0)))
    lines.append("  Directory groups: {}".format(int(torrent.get("directory_group_count") or 0)))
    lines.append("  Total size: {}".format(format_bytes(int(torrent.get("total_size") or 0))))

    lines.append("")
    lines.append("Likely Books")
    if groups:
        for group in groups:
            lines.append(
                "  - {} | {} variants | {} | {} | {}".format(
                    group.get("stem") or group.get("group_key") or "-",
                    int(group.get("variant_count") or 0),
                    ", ".join(group.get("extensions") or []) or "-",
                    format_bytes(int(group.get("total_size") or 0)),
                    group.get("primary_path") or "-",
                )
            )
    else:
        lines.append("  - none")

    multi_stem_dirs = [group for group in directory_groups if int(group.get("stem_count") or 0) > 1]
    lines.append("")
    lines.append("Messy Directories")
    if multi_stem_dirs:
        for group in multi_stem_dirs:
            lines.append(
                "  - {} | {} files | {} stems | {} | stems={}".format(
                    group.get("directory") or ".",
                    int(group.get("file_count") or 0),
                    int(group.get("stem_count") or 0),
                    format_bytes(int(group.get("total_size") or 0)),
                    ", ".join(group.get("stems") or []) or "-",
                )
            )
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Ebook Files")
    if ebook_files:
        for item in ebook_files:
            lines.append(
                "  - {} | {} | {}".format(
                    item.get("path") or "-",
                    item.get("extension") or "-",
                    format_bytes(int(item.get("size") or 0)),
                )
            )
    else:
        lines.append("  - none")

    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("torrent_file", help="Path to the .torrent file to inspect")
    parser.add_argument("--output", help="Optional JSON output path; defaults to stdout")
    parser.add_argument(
        "--report",
        choices=("json", "text"),
        default="json",
        help="Output format (default: %(default)s)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    payload = analyze_torrent_file(args.torrent_file)
    if args.report == "text":
        encoded = render_text_report(payload)
    else:
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(encoded, encoding="utf-8")
        print(
            "wrote torrent inventory: file_count={} ebook_file_count={} group_count={} format={} output={}".format(
                payload["torrent"]["file_count"],
                payload["torrent"]["ebook_file_count"],
                payload["torrent"]["group_count"],
                args.report,
                out,
            ),
            flush=True,
        )
    else:
        sys.stdout.write(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
