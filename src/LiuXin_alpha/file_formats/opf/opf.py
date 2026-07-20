#!/usr/bin/env python2
# vim:fileencoding=utf-8
# License: GPLv3 Copyright: 2016, Kovid Goyal <kovid at kovidgoyal.net>

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing
import os
from copy import deepcopy
from collections.abc import Mapping

from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars

from LiuXin_alpha.file_formats.opf.opf2 import OPF, pretty_print
from LiuXin_alpha.file_formats.opf.opf3 import apply_metadata, read_metadata

from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata as calibreMetadata

from LiuXin_alpha.metadata.utils import (
    parse_opf,
    normalize_languages,
    create_manifest_item,
    parse_opf_version,
)

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems


class DummyFile(object):
    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        self.raw = raw

    def read(self: _typing.Self, size: _typing.Any = -1) -> _typing.Any:
        if size is None or size < 0:
            return self.raw
        return self.raw[:size]


def _coerce_input_stream(stream: _typing.Any) -> _typing.Any:
    if isinstance(stream, (bytes, bytearray, memoryview)):
        return DummyFile(bytes(stream))
    if isinstance(stream, os.PathLike):
        return os.fspath(stream)
    return stream


def _parse_opf_from_input(stream: _typing.Any) -> _typing.Any:
    stream = _coerce_input_stream(stream)
    if hasattr(stream, "read"):
        # parse_opf() reads from current position; for robustness we parse from
        # start and then restore the original stream position.
        pos = None
        if hasattr(stream, "tell"):
            try:
                pos = stream.tell()
            except Exception:
                pos = None
        try:
            if hasattr(stream, "seek"):
                try:
                    stream.seek(0)
                except Exception:
                    pass
            return parse_opf(stream)
        finally:
            if pos is not None and hasattr(stream, "seek"):
                try:
                    stream.seek(pos)
                except Exception:
                    pass
    return parse_opf(stream)


def _clean_xml_text(value: _typing.Any) -> _typing.Any:
    if value is None:
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8", "replace")
    if isinstance(value, str):
        return clean_xml_chars(value)
    return value


def _clean_xml_list(values: _typing.Any) -> _typing.Any:
    if not values:
        return []
    ans = []
    for item in values:
        clean_item = _clean_xml_text(item)
        if clean_item is None:
            continue
        if isinstance(clean_item, str) and not clean_item.strip():
            continue
        ans.append(clean_item)
    return ans


def _sanitize_metadata_for_xml(mi: _typing.Any) -> _typing.Any:
    """
    Return a metadata object safe for XML serialization.

    This strips XML-invalid control characters from free-text fields before
    handing off to OPF2/OPF3 writers, so callers get robust behavior instead
    of hard lxml failures.
    """
    safe_mi = None
    for clone_method in ("deepcopy_metadata", "deepcopy"):
        fn = getattr(mi, clone_method, None)
        if callable(fn):
            try:
                safe_mi = fn()
                break
            except Exception:
                safe_mi = None
    if safe_mi is None:
        try:
            safe_mi = deepcopy(mi)
        except Exception:
            safe_mi = mi

    for attr in (
        "title",
        "author_sort",
        "title_sort",
        "publisher",
        "comments",
        "book_producer",
        "series",
        "isbn",
    ):
        val = getattr(safe_mi, attr, None)
        clean_val = _clean_xml_text(val)
        if clean_val != val:
            setattr(safe_mi, attr, clean_val)

    for attr in ("authors", "tags", "languages"):
        val = getattr(safe_mi, attr, None)
        if val is None:
            continue
        if isinstance(val, str):
            val = [val]
        try:
            clean_val = _clean_xml_list(list(val))
        except Exception:
            clean_val = _clean_xml_list((val,))
        setattr(safe_mi, attr, clean_val)

    # Keep identifiers stable, but sanitize problematic control chars in keys and values.
    if hasattr(safe_mi, "get_identifiers") and hasattr(safe_mi, "set_identifiers"):
        try:
            idents = safe_mi.get_identifiers() or {}
        except Exception:
            idents = {}
        if isinstance(idents, Mapping):
            clean_idents = {}
            for key, val in iteritems(idents):
                clean_key = _clean_xml_text(key)
                clean_val = _clean_xml_text(val)
                if clean_key and clean_val:
                    clean_idents[clean_key] = clean_val
            safe_mi.set_identifiers(clean_idents)

    return safe_mi


def get_metadata2(root: _typing.Any, ver: _typing.Any) -> tuple[_typing.Any, ...]:
    opf = OPF(None, preparsed_opf=root, read_toc=False)
    return opf.to_book_metadata(), ver, opf.raster_cover, opf.first_spine_item()


def get_metadata3(root: _typing.Any, ver: _typing.Any) -> _typing.Any:
    return read_metadata(root, ver=ver, return_extra_data=True)


def get_metadata_from_parsed(root: _typing.Any) -> _typing.Any:
    ver = parse_opf_version(root.get("version"))
    f = get_metadata2 if ver.major < 3 else get_metadata3
    return f(root, ver)


def get_metadata(stream: _typing.Any) -> _typing.Any:
    root = _parse_opf_from_input(stream)
    return get_metadata_from_parsed(root)


def set_metadata(
    stream: _typing.Any,
    mi: _typing.Any,
    cover_prefix: str = "",
    cover_data: _typing.Any = None,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
) -> tuple[_typing.Any, ...]:
    """
    Front end for the set_metadata_opf2 and set_metadata_opf3 methods - detects the version then  calls the appropriate
    method to actually set the metadata.
    :param stream:
    :param mi:
    :param cover_prefix:
    :param cover_data:
    :param apply_null:
    :param update_timestamp:
    :param force_identifiers:
    :param add_missing_cover:
    :return:
    """
    root = _parse_opf_from_input(stream)
    ver = parse_opf_version(root.get("version"))
    f = set_metadata_opf2 if ver.major < 3 else set_metadata_opf3
    safe_mi = _sanitize_metadata_for_xml(mi)
    opfbytes, raster_cover = f(
        root,
        cover_prefix,
        safe_mi,
        ver,
        cover_data=cover_data,
        apply_null=apply_null,
        update_timestamp=update_timestamp,
        force_identifiers=force_identifiers,
        add_missing_cover=add_missing_cover,
    )
    return opfbytes, ver, raster_cover


def set_metadata_opf2(
    root: _typing.Any,
    cover_prefix: _typing.Any,
    mi: _typing.Any,
    opf_version: _typing.Any,
    cover_data: _typing.Any = None,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
) -> tuple[_typing.Any, ...]:
    """
    Set the metadata for an opf 2 file.
    :param root:
    :param cover_prefix:
    :param mi:
    :param opf_version:
    :param cover_data:
    :param apply_null:
    :param update_timestamp:
    :param force_identifiers:
    :param add_missing_cover:
    :return:
    """
    assert isinstance(mi, calibreMetadata), "Method can only run on calibreMetadata object"

    for x in ("guide", "toc", "manifest", "spine"):
        setattr(mi, x, None)

    opf = OPF(None, preparsed_opf=root, read_toc=False)
    if mi.languages:
        mi.languages = normalize_languages(list(opf.raw_languages) or [], mi.languages)

    opf.smart_update(mi, apply_null=apply_null)
    if getattr(mi, "uuid", None):
        opf.application_id = mi.uuid
    if apply_null or force_identifiers:
        opf.set_identifiers(mi.get_identifiers())
    else:
        orig = opf.get_identifiers()
        orig.update(mi.get_identifiers())
        opf.set_identifiers({k: v for k, v in iteritems(orig) if k and v})
    if update_timestamp and mi.timestamp is not None:
        opf.timestamp = mi.timestamp
    raster_cover = opf.raster_cover
    if raster_cover is None and cover_data is not None and add_missing_cover:
        guide_raster_cover = opf.guide_raster_cover

        if guide_raster_cover is not None:
            i = guide_raster_cover
            raster_cover = i.get("href")
        else:
            if cover_prefix and not cover_prefix.endswith("/"):
                cover_prefix += "/"
            name = cover_prefix + "cover.jpg"
            i = create_manifest_item(opf.root, name, "cover")
            if i is not None:
                raster_cover = name
        if i is not None:
            if opf_version.major < 3:
                [x.getparent().remove(x) for x in opf.root.xpath('//*[local-name()="meta" and @name="cover"]')]
                m = opf.create_metadata_element("meta", is_dc=False)
                m.set("name", "cover"), m.set("content", i.get("id"))
            else:
                for x in opf.root.xpath('//*[local-name()="item" and contains(@properties, "cover-image")]'):
                    x.set(
                        "properties",
                        x.get("properties").replace("cover-image", "").strip(),
                    )
                i.set("properties", "cover-image")

    with pretty_print:
        return opf.render(), raster_cover


# Todo: Current md test file is for epub version 2 - find one for epub version 3 for testing
def set_metadata_opf3(
    root: _typing.Any,
    cover_prefix: _typing.Any,
    mi: _typing.Any,
    opf_version: _typing.Any,
    cover_data: _typing.Any = None,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
) -> tuple[_typing.Any, ...]:
    """
    Sets metadata for the OPF3 standard.
    :param root:
    :param cover_prefix:
    :param mi:
    :param opf_version:
    :param cover_data:
    :param apply_null:
    :param update_timestamp:
    :param force_identifiers:
    :param add_missing_cover:
    :return:
    """
    raster_cover = apply_metadata(
        root,
        mi,
        cover_prefix=cover_prefix,
        cover_data=cover_data,
        apply_null=apply_null,
        update_timestamp=update_timestamp,
        force_identifiers=force_identifiers,
        add_missing_cover=add_missing_cover,
    )
    return etree.tostring(root, encoding="utf-8"), raster_cover
