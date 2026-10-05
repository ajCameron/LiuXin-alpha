"""
Adapt metadata containers to serialized OPF, update existing packages, and parse Calibre, LiuXin, or WEMI views.

File helpers write directly and overwrite existing destinations without creating
parent directories. Parsing delegates to the OPF readers; supplied readable streams
remain caller-owned.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_opf_tools.py
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from LiuXin_alpha.file_formats.opf.opf import (
    _sanitize_metadata_for_xml,
    set_metadata as _set_opf_metadata,
)
from LiuXin_alpha.file_formats.opf.opf2 import metadata_to_opf as _metadata_to_opf
from LiuXin_alpha.metadata.book.base import calibreMetadata as CoreCalibreMetadata
from LiuXin_alpha.metadata.file_sources.opf import get_metadata as _get_opf_metadata
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import (
    Metadata as OPFCalibreMetadata,
)


OPFMetadataKind = Literal["calibre", "liuxin", "wemi"]


def metadata_to_opf_bytes(metadata: Any, *, default_lang: str | None = None) -> bytes:
    """
    Convert metadata to an OPF-compatible copy, sanitize XML text, and request serialized package output.

    Example:
        >>> raw = metadata_to_opf_bytes(CoreCalibreMetadata('Example', ['Ada']))
        >>> b'Example' in raw
        True


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param default_lang: Optional default language forwarded to OPF serialization.
    :return: OPF bytes, coercing a non-bytes serializer result through _ensure_bytes.
    """
    raw = _metadata_to_opf(
        _sanitize_metadata_for_xml(_as_calibre_metadata(metadata)),
        as_string=True,
        default_lang=default_lang,
    )
    return _ensure_bytes(raw)


def metadata_to_opf_file(
    metadata: Any,
    path: str | os.PathLike[str],
    *,
    default_lang: str | None = None,
) -> Path:
    """
    Serialize metadata and write bytes directly to the supplied path.

    Overwrite an existing file; do not create parent directories or provide atomic
    replacement.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param path: Filesystem destination accepted by pathlib.Path.
    :param default_lang: Optional default language forwarded to OPF serialization.
    :return: Destination pathlib.Path after a successful write.
    """
    target = Path(path)
    target.write_bytes(metadata_to_opf_bytes(metadata, default_lang=default_lang))
    return target


def update_opf_bytes(
    opf_source: Any,
    metadata: Any,
    *,
    cover_prefix: str = "",
    cover_data: Any = None,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
) -> bytes:
    """
    Coerce XML text to bytes, convert metadata, and delegate to the version-aware OPF updater.

    Keep package structure through the updater and discard its version/cover return
    details. Caller stream positions are restored when the parser can seek/tell; caller
    streams are not closed.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param opf_source: Existing OPF path, bytes-like payload, XML string, or readable
        stream; caller streams are not closed.
    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param cover_prefix: Path prefix forwarded for a newly referenced cover image.
    :param cover_data: Optional cover data forwarded to the package updater; this
        adapter does not write an image file.
    :param apply_null: Whether the package updater may apply null metadata fields.
    :param update_timestamp: Whether to request timestamp updates from the package
        updater.
    :param force_identifiers: Whether to force the supplied identifier mapping instead
        of the updater’s merge behavior.
    :param add_missing_cover: Whether the package updater may add a missing cover
        reference.
    :return: Updated OPF bytes; parser and metadata-conversion errors propagate.
    """
    raw, _version, _raster_cover = _set_opf_metadata(
        _coerce_opf_update_source(opf_source),
        _as_calibre_metadata(metadata),
        cover_prefix=cover_prefix,
        cover_data=cover_data,
        apply_null=apply_null,
        update_timestamp=update_timestamp,
        force_identifiers=force_identifiers,
        add_missing_cover=add_missing_cover,
    )
    return _ensure_bytes(raw)


def update_opf_file(
    opf_source: str | os.PathLike[str],
    metadata: Any,
    output_path: str | os.PathLike[str] | None = None,
    *,
    cover_prefix: str = "",
    cover_data: Any = None,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
) -> Path:
    """
    Compute updated OPF bytes and write directly to output_path or the original source path.

    An omitted output_path overwrites the source. Existing destinations are replaced by
    a normal write, without parent creation or atomic replacement.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param opf_source: Existing OPF filesystem path; used as the destination when
        output_path is None.
    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param output_path: Optional destination; omission updates the original source path.
    :param cover_prefix: Path prefix forwarded for a newly referenced cover image.
    :param cover_data: Optional cover data forwarded to the package updater; this
        adapter does not write an image file.
    :param apply_null: Whether the package updater may apply null metadata fields.
    :param update_timestamp: Whether to request timestamp updates from the package
        updater.
    :param force_identifiers: Whether to force the supplied identifier mapping instead
        of the updater’s merge behavior.
    :param add_missing_cover: Whether the package updater may add a missing cover
        reference.
    :return: Destination pathlib.Path after a successful write.
    """
    target = Path(output_path) if output_path is not None else Path(opf_source)
    target.write_bytes(
        update_opf_bytes(
            opf_source,
            metadata,
            cover_prefix=cover_prefix,
            cover_data=cover_data,
            apply_null=apply_null,
            update_timestamp=update_timestamp,
            force_identifiers=force_identifiers,
            add_missing_cover=add_missing_cover,
        )
    )
    return target


def calibre_metadata_from_opf(source: Any) -> CoreCalibreMetadata:
    """
    Parse an OPF source with Calibre output selected and automatic leading-XML-string detection.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param source: OPF path, bytes/bytearray, XML string, or readable stream. Reader
        attempts to restore stream position and leaves caller streams open.
    :return: Core Calibre-shaped metadata; I/O and strict parsing failures propagate.
    """
    return _get_opf_metadata(source, calibre=True, text=_is_xml_text(source))


def liuxin_metadata_from_opf(source: Any) -> Any:
    """
    Parse an OPF source with LiuXin Calibre-like output selected and leading-XML-string detection.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param source: OPF path, bytes/bytearray, XML string, or readable stream; caller
        streams remain open.
    :return: LiuXin Calibre-like metadata container; I/O and strict parsing failures
        propagate.
    """
    return _get_opf_metadata(source, calibre=False, text=_is_xml_text(source))


def liuxin_wemi_metadata_from_opf(
    source: Any,
    *,
    database: Any = None,
    item_id: int | None = None,
    source_row: Any = None,
    replace_metadata: bool = False,
) -> Any:
    """
    Parse legacy OPF fields, infer an item ID, and optionally overlay them onto database-hydrated WEMI metadata.

    Hydrate only when a database and an inferred ID or source row are supplied;
    otherwise construct WEMI metadata from the parsed object. Attach ItemIdentity only
    when an inferred ID exists and the result has no item database ID.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param source: OPF source accepted by liuxin_metadata_from_opf.
    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param item_id: Explicit item ID, converted with int and preferred over source_row.
    :param source_row: Row object with a mapping row_dict, or mapping containing
        item_id.
    :param replace_metadata: Overlay policy passed to smart_update when hydrating WEMI
        from a database.
    :return: WEMI metadata container; OPF alone does not reconstruct the full WEMI
        graph.
    """
    from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata import (
        LiuXinWEMIMetadata,
    )
    from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import (
        ItemIdentity,
    )

    opf_metadata = liuxin_metadata_from_opf(source)
    source_item_id = _item_id_from_inputs(item_id=item_id, source_row=source_row)

    if database is not None and (source_item_id is not None or source_row is not None):
        metadata = LiuXinWEMIMetadata.from_database(
            database,
            item_id=source_item_id,
            source_row=source_row,
        )
        metadata.smart_update(opf_metadata, replace_metadata=replace_metadata)
    else:
        metadata = LiuXinWEMIMetadata(other=opf_metadata)

    if source_item_id is not None and metadata.get_database_id("item") is None:
        metadata.item = ItemIdentity(item_id=source_item_id)

    return metadata


def metadata_from_opf(
    source: Any,
    *,
    kind: OPFMetadataKind | str = "liuxin",
    database: Any = None,
    item_id: int | None = None,
    source_row: Any = None,
    replace_metadata: bool = False,
) -> Any:
    """
    Normalize a representation selector and dispatch to Calibre, LiuXin, or WEMI parsing.

    Accept calibre/calibre_metadata, liuxin/liu_xin/metadata, and
    wemi/liuxin_wemi/liu_xin_wemi. Case, outer whitespace, and hyphens are normalized.
    Only WEMI parsing consumes database/overlay options; unknown selectors raise
    ValueError before reading the source.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param source: OPF path, bytes/bytearray, XML string, or readable stream.
    :param kind: Output representation, defaulting to liuxin.
    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param item_id: Optional item identifier forwarded to the hydrator or OPF adapter.
    :param source_row: Optional source row identifying the item or WEMI slice.
    :param replace_metadata: Overlay policy passed to smart_update when hydrating WEMI
        from a database.
    :return: Metadata object in the selected representation.
    """
    normalized = str(kind).strip().lower().replace("-", "_")
    if normalized in {"calibre", "calibre_metadata"}:
        return calibre_metadata_from_opf(source)
    if normalized in {"liuxin", "liu_xin", "metadata"}:
        return liuxin_metadata_from_opf(source)
    if normalized in {"wemi", "liuxin_wemi", "liu_xin_wemi"}:
        return liuxin_wemi_metadata_from_opf(
            source,
            database=database,
            item_id=item_id,
            source_row=source_row,
            replace_metadata=replace_metadata,
        )
    raise ValueError(
        "Unknown OPF metadata kind {!r}. Expected 'liuxin', 'calibre', or 'wemi'.".format(
            kind,
        )
    )


def _as_calibre_metadata(metadata: Any) -> OPFCalibreMetadata:
    """
    Resolve a compatible Calibre copy and normalize its title-sort, tag, and language fields.

    Copy OPF-compatible objects directly, adapt core objects, or try callable
    as_calibre_metadata then to_calibre methods before the legacy factory. Conversion
    callbacks may have their own side effects; conversion errors propagate.

    Example:
        >>> original = CoreCalibreMetadata('Example', ['Ada'])
        >>> converted = _as_calibre_metadata(original)
        >>> (converted.title, converted is original)
        ('Example', False)


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :return: OPFCalibreMetadata copy used by serialization and updates.
    """
    if isinstance(metadata, OPFCalibreMetadata):
        clone = metadata.deepcopy_metadata()
    elif isinstance(metadata, CoreCalibreMetadata):
        clone = OPFCalibreMetadata(metadata.title, metadata.authors, other=metadata)
    else:
        getter = getattr(metadata, "as_calibre_metadata", None)
        if not callable(getter):
            getter = getattr(metadata, "to_calibre", None)
        clone = getter() if callable(getter) else calibreMetaInformation(metadata)
        if isinstance(clone, OPFCalibreMetadata):
            clone = clone.deepcopy_metadata()
        elif isinstance(clone, CoreCalibreMetadata):
            clone = OPFCalibreMetadata(clone.title, clone.authors, other=clone)
        else:
            core_clone = calibreMetaInformation(clone)
            clone = OPFCalibreMetadata(core_clone.title, core_clone.authors, other=core_clone)

    _normalize_calibre_for_opf(clone)
    return clone


def _normalize_calibre_for_opf(metadata: CoreCalibreMetadata) -> None:
    """
    Mutate a metadata copy to expose title_sort and list-shaped tags/languages when missing or scalar strings.

    Prefer a truthy title_sort over titlesort; leave other iterable field types
    unchanged.

    Example:
        >>> from types import SimpleNamespace
        >>> value = SimpleNamespace(tags='tag', languages=None)
        >>> _normalize_calibre_for_opf(value)
        >>> (value.tags, value.languages)
        (['tag'], [])


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :return: None; updates the supplied object in place.
    """
    title_sort = getattr(metadata, "title_sort", None) or getattr(metadata, "titlesort", None)
    if title_sort:
        metadata.title_sort = title_sort

    tags = getattr(metadata, "tags", None)
    if tags is None:
        metadata.tags = []
    elif isinstance(tags, str):
        metadata.tags = [tags]

    languages = getattr(metadata, "languages", None)
    if languages is None:
        metadata.languages = []
    elif isinstance(languages, str):
        metadata.languages = [languages]


def _ensure_bytes(raw: Any) -> bytes:
    """
    Preserve bytes, convert bytearray/memoryview, encode strings as UTF-8, or fall back to bytes(raw).

    Example:
        >>> _ensure_bytes('é') == 'é'.encode('utf-8')
        True
        >>> _ensure_bytes(memoryview(b'opf'))
        b'opf'


    :param raw: Serializer result or other value accepted by the conversion branches.
    :return: Bytes result; fallback accepts the same inputs and raises the same errors
        as bytes.
    """
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, memoryview):
        return raw.tobytes()
    if isinstance(raw, str):
        return raw.encode("utf-8")
    return bytes(raw)


def _is_xml_text(source: Any) -> bool:
    """
    Recognize only strings whose first character after leading whitespace is an opening angle bracket.

    Example:
        >>> (_is_xml_text('  <package/>'), _is_xml_text(b'<package/>'))
        (True, False)


    :param source: Candidate source object.
    :return: Boolean heuristic; no XML parse or BOM removal occurs.
    """
    return isinstance(source, str) and source.lstrip().startswith("<")


def _coerce_opf_update_source(source: Any) -> Any:
    """
    Encode recognized XML strings as UTF-8 and otherwise retain the source object unchanged.

    Example:
        >>> _coerce_opf_update_source('<package/>')
        b'<package/>'


    :param source: Possible inline XML string, path, payload, or stream.
    :return: Encoded XML bytes or the original object.
    """
    if _is_xml_text(source):
        return source.encode("utf-8")
    return source


def _item_id_from_inputs(*, item_id: int | None, source_row: Any) -> int | None:
    """
    Prefer an explicit ID, otherwise inspect a mapping row_dict or source mapping for item_id.

    A mapping row_dict takes precedence even when empty. None or an empty string in that
    mapping means no ID; all other values are converted with int and conversion errors
    propagate.

    Example:
        >>> _item_id_from_inputs(item_id=None, source_row={'item_id': '7'})
        7
        >>> _item_id_from_inputs(item_id=0, source_row={'item_id': 9})
        0


    :param item_id: Explicit candidate ID; any non-None value takes precedence.
    :param source_row: Row-like object or mapping consulted when item_id is None.
    :return: Integer item ID or None.
    """
    if item_id is not None:
        return int(item_id)

    mapping = None
    row_dict = getattr(source_row, "row_dict", None)
    if isinstance(row_dict, Mapping):
        mapping = row_dict
    elif isinstance(source_row, Mapping):
        mapping = source_row

    if mapping is None:
        return None

    value = mapping.get("item_id")
    if value in (None, ""):
        return None
    return int(value)


__all__ = [
    "OPFMetadataKind",
    "calibre_metadata_from_opf",
    "liuxin_metadata_from_opf",
    "liuxin_wemi_metadata_from_opf",
    "metadata_from_opf",
    "metadata_to_opf_bytes",
    "metadata_to_opf_file",
    "update_opf_bytes",
    "update_opf_file",
]
