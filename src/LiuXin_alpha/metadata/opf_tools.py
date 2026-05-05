"""Convenience adapters between LiuXin metadata containers and OPF files."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from LiuXin_alpha.file_formats.opf.opf import set_metadata as _set_opf_metadata
from LiuXin_alpha.file_formats.opf.opf2 import metadata_to_opf as _metadata_to_opf
from LiuXin_alpha.metadata.book.base import calibreMetadata as CoreCalibreMetadata
from LiuXin_alpha.metadata.file_sources.opf import get_metadata as _get_opf_metadata
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import (
    Metadata as OPFCalibreMetadata,
)


OPFMetadataKind = Literal["calibre", "liuxin", "wemi"]


def metadata_to_opf_bytes(metadata: Any, *, default_lang: str | None = None) -> bytes:
    """Serialize a LiuXin, WEMI, or Calibre-shaped metadata object to OPF bytes."""
    raw = _metadata_to_opf(
        _as_calibre_metadata(metadata),
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
    """Serialize metadata to a standalone OPF file and return the written path."""
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
    """Update an existing OPF payload with metadata while preserving package structure."""
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
    Update an OPF file with metadata.

    When ``output_path`` is omitted the source file is overwritten.
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
    """Read OPF/XML metadata into a Calibre-shaped metadata object."""
    return _get_opf_metadata(source, calibre=True, text=_is_xml_text(source))


def liuxin_metadata_from_opf(source: Any) -> Any:
    """Read OPF/XML metadata into LiuXin's Calibre-like metadata container."""
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
    Read OPF/XML metadata into an item-centered WEMI metadata container.

    OPF does not carry the full WEMI graph. If ``database`` plus ``item_id`` or
    ``source_row`` is supplied, this hydrates the WEMI slice first and overlays
    the OPF fields. Otherwise it returns a WEMI container with legacy fields and
    any explicit item id attached.
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
    """Read OPF/XML metadata as ``liuxin``, ``calibre``, or ``wemi`` metadata."""
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
    return isinstance(source, str) and source.lstrip().startswith("<")


def _coerce_opf_update_source(source: Any) -> Any:
    if _is_xml_text(source):
        return source.encode("utf-8")
    return source


def _item_id_from_inputs(*, item_id: int | None, source_row: Any) -> int | None:
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
