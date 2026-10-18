"""Expose persistence-free metadata value and OPF conversion workflows.

Database and cache hydration belongs to :mod:`LiuXin_alpha.catalog.retrieval.metadata`.
Keeping this facade free of persistence adapters lets callers parse, serialize and
transform metadata without assembling Catalog, caches or databases.
"""

from __future__ import annotations

from typing import Any, Literal

from LiuXin_alpha.metadata.identifiers import (
    validate_legacy_identifier as validate_identifier,
)

OPFMetadataKind = Literal["calibre", "liuxin", "wemi"]


def fmt_sidx(value: Any, fmt: str = "%.2f", use_roman: bool = False) -> str:
    """Format a series index without importing the broader metadata utility stack eagerly."""
    from LiuXin_alpha.metadata.utils import fmt_sidx as format_series_index

    return format_series_index(value, fmt=fmt, use_roman=use_roman)


def metadata_to_opf_bytes(metadata: Any, *, default_lang: str | None = None) -> bytes:
    """Serialize metadata to OPF bytes without writing a file."""
    return _opf_tools().metadata_to_opf_bytes(metadata, default_lang=default_lang)


def metadata_to_opf_file(
    metadata: Any,
    path: Any,
    *,
    default_lang: str | None = None,
) -> Any:
    """Serialize metadata to the supplied OPF path."""
    return _opf_tools().metadata_to_opf_file(metadata, path, default_lang=default_lang)


def update_opf_bytes(opf_source: Any, metadata: Any, **kwargs: Any) -> bytes:
    """Update an OPF payload with metadata and return bytes."""
    return _opf_tools().update_opf_bytes(opf_source, metadata, **kwargs)


def update_opf_file(
    opf_source: Any,
    metadata: Any,
    output_path: Any = None,
    **kwargs: Any,
) -> Any:
    """Update an OPF source at an explicit destination or its original path."""
    return _opf_tools().update_opf_file(
        opf_source,
        metadata,
        output_path=output_path,
        **kwargs,
    )


def calibre_metadata_from_opf(source: Any) -> Any:
    """Parse an OPF source into Calibre-shaped metadata."""
    return _opf_tools().calibre_metadata_from_opf(source)


def liuxin_metadata_from_opf(source: Any) -> Any:
    """Parse an OPF source into LiuXin metadata."""
    return _opf_tools().liuxin_metadata_from_opf(source)


def liuxin_wemi_metadata_from_opf(
    source: Any,
    *,
    item_id: int | None = None,
    source_row: Any = None,
) -> Any:
    """Parse an OPF source into a persistence-free WEMI metadata slice."""
    return _opf_tools().liuxin_wemi_metadata_from_opf(
        source,
        item_id=item_id,
        source_row=source_row,
    )


def metadata_from_opf(
    source: Any,
    *,
    kind: OPFMetadataKind | str = "liuxin",
    item_id: int | None = None,
    source_row: Any = None,
) -> Any:
    """Parse an OPF source into the requested persistence-free representation."""
    return _opf_tools().metadata_from_opf(
        source,
        kind=kind,
        item_id=item_id,
        source_row=source_row,
    )


def _opf_tools() -> Any:
    """Import the OPF adapter only when a conversion is requested."""
    import LiuXin_alpha.metadata.opf_tools as opf_tools

    return opf_tools


__all__ = [
    "OPFMetadataKind",
    "calibre_metadata_from_opf",
    "fmt_sidx",
    "liuxin_metadata_from_opf",
    "liuxin_wemi_metadata_from_opf",
    "metadata_from_opf",
    "metadata_to_opf_bytes",
    "metadata_to_opf_file",
    "update_opf_bytes",
    "update_opf_file",
    "validate_identifier",
]
