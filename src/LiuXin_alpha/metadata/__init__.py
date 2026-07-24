"""
Public metadata facade.

The detailed abstract contracts live in :mod:`LiuXin_alpha.metadata.api`; the
concrete WEMI object model lives in :mod:`LiuXin_alpha.metadata.containers`.
This module exports the small workflow-oriented surface most callers need:
hydrate metadata from a database/cache, convert it to and from OPF, and keep a
few legacy helpers available.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Literal

from LiuXin_alpha.metadata.containers import (
    LazyLiuXinWEMI,
    LazyLiuXinWEMIMetadata,
    LazyLiuXinWEMIMetadataHydrator,
    LiuXinWEMI,
    LiuXinWEMIMetadata,
    LiuXinWEMIMetadataHydrator,
    LiuXinWEMIMetadataWriteReport,
    LiuXinWEMIMetadataWriter,
)
from LiuXin_alpha.metadata.read_sources import (
    CacheMetadataReadSource,
    DatabaseMetadataReadSource,
    metadata_read_source_from,
)
from LiuXin_alpha.metadata.utils import fmt_sidx


MetadataDatabaseSource = Literal["database", "cache"]
MetadataObjectKind = Literal["wemi", "liuxin_wemi", "liuxin", "calibre"]
OPFMetadataKind = Literal["calibre", "liuxin", "wemi"]
ForceHydrateFields = Iterable[str] | bool | None


def metadata_from_database(
    database: Any,
    *,
    item_id: int | None = None,
    source_row: Any = None,
    kind: MetadataObjectKind | str = "wemi",
    source: MetadataDatabaseSource | str = "database",
    lazy: bool = False,
    cache: Any = None,
    cache_type: str = "schema_backed",
    cache_kwargs: Mapping[str, Any] | None = None,
    allow_database_fallback: bool = True,
    force_hydrate: ForceHydrateFields = None,
) -> Any:
    """
    Hydrate metadata from a database, or from an explicit storage cache.

    ``source`` is deliberately explicit:
    - ``"database"`` reads directly from the database.
    - ``"cache"`` reads through a storage cache, creating and loading one when
      ``cache`` is not supplied.

    ``lazy=True`` returns the lazy WEMI container before any ``kind`` coercion.
    Requesting ``kind="calibre"`` still returns a Calibre-shaped object, which
    naturally materializes any fields needed for that conversion.
    """
    read_source = _database_read_source(
        database,
        source=source,
        cache=cache,
        cache_type=cache_type,
        cache_kwargs=cache_kwargs,
        allow_database_fallback=allow_database_fallback,
    )
    hydrator = (
        LazyLiuXinWEMIMetadataHydrator(read_source)
        if lazy
        else LiuXinWEMIMetadataHydrator(read_source)
    )
    metadata = hydrator.get_liuxin_wemi_metadata(
        item_id=item_id,
        source_row=source_row,
    )
    _force_hydrate(metadata, force_hydrate)
    return _metadata_as_kind(metadata, kind)


def lazy_metadata_from_database(
    database: Any,
    *,
    item_id: int | None = None,
    source_row: Any = None,
    source: MetadataDatabaseSource | str = "database",
    cache: Any = None,
    cache_type: str = "schema_backed",
    cache_kwargs: Mapping[str, Any] | None = None,
    allow_database_fallback: bool = True,
    force_hydrate: ForceHydrateFields = None,
) -> LazyLiuXinWEMIMetadata:
    """Hydrate a lazy LiuXin/WEMI metadata container from a database or cache."""
    return metadata_from_database(
        database,
        item_id=item_id,
        source_row=source_row,
        kind="wemi",
        source=source,
        lazy=True,
        cache=cache,
        cache_type=cache_type,
        cache_kwargs=cache_kwargs,
        allow_database_fallback=allow_database_fallback,
        force_hydrate=force_hydrate,
    )


def cache_metadata_from_database(
    database: Any,
    *,
    item_id: int | None = None,
    source_row: Any = None,
    kind: MetadataObjectKind | str = "wemi",
    lazy: bool = False,
    cache: Any = None,
    cache_type: str = "schema_backed",
    cache_kwargs: Mapping[str, Any] | None = None,
    allow_database_fallback: bool = True,
    force_hydrate: ForceHydrateFields = None,
) -> Any:
    """Hydrate metadata through a storage cache instead of direct DB reads."""
    return metadata_from_database(
        database,
        item_id=item_id,
        source_row=source_row,
        kind=kind,
        source="cache",
        lazy=lazy,
        cache=cache,
        cache_type=cache_type,
        cache_kwargs=cache_kwargs,
        allow_database_fallback=allow_database_fallback,
        force_hydrate=force_hydrate,
    )


def metadata_to_opf_bytes(metadata: Any, *, default_lang: str | None = None) -> bytes:
    """Serialize a metadata object to OPF bytes without importing OPF tooling at facade import time."""
    return _opf_tools().metadata_to_opf_bytes(metadata, default_lang=default_lang)


def metadata_to_opf_file(
    metadata: Any,
    path: Any,
    *,
    default_lang: str | None = None,
) -> Any:
    """Serialize a metadata object to an OPF file."""
    return _opf_tools().metadata_to_opf_file(metadata, path, default_lang=default_lang)


def update_opf_bytes(opf_source: Any, metadata: Any, **kwargs: Any) -> bytes:
    """Update an OPF payload with metadata while preserving the package structure."""
    return _opf_tools().update_opf_bytes(opf_source, metadata, **kwargs)


def update_opf_file(opf_source: Any, metadata: Any, output_path: Any = None, **kwargs: Any) -> Any:
    """Update an OPF file with metadata."""
    return _opf_tools().update_opf_file(opf_source, metadata, output_path=output_path, **kwargs)


def calibre_metadata_from_opf(source: Any) -> Any:
    """Read OPF/XML metadata into a Calibre-shaped metadata object."""
    return _opf_tools().calibre_metadata_from_opf(source)


def liuxin_metadata_from_opf(source: Any) -> Any:
    """Read OPF/XML metadata into a LiuXin metadata object."""
    return _opf_tools().liuxin_metadata_from_opf(source)


def liuxin_wemi_metadata_from_opf(
    source: Any,
    *,
    database: Any = None,
    item_id: int | None = None,
    source_row: Any = None,
    replace_metadata: bool = False,
) -> Any:
    """Read OPF/XML metadata into an item-centred WEMI metadata object."""
    return _opf_tools().liuxin_wemi_metadata_from_opf(
        source,
        database=database,
        item_id=item_id,
        source_row=source_row,
        replace_metadata=replace_metadata,
    )


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
    return _opf_tools().metadata_from_opf(
        source,
        kind=kind,
        database=database,
        item_id=item_id,
        source_row=source_row,
        replace_metadata=replace_metadata,
    )


def _opf_tools() -> Any:
    from LiuXin_alpha.metadata import opf_tools

    return opf_tools


def _database_read_source(
    database: Any,
    *,
    source: MetadataDatabaseSource | str,
    cache: Any,
    cache_type: str,
    cache_kwargs: Mapping[str, Any] | None,
    allow_database_fallback: bool,
) -> Any:
    normalized = _normalize_option(source)
    if normalized in {"database", "db"}:
        return DatabaseMetadataReadSource(database)
    if normalized not in {"cache", "storage_cache"}:
        raise ValueError(
            "Unknown metadata database source {!r}. Expected 'database' or 'cache'.".format(
                source,
            )
        )

    resolved_cache = _loaded_cache(
        database,
        cache=cache,
        cache_type=cache_type,
        cache_kwargs=cache_kwargs,
    )
    return CacheMetadataReadSource(
        resolved_cache,
        database=database,
        allow_database_fallback=allow_database_fallback,
    )


def _loaded_cache(
    database: Any,
    *,
    cache: Any,
    cache_type: str,
    cache_kwargs: Mapping[str, Any] | None,
) -> Any:
    if cache is None:
        from LiuXin_alpha.caches import create_cache

        return create_cache(
            database,
            cache_type,
            **dict(cache_kwargs or {}),
        )

    from LiuXin_alpha.caches import Cache, CacheAPI

    if isinstance(cache, CacheAPI):
        return cache
    return Cache.from_storage(cache)


def _metadata_as_kind(metadata: Any, kind: MetadataObjectKind | str) -> Any:
    normalized = _normalize_option(kind)
    if normalized in {"wemi", "liuxin_wemi", "liu_xin_wemi"}:
        return metadata
    if normalized in {"liuxin", "liu_xin"}:
        return metadata.as_liuxin_metadata()
    if normalized in {"calibre", "caliber"}:
        return metadata.as_calibre_metadata()
    raise ValueError(
        "Unknown metadata kind {!r}. Expected 'wemi', 'liuxin', or 'calibre'.".format(
            kind,
        )
    )


def _force_hydrate(metadata: Any, fields: ForceHydrateFields) -> None:
    if fields in (None, False):
        return
    force_hydrate_method = getattr(metadata, "force_hydrate", None)
    if not callable(force_hydrate_method):
        return
    if fields is True:
        force_hydrate_method()
    else:
        force_hydrate_method(fields=fields)


def _normalize_option(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_")


__all__ = [
    "CacheMetadataReadSource",
    "DatabaseMetadataReadSource",
    "ForceHydrateFields",
    "LazyLiuXinWEMI",
    "LazyLiuXinWEMIMetadata",
    "LazyLiuXinWEMIMetadataHydrator",
    "LiuXinWEMI",
    "LiuXinWEMIMetadata",
    "LiuXinWEMIMetadataHydrator",
    "LiuXinWEMIMetadataWriteReport",
    "LiuXinWEMIMetadataWriter",
    "MetadataDatabaseSource",
    "MetadataObjectKind",
    "OPFMetadataKind",
    "cache_metadata_from_database",
    "calibre_metadata_from_opf",
    "fmt_sidx",
    "lazy_metadata_from_database",
    "liuxin_metadata_from_opf",
    "liuxin_wemi_metadata_from_opf",
    "metadata_from_database",
    "metadata_from_opf",
    "metadata_read_source_from",
    "metadata_to_opf_bytes",
    "metadata_to_opf_file",
    "update_opf_bytes",
    "update_opf_file",
]
