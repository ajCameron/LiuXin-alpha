"""
Expose metadata hydration, cache access, OPF conversion, and selected workflow helpers.

Concrete WEMI containers and read-source adapters are imported here. OPF-specific
tooling is resolved lazily when a conversion wrapper runs.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_metadata_top_level_facade.py
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
from LiuXin_alpha.metadata.write_workflows import (
    metadata_write_report_summary,
    normalize_metadata_write_field,
    write_wemi_metadata_values,
)


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
    Resolve a read source, hydrate eager or lazy WEMI metadata, optionally force fields, and convert to the requested kind.

    An invalid source raises before hydration; an invalid kind raises after hydration
    and forced-field work. Lazy mode chooses the hydrator before kind conversion, so
    converting to Calibre or LiuXin may materialize fields.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param item_id: Optional item identifier forwarded to the hydrator or OPF adapter.
    :param source_row: Optional source row identifying the item or WEMI slice.
    :param kind: Output selector: wemi/liuxin_wemi/liu_xin_wemi, liuxin/liu_xin, or
        calibre/caliber.
    :param source: Read selector: database/db or cache/storage_cache, ignoring
        surrounding spaces, case, and hyphen versus underscore.
    :param lazy: Whether to use the lazy WEMI hydrator before optional conversion.
    :param cache: Existing cache facade or storage plugin; None requests creation.
    :param cache_type: Cache implementation selector used only when creating a cache.
    :param cache_kwargs: Optional cache-creation keywords copied into a new dictionary;
        ignored for an existing cache.
    :param allow_database_fallback: Whether the cache read adapter may consult the
        database when needed.
    :param force_hydrate: None/False to leave deferred fields alone, True for all
        fields, or an iterable of field names.
    :return: WEMI metadata or its requested LiuXin/Calibre view; lazy mode is retained
        for WEMI output.
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
    """
    Hydrate through the facade with lazy=True and WEMI output, forwarding source and field-loading options.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param item_id: Optional item identifier forwarded to the hydrator or OPF adapter.
    :param source_row: Optional source row identifying the item or WEMI slice.
    :param source: Database/db or cache/storage_cache read selector.
    :param cache: Existing cache facade or storage plugin; None requests creation.
    :param cache_type: Cache implementation selector used only when creating a cache.
    :param cache_kwargs: Optional cache-creation keywords copied into a new dictionary;
        ignored for an existing cache.
    :param allow_database_fallback: Whether the cache read adapter may consult the
        database when needed.
    :param force_hydrate: None/False to leave deferred fields alone, True for all
        fields, or an iterable of field names.
    :return: Lazy WEMI metadata container.
    """
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
    """
    Hydrate through the facade with the cache read source selected explicitly.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param item_id: Optional item identifier forwarded to the hydrator or OPF adapter.
    :param source_row: Optional source row identifying the item or WEMI slice.
    :param kind: Output kind accepted by metadata_from_database.
    :param lazy: Whether to use the lazy hydrator before kind conversion.
    :param cache: Existing cache facade or storage plugin; None requests creation.
    :param cache_type: Cache implementation selector used only when creating a cache.
    :param cache_kwargs: Optional cache-creation keywords copied into a new dictionary;
        ignored for an existing cache.
    :param allow_database_fallback: Whether the cache read adapter may consult the
        database when needed.
    :param force_hydrate: None/False to leave deferred fields alone, True for all
        fields, or an iterable of field names.
    :return: Metadata in the requested kind, with lazy hydration when requested.
    """
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
    """
    Load the OPF adapter lazily and serialize a converted metadata copy with XML sanitization.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param default_lang: Optional default language forwarded to OPF serialization.
    :return: Serialized OPF bytes; the facade does not write a file.
    """
    return _opf_tools().metadata_to_opf_bytes(metadata, default_lang=default_lang)


def metadata_to_opf_file(
    metadata: Any,
    path: Any,
    *,
    default_lang: str | None = None,
) -> Any:
    """
    Load the OPF adapter lazily and write serialized metadata to the supplied path.

    An existing file is overwritten; parent directories are not created.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param path: Destination path accepted by pathlib.Path.
    :param default_lang: Optional default language forwarded to OPF serialization.
    :return: Path returned by the OPF adapter.
    """
    return _opf_tools().metadata_to_opf_file(metadata, path, default_lang=default_lang)


def update_opf_bytes(opf_source: Any, metadata: Any, **kwargs: Any) -> bytes:
    """
    Load the OPF adapter lazily and update an existing package payload with the supplied metadata.

    The underlying parser attempts to restore readable-stream position and does not
    close caller streams.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param opf_source: Existing OPF path, bytes-like payload, XML string, or readable
        stream; caller streams are not closed.
    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param kwargs: Keyword update options forwarded to opf_tools; unsupported names
        raise TypeError.
    :return: Updated OPF bytes; version and cover-result metadata are discarded by the
        adapter.
    """
    return _opf_tools().update_opf_bytes(opf_source, metadata, **kwargs)


def update_opf_file(opf_source: Any, metadata: Any, output_path: Any = None, **kwargs: Any) -> Any:
    """
    Load the OPF adapter lazily and write an updated package to the explicit destination or original path.

    The write is direct, not an atomic file replacement.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param opf_source: Existing OPF path, bytes-like payload, XML string, or readable
        stream; caller streams are not closed.
    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param output_path: Optional destination; omission updates the original source path.
    :param kwargs: Keyword update options forwarded to opf_tools; unsupported names
        raise TypeError.
    :return: Destination Path returned by the OPF adapter.
    """
    return _opf_tools().update_opf_file(opf_source, metadata, output_path=output_path, **kwargs)


def calibre_metadata_from_opf(source: Any) -> Any:
    """
    Load the OPF adapter lazily and parse a source into Calibre-shaped metadata.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param source: OPF path, bytes/bytearray, XML string, or readable stream; the reader
        attempts stream-position restoration without closing it.
    :return: Parsed Calibre metadata; read/parse errors propagate.
    """
    return _opf_tools().calibre_metadata_from_opf(source)


def liuxin_metadata_from_opf(source: Any) -> Any:
    """
    Load the OPF adapter lazily and parse a source into LiuXin’s Calibre-like metadata container.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param source: OPF path, bytes/bytearray, XML string, or readable stream; caller
        streams remain open.
    :return: Parsed LiuXin metadata; read/parse errors propagate.
    """
    return _opf_tools().liuxin_metadata_from_opf(source)


def liuxin_wemi_metadata_from_opf(
    source: Any,
    *,
    database: Any = None,
    item_id: int | None = None,
    source_row: Any = None,
    replace_metadata: bool = False,
) -> Any:
    """
    Load the OPF adapter lazily and overlay parsed fields onto an optional hydrated WEMI slice.

    Without a usable database/item source, construct WEMI metadata from the parsed
    legacy fields and attach an explicit item ID when available.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param source: OPF path, bytes/bytearray, XML string, or readable stream.
    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param item_id: Optional item identifier forwarded to the hydrator or OPF adapter.
    :param source_row: Optional source row identifying the item or WEMI slice.
    :param replace_metadata: Overlay policy passed to smart_update when hydrating WEMI
        from a database.
    :return: Item-centered WEMI metadata; OPF alone does not reconstruct its full graph.
    """
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
    """
    Load the OPF adapter lazily and dispatch to the requested metadata representation.

    The database and overlay arguments are used only by the WEMI branch. Unknown kinds
    raise ValueError.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param source: OPF path, bytes/bytearray, XML string, or readable stream.
    :param kind: OPF output selector, default liuxin; accepted aliases are defined by
        opf_tools.metadata_from_opf.
    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param item_id: Optional item identifier forwarded to the hydrator or OPF adapter.
    :param source_row: Optional source row identifying the item or WEMI slice.
    :param replace_metadata: Overlay policy passed to smart_update when hydrating WEMI
        from a database.
    :return: Parsed LiuXin, Calibre, or WEMI metadata.
    """
    return _opf_tools().metadata_from_opf(
        source,
        kind=kind,
        database=database,
        item_id=item_id,
        source_row=source_row,
        replace_metadata=replace_metadata,
    )


def _opf_tools() -> Any:
    """
    Import and return the OPF adapter module when a facade wrapper needs it.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :return: Cached or newly imported metadata.opf_tools module; import errors
        propagate.
    """
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
    """
    Normalize the source selector and build a direct-database or cache-backed read adapter.

    The database branch ignores cache options; unknown selectors raise ValueError before
    cache resolution.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param source: Database/db or cache/storage_cache selector, normalized by
        _normalize_option.
    :param cache: Existing cache facade or storage plugin; None requests creation.
    :param cache_type: Cache implementation selector used only when creating a cache.
    :param cache_kwargs: Optional cache-creation keywords copied into a new dictionary;
        ignored for an existing cache.
    :param allow_database_fallback: Whether the cache read adapter may consult the
        database when needed.
    :return: DatabaseMetadataReadSource or CacheMetadataReadSource retaining its
        underlying resources.
    """
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
    """
    Create a cache when absent, retain an existing CacheAPI facade, or wrap a storage plugin.

    For a supplied cache, call load only when its state equals EMPTY. Creation is
    delegated to create_cache with copied options.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_metadata_top_level_facade.py


    :param database: Database-like source retained for metadata reads; this facade does
        not close it.
    :param cache: Existing cache facade or storage plugin; None requests creation.
    :param cache_type: Cache implementation selector used only when creating a cache.
    :param cache_kwargs: Optional cache-creation keywords copied into a new dictionary;
        ignored for an existing cache.
    :return: Resolved cache facade; the helper does not close or dispose of it.
    """
    if cache is None:
        from LiuXin_alpha.caches import create_cache

        return create_cache(
            database,
            cache_type,
            **dict(cache_kwargs or {}),
        )

    from LiuXin_alpha.caches import Cache, CacheAPI, CacheState

    if isinstance(cache, CacheAPI):
        resolved_cache = cache
    else:
        resolved_cache = Cache.from_storage(cache)
    if resolved_cache.state == CacheState.EMPTY:
        resolved_cache.load()
    return resolved_cache


def _metadata_as_kind(metadata: Any, kind: MetadataObjectKind | str) -> Any:
    """
    Normalize a kind and return the WEMI object unchanged or call its LiuXin/Calibre conversion method.

    Example:
        >>> marker = object()
        >>> _metadata_as_kind(marker, ' liu-xin-wemi ') is marker
        True


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param kind: WEMI, LiuXin, or Calibre selector with accepted spelling aliases.
    :return: Original metadata for WEMI aliases, a converted object otherwise; unknown
        kinds raise ValueError.
    """
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
    """
    Skip values equal to None/False or objects without a callable force_hydrate method.

    For fields is True, call the method without arguments; otherwise forward fields as a
    keyword.

    Example:
        >>> _force_hydrate(object(), True)


    :param metadata: Metadata object to convert, serialize, or inspect, as described
        above.
    :param fields: Optional force-loading flag or field iterable.
    :return: None; may materialize deferred metadata fields, and method errors
        propagate.
    """
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
    """
    Stringify a selector, strip outer whitespace, lowercase it, and replace hyphens with underscores.

    Example:
        >>> _normalize_option(' Liu-Xin ' )
        'liu_xin'


    :param value: Object whose string representation is normalized.
    :return: Normalized selector string; internal spaces are preserved.
    """
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
    "metadata_write_report_summary",
    "normalize_metadata_write_field",
    "update_opf_bytes",
    "update_opf_file",
    "write_wemi_metadata_values",
]
