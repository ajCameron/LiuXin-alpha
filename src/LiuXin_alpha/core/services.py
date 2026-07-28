"""Service composition owned by :class:`LiuXin_alpha.core.CoreRuntime`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class CoreServiceReconciliationError(RuntimeError):
    """A canonical write committed but the configured cache did not refresh."""

    def __init__(
        self,
        message: str,
        *,
        receipt: Mapping[str, Any],
    ) -> None:
        super().__init__(message)
        self.receipt = dict(receipt)


class CoreServices:
    """Own and lazily compose Core's library, Catalog, cache, and read source."""

    def __init__(
        self,
        *,
        library: Any,
        catalog: Any | None = None,
        cache: Any | None = None,
        cache_type: str | None = None,
        cache_kwargs: Mapping[str, Any] | None = None,
        read_source: Any | None = None,
        preferences: Any | None = None,
        library_preferences: Any | None = None,
        field_metadata: Any | None = None,
        maintenance: Any | None = None,
        cache_allow_database_fallback: bool = True,
        close_cache_on_shutdown: bool | None = None,
        close_library_on_shutdown: bool = False,
    ) -> None:
        if library is None:
            raise ValueError("CoreServices requires a library.")
        database = getattr(library, "database", None)
        if database is None:
            database = getattr(library, "db", None)
        if database is None:
            raise TypeError("Core library must expose `database` or `db`.")
        if cache is not None and cache_type is not None:
            raise ValueError("Provide either `cache` or `cache_type`, not both.")

        self.library = library
        self.database = database
        self._catalog = catalog
        self._cache = cache
        self._read_source = read_source
        self._preferences = preferences
        self._library_preferences = library_preferences
        self._field_metadata = field_metadata
        self._maintenance = maintenance
        self._owns_maintenance = False
        self.cache_allow_database_fallback = bool(
            cache_allow_database_fallback
        )
        self.close_library_on_shutdown = bool(close_library_on_shutdown)
        self._closed = False

        owns_cache = False
        if cache_type is not None:
            from LiuXin_alpha.caches import create_cache

            self._cache = create_cache(
                self.database,
                str(cache_type),
                **dict(cache_kwargs or {}),
            )
            owns_cache = True
        elif self._cache is not None:
            self._prepare_supplied_cache()

        if close_cache_on_shutdown is None:
            close_cache_on_shutdown = owns_cache
        self.close_cache_on_shutdown = bool(close_cache_on_shutdown)
        self._validate_dependencies()

    @property
    def catalog(self) -> Any:
        if self._catalog is None:
            from LiuXin_alpha.catalog import Catalog

            self._catalog = Catalog(self.database)
        return self._catalog

    @property
    def cache(self) -> Any | None:
        return self._cache

    @property
    def read_source(self) -> Any:
        if self._read_source is None:
            if self.cache is None:
                from LiuXin_alpha.metadata.read_sources import (
                    DatabaseMetadataReadSource,
                )

                self._read_source = DatabaseMetadataReadSource(self.database)
            else:
                from LiuXin_alpha.metadata.read_sources import (
                    CacheMetadataReadSource,
                )

                self._read_source = CacheMetadataReadSource(
                    self.cache,
                    self.database,
                    allow_database_fallback=self.cache_allow_database_fallback,
                )
        return self._read_source

    @property
    def preferences(self) -> Any:
        """Return the process/application preference mapping."""

        if self._preferences is None:
            configured = getattr(self.database, "preferences", None)
            if configured is None:
                from LiuXin_alpha.preferences import preferences

                configured = preferences
            self._preferences = configured
        return self._preferences

    @property
    def library_preferences(self) -> Any:
        """Return the preference mapping persisted in this library database."""

        if self._library_preferences is None:
            from LiuXin_alpha.databases.dbprefs import DBPrefs

            self._library_preferences = DBPrefs(self.database)
        return self._library_preferences

    @property
    def field_metadata(self) -> Any:
        """Return display/search field metadata used by compatibility clients."""

        if self._field_metadata is None:
            from LiuXin_alpha.catalog.field_metadata import FieldMetadata

            self._field_metadata = FieldMetadata()
            custom_metadata = getattr(
                self.database,
                "custom_column_label_map",
                None,
            )
            add_custom = getattr(self._field_metadata, "add_custom_field", None)
            if isinstance(custom_metadata, Mapping) and callable(add_custom):
                for item in custom_metadata.values():
                    if isinstance(item, Mapping):
                        values = dict(item)
                        add_custom(
                            label=values["label"],
                            table=values["table"],
                            column=values["column"],
                            datatype=values["datatype"],
                            colnum=values.get("colnum", values.get("num")),
                            name=values["name"],
                            display=values.get("display", {}),
                            is_editable=values.get("is_editable", True),
                            is_multiple=values.get("is_multiple", False),
                            is_category=values.get("is_category", False),
                            is_csp=values.get("is_csp", False),
                            in_table=values.get("in_table", "books"),
                        )
        return self._field_metadata

    def refresh_field_metadata(self) -> Any:
        """Rebuild lazily-owned field metadata after custom-column changes."""

        self._field_metadata = None
        return self.field_metadata

    @property
    def maintenance(self) -> Any:
        """Return the database-bound maintenance service."""

        if self._maintenance is None:
            self._maintenance = getattr(self.database, "maintenance", None)
        if self._maintenance is None:
            from LiuXin_alpha.databases.maintenance.service import Maintainer

            self._maintenance = Maintainer(self.database)
            self._owns_maintenance = True
        return self._maintenance

    def _prepare_supplied_cache(self) -> None:
        from LiuXin_alpha.caches import Cache, CacheAPI, CacheState

        cache = self._cache
        assert cache is not None
        if not isinstance(cache, CacheAPI):
            cache = Cache.from_storage(cache)
            self._cache = cache
        state = cache.state
        if state == CacheState.EMPTY:
            cache.load()
        elif state == CacheState.DIRTY:
            cache.reload()
        elif state == CacheState.CLOSED:
            raise ValueError("Core cannot attach a closed cache.")

    def _validate_dependencies(self) -> None:
        for label, dependency in (
            ("catalog", self._catalog),
            ("cache", self._cache),
            ("read source", self._read_source),
        ):
            if dependency is None:
                continue
            attached = getattr(
                dependency,
                "database",
                (
                    getattr(dependency, "db", None)
                    if label != "read source"
                    else None
                ),
            )
            if attached is not None and attached is not self.database:
                raise ValueError(
                    "Core {} must use the Core library database.".format(label)
                )

    def describe(self) -> dict[str, Any]:
        cache = self.cache
        cache_payload: dict[str, Any] = {
            "configured": cache is not None,
        }
        if cache is not None:
            cache_payload.update(
                {
                    "state": str(cache.state),
                    "generation": int(cache.generation),
                    "consistency": str(cache.capabilities.consistency),
                    "allow_database_fallback": bool(
                        self.cache_allow_database_fallback
                    ),
                }
            )
        return {
            "library": type(self.library).__name__,
            "database": type(self.database).__name__,
            "catalog": (
                type(self._catalog).__name__
                if self._catalog is not None
                else "Catalog (lazy)"
            ),
            "cache": cache_payload,
            "read_source": (
                type(self._read_source).__name__
                if self._read_source is not None
                else (
                    "CacheMetadataReadSource (lazy)"
                    if cache is not None
                    else "DatabaseMetadataReadSource (lazy)"
                )
            ),
            "preferences": type(self.preferences).__name__,
            "library_preferences": (
                type(self._library_preferences).__name__
                if self._library_preferences is not None
                else "DBPrefs (lazy)"
            ),
            "field_metadata": (
                type(self._field_metadata).__name__
                if self._field_metadata is not None
                else "FieldMetadata (lazy)"
            ),
            "maintenance": (
                type(self._maintenance).__name__
                if self._maintenance is not None
                else (
                    type(getattr(self.database, "maintenance", None)).__name__
                    if getattr(self.database, "maintenance", None) is not None
                    else "Maintainer (lazy)"
                )
            ),
        }

    def refresh_read_source(self) -> bool:
        refresh = getattr(self.read_source, "refresh", None)
        if not callable(refresh):
            return False
        return bool(refresh())

    def reconcile(
        self,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reload the optional cache after a semantic or explicit admin write."""

        cache = self.cache
        if cache is None:
            return {
                **dict(receipt),
                "cache": {
                    "configured": False,
                    "reconciled": False,
                },
            }
        try:
            schema_changed = bool(receipt.get("schema_changed", False))
            reload_data = getattr(cache, "reload_data", None)
            if not schema_changed and callable(reload_data):
                reload_data()
            else:
                cache.reload()
        except Exception as exc:
            raise CoreServiceReconciliationError(
                "Canonical write committed but Core cache reconciliation failed.",
                receipt=receipt,
            ) from exc
        return {
            **dict(receipt),
            "cache": {
                "configured": True,
                "reconciled": True,
                "state": str(cache.state),
                "generation": int(cache.generation),
            },
        }

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.close_cache_on_shutdown and self.cache is not None:
            self.cache.close()
        if self._owns_maintenance and self._maintenance is not None:
            stop = getattr(self._maintenance, "stop", None)
            if callable(stop):
                stop()
        if self.close_library_on_shutdown:
            close = getattr(self.library, "close", None)
            if callable(close):
                close()


__all__ = [
    "CoreServiceReconciliationError",
    "CoreServices",
]
