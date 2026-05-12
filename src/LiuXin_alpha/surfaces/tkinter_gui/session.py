"""Core/session ownership for the Tkinter GUI surface."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.queries import CoreQuery

from .state import TkGuiConfig


@dataclass
class TkGuiSession:
    """Own the database, library facade, core runtime, and local proxies."""

    config: TkGuiConfig
    database: Any
    library: Any
    core_runtime: Any
    library_proxy: Any
    database_proxy: Any
    jobs_proxy: Any
    storage_cache: Any | None = None
    storage_cache_type: str | None = None
    read_model: Any | None = None
    metadata_read_source: Any | None = None
    read_source: Any | None = None
    _closed: bool = field(default=False, init=False, repr=False)

    @classmethod
    def open_database(cls, config: TkGuiConfig, *, job_manager: Any | None = None) -> "TkGuiSession":
        from LiuXin_alpha.databases.database import Database

        db_path = Path(config.database).expanduser()
        db = Database(
            metadata={"database_path": str(db_path)},
            db_type=str(config.db_type),
            create=False,
            backup=False,
            enable_storage_manager=bool(config.enable_storage_manager),
            enable_maintenance=bool(config.enable_maintenance),
            repair_bootstrap_rows=bool(config.repair_bootstrap_rows),
        )
        return cls.from_database(db, config=config, job_manager=job_manager)

    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        config: TkGuiConfig,
        job_manager: Any | None = None,
        read_model: Any | None = None,
        metadata_read_source: Any | None = None,
        read_source: Any | None = None,
        storage_cache: Any | None = None,
    ) -> "TkGuiSession":
        from LiuXin_alpha.core.proxies.local import LocalLibraryProxy
        from LiuXin_alpha.core.runtime import CoreRuntime
        from LiuXin_alpha.library.library import Library
        from LiuXin_alpha.utils.jobs import default_job_manager

        library = Library(database=database, close_database_on_close=False)
        runtime = CoreRuntime(
            library=library,
            job_manager=job_manager if job_manager is not None else default_job_manager(),
        )
        library_proxy = LocalLibraryProxy(runtime)
        session = cls(
            config=config,
            database=database,
            library=library,
            core_runtime=runtime,
            library_proxy=library_proxy,
            database_proxy=library_proxy.database,
            jobs_proxy=library_proxy.jobs,
            storage_cache=storage_cache,
            read_model=read_model,
            metadata_read_source=metadata_read_source,
            read_source=read_source,
        )
        session.configure_read_source(
            mode=config.read_source_mode,
            cache=storage_cache,
            read_source=read_source or metadata_read_source,
        )
        return session

    @staticmethod
    def normalize_read_source_mode(mode: str | None) -> str:
        token = str(mode or "").strip().lower().replace("_", "-")
        if token in {"", "direct", "database", "db"}:
            return "direct"
        if token in {"cache", "cached", "storage-cache", "storagecache"}:
            return "cache"
        raise ValueError("Unknown Tk GUI read source mode: {!r}".format(mode))

    def configure_read_source(
        self,
        *,
        mode: str | None = None,
        cache: Any | None = None,
        read_source: Any | None = None,
    ) -> None:
        from LiuXin_alpha.metadata.read_sources import (
            CacheMetadataReadSource,
            DatabaseMetadataReadSource,
            metadata_read_source_from,
        )

        normalized = self.normalize_read_source_mode(
            mode if mode is not None else self.config.read_source_mode
        )
        if read_source is not None:
            resolved = metadata_read_source_from(read_source)
            self.read_source = resolved
            self.metadata_read_source = resolved
            self.storage_cache = cache if cache is not None else self.storage_cache
            if cache is not None:
                self.storage_cache_type = str(self.config.cache_type or "schema_backed")
            return

        if normalized == "direct":
            resolved = DatabaseMetadataReadSource(self.database)
            self.read_source = resolved
            self.metadata_read_source = resolved
            self.storage_cache = None
            self.storage_cache_type = None
            return

        cache_type = str(self.config.cache_type or "schema_backed")
        resolved_cache = cache if cache is not None else self.storage_cache
        if (
            cache is None
            and resolved_cache is not None
            and self.storage_cache_type is not None
            and self.storage_cache_type != cache_type
        ):
            self._close_storage_cache()
            resolved_cache = None
        if resolved_cache is None:
            from LiuXin_alpha.caches import create_storage_cache

            resolved_cache = create_storage_cache(
                self.database,
                cache_type,
            )
        read = getattr(resolved_cache, "read", None)
        is_loaded = getattr(resolved_cache, "is_loaded", True)
        is_initialized = getattr(resolved_cache, "is_initialized", True)
        if callable(read) and (is_loaded is False or is_initialized is False):
            read()
        resolved = CacheMetadataReadSource(
            resolved_cache,
            database=self.database,
            allow_database_fallback=bool(self.config.allow_cache_database_fallback),
        )
        self.storage_cache = resolved_cache
        self.storage_cache_type = cache_type
        self.read_source = resolved
        self.metadata_read_source = resolved

    def select_read_source(
        self,
        *,
        mode: str | None = None,
        cache_type: str | None = None,
        allow_database_fallback: bool | None = None,
        cache: Any | None = None,
    ) -> bool:
        normalized = self.normalize_read_source_mode(
            mode if mode is not None else self.config.read_source_mode
        )
        requested_cache_type = (
            cache_type if cache_type not in (None, "") else self.config.cache_type or "schema_backed"
        )
        resolved_cache_type = str(requested_cache_type or "schema_backed")
        resolved_fallback = (
            bool(self.config.allow_cache_database_fallback)
            if allow_database_fallback is None
            else bool(allow_database_fallback)
        )
        old_state = (
            self.normalize_read_source_mode(self.config.read_source_mode),
            str(self.config.cache_type or "schema_backed"),
            bool(self.config.allow_cache_database_fallback),
        )
        new_state = (normalized, resolved_cache_type, resolved_fallback)
        changed = old_state != new_state or self.read_source is None
        if changed and old_state[0] == "cache" and new_state[0] != "cache":
            self._close_storage_cache()
        self.config = replace(
            self.config,
            read_source_mode=normalized,
            cache_type=resolved_cache_type,
            allow_cache_database_fallback=resolved_fallback,
        )
        self.configure_read_source(mode=normalized, cache=cache)
        return changed

    @property
    def db(self) -> Any:
        return self.database

    @property
    def runtime(self) -> Any:
        return self.core_runtime

    @property
    def closed(self) -> bool:
        return self._closed

    def execute_query(self, name: str, payload: Mapping[str, Any] | None = None) -> Any:
        envelope = CoreQuery(name=str(name), payload=dict(payload or {}))
        return self.core_runtime.execute_query(envelope).result

    def execute_command(self, name: str, payload: Mapping[str, Any] | None = None) -> Any:
        envelope = CoreCommand(name=str(name), payload=dict(payload or {}))
        return self.core_runtime.execute_command(envelope).result

    def invoke_query(
        self,
        *,
        target: str,
        method: str,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        return self.core_runtime.invoke_query(
            target=target,
            method=method,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
        )

    def invoke_command(
        self,
        *,
        target: str,
        method: str,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        return self.core_runtime.invoke_command(
            target=target,
            method=method,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
        )

    def health(self) -> dict[str, Any]:
        return dict(self.execute_query("health"))

    def describe_api(self, *, include_targets: bool = True, target: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"include_targets": bool(include_targets)}
        if target is not None:
            payload["target"] = str(target)
        return dict(self.execute_query("api.describe", payload))

    def core_status_text(self) -> str:
        try:
            health = self.health()
        except Exception:
            return "core unavailable"
        if bool(health.get("shutdown")):
            return "core shut down"
        version = str(health.get("core_version", "") or "").strip()
        return "core {} ready".format(version) if version else "core ready"

    def read_source_status_text(self) -> str:
        mode = self.normalize_read_source_mode(self.config.read_source_mode)
        if mode == "direct":
            return "source direct"
        cache_type = str(self.config.cache_type or "cache")
        fallback = " with DB fallback" if self.config.allow_cache_database_fallback else ""
        return "source cache:{}{}".format(cache_type, fallback)

    def refresh_read_source(self) -> bool:
        from LiuXin_alpha.surfaces.write_refresh import refresh_metadata_read_source_after_write

        return refresh_metadata_read_source_after_write(self)

    def _close_storage_cache(self) -> None:
        close_cache = getattr(self.storage_cache, "close", None)
        if callable(close_cache):
            close_cache()
        self.storage_cache = None
        self.storage_cache_type = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            shutdown = getattr(self.core_runtime, "shutdown", None)
            if callable(shutdown):
                shutdown()
        finally:
            try:
                close_library = getattr(self.library, "close", None)
                if callable(close_library):
                    close_library()
            finally:
                try:
                    self._close_storage_cache()
                finally:
                    close_database = getattr(self.database, "close", None)
                    if callable(close_database):
                        close_database()


__all__ = ["TkGuiSession"]
