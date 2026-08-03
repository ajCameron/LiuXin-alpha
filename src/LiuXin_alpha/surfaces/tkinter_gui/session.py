"""Core-client lifecycle for the Tkinter GUI surface."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from LiuXin_alpha.core import CoreClientAPI, core_client, create_core
from LiuXin_alpha.surfaces.core import (
    CoreDatabaseView,
    CoreSurfaceModel,
    SurfaceCoreSession,
)

from .state import TkGuiConfig


@dataclass
class TkGuiSession:
    """Own or borrow one transport-neutral Core client for the GUI."""

    config: TkGuiConfig
    core: CoreClientAPI
    core_session: SurfaceCoreSession
    model: CoreSurfaceModel
    database: CoreDatabaseView
    _closed: bool = field(default=False, init=False, repr=False)

    @classmethod
    def open_database(
        cls,
        config: TkGuiConfig,
        *,
        job_manager: Any | None = None,
    ) -> "TkGuiSession":
        del job_manager
        cache_type = (
            str(config.cache_type or "schema_backed")
            if cls.normalize_read_source_mode(config.read_source_mode) == "cache"
            else None
        )
        session = SurfaceCoreSession.open(
            database_path=config.database,
            endpoint=config.core_endpoint,
            db_type=str(config.db_type),
            cache_type=cache_type,
            cache_allow_database_fallback=bool(
                config.allow_cache_database_fallback
            ),
            enable_storage_manager=bool(config.enable_storage_manager),
            enable_maintenance=bool(config.enable_maintenance),
            repair_bootstrap_rows=bool(config.repair_bootstrap_rows),
            timeout_seconds=float(config.core_timeout),
        )
        return cls.from_core_session(config=config, core_session=session)

    @classmethod
    def from_core_session(
        cls,
        *,
        config: TkGuiConfig,
        core_session: SurfaceCoreSession,
    ) -> "TkGuiSession":
        model = CoreSurfaceModel(core_session.client)
        return cls(
            config=config,
            core=core_session.client,
            core_session=core_session,
            model=model,
            database=CoreDatabaseView(
                core_session.client,
                model=model,
            ),
        )

    @classmethod
    def from_client(
        cls,
        client: CoreClientAPI,
        *,
        config: TkGuiConfig,
    ) -> "TkGuiSession":
        return cls.from_core_session(
            config=config,
            core_session=SurfaceCoreSession.from_client(client),
        )

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
        """Compatibility composition for existing embedders and tests.

        The supplied database is immediately enclosed by Core; GUI code only
        receives the client and wire-shaped surface model.
        """

        del read_model, metadata_read_source, read_source
        runtime = create_core(
            database=database,
            cache=storage_cache,
            job_manager=job_manager,
            enable_storage_manager=bool(config.enable_storage_manager),
            enable_maintenance=bool(config.enable_maintenance),
            repair_bootstrap_rows=bool(config.repair_bootstrap_rows),
        )
        session = SurfaceCoreSession(
            client=core_client(runtime=runtime),
            runtime=runtime,
            owns_runtime=True,
        )
        return cls.from_core_session(config=config, core_session=session)

    @staticmethod
    def normalize_read_source_mode(mode: str | None) -> str:
        token = str(mode or "").strip().lower().replace("_", "-")
        if token in {"", "direct", "database", "db"}:
            return "direct"
        if token in {"cache", "cached", "storage-cache", "storagecache"}:
            return "cache"
        raise ValueError(
            "Unknown Tk GUI read source mode: {!r}".format(mode)
        )

    @property
    def db(self) -> CoreDatabaseView:
        return self.database

    @property
    def read_source(self) -> CoreDatabaseView:
        return self.database

    @property
    def metadata_read_source(self) -> CoreSurfaceModel:
        return self.model

    @property
    def runtime(self) -> None:
        """Runtime internals are intentionally not exposed to the GUI."""

        return None

    @property
    def closed(self) -> bool:
        return self._closed

    def execute_query(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        return self.core.query(str(name), dict(payload or {}))

    def execute_command(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        return self.core.command(str(name), dict(payload or {}))

    def health(self) -> dict[str, Any]:
        return dict(self.execute_query("health"))

    def describe_api(
        self,
        *,
        include_targets: bool = True,
        target: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "include_targets": bool(include_targets)
        }
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
        fallback = (
            " with DB fallback"
            if self.config.allow_cache_database_fallback
            else ""
        )
        return "source cache:{}{}".format(cache_type, fallback)

    def select_read_source(
        self,
        *,
        mode: str | None = None,
        cache_type: str | None = None,
        allow_database_fallback: bool | None = None,
        cache: Any | None = None,
    ) -> bool:
        del cache
        normalized = self.normalize_read_source_mode(
            mode if mode is not None else self.config.read_source_mode
        )
        resolved_cache_type = str(
            cache_type or self.config.cache_type or "schema_backed"
        )
        resolved_fallback = (
            self.config.allow_cache_database_fallback
            if allow_database_fallback is None
            else bool(allow_database_fallback)
        )
        previous = (
            self.normalize_read_source_mode(self.config.read_source_mode),
            str(self.config.cache_type or "schema_backed"),
            bool(self.config.allow_cache_database_fallback),
        )
        requested = (
            normalized,
            resolved_cache_type,
            bool(resolved_fallback),
        )
        if requested != previous:
            raise RuntimeError(
                "Changing the Core read-source composition requires reopening "
                "the database or reconfiguring the remote Core daemon."
            )
        return self.refresh_read_source()

    def refresh_read_source(self) -> bool:
        result = self.execute_command("read-source.refresh")
        self.model.invalidate_schema()
        return bool(
            result.get("refreshed", True)
            if isinstance(result, Mapping)
            else result
        )

    def write_metadata_values(
        self,
        *,
        item_id: int,
        values: Mapping[str, Any],
        fields: tuple[str, ...] | list[str] | None = None,
        kind: str = "liuxin",
        replace: bool = True,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "item_id": int(item_id),
            "values": dict(values),
            "kind": str(kind),
            "replace": bool(replace),
        }
        if fields is not None:
            payload["fields"] = [
                str(field_name)
                for field_name in fields
            ]
        result = dict(self.execute_command("metadata.write", payload) or {})
        result["read_source_refreshed"] = self.refresh_read_source()
        return result

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.core_session.close()


__all__ = ["TkGuiSession"]
