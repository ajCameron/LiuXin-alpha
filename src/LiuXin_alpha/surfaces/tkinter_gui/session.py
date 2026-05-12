"""Core/session ownership for the Tkinter GUI surface."""

from __future__ import annotations

from dataclasses import dataclass, field
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
        return cls(
            config=config,
            database=database,
            library=library,
            core_runtime=runtime,
            library_proxy=library_proxy,
            database_proxy=library_proxy.database,
            jobs_proxy=library_proxy.jobs,
            read_model=read_model,
            metadata_read_source=metadata_read_source,
            read_source=read_source,
        )

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

    def refresh_read_source(self) -> bool:
        from LiuXin_alpha.surfaces.write_refresh import refresh_metadata_read_source_after_write

        return refresh_metadata_read_source_after_write(self)

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
                close_database = getattr(self.database, "close", None)
                if callable(close_database):
                    close_database()


__all__ = ["TkGuiSession"]
