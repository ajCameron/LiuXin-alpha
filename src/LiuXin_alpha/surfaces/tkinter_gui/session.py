"""
Manage Tk application session resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise session through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

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
    """
    Own or borrow one transport-neutral Core client for the GUI.

    Example:
        Exercise TkGuiSession through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

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
        """
        Perform the open database operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.open database through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param config: Value supplied for config under the utility contract.
        :param job_manager: Value supplied for job manager under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the from core session operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.from core session through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param config: Value supplied for config under the utility contract.
        :param core_session: Value supplied for core session under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the from client operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.from client through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param client: Value supplied for client under the utility contract.
        :param config: Value supplied for config under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Compatibility composition for existing embedders and tests.

        Example:
            Exercise TkGuiSession.from database through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param database: Value supplied for database under the utility contract.
        :param config: Value supplied for config under the utility contract.
        :param job_manager: Value supplied for job manager under the utility contract.
        :param read_model: Value supplied for read model under the utility contract.
        :param metadata_read_source: Value supplied for metadata read source under the
            utility contract.
        :param read_source: Value supplied for read source under the utility contract.
        :param storage_cache: Value supplied for storage cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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
        """
        Normalize read source mode under the format's safety and compatibility rules.

        Example:
            Exercise TkGuiSession.normalize read source mode through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param mode: Open or adapter mode controlling read/write behavior.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the db operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.db through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.database

    @property
    def read_source(self) -> CoreDatabaseView:
        """
        Read source under the format's safety and compatibility rules.

        Example:
            Exercise TkGuiSession.read source through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.database

    @property
    def metadata_read_source(self) -> CoreSurfaceModel:
        """
        Perform the metadata read source operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.metadata read source through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.model

    @property
    def runtime(self) -> None:
        """
        Runtime internals are intentionally not exposed to the GUI.

        Example:
            Exercise TkGuiSession.runtime through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return None

    @property
    def closed(self) -> bool:
        """
        Perform the closed operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.closed through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._closed

    def execute_query(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Perform the execute query operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.execute query through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param payload: Value supplied for payload under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.core.query(str(name), dict(payload or {}))

    def execute_command(
        self,
        name: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Perform the execute command operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.execute command through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param payload: Value supplied for payload under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.core.command(str(name), dict(payload or {}))

    def health(self) -> dict[str, Any]:
        """
        Perform the health operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.health through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return dict(self.execute_query("health"))

    def describe_api(
        self,
        *,
        include_targets: bool = True,
        target: str | None = None,
    ) -> dict[str, Any]:
        """
        Perform the describe api operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.describe api through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param include_targets: Value supplied for include targets under the utility
            contract.
        :param target: Value supplied for target under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        payload: dict[str, Any] = {
            "include_targets": bool(include_targets)
        }
        if target is not None:
            payload["target"] = str(target)
        return dict(self.execute_query("api.describe", payload))

    def core_status_text(self) -> str:
        """
        Perform the core status text operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.core status text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            health = self.health()
        except Exception:
            return "core unavailable"
        if bool(health.get("shutdown")):
            return "core shut down"
        version = str(health.get("core_version", "") or "").strip()
        return "core {} ready".format(version) if version else "core ready"

    def read_source_status_text(self) -> str:
        """
        Read source status text under the format's safety and compatibility rules.

        Example:
            Exercise TkGuiSession.read source status text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the select read source operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.select read source through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param mode: Open or adapter mode controlling read/write behavior.
        :param cache_type: Value supplied for cache type under the utility contract.
        :param allow_database_fallback: Value supplied for allow database fallback under the
            utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the refresh read source operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.refresh read source through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Write metadata values under the format's safety and compatibility rules.

        Example:
            Exercise TkGuiSession.write metadata values through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param item_id: Value supplied for item id under the utility contract.
        :param values: Value supplied for values under the utility contract.
        :param fields: Value supplied for fields under the utility contract.
        :param kind: Value supplied for kind under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the close operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiSession.close through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._closed:
            return
        self._closed = True
        self.core_session.close()


__all__ = ["TkGuiSession"]
