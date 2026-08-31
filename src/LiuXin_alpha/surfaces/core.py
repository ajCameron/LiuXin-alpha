"""Transport-neutral Core access shared by application interfaces.

Surface code deliberately works with wire-shaped records and stable named Core
operations.  Local composition is confined to :class:`SurfaceCoreSession`;
the same model works unchanged with an HTTP ``RemoteCoreClient``.
"""

from __future__ import annotations

import argparse
import base64

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from LiuXin_alpha.core import CoreClientAPI, core_client, create_core


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("{} must be a mapping.".format(label))
    return {str(key): item for key, item in value.items()}


@dataclass(frozen=True)
class CoreRow(Mapping[str, Any]):
    """A presentation-safe row DTO returned by Core."""

    table: str
    row_id: int | None
    values: Mapping[str, Any]
    linkable_tables: tuple[str, ...] = ()

    @property
    def row_dict(self) -> dict[str, Any]:
        return dict(self.values)

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


@dataclass(frozen=True)
class CoreRowPage:
    records: tuple[CoreRow, ...]
    total_count: int
    offset: int
    limit: int | None
    complete: bool
    source: str


@dataclass
class SurfaceCoreSession:
    """Own or borrow one Core client for an application interface."""

    client: CoreClientAPI
    runtime: Any | None = None
    owns_runtime: bool = False
    _closed: bool = field(default=False, init=False, repr=False)

    @classmethod
    def open(
        cls,
        *,
        database_path: str | Path | None = None,
        endpoint: str | None = None,
        db_type: str = "SQLite",
        database_metadata: Mapping[str, Any] | None = None,
        create: bool = False,
        backup: bool = False,
        cache_type: str | None = None,
        cache_allow_database_fallback: bool = True,
        enable_storage_manager: bool = True,
        strict_storage_manager_bootstrap: bool = False,
        storage_startup_on_add: bool = False,
        enable_maintenance: bool = False,
        repair_bootstrap_rows: bool = False,
        timeout_seconds: float = 10.0,
    ) -> "SurfaceCoreSession":
        if (database_path is None) == (endpoint is None):
            raise ValueError(
                "Provide exactly one of `database_path` or `endpoint`."
            )
        if endpoint is not None:
            return cls(
                client=core_client(
                    endpoint=str(endpoint),
                    timeout_seconds=float(timeout_seconds),
                )
            )

        assert database_path is not None

        # The application boundary owns composition. Surface modules never
        # construct Database, Library, Catalog, Cache, or StorageManager.
        server_database = str(db_type).strip().casefold() in {
            "postgres",
            "postgresql",
            "pg",
        }
        runtime = create_core(
            database_path=(
                str(database_path)
                if server_database
                else Path(database_path).expanduser()
            ),
            db_type=str(db_type),
            database_metadata=database_metadata,
            create=bool(create),
            backup=bool(backup),
            cache_type=cache_type,
            cache_allow_database_fallback=bool(
                cache_allow_database_fallback
            ),
            enable_storage_manager=bool(enable_storage_manager),
            strict_storage_manager_bootstrap=bool(
                strict_storage_manager_bootstrap
            ),
            storage_startup_on_add=bool(storage_startup_on_add),
            enable_maintenance=bool(enable_maintenance),
            repair_bootstrap_rows=bool(repair_bootstrap_rows),
        )
        return cls(
            client=core_client(runtime=runtime),
            runtime=runtime,
            owns_runtime=True,
        )

    @classmethod
    def from_client(cls, client: CoreClientAPI) -> "SurfaceCoreSession":
        return cls(client=client)

    @classmethod
    def enclose_legacy_database(
        cls,
        database: Any,
        *,
        job_manager: Any | None = None,
        read_source: Any | None = None,
        cache_type: str | None = None,
        cache_allow_database_fallback: bool = True,
    ) -> "SurfaceCoreSession":
        """Enclose a pre-Core database at the composition boundary.

        This compatibility seam is intentionally centralized here. Application
        interfaces still receive and retain only the Core client.
        """

        runtime = create_core(
            database=database,
            job_manager=job_manager,
            close_job_manager_on_shutdown=False,
            read_source=read_source,
            cache_type=cache_type,
            cache_allow_database_fallback=cache_allow_database_fallback,
            enable_maintenance=False,
            repair_bootstrap_rows=False,
        )
        return cls(
            client=core_client(runtime=runtime),
            runtime=runtime,
            owns_runtime=True,
        )

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.owns_runtime and self.runtime is not None:
            self.runtime.shutdown()

    def __enter__(self) -> "SurfaceCoreSession":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        self.close()


def coerce_surface_core(
    value: Any,
    *,
    job_manager: Any | None = None,
    read_source: Any | None = None,
    cache_type: str | None = None,
    cache_allow_database_fallback: bool = True,
) -> tuple[CoreClientAPI, SurfaceCoreSession | None]:
    """Return a Core client, enclosing legacy database inputs if necessary."""

    if isinstance(value, CoreClientAPI):
        return value, None
    session = SurfaceCoreSession.enclose_legacy_database(
        value,
        job_manager=job_manager,
        read_source=read_source,
        cache_type=cache_type,
        cache_allow_database_fallback=cache_allow_database_fallback,
    )
    return session.client, session


class CoreSurfaceModel:
    """Convenience model made exclusively from stable Core operations."""

    def __init__(self, client: CoreClientAPI) -> None:
        self.client = client
        self._schema: dict[str, dict[str, Any]] | None = None

    @property
    def core(self) -> CoreClientAPI:
        return self.client

    def invalidate_schema(self) -> None:
        self._schema = None

    def _schema_map(self) -> dict[str, dict[str, Any]]:
        if self._schema is None:
            result = _mapping(
                self.client.query("schema.tables"),
                label="schema.tables result",
            )
            raw_tables = result.get("tables", ())
            if not isinstance(raw_tables, Sequence) or isinstance(
                raw_tables,
                (str, bytes),
            ):
                raise TypeError("schema.tables `tables` must be an array.")
            schemas: dict[str, dict[str, Any]] = {}
            for raw in raw_tables:
                schema = _mapping(raw, label="table schema")
                name = str(schema.get("name") or "")
                if name:
                    schemas[name] = schema
            self._schema = schemas
        return self._schema

    def table_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._schema_map()))

    def table_schema(self, table: str) -> dict[str, Any]:
        name = str(table)
        cached = self._schema_map().get(name)
        if cached is not None and bool(
            cached.get("relations_included", False)
        ):
            return dict(cached)
        result = _mapping(
            self.client.query("schema.table", {"table": name}),
            label="schema.table result",
        )
        self._schema_map()[name] = result
        return dict(result)

    def _table_summary(self, table: str) -> dict[str, Any]:
        name = str(table)
        cached = self._schema_map().get(name)
        if cached is not None:
            return cached
        return self.table_schema(name)

    def table_exists(self, table: str) -> bool:
        return str(table) in self._schema_map()

    def tables_and_columns(self) -> dict[str, tuple[str, ...]]:
        return {
            table: tuple(str(value) for value in schema.get("columns", ()))
            for table, schema in self._schema_map().items()
        }

    def columns(self, table: str) -> tuple[str, ...]:
        return tuple(
            str(value)
            for value in self._table_summary(str(table)).get("columns", ())
        )

    def id_column(self, table: str) -> str:
        schema = self._table_summary(str(table))
        value = schema.get("id_column")
        if value not in (None, ""):
            return str(value)
        columns = self.columns(str(table))
        return next(
            (
                column
                for column in columns
                if column == "id" or column.endswith("_id")
            ),
            columns[0] if columns else "id",
        )

    def is_view(self, table: str) -> bool:
        return bool(
            self._table_summary(str(table)).get("is_view", False)
        )

    def related_tables(self, table: str) -> tuple[str, ...]:
        return tuple(
            str(value)
            for value in self.table_schema(str(table)).get(
                "related_tables",
                (),
            )
        )

    def _row(self, record: Any) -> CoreRow:
        raw = _mapping(record, label="Core row record")
        table = str(raw.get("table") or "")
        raw_id = raw.get("row_id")
        row_id = None if raw_id is None else int(raw_id)
        values = _mapping(raw.get("values", {}), label="Core row values")
        return CoreRow(
            table=table,
            row_id=row_id,
            values=values,
            linkable_tables=(
                self.related_tables(table)
                if table and self.table_exists(table)
                else ()
            ),
        )

    def row_from_record(self, record: Any) -> CoreRow:
        """Convert a wire record returned by any named Core operation."""

        return self._row(record)

    def row(self, table: str, row_id: int) -> CoreRow | None:
        result = _mapping(
            self.client.query(
                "rows.get",
                {"table": str(table), "row_id": int(row_id)},
            ),
            label="rows.get result",
        )
        record = result.get("record")
        return None if record is None else self._row(record)

    def query_rows(
        self,
        table: str,
        *,
        predicates: Sequence[Mapping[str, Any]] = (),
        relation: Mapping[str, Any] | None = None,
        text: str = "",
        text_fields: Sequence[str] = (),
        sort: Sequence[Mapping[str, Any] | str] = (),
        projection: Sequence[str] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> CoreRowPage:
        payload: dict[str, Any] = {
            "table": str(table),
            "predicates": [dict(value) for value in predicates],
            "text": str(text),
            "text_fields": [str(value) for value in text_fields],
            "sort": [
                dict(value) if isinstance(value, Mapping) else str(value)
                for value in sort
            ],
            "projection": [str(value) for value in projection],
            "offset": max(0, int(offset)),
            "limit": None if limit is None else max(0, int(limit)),
        }
        if relation is not None:
            payload["relation"] = dict(relation)
        result = _mapping(
            self.client.query("rows.query", payload),
            label="rows.query result",
        )
        records = result.get("records", ())
        if not isinstance(records, Sequence) or isinstance(
            records,
            (str, bytes),
        ):
            raise TypeError("rows.query `records` must be an array.")
        return CoreRowPage(
            records=tuple(self._row(record) for record in records),
            total_count=int(result.get("total_count") or 0),
            offset=int(result.get("offset") or 0),
            limit=(
                None
                if result.get("limit") is None
                else int(result["limit"])
            ),
            complete=bool(result.get("complete", False)),
            source=str(result.get("source") or ""),
        )

    def rows(self, table: str) -> tuple[CoreRow, ...]:
        return self.query_rows(str(table)).records

    def record_count(self, table: str) -> int:
        return self.query_rows(str(table), limit=0).total_count

    def search(
        self,
        table: str,
        column: str,
        value: Any,
        *,
        contains: bool = False,
    ) -> tuple[CoreRow, ...]:
        operator = "contains" if contains else "eq"
        return self.query_rows(
            str(table),
            predicates=(
                {
                    "field": str(column),
                    "operator": operator,
                    "value": value,
                },
            ),
        ).records

    def related(
        self,
        row: CoreRow,
        related_table: str,
        *,
        type_filter: str | None = None,
        include_link_rows: bool = False,
    ) -> tuple[tuple[CoreRow, ...], tuple[CoreRow, ...]]:
        if row.row_id is None:
            return (), ()
        payload: dict[str, Any] = {
            "table": row.table,
            "row_id": row.row_id,
            "related_table": str(related_table),
            "include_link_rows": bool(include_link_rows),
        }
        if type_filter is not None:
            payload["type_filter"] = str(type_filter)
        result = _mapping(
            self.client.query("relations.list", payload),
            label="relations.list result",
        )
        records = result.get("records", ())
        links = result.get("link_records", ())
        return (
            tuple(self._row(record) for record in records),
            tuple(self._row(record) for record in links),
        )

    def link_capabilities(
        self,
        table: str,
        related_table: str,
    ) -> dict[str, Any] | None:
        result = _mapping(
            self.client.query(
                "schema.link",
                {
                    "table": str(table),
                    "related_table": str(related_table),
                },
            ),
            label="schema.link result",
        )
        value = result.get("capabilities")
        return None if value is None else _mapping(
            value,
            label="schema.link capabilities",
        )

    def global_search(
        self,
        text: str,
        *,
        tables: Sequence[str] = (),
        offset: int = 0,
        limit: int = 1000,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "text": str(text),
            "offset": max(0, int(offset)),
            "limit": max(0, int(limit)),
        }
        if tables:
            payload["tables"] = [str(table) for table in tables]
        return _mapping(
            self.client.query("search.global", payload),
            label="search.global result",
        )

    def refresh(self) -> bool:
        result = self.client.command("read-source.refresh")
        if isinstance(result, Mapping):
            return bool(
                result.get("refreshed", result.get("reloaded", True))
            )
        return bool(result)

    def create_row(self, table: str, values: Mapping[str, Any]) -> dict[str, Any]:
        return _mapping(
            self.client.command(
                "admin.row.create",
                {"table": str(table), "values": dict(values)},
            ),
            label="admin.row.create result",
        )

    def update_row(
        self,
        table: str,
        row_id: int,
        values: Mapping[str, Any],
    ) -> dict[str, Any]:
        return _mapping(
            self.client.command(
                "admin.row.update",
                {
                    "table": str(table),
                    "row_id": int(row_id),
                    "updates": dict(values),
                },
            ),
            label="admin.row.update result",
        )

    def delete_row(self, table: str, row_id: int) -> dict[str, Any]:
        return _mapping(
            self.client.command(
                "admin.row.delete",
                {"table": str(table), "row_id": int(row_id)},
            ),
            label="admin.row.delete result",
        )

    def delete_impact(
        self,
        table: str,
        row_id: int,
        *,
        sample_limit: int = 3,
    ) -> dict[str, Any]:
        return _mapping(
            self.client.query(
                "admin.row.delete-impact",
                {
                    "table": str(table),
                    "row_id": int(row_id),
                    "sample_limit": max(0, int(sample_limit)),
                },
            ),
            label="admin.row.delete-impact result",
        )

    def link(
        self,
        primary: CoreRow,
        secondary: CoreRow,
        *,
        priority: int | None = None,
        link_type: str | None = None,
        values: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if primary.row_id is None or secondary.row_id is None:
            raise ValueError("Both rows must have identifiers.")
        payload: dict[str, Any] = {
            "table": primary.table,
            "row_id": primary.row_id,
            "related_table": secondary.table,
            "related_row_id": secondary.row_id,
            "extra": dict(values or {}),
        }
        if priority is not None:
            payload["priority"] = int(priority)
        if link_type is not None:
            payload["type"] = str(link_type)
        return _mapping(
            self.client.command("admin.relation.link", payload),
            label="admin.relation.link result",
        )

    def unlink(
        self,
        primary: CoreRow,
        secondary: CoreRow,
        *,
        link_type: str | None = None,
    ) -> dict[str, Any]:
        if primary.row_id is None or secondary.row_id is None:
            raise ValueError("Both rows must have identifiers.")
        payload: dict[str, Any] = {
            "table": primary.table,
            "row_id": primary.row_id,
            "related_table": secondary.table,
            "related_row_id": secondary.row_id,
        }
        if link_type is not None:
            payload["type"] = str(link_type)
        return _mapping(
            self.client.command("admin.relation.unlink", payload),
            label="admin.relation.unlink result",
        )

    def acquisition_resolve(self, kind: str, resource_id: int) -> dict[str, Any]:
        return _mapping(
            self.client.query(
                "acquisition.resolve",
                {"kind": str(kind), "id": int(resource_id)},
            ),
            label="acquisition.resolve result",
        )

    def acquisition_read(self, kind: str, resource_id: int) -> tuple[dict[str, Any], bytes]:
        result = _mapping(
            self.client.query(
                "acquisition.read",
                {"kind": str(kind), "id": int(resource_id)},
            ),
            label="acquisition.read result",
        )
        resource = _mapping(
            result.get("resource", {}),
            label="acquisition resource",
        )
        content = result.get("content")
        if isinstance(content, bytes):
            payload = content
        elif isinstance(content, Mapping) and content.get("$type") == "bytes":
            payload = base64.b64decode(str(content.get("base64") or ""))
        else:
            raise TypeError("Core acquisition content is not bytes.")
        return resource, payload


class CoreDriverView:
    """Database-driver-shaped schema view implemented through Core.

    This exists to keep mature presentation code small while it moves away
    from driver vocabulary.  It deliberately exposes no connection, cursor,
    transaction, or underlying driver object.
    """

    def __init__(self, model: CoreSurfaceModel) -> None:
        self.model = model

    def get_id_column(self, table: str) -> str:
        return self.model.id_column(table)

    def get_tables(self, get_views: bool = True) -> list[str]:
        names = self.model.table_names()
        if get_views:
            return list(names)
        return [name for name in names if not self.model.is_view(name)]

    def get_column_headings(self, table: str) -> list[str]:
        return list(self.model.columns(table))

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = False,
    ) -> list[CoreRow] | Iterator[CoreRow]:
        rows = list(self.model.rows(table))
        return iter(rows) if iterator_return else rows

    def is_view(self, table: str) -> bool:
        return self.model.is_view(table)

    def get_interlinked_tables(self, table: str) -> list[str]:
        return list(self.model.related_tables(table))

    def get_link_capabilities(
        self,
        table: str,
        related_table: str,
    ) -> dict[str, Any] | None:
        return self.model.link_capabilities(table, related_table)

    def get_link_table_name(
        self,
        table: str,
        related_table: str,
    ) -> str | None:
        capabilities = self.get_link_capabilities(table, related_table)
        if not capabilities:
            return None
        value = capabilities.get("link_table")
        return None if value in (None, "") else str(value)

    def get_link_column(
        self,
        table: str,
        related_table: str,
        column: str,
    ) -> str:
        capabilities = self.get_link_capabilities(table, related_table)
        if not capabilities:
            raise ValueError(
                "No relation exists between {!r} and {!r}.".format(
                    table,
                    related_table,
                )
            )
        requested = str(column)
        if requested == "type" and capabilities.get("type_column"):
            return str(capabilities["type_column"])
        if requested == "priority" and capabilities.get("priority_column"):
            return str(capabilities["priority_column"])
        link_table = str(capabilities["link_table"])
        columns = self.model.columns(link_table)
        if requested in columns:
            return requested
        candidates = [
            value
            for value in columns
            if value.endswith("_{}".format(requested))
            or (
                requested.endswith("_id")
                and value.endswith(requested)
            )
        ]
        if len(candidates) == 1:
            return candidates[0]
        raise ValueError(
            "Cannot identify link column {!r} in {!r}.".format(
                requested,
                link_table,
            )
        )

    def get_column_base(self, table: str) -> str:
        columns = self.model.columns(table)
        for suffix in ("_priority", "_type"):
            for column in columns:
                if column.endswith(suffix):
                    return column[: -len(suffix)]
        return str(table).removesuffix("_links").removesuffix("_link")

    def get_datestamp_column(self, table: str) -> str | None:
        for column in self.model.columns(table):
            lowered = column.lower()
            if "datestamp" in lowered or lowered.endswith(
                ("_timestamp_ep_k", "_modified")
            ):
                return column
        return None

    def get_blank_row(self, table: str) -> dict[str, Any]:
        return {column: None for column in self.model.columns(table)}

    def identify_table_from_column(
        self,
        column: str,
        error: bool = True,
    ) -> str | None:
        requested = str(column)
        candidates = [
            table
            for table in self.model.table_names()
            if self.model.id_column(table) == requested
        ]
        if len(candidates) == 1:
            return candidates[0]
        if error:
            raise ValueError(
                "Cannot identify one table from column {!r}.".format(
                    requested
                )
            )
        return None


class CoreDatabaseView:
    """Read-oriented legacy vocabulary over a :class:`CoreClientAPI`.

    No interface using this view can reach Core's Database or driver.  The
    methods translate to the stable schema, row, relation, and administrative
    operations and return :class:`CoreRow` DTOs.
    """

    def __init__(
        self,
        client: CoreClientAPI,
        *,
        model: CoreSurfaceModel | None = None,
    ) -> None:
        self.core = client
        self.model = model or CoreSurfaceModel(client)
        self.driver_wrapper = CoreDriverView(self.model)
        self._metadata: dict[str, Any] | None = None

    @property
    def metadata(self) -> dict[str, Any]:
        if self._metadata is None:
            result = _mapping(
                self.core.query("database.info"),
                label="database.info result",
            )
            self._metadata = _mapping(
                result.get("metadata", {}),
                label="database metadata",
            )
        return dict(self._metadata)

    @property
    def type(self) -> str:
        result = _mapping(
            self.core.query("database.info"),
            label="database.info result",
        )
        return str(
            result.get("database_type")
            or result.get("type")
            or "SQLite"
        )

    def get_tables(self, get_views: bool = True) -> list[str]:
        return self.driver_wrapper.get_tables(get_views)

    def get_tables_and_columns(self) -> dict[str, list[str]]:
        return {
            table: list(columns)
            for table, columns in self.model.tables_and_columns().items()
        }

    def get_column_headings(self, table: str) -> list[str]:
        return list(self.model.columns(table))

    def get_record_count(self, table: str) -> int:
        return self.model.record_count(table)

    def get_row_from_id(self, table: str, row_id: int) -> CoreRow | None:
        return self.model.row(table, row_id)

    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
    ) -> list[CoreRow] | Iterator[CoreRow]:
        rows = list(self.model.rows(table))
        return iter(rows) if iterator_return else rows

    def search(
        self,
        table: str,
        column: str,
        value: Any,
    ) -> list[CoreRow]:
        return list(self.model.search(table, column, value))

    @staticmethod
    def _source_row(
        *,
        primary_row: CoreRow | None = None,
        target_row: CoreRow | None = None,
    ) -> CoreRow:
        row = primary_row or target_row
        if row is None:
            raise ValueError("A source row is required.")
        return row

    def get_interlinked_rows(
        self,
        *,
        primary_row: CoreRow | None = None,
        target_row: CoreRow | None = None,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> list[CoreRow]:
        row = self._source_row(
            primary_row=primary_row,
            target_row=target_row,
        )
        records, _links = self.model.related(
            row,
            secondary_table,
            type_filter=type_filter,
        )
        return list(records)

    def get_interlink_rows(
        self,
        *,
        primary_row: CoreRow | None = None,
        target_row: CoreRow | None = None,
        secondary_table: str,
        type_filter: str | None = None,
    ) -> list[CoreRow]:
        row = self._source_row(
            primary_row=primary_row,
            target_row=target_row,
        )
        _records, links = self.model.related(
            row,
            secondary_table,
            type_filter=type_filter,
            include_link_rows=True,
        )
        return list(links)

    def get_interlink_row(
        self,
        *,
        primary_row: CoreRow,
        secondary_row: CoreRow,
        onelink: bool = True,
    ) -> CoreRow | list[CoreRow] | None:
        records, links = self.model.related(
            primary_row,
            secondary_row.table,
            include_link_rows=True,
        )
        matching = [
            link
            for record, link in zip(records, links)
            if record.row_id == secondary_row.row_id
        ]
        if onelink:
            return matching[0] if matching else None
        return matching

    def interlink_rows(
        self,
        *,
        primary_row: CoreRow,
        secondary_row: CoreRow,
        priority: int | str | None = None,
        type: str | None = None,
        **extra: Any,
    ) -> CoreRow | None:
        normalized_priority = (
            None
            if priority in (None, "", "not_set", "highest")
            else int(priority)
        )
        self.model.link(
            primary_row,
            secondary_row,
            priority=normalized_priority,
            link_type=type,
            values=extra,
        )
        candidate = self.get_interlink_row(
            primary_row=primary_row,
            secondary_row=secondary_row,
        )
        return candidate if isinstance(candidate, CoreRow) else None

    def unlink_interlink(
        self,
        primary_row: CoreRow,
        secondary_row: CoreRow,
    ) -> bool:
        self.model.unlink(primary_row, secondary_row)
        return True

    def delete(self, row: CoreRow) -> bool:
        if row.row_id is None:
            raise ValueError("Cannot delete a row without an identifier.")
        self.model.delete_row(row.table, row.row_id)
        return True


def add_core_client_arguments(
    parser: argparse.ArgumentParser,
    *,
    database_help: str = "Path to the LiuXin database.",
) -> argparse.ArgumentParser:
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--database", help=database_help)
    group.add_argument(
        "--core-endpoint",
        help="HTTP endpoint of an existing LiuXin Core daemon.",
    )
    group.add_argument(
        "--system-root",
        help="Read the Core connection from SYSTEM_ROOT/liuxin-system.json.",
    )
    group.add_argument(
        "--profile",
        help=(
            "Named profile, manifest path, or directory containing "
            "liuxin-system.json."
        ),
    )
    parser.add_argument(
        "--core-timeout",
        type=float,
        default=10.0,
        help="Remote Core request timeout in seconds. Default: 10",
    )
    return parser


def open_surface_core_from_args(
    args: argparse.Namespace,
    *,
    cache_type: str | None = None,
    cache_allow_database_fallback: bool = True,
    enable_storage_manager: bool = True,
    create: bool = False,
    backup: bool = False,
    strict_storage_manager_bootstrap: bool = False,
    storage_startup_on_add: bool = False,
    enable_maintenance: bool = False,
    repair_bootstrap_rows: bool = False,
) -> SurfaceCoreSession:
    from LiuXin_alpha.surfaces.system_profile import apply_system_profile

    apply_system_profile(args)
    return SurfaceCoreSession.open(
        database_path=getattr(args, "database", None),
        endpoint=getattr(args, "core_endpoint", None),
        db_type=str(getattr(args, "db_type", "SQLite")),
        database_metadata=getattr(args, "database_metadata", None),
        create=create,
        backup=backup,
        cache_type=cache_type,
        cache_allow_database_fallback=cache_allow_database_fallback,
        enable_storage_manager=enable_storage_manager,
        strict_storage_manager_bootstrap=strict_storage_manager_bootstrap,
        storage_startup_on_add=storage_startup_on_add,
        enable_maintenance=enable_maintenance,
        repair_bootstrap_rows=repair_bootstrap_rows,
        timeout_seconds=float(getattr(args, "core_timeout", 10.0)),
    )


__all__ = [
    "CoreRow",
    "CoreRowPage",
    "CoreDatabaseView",
    "CoreDriverView",
    "CoreSurfaceModel",
    "SurfaceCoreSession",
    "add_core_client_arguments",
    "coerce_surface_core",
    "open_surface_core_from_args",
]
