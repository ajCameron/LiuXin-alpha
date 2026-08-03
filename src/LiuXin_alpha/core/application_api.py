"""Stable named application commands and queries hosted by Core."""

# pyright: reportImportCycles=false

from __future__ import annotations

import base64

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.catalog.api.repositories import CATALOG_REPOSITORY_NAMES

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


_CATALOG_REPOSITORIES = frozenset(CATALOG_REPOSITORY_NAMES)
_WEMI_LEVELS = frozenset({"work", "expression", "manifestation", "item"})
_MATCHABLE_REPOSITORIES = _CATALOG_REPOSITORIES - {"titles"}
_PARENT_SCOPED_REPOSITORIES = frozenset(
    {
        "expressions",
        "manifestations",
        "items",
        "item_identifiers",
    }
)


def _field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
    description: str = "",
) -> CorePayloadFieldDescription:
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
        description=description,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_text(
    payload: Mapping[str, Any],
    name: str,
    *,
    choices: Iterable[str] | None = None,
) -> str:
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError("`{}` is required.".format(name))
    if choices is not None and value not in choices:
        raise CoreDispatchError(
            "`{}` must be one of: {}.".format(
                name,
                ", ".join(sorted(str(choice) for choice in choices)),
            )
        )
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    if name not in payload:
        raise CoreDispatchError("`{}` is required.".format(name))
    value = payload[name]
    if isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except Exception as exc:
        raise CoreDispatchError(
            "`{}` must be an integer.".format(name)
        ) from exc


def _optional_int(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: int | None,
    minimum: int = 0,
) -> int | None:
    raw = payload.get(name, default)
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise CoreDispatchError("`{}` must be an integer or null.".format(name))
    try:
        value = int(raw)
    except Exception as exc:
        raise CoreDispatchError(
            "`{}` must be an integer or null.".format(name)
        ) from exc
    if value < minimum:
        raise CoreDispatchError(
            "`{}` must be >= {}.".format(name, minimum)
        )
    return value


def _mapping(payload: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = payload.get(name)
    if not isinstance(value, Mapping):
        raise CoreDispatchError("`{}` must be an object.".format(name))
    return dict(value)


def _row_values(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    values = getattr(row, "row_dict", None)
    if isinstance(values, Mapping):
        return dict(values)
    keys = getattr(row, "keys", None)
    get_item = getattr(row, "__getitem__", None)
    if callable(keys) and callable(get_item):
        raw_keys = keys()
        if isinstance(raw_keys, Iterable):
            return {
                str(key): get_item(key)
                for key in raw_keys
            }
    raise CoreDispatchError(
        "Read source returned a non-row value: {}.".format(
            type(row).__name__
        )
    )


def _sort_value(value: Any) -> tuple[int, Any]:
    if value is None:
        return (5, "")
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        return (1, value)
    if isinstance(value, str):
        return (2, (value.casefold(), value))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return (3, tuple(_sort_value(item) for item in value))
    return (4, repr(value))


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value).casefold()


class CoreApplicationAPI:
    """Named, transport-stable adapter over services composed by Core."""

    def install(self, runtime: "CoreRuntime") -> None:
        query = runtime.register_query_handler
        command = runtime.register_command_handler

        query(
            "schema.tables",
            self.schema_tables,
            summary="List readable tables and their transport-safe schema.",
            tags=("schema", "read"),
        )
        query(
            "schema.table",
            self.schema_table,
            summary="Describe one readable table.",
            payload_fields=(_field("table", required=True, field_type="string"),),
            tags=("schema", "read"),
        )
        query(
            "rows.get",
            self.rows_get,
            summary="Read one row through Core's selected read source.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
            ),
            tags=("rows", "read"),
        )
        query(
            "rows.query",
            self.rows_query,
            summary="Execute a structured, paginated row query.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("predicates", field_type="array"),
                _field("relation", field_type="object"),
                _field("text", field_type="string"),
                _field("text_fields", field_type="array"),
                _field("sort", field_type="array"),
                _field("projection", field_type="array"),
                _field("offset", field_type="integer"),
                _field("limit", field_type="integer|null"),
            ),
            tags=("rows", "read", "cache"),
        )
        query(
            "relations.list",
            self.relations_list,
            summary="Read rows related to one source row.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
                _field("related_table", required=True, field_type="string"),
                _field("type_filter", field_type="string|null"),
                _field("include_link_rows", field_type="boolean"),
            ),
            tags=("relations", "read"),
        )
        query(
            "admin.row.delete-impact",
            self.admin_row_delete_impact,
            summary="Describe the impact of an explicit administrative row delete.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
                _field("sample_limit", field_type="integer"),
            ),
            tags=("admin", "rows", "read"),
        )
        query(
            "catalog.entity.get",
            self.catalog_entity_get,
            summary="Read one semantic Catalog entity.",
            payload_fields=(
                _field("repository", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "read"),
        )
        query(
            "catalog.entity.list",
            self.catalog_entity_list,
            summary="Read a stable page from one Catalog repository.",
            payload_fields=(
                _field("repository", required=True, field_type="string"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("catalog", "read"),
        )
        query(
            "catalog.bundle.get",
            self.catalog_bundle_get,
            summary="Read a coherent Catalog WEMI path.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "metadata", "read"),
        )
        query(
            "catalog.graph.get",
            self.catalog_graph_get,
            summary="Read a bounded full descendant graph for one Work.",
            payload_fields=(
                _field("work_id", required=True, field_type="integer"),
                _field("max_expressions", field_type="integer"),
                _field("max_manifestations", field_type="integer"),
                _field("max_items", field_type="integer"),
            ),
            tags=("catalog", "wemi", "read"),
        )
        query(
            "catalog.item.summary",
            self.catalog_item_summary,
            summary="Read a compact display-neutral Catalog Item summary.",
            payload_fields=(
                _field("item_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "metadata", "read"),
        )
        query(
            "catalog.match",
            self.catalog_match,
            summary="Return an explained Catalog match decision.",
            payload_fields=(
                _field("repository", required=True, field_type="string"),
                _field("candidate", required=True, field_type="object"),
                _field("source", field_type="string|null"),
                _field("hints", field_type="object"),
                _field("parent_id", field_type="integer"),
            ),
            tags=("catalog", "matching", "read"),
        )
        query(
            "catalog.agent.resolve",
            self.catalog_agent_resolve,
            summary="Resolve an Agent by name and optional role.",
            payload_fields=(
                _field("name", required=True, field_type="string"),
                _field("role", field_type="string|null"),
            ),
            tags=("catalog", "agents", "read"),
        )
        query(
            "catalog.annotations.list",
            self.catalog_annotations_list,
            summary="List Item-scoped Annotations with optional filters.",
            payload_fields=(
                _field("item_id", required=True, field_type="integer"),
                _field("user_id", field_type="integer|null"),
                _field("kind", field_type="string|null"),
            ),
            tags=("catalog", "annotations", "read"),
        )
        query(
            "metadata.get",
            self.metadata_get,
            summary="Hydrate one item-centred WEMI metadata mapping.",
            payload_fields=(
                _field("item_id", required=True, field_type="integer"),
                _field("include_related", field_type="boolean"),
                _field("include_legacy", field_type="boolean"),
            ),
            tags=("metadata", "read", "cache"),
        )
        query(
            "metadata.opf.export",
            self.metadata_opf_export,
            summary="Hydrate one Item and export OPF as a wire-encoded byte value.",
            payload_fields=(
                _field("item_id", required=True, field_type="integer"),
                _field("default_lang", field_type="string|null"),
            ),
            tags=("metadata", "opf", "read", "cache"),
        )
        query(
            "cache.status",
            self.cache_status,
            summary="Describe Core's optional modern cache.",
            tags=("cache", "read"),
        )
        query(
            "storage.stores.list",
            self.storage_stores_list,
            summary="List Core-managed stores and their current status.",
            payload_fields=(
                _field("refresh", field_type="boolean"),
            ),
            tags=("storage", "read"),
        )
        query(
            "storage.files.list",
            self.storage_files_list,
            summary="List Core-managed file locations with pagination.",
            payload_fields=(
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("storage", "read"),
        )
        query(
            "storage.file.locate",
            self.storage_file_locate,
            summary="Resolve one file URL to a Core-managed location.",
            payload_fields=(
                _field("file_url", required=True, field_type="string"),
                _field("preferred_store", field_type="string|null"),
            ),
            tags=("storage", "read"),
        )
        query(
            "storage.file.read",
            self.storage_file_read,
            summary="Read one Core-managed file as a wire-encoded byte value.",
            payload_fields=(
                _field("file_url", required=True, field_type="string"),
                _field("preferred_store", field_type="string|null"),
            ),
            tags=("storage", "read"),
        )

        command(
            "catalog.entity.create",
            self.catalog_entity_create,
            summary="Create one entity through a Catalog repository.",
            payload_fields=(
                _field("repository", required=True, field_type="string"),
                _field("data", required=True, field_type="object"),
            ),
            tags=("catalog", "write"),
        )
        command(
            "catalog.entity.match-or-create",
            self.catalog_entity_match_or_create,
            summary="Resolve or create one Catalog entity under matching policy.",
            payload_fields=(
                _field("repository", required=True, field_type="string"),
                _field("candidate", required=True, field_type="object"),
                _field("source", field_type="string|null"),
                _field("hints", field_type="object"),
                _field("parent_id", field_type="integer"),
            ),
            tags=("catalog", "matching", "write"),
        )
        command(
            "catalog.agent.create-person",
            self.catalog_agent_create_person,
            summary="Atomically create a person Agent and sidecar metadata.",
            payload_fields=(
                _field("data", required=True, field_type="object"),
                _field("details", field_type="object"),
                _field("identifiers", field_type="array"),
                _field("language_ids", field_type="array"),
                _field("notes", field_type="array"),
            ),
            tags=("catalog", "agents", "write"),
        )
        command(
            "catalog.agent.create-organisation",
            self.catalog_agent_create_organisation,
            summary="Atomically create an organisation Agent and sidecar metadata.",
            payload_fields=(
                _field("data", required=True, field_type="object"),
                _field("details", field_type="object"),
                _field("parent_id", field_type="integer|null"),
                _field("relation_type", field_type="string"),
                _field("relation_note", field_type="string|null"),
                _field("identifiers", field_type="array"),
                _field("language_ids", field_type="array"),
                _field("notes", field_type="array"),
                _field("synopses", field_type="array"),
            ),
            tags=("catalog", "agents", "write"),
        )
        command(
            "catalog.entity.update",
            self.catalog_entity_update,
            summary="Update one entity through a Catalog repository.",
            payload_fields=(
                _field("repository", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
                _field("data", required=True, field_type="object"),
            ),
            tags=("catalog", "write"),
        )
        command(
            "catalog.entity.delete",
            self.catalog_entity_delete,
            summary="Delete one entity through a Catalog repository.",
            payload_fields=(
                _field("repository", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "write"),
        )
        command(
            "catalog.wemi.create",
            self.catalog_wemi_create,
            summary="Atomically create and link one WEMI path.",
            payload_fields=(
                _field("work", required=True, field_type="object"),
                _field("expression", required=True, field_type="object"),
                _field("manifestation", required=True, field_type="object"),
                _field("items", field_type="array"),
                _field("origin", field_type="string|null"),
                _field("work_id", field_type="integer|null"),
            ),
            tags=("catalog", "wemi", "write"),
        )
        command(
            "catalog.wemi.link",
            self.catalog_wemi_link,
            summary="Link two existing adjacent WEMI entities.",
            payload_fields=(
                _field("parent_level", required=True, field_type="string"),
                _field("parent_id", required=True, field_type="integer"),
                _field("child_level", required=True, field_type="string"),
                _field("child_id", required=True, field_type="integer"),
                _field("primary", field_type="boolean|null"),
                _field("priority", field_type="integer|null"),
                _field("origin", field_type="string|null"),
            ),
            tags=("catalog", "wemi", "write"),
        )
        command(
            "catalog.wemi.unlink",
            self.catalog_wemi_unlink,
            summary="Unlink two existing adjacent WEMI entities.",
            payload_fields=(
                _field("parent_level", required=True, field_type="string"),
                _field("parent_id", required=True, field_type="integer"),
                _field("child_level", required=True, field_type="string"),
                _field("child_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "wemi", "write"),
        )
        command(
            "catalog.metadata.attach",
            self.catalog_metadata_attach,
            summary="Atomically attach structured metadata to a WEMI entity.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
                _field("data", required=True, field_type="object"),
            ),
            tags=("catalog", "metadata", "write"),
        )
        command(
            "catalog.metadata.replace",
            self.catalog_metadata_replace,
            summary="Atomically replace selected semantic metadata groups.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("entity_id", required=True, field_type="integer"),
                _field("data", required=True, field_type="object"),
            ),
            tags=("catalog", "metadata", "write"),
        )
        command(
            "catalog.metadata.merge",
            self.catalog_metadata_merge,
            summary="Atomically merge one WEMI entity into another.",
            payload_fields=(
                _field("level", required=True, field_type="string"),
                _field("source_id", required=True, field_type="integer"),
                _field("target_id", required=True, field_type="integer"),
            ),
            tags=("catalog", "metadata", "write"),
        )
        command(
            "catalog.field.write",
            self.catalog_field_write,
            summary="Apply a normalized Catalog field update, cache-aware when configured.",
            payload_fields=(
                _field("src_table", required=True, field_type="string"),
                _field("dst_column", required=True, field_type="string"),
                _field("args", field_type="array"),
                _field("kwargs", field_type="object"),
                _field("force_refresh", field_type="boolean"),
                _field("destination_owned", field_type="boolean|null"),
            ),
            tags=("catalog", "cache", "write"),
        )
        command(
            "catalog.field.write-one",
            self.catalog_field_write_one,
            summary="Apply one normalized Catalog field instruction, cache-aware when configured.",
            payload_fields=(
                _field("src_table", required=True, field_type="string"),
                _field("dst_column", required=True, field_type="string"),
                _field("src_id", required=True),
                _field("dst_value", required=True),
                _field("kwargs", field_type="object"),
                _field("force_refresh", field_type="boolean"),
                _field("destination_owned", field_type="boolean|null"),
            ),
            tags=("catalog", "cache", "write"),
        )
        command(
            "admin.row.create",
            self.admin_row_create,
            summary="Explicitly create a raw row for administrative tooling.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("values", required=True, field_type="object"),
            ),
            tags=("admin", "rows", "write"),
        )
        command(
            "admin.row.update",
            self.admin_row_update,
            summary="Explicitly update raw row fields for administrative tooling.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
                _field("updates", required=True, field_type="object"),
            ),
            tags=("admin", "rows", "write"),
        )
        command(
            "admin.row.delete",
            self.admin_row_delete,
            summary="Explicitly delete a raw row for administrative tooling.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
            ),
            tags=("admin", "rows", "write"),
        )
        command(
            "admin.relation.link",
            self.admin_relation_link,
            summary="Explicitly create a raw relationship for administrative tooling.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
                _field("related_table", required=True, field_type="string"),
                _field("related_row_id", required=True, field_type="integer"),
                _field("priority", field_type="integer|null"),
                _field("type", field_type="string|null"),
                _field("extra", field_type="object"),
            ),
            tags=("admin", "relations", "write"),
        )
        command(
            "admin.relation.unlink",
            self.admin_relation_unlink,
            summary="Explicitly remove a raw relationship for administrative tooling.",
            payload_fields=(
                _field("table", required=True, field_type="string"),
                _field("row_id", required=True, field_type="integer"),
                _field("related_table", required=True, field_type="string"),
                _field("related_row_id", required=True, field_type="integer"),
            ),
            tags=("admin", "relations", "write"),
        )
        command(
            "storage.store.save",
            self.storage_store_save,
            summary="Create or update a storage configuration row.",
            payload_fields=(
                _field("store", required=True, field_type="object"),
            ),
            tags=("storage", "write"),
        )
        command(
            "storage.refresh",
            self.storage_refresh,
            summary="Refresh Core's storage manager from canonical rows.",
            payload_fields=(
                _field("startup_on_add", field_type="boolean"),
                _field("include_offline", field_type="boolean"),
                _field("clear_existing", field_type="boolean"),
                _field("strict", field_type="boolean"),
            ),
            tags=("storage", "lifecycle", "write"),
        )
        command(
            "storage.file.put",
            self.storage_file_put,
            summary="Store a base64-encoded file through Core's storage policy.",
            payload_fields=(
                _field("content_base64", required=True, field_type="string"),
                _field("metadata", field_type="object"),
                _field("preferred_store", field_type="string|null"),
            ),
            tags=("storage", "files", "write"),
        )
        command(
            "storage.file.delete",
            self.storage_file_delete,
            summary="Delete one Core-managed file location.",
            payload_fields=(
                _field("file_url", required=True, field_type="string"),
            ),
            tags=("storage", "files", "write"),
        )
        command(
            "cache.reload",
            self.cache_reload,
            summary="Reload Core's configured modern cache.",
            tags=("cache", "lifecycle", "write"),
        )
        command(
            "read-source.refresh",
            self.read_source_refresh,
            summary="Refresh Core's selected application read source.",
            tags=("read", "cache", "lifecycle"),
        )

    @staticmethod
    def _repository(runtime: "CoreRuntime", payload: Mapping[str, Any]) -> Any:
        name = _required_text(
            payload,
            "repository",
            choices=_CATALOG_REPOSITORIES,
        )
        repositories = runtime.services.catalog.repositories
        resolver = getattr(repositories, "for_name", None)
        if callable(resolver):
            return resolver(name)
        # Lightweight alternate Core test/composition adapters may predate the
        # registry convenience while still exposing the declared attributes.
        return getattr(repositories, name)

    @staticmethod
    def _array(
        payload: Mapping[str, Any],
        name: str,
    ) -> tuple[Any, ...]:
        raw = payload.get(name, ())
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise CoreDispatchError("`{}` must be an array.".format(name))
        return tuple(raw)

    @staticmethod
    def _catalog_candidate(
        payload: Mapping[str, Any],
        repository_name: str,
    ) -> Any:
        from LiuXin_alpha.catalog.api.common import (
            IdentifierCandidate,
            MetadataCandidate,
        )

        candidate_data = _mapping(payload, "candidate")
        source = (
            None
            if payload.get("source") is None
            else str(payload.get("source"))
        )
        hints_raw = payload.get("hints", {})
        if not isinstance(hints_raw, Mapping):
            raise CoreDispatchError("`hints` must be an object.")
        if repository_name in {"identifiers", "item_identifiers"}:
            return IdentifierCandidate(
                identifier_type=_required_text(
                    candidate_data,
                    "identifier_type",
                ),
                value=_required_text(candidate_data, "value"),
                normalised_value=candidate_data.get("normalised_value"),
                source=source,
                hints=dict(hints_raw),
            )
        return MetadataCandidate(
            data=candidate_data,
            source=source,
            hints=dict(hints_raw),
        )

    @staticmethod
    def _schema_for_table(
        runtime: "CoreRuntime",
        table: str,
        *,
        include_relations: bool = True,
    ) -> dict[str, Any]:
        source = runtime.services.read_source
        database = runtime.services.database
        wrapper = database.driver_wrapper
        columns = [str(value) for value in source.get_column_headings(table)]
        try:
            is_view = bool(wrapper.is_view(table))
        except Exception:
            is_view = False
        id_column: str | None = None
        obvious_ids = [
            column
            for column in columns
            if column == "id" or column.endswith("_id")
        ]
        if obvious_ids:
            try:
                id_column = str(wrapper.get_id_column(table))
            except Exception:
                id_column = obvious_ids[0]
        related_tables: list[str] = []
        if include_relations:
            try:
                related_tables = sorted(
                    str(value)
                    for value in wrapper.get_interlinked_tables(table)
                    if str(value) != table
                )
            except Exception:
                related_tables = []
        return {
            "name": table,
            "columns": columns,
            "id_column": id_column,
            "is_view": is_view,
            "related_tables": related_tables,
            "relations_included": include_relations,
        }

    def schema_tables(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        tables = sorted(
            str(value)
            for value in runtime.services.read_source.get_tables(
                force_refresh=False
            )
        )
        return {
            "tables": [
                self._schema_for_table(
                    runtime,
                    table,
                    include_relations=False,
                )
                for table in tables
            ],
            "count": len(tables),
        }

    def schema_table(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        table = _required_text(_payload(query), "table")
        tables = {
            str(value)
            for value in runtime.services.read_source.get_tables(
                force_refresh=False
            )
        }
        if table not in tables:
            raise CoreDispatchError("Unknown readable table: {!r}.".format(table))
        return self._schema_for_table(
            runtime,
            table,
            include_relations=True,
        )

    @staticmethod
    def _row_record(
        runtime: "CoreRuntime",
        table: str,
        row: Any,
        *,
        projection: Sequence[str] = (),
    ) -> dict[str, Any]:
        values = _row_values(row)
        try:
            id_column = str(
                runtime.services.database.driver_wrapper.get_id_column(table)
            )
        except Exception:
            id_column = next(
                (
                    key
                    for key in values
                    if str(key) == "id" or str(key).endswith("_id")
                ),
                "id",
            )
        if projection:
            projected = {
                str(name): values.get(str(name))
                for name in projection
            }
            if id_column not in projected:
                projected[id_column] = values.get(id_column)
            values = projected
        raw_id = values.get(id_column)
        return {
            "table": table,
            "row_id": None if raw_id is None else int(raw_id),
            "values": values,
        }

    def rows_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        row = runtime.services.read_source.get_row_from_id(table, row_id)
        return {
            "record": (
                None
                if row is None
                else self._row_record(runtime, table, row)
            ),
            "complete": True,
            "source": (
                "cache"
                if runtime.services.cache is not None
                else "database"
            ),
        }

    @staticmethod
    def _cache_query(payload: Mapping[str, Any]) -> Any:
        from LiuXin_alpha.caches.api import (
            CacheFilterOperator,
            CachePredicate,
            CacheQuery,
            CacheRelation,
            CacheSort,
        )

        table = _required_text(payload, "table")
        raw_predicates = payload.get("predicates", ())
        if not isinstance(raw_predicates, Sequence) or isinstance(
            raw_predicates,
            (str, bytes),
        ):
            raise CoreDispatchError("`predicates` must be an array.")
        predicates = []
        for raw in raw_predicates:
            if not isinstance(raw, Mapping):
                raise CoreDispatchError(
                    "Every `predicates` entry must be an object."
                )
            field_name = _required_text(raw, "field")
            operator_name = _required_text(raw, "operator")
            try:
                operator = CacheFilterOperator(operator_name)
            except ValueError as exc:
                raise CoreDispatchError(
                    "Unknown predicate operator: {!r}.".format(operator_name)
                ) from exc
            predicates.append(
                CachePredicate(
                    field=field_name,
                    operator=operator,
                    value=raw.get("value"),
                )
            )

        relation = None
        raw_relation = payload.get("relation")
        if raw_relation is not None:
            if not isinstance(raw_relation, Mapping):
                raise CoreDispatchError("`relation` must be an object or null.")
            raw_ids = raw_relation.get("ids", ())
            if not isinstance(raw_ids, Sequence) or isinstance(
                raw_ids,
                (str, bytes),
            ):
                raise CoreDispatchError("`relation.ids` must be an array.")
            relation_ids: list[int] = []
            for value in raw_ids:
                if isinstance(value, bool):
                    raise CoreDispatchError(
                        "`relation.ids` values must be integers."
                    )
                try:
                    relation_ids.append(int(str(value)))
                except Exception as exc:
                    raise CoreDispatchError(
                        "`relation.ids` values must be integers."
                    ) from exc
            relation = CacheRelation(
                table=_required_text(raw_relation, "table"),
                ids=tuple(relation_ids),
                type_filter=(
                    None
                    if raw_relation.get("type_filter") is None
                    else str(raw_relation["type_filter"])
                ),
            )

        raw_sort = payload.get("sort", ())
        if not isinstance(raw_sort, Sequence) or isinstance(
            raw_sort,
            (str, bytes),
        ):
            raise CoreDispatchError("`sort` must be an array.")
        sort = []
        for raw in raw_sort:
            if isinstance(raw, str):
                sort.append(CacheSort(raw))
                continue
            if not isinstance(raw, Mapping):
                raise CoreDispatchError(
                    "Every `sort` entry must be a string or object."
                )
            sort.append(
                CacheSort(
                    field=_required_text(raw, "field"),
                    ascending=bool(raw.get("ascending", True)),
                )
            )

        text_fields = payload.get("text_fields", ())
        projection = payload.get("projection", ())
        for name, raw in (
            ("text_fields", text_fields),
            ("projection", projection),
        ):
            if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
                raise CoreDispatchError("`{}` must be an array.".format(name))

        return CacheQuery(
            table=table,
            predicates=tuple(predicates),
            relation=relation,
            text=str(payload.get("text", "") or ""),
            text_fields=tuple(str(value) for value in text_fields),
            sort=tuple(sort),
            projection=tuple(str(value) for value in projection),
            offset=int(_optional_int(payload, "offset", default=0) or 0),
            limit=_optional_int(payload, "limit", default=None),
        )

    @staticmethod
    def _matches(value: Any, predicate: Any) -> bool:
        from LiuXin_alpha.caches.api import CacheFilterOperator

        operator = predicate.operator
        expected = predicate.value
        if operator == CacheFilterOperator.IS_NULL:
            is_null = value is None
            return is_null if expected is not False else not is_null
        if operator == CacheFilterOperator.EQ:
            if isinstance(value, Sequence) and not isinstance(
                value,
                (str, bytes),
            ):
                return expected in value
            return bool(value == expected)
        if operator == CacheFilterOperator.IN:
            expected_values = tuple(expected)
            if isinstance(value, Sequence) and not isinstance(
                value,
                (str, bytes),
            ):
                return any(item in expected_values for item in value)
            return value in expected_values
        if operator == CacheFilterOperator.CONTAINS:
            return str(expected).casefold() in _flatten_text(value)
        if operator == CacheFilterOperator.PREFIX:
            return _flatten_text(value).startswith(str(expected).casefold())
        try:
            if operator == CacheFilterOperator.LT:
                return bool(value < expected)
            if operator == CacheFilterOperator.LTE:
                return bool(value <= expected)
            if operator == CacheFilterOperator.GT:
                return bool(value > expected)
            if operator == CacheFilterOperator.GTE:
                return bool(value >= expected)
        except TypeError:
            return False
        return False

    def _database_query(
        self,
        runtime: "CoreRuntime",
        spec: Any,
    ) -> dict[str, Any]:
        source = runtime.services.read_source
        schema = self._schema_for_table(
            runtime,
            str(spec.table),
            include_relations=False,
        )
        if schema.get("id_column") is None:
            # Identifier-less lookup views cannot be materialized as Database
            # Row objects because their values are intentionally non-unique.
            # Keep that driver quirk inside Core and return wire records.
            rows = list(
                runtime.services.database.driver_wrapper.get_all_rows(
                    spec.table
                )
            )
        else:
            rows = list(
                source.get_all_rows(
                    spec.table,
                    iterator_return=False,
                )
            )

        if spec.relation is not None:
            related_ids: set[int] = set()
            for target_id in spec.relation.ids:
                target = source.get_row_from_id(
                    spec.relation.table,
                    int(target_id),
                )
                if target is None:
                    continue
                for row in source.get_interlinked_rows(
                    target_row=target,
                    secondary_table=spec.table,
                    type_filter=spec.relation.type_filter,
                ):
                    record = self._row_record(runtime, spec.table, row)
                    if record["row_id"] is not None:
                        related_ids.add(int(record["row_id"]))
            rows = [
                row
                for row in rows
                if self._row_record(runtime, spec.table, row)["row_id"]
                in related_ids
            ]

        materialized = [
            (row, _row_values(row))
            for row in rows
        ]
        for predicate in spec.predicates:
            materialized = [
                (row, values)
                for row, values in materialized
                if self._matches(values.get(predicate.field), predicate)
            ]

        terms = tuple(
            term
            for term in str(spec.text).casefold().split()
            if term
        )
        if terms:
            fields = tuple(spec.text_fields)
            if not fields:
                fields = tuple(
                    str(value)
                    for value in source.get_column_headings(spec.table)
                )
            materialized = [
                (row, values)
                for row, values in materialized
                if all(
                    any(
                        term in _flatten_text(values.get(field))
                        for field in fields
                    )
                    for term in terms
                )
            ]

        for sort_spec in reversed(tuple(spec.sort)):
            materialized.sort(
                key=lambda item: _sort_value(
                    item[1].get(sort_spec.field)
                ),
                reverse=not sort_spec.ascending,
            )
        if not spec.sort:
            materialized.sort(
                key=lambda item: (
                    self._row_record(runtime, spec.table, item[0])["row_id"]
                    is None,
                    self._row_record(runtime, spec.table, item[0])["row_id"]
                    or 0,
                )
            )

        total_count = len(materialized)
        end = (
            None
            if spec.limit is None
            else spec.offset + spec.limit
        )
        visible = materialized[spec.offset:end]
        return {
            "records": [
                self._row_record(
                    runtime,
                    spec.table,
                    row,
                    projection=spec.projection,
                )
                for row, _values in visible
            ],
            "total_count": total_count,
            "offset": spec.offset,
            "limit": spec.limit,
            "complete": True,
            "generation": 0,
            "source": "database",
        }

    def rows_query(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        spec = self._cache_query(_payload(query))
        source = runtime.services.read_source
        query_cache = getattr(source, "query_cache", None)
        if not callable(query_cache):
            return self._database_query(runtime, spec)
        from LiuXin_alpha.caches.api import CacheQueryResult

        result = query_cache(spec)
        if not isinstance(result, CacheQueryResult):
            raise CoreDispatchError(
                "Cache read source returned an invalid query result."
            )
        return {
            "records": [
                {
                    "table": str(record.table),
                    "row_id": int(record.row_id),
                    "values": dict(record.values),
                }
                for record in result.records
            ],
            "total_count": int(result.total_count),
            "offset": int(result.offset),
            "limit": result.limit,
            "complete": bool(result.complete),
            "generation": int(result.generation),
            "source": "cache",
        }

    def relations_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        related_table = _required_text(payload, "related_table")
        source = runtime.services.read_source
        row = source.get_row_from_id(table, row_id)
        if row is None:
            return {
                "records": [],
                "link_records": [],
                "complete": True,
            }
        type_filter = payload.get("type_filter")
        related_rows = source.get_interlinked_rows(
            target_row=row,
            secondary_table=related_table,
            type_filter=(
                None if type_filter is None else str(type_filter)
            ),
        )
        link_rows: Sequence[Any] = ()
        if bool(payload.get("include_link_rows", False)):
            link_rows = source.get_interlink_rows(
                primary_row=row,
                secondary_table=related_table,
            )
        return {
            "records": [
                self._row_record(runtime, related_table, related)
                for related in related_rows
            ],
            "link_records": [
                self._row_record(
                    runtime,
                    str(getattr(link, "table", "") or "link"),
                    link,
                )
                for link in link_rows
            ],
            "complete": True,
            "source": (
                "cache"
                if runtime.services.cache is not None
                else "database"
            ),
        }

    def admin_row_delete_impact(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        return dict(
            runtime.services.library.describe_row_delete_impact(
                table=_required_text(payload, "table"),
                row_id=_required_int(payload, "row_id"),
                sample_limit=int(
                    _optional_int(
                        payload,
                        "sample_limit",
                        default=3,
                    )
                    or 0
                )
            )
        )

    def catalog_entity_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        repository = self._repository(runtime, payload)
        return {
            "repository": _required_text(payload, "repository"),
            "entity": repository.get(
                _required_int(payload, "entity_id")
            ),
        }

    def catalog_entity_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        repository = self._repository(runtime, payload)
        limit = int(
            _optional_int(payload, "limit", default=100)
            or 0
        )
        offset = int(
            _optional_int(payload, "offset", default=0)
            or 0
        )
        rows = repository.list(limit=limit, offset=offset)
        return {
            "repository": _required_text(payload, "repository"),
            "entities": list(rows),
            "limit": limit,
            "offset": offset,
        }

    def catalog_bundle_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> Any:
        payload = _payload(query)
        level = _required_text(
            payload,
            "level",
            choices=_WEMI_LEVELS,
        )
        entity_id = _required_int(payload, "entity_id")
        retriever = getattr(
            runtime.services.catalog.retrieval.bundles,
            "for_{}".format(level),
        )
        return retriever(entity_id)

    def catalog_graph_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> Any:
        payload = _payload(query)
        max_expressions = _optional_int(
            payload,
            "max_expressions",
            default=100,
            minimum=0,
        )
        max_manifestations = _optional_int(
            payload,
            "max_manifestations",
            default=500,
            minimum=0,
        )
        max_items = _optional_int(
            payload,
            "max_items",
            default=1000,
            minimum=0,
        )
        assert max_expressions is not None
        assert max_manifestations is not None
        assert max_items is not None
        return runtime.services.catalog.retrieval.graph.for_work(
            _required_int(payload, "work_id"),
            max_expressions=max_expressions,
            max_manifestations=max_manifestations,
            max_items=max_items,
        )

    def catalog_item_summary(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        return dict(
            runtime.services.catalog.retrieval.projections.item_summary(
                _required_int(_payload(query), "item_id")
            )
        )

    def catalog_match(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> Any:
        payload = _payload(query)
        repository_name = _required_text(
            payload,
            "repository",
            choices=_MATCHABLE_REPOSITORIES,
        )
        repository = self._repository(runtime, payload)
        candidate = self._catalog_candidate(payload, repository_name)
        if repository_name == "item_identifiers":
            return repository.match(
                candidate,
                item_id=_required_int(payload, "parent_id"),
            )
        if repository_name in _PARENT_SCOPED_REPOSITORIES:
            return repository.match(
                _required_int(payload, "parent_id"),
                candidate,
            )
        return repository.match(candidate)

    def catalog_agent_resolve(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        return {
            "agent": runtime.services.catalog.repositories.agents.resolve(
                name=_required_text(payload, "name"),
                role=(
                    None
                    if payload.get("role") is None
                    else str(payload.get("role"))
                ),
            )
        }

    def catalog_annotations_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        item_id = _required_int(payload, "item_id")
        user_id = _optional_int(
            payload,
            "user_id",
            default=None,
            minimum=0,
        )
        kind = payload.get("kind")
        if kind is not None and not isinstance(kind, str):
            raise CoreDispatchError("`kind` must be a string or null.")
        annotations = (
            runtime.services.catalog.repositories.annotations.list_for_item(
                item_id,
                user_id=user_id,
                kind=kind,
            )
        )
        return {
            "item_id": item_id,
            "user_id": user_id,
            "kind": kind,
            "annotations": list(annotations),
            "count": len(annotations),
        }

    @staticmethod
    def _hydrated_metadata(
        runtime: "CoreRuntime",
        item_id: int,
    ) -> Any:
        from LiuXin_alpha.metadata.containers import (
            LiuXinWEMIMetadataHydrator,
        )

        return LiuXinWEMIMetadataHydrator(
            runtime.services.read_source
        ).get_liuxin_wemi_metadata(item_id=item_id)

    def metadata_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        metadata = self._hydrated_metadata(
            runtime,
            _required_int(payload, "item_id"),
        )
        return dict(
            metadata.to_mapping(
                include_related=bool(
                    payload.get("include_related", True)
                ),
                include_legacy=bool(
                    payload.get("include_legacy", True)
                ),
            )
        )

    def metadata_opf_export(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        from LiuXin_alpha.metadata import metadata_to_opf_bytes

        payload = _payload(query)
        item_id = _required_int(payload, "item_id")
        metadata = self._hydrated_metadata(runtime, item_id)
        return {
            "item_id": item_id,
            "content": metadata_to_opf_bytes(
                metadata,
                default_lang=(
                    None
                    if payload.get("default_lang") is None
                    else str(payload.get("default_lang"))
                ),
            ),
        }

    def cache_status(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        return dict(runtime.services.describe()["cache"])

    @staticmethod
    def _semantic_receipt(
        runtime: "CoreRuntime",
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        return runtime.services.reconcile(receipt)

    def catalog_entity_create(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        name = _required_text(payload, "repository")
        repository = self._repository(runtime, payload)
        entity_id = repository.create(_mapping(payload, "data"))
        return self._semantic_receipt(
            runtime,
            {
                "repository": name,
                "entity_id": int(entity_id),
                "entity": repository.require(entity_id),
            },
        )

    def catalog_entity_match_or_create(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        name = _required_text(
            payload,
            "repository",
            choices=_MATCHABLE_REPOSITORIES,
        )
        repository = self._repository(runtime, payload)
        candidate = self._catalog_candidate(payload, name)
        if name in _PARENT_SCOPED_REPOSITORIES:
            entity_id = repository.match_or_create(
                _required_int(payload, "parent_id"),
                candidate,
            )
        else:
            entity_id = repository.match_or_create(candidate)
        return self._semantic_receipt(
            runtime,
            {
                "repository": name,
                "entity_id": int(entity_id),
                "entity": repository.require(entity_id),
            },
        )

    def catalog_agent_create_person(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        details = payload.get("details")
        if details is not None and not isinstance(details, Mapping):
            raise CoreDispatchError("`details` must be an object or null.")
        identifiers = self._array(payload, "identifiers")
        if any(not isinstance(value, Mapping) for value in identifiers):
            raise CoreDispatchError(
                "Every `identifiers` entry must be an object."
            )
        agent_id = runtime.services.catalog.repositories.agents.create_person(
            _mapping(payload, "data"),
            details=None if details is None else dict(details),
            identifiers=tuple(dict(value) for value in identifiers),
            language_ids=tuple(
                int(value)
                for value in self._array(payload, "language_ids")
            ),
            notes=self._array(payload, "notes"),
        )
        return self._semantic_receipt(
            runtime,
            {
                "agent_id": int(agent_id),
                "agent": runtime.services.catalog.repositories.agents.require(
                    agent_id
                ),
                "kind": "person",
            },
        )

    def catalog_agent_create_organisation(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        details = payload.get("details")
        if details is not None and not isinstance(details, Mapping):
            raise CoreDispatchError("`details` must be an object or null.")
        identifiers = self._array(payload, "identifiers")
        if any(not isinstance(value, Mapping) for value in identifiers):
            raise CoreDispatchError(
                "Every `identifiers` entry must be an object."
            )
        agent_id = (
            runtime.services.catalog.repositories.agents.create_organisation(
                _mapping(payload, "data"),
                details=None if details is None else dict(details),
                parent_id=(
                    None
                    if payload.get("parent_id") is None
                    else _required_int(payload, "parent_id")
                ),
                relation_type=str(
                    payload.get("relation_type", "imprint_of")
                ),
                relation_note=(
                    None
                    if payload.get("relation_note") is None
                    else str(payload.get("relation_note"))
                ),
                identifiers=tuple(dict(value) for value in identifiers),
                language_ids=tuple(
                    int(value)
                    for value in self._array(payload, "language_ids")
                ),
                notes=self._array(payload, "notes"),
                synopses=self._array(payload, "synopses"),
            )
        )
        return self._semantic_receipt(
            runtime,
            {
                "agent_id": int(agent_id),
                "agent": runtime.services.catalog.repositories.agents.require(
                    agent_id
                ),
                "kind": "organisation",
            },
        )

    def catalog_entity_update(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        name = _required_text(payload, "repository")
        repository = self._repository(runtime, payload)
        entity_id = _required_int(payload, "entity_id")
        repository.update(entity_id, _mapping(payload, "data"))
        return self._semantic_receipt(
            runtime,
            {
                "repository": name,
                "entity_id": entity_id,
                "entity": repository.require(entity_id),
            },
        )

    def catalog_entity_delete(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        name = _required_text(payload, "repository")
        repository = self._repository(runtime, payload)
        entity_id = _required_int(payload, "entity_id")
        deleted = repository.require(entity_id)
        repository.delete(entity_id)
        return self._semantic_receipt(
            runtime,
            {
                "repository": name,
                "entity_id": entity_id,
                "deleted": deleted,
            },
        )

    def catalog_wemi_create(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        raw_items = payload.get("items", ())
        if not isinstance(raw_items, Sequence) or isinstance(
            raw_items,
            (str, bytes),
        ):
            raise CoreDispatchError("`items` must be an array.")
        if any(not isinstance(item, Mapping) for item in raw_items):
            raise CoreDispatchError(
                "Every `items` entry must be an object."
            )
        created = runtime.services.catalog.mutations.writer.create_wemi_stack(
            work=_mapping(payload, "work"),
            expression=_mapping(payload, "expression"),
            manifestation=_mapping(payload, "manifestation"),
            items=tuple(
                dict(item)
                for item in raw_items
            ),
            origin=(
                None
                if payload.get("origin") is None
                else str(payload.get("origin"))
            ),
            work_id=(
                None
                if payload.get("work_id") is None
                else _required_int(payload, "work_id")
            ),
        )
        receipt = {
            "work_id": int(created.work_id),
            "expression_id": int(created.expression_id),
            "manifestation_id": int(created.manifestation_id),
            "item_ids": [int(value) for value in created.item_ids],
        }
        return self._semantic_receipt(runtime, receipt)

    def catalog_wemi_link(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        raw_primary = payload.get("primary")
        if raw_primary is not None and not isinstance(raw_primary, bool):
            raise CoreDispatchError("`primary` must be a boolean or null.")
        receipt = runtime.services.catalog.mutations.writer.link_wemi(
            parent_level=_required_text(
                payload,
                "parent_level",
                choices=_WEMI_LEVELS,
            ),
            parent_id=_required_int(payload, "parent_id"),
            child_level=_required_text(
                payload,
                "child_level",
                choices=_WEMI_LEVELS,
            ),
            child_id=_required_int(payload, "child_id"),
            primary=raw_primary,
            priority=_optional_int(
                payload,
                "priority",
                default=None,
            ),
            origin=(
                None
                if payload.get("origin") is None
                else str(payload["origin"])
            ),
        )
        return self._semantic_receipt(runtime, dict(receipt))

    def catalog_wemi_unlink(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        parent_level = _required_text(
            payload,
            "parent_level",
            choices=_WEMI_LEVELS,
        )
        parent_id = _required_int(payload, "parent_id")
        child_level = _required_text(
            payload,
            "child_level",
            choices=_WEMI_LEVELS,
        )
        child_id = _required_int(payload, "child_id")
        unlinked = runtime.services.catalog.mutations.writer.unlink_wemi(
            parent_level=parent_level,
            parent_id=parent_id,
            child_level=child_level,
            child_id=child_id,
        )
        return self._semantic_receipt(
            runtime,
            {
                "parent_level": parent_level,
                "parent_id": parent_id,
                "child_level": child_level,
                "child_id": child_id,
                "unlinked": unlinked,
            },
        )

    def catalog_metadata_attach(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        level = _required_text(
            payload,
            "level",
            choices=_WEMI_LEVELS,
        )
        entity_id = _required_int(payload, "entity_id")
        runtime.services.catalog.mutations.writer.attach_metadata(
            level=level,
            entity_id=entity_id,
            data=_mapping(payload, "data"),
        )
        return self._semantic_receipt(
            runtime,
            {
                "level": level,
                "entity_id": entity_id,
                "attached": True,
            },
        )

    def catalog_metadata_replace(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        level = _required_text(
            payload,
            "level",
            choices=_WEMI_LEVELS,
        )
        entity_id = _required_int(payload, "entity_id")
        runtime.services.catalog.mutations.writer.replace_metadata(
            level=level,
            entity_id=entity_id,
            data=_mapping(payload, "data"),
        )
        return self._semantic_receipt(
            runtime,
            {
                "level": level,
                "entity_id": entity_id,
                "replaced": True,
            },
        )

    def catalog_metadata_merge(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        level = _required_text(
            payload,
            "level",
            choices=_WEMI_LEVELS,
        )
        source_id = _required_int(payload, "source_id")
        target_id = _required_int(payload, "target_id")
        runtime.services.catalog.mutations.writer.merge_entities(
            level=level,
            source_id=source_id,
            target_id=target_id,
        )
        return self._semantic_receipt(
            runtime,
            {
                "level": level,
                "source_id": source_id,
                "target_id": target_id,
                "merged": True,
            },
        )

    @staticmethod
    def _writer_target(runtime: "CoreRuntime") -> tuple[Any, bool]:
        if runtime.services.cache is not None:
            return runtime.services.cache, True
        return runtime.services.catalog, False

    def catalog_field_write(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        args = payload.get("args", ())
        kwargs = payload.get("kwargs", {})
        if not isinstance(args, Sequence) or isinstance(args, (str, bytes)):
            raise CoreDispatchError("`args` must be an array.")
        if not isinstance(kwargs, Mapping):
            raise CoreDispatchError("`kwargs` must be an object.")
        target, cache_aware = self._writer_target(runtime)
        result = target.write(
            _required_text(payload, "src_table"),
            _required_text(payload, "dst_column"),
            *tuple(args),
            force_refresh=bool(payload.get("force_refresh", False)),
            destination_owned=payload.get("destination_owned"),
            **dict(kwargs),
        )
        return {
            "result": result,
            "cache": {
                "configured": runtime.services.cache is not None,
                "reconciled": cache_aware,
            },
        }

    def catalog_field_write_one(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        if "dst_value" not in payload:
            raise CoreDispatchError("`dst_value` is required.")
        kwargs = payload.get("kwargs", {})
        if not isinstance(kwargs, Mapping):
            raise CoreDispatchError("`kwargs` must be an object.")
        target, cache_aware = self._writer_target(runtime)
        result = target.write_one(
            _required_text(payload, "src_table"),
            _required_text(payload, "dst_column"),
            payload.get("src_id"),
            payload.get("dst_value"),
            force_refresh=bool(payload.get("force_refresh", False)),
            destination_owned=payload.get("destination_owned"),
            **dict(kwargs),
        )
        return {
            "result": result,
            "cache": {
                "configured": runtime.services.cache is not None,
                "reconciled": cache_aware,
            },
        }

    def admin_row_create(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        from LiuXin_alpha.databases.row import Row

        payload = _payload(command)
        table = _required_text(payload, "table")
        row = Row.from_idless_row_dict(
            runtime.services.database,
            row_dict=_mapping(payload, "values"),
            table=table,
        )
        return self._semantic_receipt(
            runtime,
            {
                "table": table,
                "record": self._row_record(runtime, table, row),
            },
        )

    def admin_row_update(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        row = runtime.services.library.update_row_fields(
            table=table,
            row_id=row_id,
            updates=_mapping(payload, "updates"),
        )
        return self._semantic_receipt(
            runtime,
            {
                "table": table,
                "row_id": row_id,
                "record": {
                    "table": table,
                    "row_id": row_id,
                    "values": row,
                },
            },
        )

    def admin_row_delete(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        deleted = runtime.services.library.delete_row(
            table=table,
            row_id=row_id,
        )
        return self._semantic_receipt(
            runtime,
            {
                "table": table,
                "row_id": row_id,
                "deleted": deleted,
            },
        )

    def admin_relation_link(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        related_table = _required_text(payload, "related_table")
        related_row_id = _required_int(payload, "related_row_id")
        database = runtime.services.database
        primary = database.get_row_from_id(table, row_id)
        secondary = database.get_row_from_id(
            related_table,
            related_row_id,
        )
        if primary is None or secondary is None:
            raise CoreDispatchError(
                "Both relationship endpoint rows must exist."
            )
        extra = payload.get("extra", {})
        if not isinstance(extra, Mapping):
            raise CoreDispatchError("`extra` must be an object.")
        database.interlink_rows(
            primary_row=primary,
            secondary_row=secondary,
            priority=payload.get("priority"),
            type=payload.get("type"),
            **dict(extra),
        )
        return self._semantic_receipt(
            runtime,
            {
                "table": table,
                "row_id": row_id,
                "related_table": related_table,
                "related_row_id": related_row_id,
                "linked": True,
            },
        )

    def admin_relation_unlink(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        related_table = _required_text(payload, "related_table")
        related_row_id = _required_int(payload, "related_row_id")
        database = runtime.services.database
        primary = database.get_row_from_id(table, row_id)
        secondary = database.get_row_from_id(
            related_table,
            related_row_id,
        )
        if primary is None or secondary is None:
            raise CoreDispatchError(
                "Both relationship endpoint rows must exist."
            )
        database.unlink_interlink(primary, secondary)
        return self._semantic_receipt(
            runtime,
            {
                "table": table,
                "row_id": row_id,
                "related_table": related_table,
                "related_row_id": related_row_id,
                "unlinked": True,
            },
        )

    @staticmethod
    def _store_record(container: Any, *, refresh: bool) -> dict[str, Any]:
        status = container.status(refresh=refresh)
        return {
            "store_id": container.store_id,
            "store_uuid": container.store_uuid,
            "store_name": str(container.store_name),
            "store_url": str(container.store_url),
            "spec": container.spec,
            "status": status,
        }

    @staticmethod
    def _location_record(location: Any) -> dict[str, Any]:
        store = getattr(location, "store", None)
        store_name = getattr(
            store,
            "name",
            getattr(store, "store_name", None),
        )
        try:
            store_key = str(location.as_store_key())
        except Exception:
            store_key = str(location)
        try:
            file_url = str(location.file_url)
        except Exception:
            file_url = str(getattr(location, "url", "") or "")
        try:
            exists = bool(location.exists())
        except Exception:
            exists = None
        return {
            "store_name": (
                None if store_name is None else str(store_name)
            ),
            "store_key": store_key,
            "file_url": file_url,
            "name": str(getattr(location, "name", "") or ""),
            "suffix": str(getattr(location, "suffix", "") or ""),
            "exists": exists,
            "cached_size": getattr(location, "cached_size", None),
            "cached_hash": getattr(location, "cached_hash", None),
        }

    def storage_stores_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        records = [
            self._store_record(
                container,
                refresh=bool(payload.get("refresh", False)),
            )
            for container in runtime.services.library.iter_stores()
        ]
        return {
            "stores": records,
            "count": len(records),
        }

    def storage_files_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        limit = int(
            _optional_int(payload, "limit", default=100)
            or 0
        )
        offset = int(
            _optional_int(payload, "offset", default=0)
            or 0
        )
        locations = list(runtime.services.library.iter_files())
        return {
            "files": [
                self._location_record(location)
                for location in locations[offset : offset + limit]
            ],
            "total_count": len(locations),
            "limit": limit,
            "offset": offset,
        }

    def storage_file_locate(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        location = runtime.services.library.retrieve_file(
            file_url=_required_text(payload, "file_url"),
            preferred_store=(
                None
                if payload.get("preferred_store") is None
                else str(payload.get("preferred_store"))
            ),
        )
        return {
            "location": self._location_record(location),
        }

    def storage_file_read(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        location = runtime.services.library.retrieve_file(
            file_url=_required_text(payload, "file_url"),
            preferred_store=(
                None
                if payload.get("preferred_store") is None
                else str(payload.get("preferred_store"))
            ),
        )
        return {
            "location": self._location_record(location),
            "content": location.read_bytes(),
        }

    def storage_store_save(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        row = runtime.services.library.save_store_row(
            store_payload=_mapping(_payload(command), "store"),
        )
        return self._semantic_receipt(
            runtime,
            {
                "store": row,
            },
        )

    def storage_refresh(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        report = runtime.services.library.refresh_storage(
            startup_on_add=bool(payload.get("startup_on_add", False)),
            include_offline=bool(payload.get("include_offline", False)),
            clear_existing=bool(payload.get("clear_existing", True)),
            strict=bool(payload.get("strict", False)),
        )
        return {
            "report": report,
            "refreshed": True,
        }

    def storage_file_put(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        encoded = _required_text(payload, "content_base64")
        try:
            content = base64.b64decode(encoded, validate=True)
        except Exception as exc:
            raise CoreDispatchError(
                "`content_base64` is not valid base64."
            ) from exc
        metadata = payload.get("metadata")
        if metadata is not None and not isinstance(metadata, Mapping):
            raise CoreDispatchError("`metadata` must be an object or null.")
        location = runtime.services.library.add_file(
            content,
            metadata=None if metadata is None else dict(metadata),
            preferred_store=(
                None
                if payload.get("preferred_store") is None
                else str(payload.get("preferred_store"))
            ),
        )
        return {
            "location": self._location_record(location),
            "size": len(content),
        }

    def storage_file_delete(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        file_url = _required_text(payload, "file_url")
        return {
            "file_url": file_url,
            "deleted": bool(
                runtime.services.library.delete_file(file_url=file_url)
            ),
        }

    def cache_reload(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        del command
        cache = runtime.services.cache
        if cache is None:
            raise CoreDispatchError("Core has no configured cache.")
        cache.reload()
        return dict(runtime.services.describe()["cache"])

    def read_source_refresh(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        del command
        return {
            "refreshed": bool(runtime.services.refresh_read_source()),
            "source": type(runtime.services.read_source).__name__,
        }


def install_application_api(runtime: "CoreRuntime") -> CoreApplicationAPI:
    """Install the stable named application API and return its handler owner."""

    api = CoreApplicationAPI()
    api.install(runtime)
    return api


__all__ = [
    "CoreApplicationAPI",
    "install_application_api",
]
