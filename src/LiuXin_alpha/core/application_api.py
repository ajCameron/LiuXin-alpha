"""
Adapt named Core application requests to composed database, read-source, Catalog, metadata, cache, and storage services.

Handlers normalize request fields and shape receipts while retaining subsystem
ownership of validation and transactional behavior. This adapter adds no enclosing
transaction or authorization layer; readback, projection, or reconciliation can fail
after writes. Direct calls bypass runtime dispatch locking/events and final wire
encoding. Cache query failures are not retried against the database, and raw-row
administration is deliberately separate from semantic Catalog operations.
"""

# pyright: reportImportCycles=false

from __future__ import annotations

import base64

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID

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
    """
    Declare a public payload field for endpoint introspection without validating request values.

    Example:
        >>> _field("table", required=True, field_type="string").required
        True


    :param name: Public request key described by the declaration.
    :param required: Whether clients should see the field as mandatory.
    :param field_type: Optional transport-facing type label, not an executable validator.
    :param description: Human-readable explanation retained in the field metadata.
    :return: New payload-field description containing the supplied values.
    """
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
        description=description,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    """
    Shallow-copy a Mapping payload, treating missing or None payload as an empty request.

    Example:
        >>> from types import SimpleNamespace
        >>> _payload(SimpleNamespace(payload={"table": "works"}))
        {'table': 'works'}


    :param envelope: Object whose optional payload attribute carries request data.
    :return: New dictionary retaining original keys and references to nested values.
    :raises CoreDispatchError: If a non-None payload is not a Mapping.
    """
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
    """
    Stringify and strip a required field, optionally checking exact case-sensitive membership in choices.

    Explicit None becomes "None". Choice membership can consume a one-shot iterable;
    the error lists whatever choices remain when it subsequently iterates them.

    Example:
        >>> _required_text({"level": " work "}, "level", choices=("work", "item"))
        'work'


    :param payload: Mapping containing the requested field.
    :param name: Exact key to retrieve and identify in validation errors.
    :param choices: Optional allowed values checked without case normalization; None disables membership validation.
    :return: Nonempty stripped text, unchanged after optional membership validation.
    :raises CoreDispatchError: If text is empty/missing or not among the supplied choices.
    """
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
    """
    Require a present non-boolean value convertible by int, without imposing a numeric range.

    Fractional numeric inputs may truncate. Every ordinary conversion exception,
    including overflow or None conversion, is chained into a dispatch error.

    Example:
        >>> _required_int({"row_id": "7"}, "row_id")
        7


    :param payload: Request mapping supplying an integer-convertible field.
    :param name: Required field name, also used in absence/conversion error messages.
    :return: Converted integer, including zero or negative values.
    :raises CoreDispatchError: If the field is absent, bool, or int conversion raises Exception.
    """
    if name not in payload:
        raise CoreDispatchError("`{}` is required.".format(name))
    value = payload[name]
    if isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except Exception as exc:
        raise CoreDispatchError("`{}` must be an integer.".format(name)) from exc


def _optional_int(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: int | None,
    minimum: int = 0,
) -> int | None:
    """
    Convert an optional non-boolean integer field and enforce its inclusive minimum, preserving None.

    Missing values use default; explicit None bypasses conversion and the minimum.
    Numeric fractions may truncate, and all ordinary conversion errors are wrapped.

    Example:
        >>> _optional_int({}, "limit", default=100), _optional_int({"limit": None}, "limit", default=100)
        (100, None)


    :param payload: Request mapping carrying an optional field.
    :param name: Exact field key used for lookup and errors.
    :param default: Value used only when the key is absent, subject to the same conversion/minimum rules.
    :param minimum: Inclusive lower bound for non-None converted values; defaults to zero.
    :return: None or an integer at least minimum.
    :raises CoreDispatchError: For bool values, conversion failures, or integers below minimum.
    """
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
        raise CoreDispatchError("`{}` must be >= {}.".format(name, minimum))
    return value


def _mapping(payload: Mapping[str, Any], name: str) -> dict[str, Any]:
    """
    Require a Mapping-valued request field and copy it shallowly without normalizing its keys or values.

    Example:
        >>> _mapping({"data": {"title": "A Book"}}, "data")
        {'title': 'A Book'}


    :param payload: Request mapping carrying the nested object.
    :param name: Required object-valued key.
    :return: New dictionary retaining nested value references.
    :raises CoreDispatchError: If the field is absent, None, or not a Mapping.
    """
    value = payload.get(name)
    if not isinstance(value, Mapping):
        raise CoreDispatchError("`{}` must be an object.".format(name))
    return dict(value)


def _row_values(row: Any) -> dict[str, Any]:
    """
    Copy row fields from Mapping, row_dict, or keys/__getitem__ support in that precedence order.

    Only the keys-based fallback stringifies keys, potentially overwriting earlier
    collisions. Attribute, key iteration, and item lookup failures propagate.

    Example:
        >>> from types import SimpleNamespace
        >>> _row_values(SimpleNamespace(row_dict={"work_id": 7}))
        {'work_id': 7}


    :param row: Mapping, row-like object, or keys/item-access object returned by a read source.
    :return: Shallow dictionary of row fields; nested values remain unconverted.
    :raises CoreDispatchError: If no supported row representation is available.
    """
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
            return {str(key): get_item(key) for key in raw_keys}
    raise CoreDispatchError(
        "Read source returned a non-row value: {}.".format(type(row).__name__)
    )


def _sort_value(value: Any) -> tuple[int, Any]:
    """
    Build an ascending sort key grouping bools, numbers, text, sequences, fallback objects, then None.

    Text uses casefolded value then original spelling as a tie-breaker. Sequences
    recurse and fallback objects use repr. No cycle guard or special nonfinite-
    number ordering is provided; reversing these keys also reverses type groups.

    Example:
        >>> sorted([None, "a", 3, False], key=_sort_value)
        [False, 3, 'a', None]


    :param value: Row-field value to translate into a type-ranked sorting key.
    :return: Rank and within-rank key; ordinary comparable values are ordered without mixing their native types.
    """
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
    """
    Join Mapping values and non-string Sequences into casefolded text, using empty text for None.

    Mapping keys are ignored. Bytes, sets, generators, and other values are
    stringified rather than traversed. No cycle or output-size limit is imposed.

    Example:
        >>> _flatten_text({"title": "Straße", "year": 2026})
        'strasse 2026'


    :param value: Scalar or nested row data to search textually.
    :return: Casefolded text with spaces between recursively flattened container values.
    """
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value).casefold()


class CoreApplicationAPI:
    """
    Register application-level read and mutation handlers over services already composed by Core.

    The adapter owns no database/cache/library lifecycle and caches no handler state.
    Subsystems retain semantic transaction and policy responsibilities; successful
    mutation calls can be followed by failing readback or cache reconciliation.

    Example:
        >>> adapter = CoreApplicationAPI()
        >>> adapter.install(runtime)  # doctest: +SKIP
    """

    def install(self, runtime: "CoreRuntime") -> None:
        """
        Register 21 application queries followed by 25 commands with explicit transport metadata.

        Registration invokes no subsystem operation and performs no capability
        probes. Earlier bindings remain after a later registration error, and
        duplicate-name handling belongs to the supplied runtime.

        Example:
            >>> CoreApplicationAPI().install(runtime)  # doctest: +SKIP


        :param runtime: Registrar receiving schema/row/Catalog/metadata/cache/storage handlers, summaries, fields, and tags.
        :return: None after every registration succeeds; no resource ownership is transferred.
        """
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
            payload_fields=(_field("item_id", required=True, field_type="integer"),),
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
            payload_fields=(_field("refresh", field_type="boolean"),),
            tags=("storage", "read"),
        )
        query(
            "storage.files.list",
            self.storage_files_list,
            summary="List Core-managed Digital Assets with pagination.",
            payload_fields=(
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("storage", "read"),
        )
        query(
            "storage.file.locate",
            self.storage_file_locate,
            summary="Resolve one Digital Asset to a Core-managed Location.",
            payload_fields=(
                _field("asset_id", required=True, field_type="integer"),
                _field("store_uuid", field_type="string|null"),
            ),
            tags=("storage", "read"),
        )
        query(
            "storage.file.read",
            self.storage_file_read,
            summary="Read one Core-managed file as a wire-encoded byte value.",
            payload_fields=(
                _field("asset_id", required=True, field_type="integer"),
                _field("store_uuid", field_type="string|null"),
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
            payload_fields=(_field("store", required=True, field_type="object"),),
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
                _field("store_uuid", field_type="string|null"),
                _field("name", field_type="string|null"),
                _field("original_name", field_type="string|null"),
                _field("media_type", field_type="string|null"),
            ),
            tags=("storage", "files", "write"),
        )
        command(
            "storage.file.delete",
            self.storage_file_delete,
            summary="Delete one exact Replica and retain its tombstone.",
            payload_fields=(_field("replica_id", required=True, field_type="integer"),),
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
        """
        Resolve an allowlisted Catalog repository through for_name, falling back to named attributes only if unavailable.

        Resolver failures propagate without attribute fallback. Repository names
        are stripped but case-sensitive; arbitrary Catalog attributes are not accepted.

        Example:
            >>> repository = CoreApplicationAPI._repository(runtime, {"repository": "works"})  # doctest: +SKIP


        :param runtime: Runtime whose Catalog service exposes the repository registry.
        :param payload: Request mapping with a required allowlisted repository name.
        :return: Repository object resolved by the registry or compatible attribute-only adapter.
        :raises CoreDispatchError: If repository text is absent/blank or outside the allowed set.
        """
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
        """
        Copy a non-string Sequence request field to a tuple, using an empty tuple when absent.

        Explicit None, sets, and generators are rejected; bytearray remains a
        Sequence and is not excluded by the str/bytes check. Elements are not validated.

        Example:
            >>> CoreApplicationAPI._array({"notes": ["first", "second"]}, "notes")
            ('first', 'second')


        :param payload: Request mapping containing the optional array field.
        :param name: Exact field key used for lookup and validation errors.
        :return: Tuple retaining element identities and input order.
        :raises CoreDispatchError: If the present value is not a non-str/non-bytes Sequence.
        """
        raw = payload.get(name, ())
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise CoreDispatchError("`{}` must be an array.".format(name))
        return tuple(raw)

    @staticmethod
    def _catalog_candidate(
        payload: Mapping[str, Any],
        repository_name: str,
    ) -> Any:
        """
        Build an identifier or metadata matching candidate from shallow-copied payload data and provenance hints.

        Identifier repositories require stripped identifier_type/value text and
        pass normalised_value through unchanged. Other repositories retain the
        candidate mapping as metadata data. Source text is not stripped; hints
        defaults empty but explicit None is invalid. No matching or persistence occurs.

        Example:
            >>> candidate = CoreApplicationAPI._catalog_candidate({"candidate": {"title": "A Book"}}, "works")
            >>> candidate.data["title"]
            'A Book'


        :param payload: Request with required Mapping candidate, optional source, and optional Mapping hints.
        :param repository_name: identifiers/item_identifiers selects IdentifierCandidate; other names select MetadataCandidate.
        :return: Typed candidate with supplied evidence, without repository-driven normalization.
        :raises CoreDispatchError: If candidate/hints or required identifier fields fail validation.
        """
        from LiuXin_alpha.catalog.api.common import (
            IdentifierCandidate,
            MetadataCandidate,
        )

        candidate_data = _mapping(payload, "candidate")
        source = None if payload.get("source") is None else str(payload.get("source"))
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
        """
        Describe table columns, heuristic ID, view status, optional relationships, and the adapter's advisory write flag.

        Heading reads must succeed. View lookup errors default to non-view; ID
        lookup is attempted only with an ID-like heading and falls back to the first
        such heading in source order. Relationship errors yield empty. Writable is
        false only for detected views or database_version/languages, not a backend
        permission check or a guard automatically applied by mutation handlers.

        Example:
            >>> schema = CoreApplicationAPI._schema_for_table(runtime, "works", include_relations=False)  # doctest: +SKIP


        :param runtime: Runtime whose database and driver wrapper supply schema introspection.
        :param table: Table name passed through without independent existence validation here.
        :param include_relations: Whether to call relationship introspection; false returns an empty related_tables list.
        :return: Table description with ordered columns, ID hint, flags, write-block explanation, and sorted relationship names.
        """
        database = runtime.services.database
        wrapper = database.driver_wrapper
        columns = [str(value) for value in database.get_column_headings(table)]
        try:
            is_view = bool(wrapper.is_view(table))
        except Exception:
            is_view = False
        id_column: str | None = None
        obvious_ids = [
            column for column in columns if column == "id" or column.endswith("_id")
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
        write_block_reason: str | None = None
        if is_view:
            write_block_reason = "Views and compatibility surfaces are read-only."
        elif table in {"database_version", "languages"}:
            write_block_reason = (
                "This table is managed reference data and is read-only."
            )
        return {
            "name": table,
            "columns": columns,
            "id_column": id_column,
            "is_view": is_view,
            "writable": write_block_reason is None,
            "write_block_reason": write_block_reason,
            "related_tables": related_tables,
            "relations_included": include_relations,
        }

    def schema_tables(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Describe every database-advertised table in sorted name order without loading relationship metadata.

        Names are stringified but not deduplicated. A failure describing any table
        aborts the result; no read-source capability check is performed separately.

        Example:
            >>> result = CoreApplicationAPI().schema_tables(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying database table enumeration and schema introspection.
        :param query: Query envelope, ignored; no filters or include-relations option is consumed.
        :return: Ordered tables descriptions and count of enumerated names.
        """
        del query
        tables = sorted(str(value) for value in runtime.services.database.get_tables())
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
        """
        Require a database-advertised table name and describe its columns and relationships.

        Example:
            >>> schema = CoreApplicationAPI().schema_table(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying the database's table names and schema methods.
        :param query: Query with required stripped, case-sensitive table text.
        :return: Single schema description including relationship introspection and advisory writability.
        :raises CoreDispatchError: If table text is absent/blank or not among advertised names.
        """
        table = _required_text(_payload(query), "table")
        tables = {str(value) for value in runtime.services.database.get_tables()}
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
        """
        Project row values into a table/row_id/values record, preserving the ID column even with a field projection.

        ID lookup errors fall back to the first ID-like row key, then literal id.
        Missing projected fields become None. Exposed row_id is int-converted when
        present, while its value inside values remains the original object; malformed
        or textual nonnumeric IDs therefore raise during record construction.

        Example:
            >>> from types import SimpleNamespace
            >>> runtime = SimpleNamespace(services=SimpleNamespace(database=object()))
            >>> CoreApplicationAPI._row_record(runtime, "works", {"id": "7", "title": "A Book"}, projection=("title",))["row_id"]
            7


        :param runtime: Runtime providing optional driver ID-column introspection.
        :param table: Table label and driver introspection argument.
        :param row: Value accepted by _row_values.
        :param projection: Field names to retain; empty keeps all fields and nonempty always retains/adds the ID column.
        :return: New record with integer-or-None row_id and shallow raw/projected values.
        """
        values = _row_values(row)
        try:
            id_column = str(
                runtime.services.database.driver_wrapper.get_id_column(table)
            )
        except Exception:
            id_column = next(
                (key for key in values if str(key) == "id" or str(key).endswith("_id")),
                "id",
            )
        if projection:
            projected = {str(name): values.get(str(name)) for name in projection}
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
        """
        Read one row through the selected read source and project it, treating None as ordinary absence.

        complete is always True. The source label depends on whether a cache is
        configured, not on a fresh measurement of where this particular read ran.

        Example:
            >>> result = CoreApplicationAPI().rows_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing read-source row lookup, schema introspection, and optional cache identity.
        :param query: Query with required table text and integer-convertible row_id, excluding bool.
        :return: record (possibly None), complete=True, and cache/database source label.
        """
        payload = _payload(query)
        table = _required_text(payload, "table")
        row_id = _required_int(payload, "row_id")
        row = runtime.services.read_source.get_row_from_id(table, row_id)
        return {
            "record": (None if row is None else self._row_record(runtime, table, row)),
            "complete": True,
            "source": ("cache" if runtime.services.cache is not None else "database"),
        }

    @staticmethod
    def _cache_query(payload: Mapping[str, Any]) -> Any:
        """
        Normalize a request into the shared structured-query records without executing it.

        Array fields require non-str/non-bytes Sequences. Predicate operator names
        are case-sensitive; predicate values pass to CachePredicate validation.
        Relation IDs reject bool and use int(str(value)), unlike scalar ID fields.
        Sort strings remain unstripped; object sort fields are stripped and their
        ascending flag is truth-tested. Projection/text-field entries are stringified.
        Offset defaults to zero; limit defaults to None (unbounded), with zero
        requesting counts only. Cache record validation errors are not wrapped.

        Example:
            >>> spec = CoreApplicationAPI._cache_query({"table": "works", "limit": 0})
            >>> spec.table, spec.offset, spec.limit
            ('works', 0, 0)


        :param payload: Mapping with required table and optional predicates, relation, text, text_fields, sort, projection, offset, and limit.
        :return: CacheQuery with tuple-valued collections and nonnegative paging bounds.
        :raises CoreDispatchError: For invalid request shapes, integer fields, or unknown operator names.
        :raises TypeError: If a shared query-record constructor rejects a value's type.
        :raises ValueError: If a shared query-record constructor rejects a value's content.
        """
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
                raise CoreDispatchError("Every `predicates` entry must be an object.")
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
                    raise CoreDispatchError("`relation.ids` values must be integers.")
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
        """
        Evaluate one structured predicate against a database-side field value.

        EQ tests membership for non-string Sequences; IN tests any element against
        the expected values. Text operations casefold flattened values. IS_NULL
        inverts only for the literal False, not every falsey value. Ordered
        comparisons treat TypeError as a non-match; other comparison failures
        propagate. Unknown operators return False.

        Example:
            >>> from LiuXin_alpha.caches.api import CacheFilterOperator, CachePredicate
            >>> predicate = CachePredicate("tags", CacheFilterOperator.EQ, "history")
            >>> CoreApplicationAPI._matches(["history", "science"], predicate)
            True


        :param value: Raw row field, including None for a missing field.
        :param predicate: Validated predicate exposing operator and expected value.
        :return: Whether this field satisfies the predicate under database-side matching rules.
        """
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
        """
        Materialize, filter, order, and page rows when the read source has no structured-query method.

        Identifier-less views use the database driver directly; other tables use
        the selected read source. Relation filtering unions linked row IDs across
        existing targets. Predicates are ANDed, and each whitespace-separated text
        term must occur in some selected field. Stable sort priorities follow request
        order; absent sorting uses numeric row IDs, with missing IDs last.

        A zero limit skips sorting and record projection, not scanning or filtering.
        All matching rows are counted before paging. complete=True describes query
        coverage rather than the last page; generation=0 and source="database" are
        fixed adapter labels. This path adds no scan cap or enclosing read transaction.

        Example:
            >>> spec = CoreApplicationAPI._cache_query({"table": "works", "limit": 0})
            >>> counts = CoreApplicationAPI()._database_query(runtime, spec)  # doctest: +SKIP


        :param runtime: Runtime supplying schema, raw driver access, and row/relation read methods.
        :param spec: Normalized CacheQuery controlling filtering, projection, sorting, and paging.
        :return: Records, pre-page total_count, paging bounds, and fixed completeness/source metadata.
        """
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
                runtime.services.database.driver_wrapper.get_all_rows(spec.table)
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
                if self._row_record(runtime, spec.table, row)["row_id"] in related_ids
            ]

        materialized = [(row, _row_values(row)) for row in rows]
        for predicate in spec.predicates:
            materialized = [
                (row, values)
                for row, values in materialized
                if self._matches(values.get(predicate.field), predicate)
            ]

        terms = tuple(term for term in str(spec.text).casefold().split() if term)
        if terms:
            fields = tuple(spec.text_fields)
            if not fields:
                fields = tuple(
                    str(value) for value in source.get_column_headings(spec.table)
                )
            materialized = [
                (row, values)
                for row, values in materialized
                if all(
                    any(term in _flatten_text(values.get(field)) for field in fields)
                    for term in terms
                )
            ]

        # Count-only queries return no row records. Ordering would needlessly
        # coerce identifiers, including valid text keys in bookkeeping tables.
        if spec.limit != 0:
            for sort_spec in reversed(tuple(spec.sort)):
                materialized.sort(
                    key=lambda item: _sort_value(item[1].get(sort_spec.field)),
                    reverse=not sort_spec.ascending,
                )
            if not spec.sort:
                materialized.sort(
                    key=lambda item: (
                        self._row_record(runtime, spec.table, item[0])["row_id"]
                        is None,
                        self._row_record(runtime, spec.table, item[0])["row_id"] or 0,
                    )
                )

        total_count = len(materialized)
        end = None if spec.limit is None else spec.offset + spec.limit
        visible = materialized[spec.offset : end]
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
        """
        Run a structured read without retrying failed cache queries in the database.

        Known cache table/query capability limits use ``read_query_unavailable``;
        other failures retain their normal Core error path. Database-backed
        count-only queries skip ordering and row projection. Only absence of a
        callable query_cache selects that database path. Cache responses must be
        CacheQueryResult instances; their completeness and generation are preserved,
        rather than inferred from page length.

        Example:
            >>> result = CoreApplicationAPI().rows_query(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the selected read source and database fallback primitives.
        :param query: Query carrying the structured payload accepted by _cache_query.
        :return: Records and query metadata, labelled cache or database according to the chosen execution path.
        :raises CoreDispatchError: If request validation fails, cache capability is unavailable, or its result type is invalid.
        """
        spec = self._cache_query(_payload(query))
        source = runtime.services.read_source
        query_cache = getattr(source, "query_cache", None)
        if not callable(query_cache):
            return self._database_query(runtime, spec)
        from LiuXin_alpha.caches.api import (
            CacheQueryResult,
            UnknownCacheTableError,
            UnsupportedCacheQueryError,
        )

        try:
            result = query_cache(spec)
        except (UnknownCacheTableError, UnsupportedCacheQueryError) as exc:
            raise CoreDispatchError(
                "The configured read source cannot serve this query.",
                code="read_query_unavailable",
                details={"table": spec.table, "reason": type(exc).__name__},
            ) from exc
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
        """
        List related rows and optionally raw link rows for one read-source row.

        Missing primary rows yield empty complete lists without a source label.
        type_filter applies to related rows but not to optional link_rows, which
        can therefore describe a broader set of links. No paging is applied.
        A populated result's source label reflects cache configuration, not a probe.

        Example:
            >>> links = CoreApplicationAPI().relations_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying row/relation reads and record projection.
        :param query: Query with table, row_id, related_table, optional type_filter, and truth-tested include_link_rows.
        :return: records, link_records, complete=True, and a source label when the primary row exists.
        """
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
            type_filter=(None if type_filter is None else str(type_filter)),
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
            "source": ("cache" if runtime.services.cache is not None else "database"),
        }

    def admin_row_delete_impact(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Ask the library to describe a raw row deletion's impact without deleting it.

        The sample limit defaults to three and must be nonnegative; explicit None
        becomes zero. Dependency discovery and report semantics belong to the library.

        Example:
            >>> impact = CoreApplicationAPI().admin_row_delete_impact(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose library supports deletion-impact inspection.
        :param query: Query containing table, row_id, and optional sample_limit.
        :return: Shallow dictionary copy of the library's impact report.
        """
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
                ),
            )
        )

    def catalog_entity_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Retrieve one entity through an allowlisted Catalog repository's optional get operation.

        The repository owns missing-entity behavior; this adapter does not replace
        a returned None with an error or require a concrete entity record type.

        Example:
            >>> result = CoreApplicationAPI().catalog_entity_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying the Catalog repository registry.
        :param query: Query with repository name and required integer-convertible entity_id.
        :return: Repository label and the entity value returned by get, possibly None.
        """
        payload = _payload(query)
        repository = self._repository(runtime, payload)
        return {
            "repository": _required_text(payload, "repository"),
            "entity": repository.get(_required_int(payload, "entity_id")),
        }

    def catalog_entity_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Request a repository-ordered entity page and materialize the returned iterable.

        Limit defaults to 100 and offset to zero, both nonnegative with no upper
        cap. Explicit None becomes zero, not an unbounded limit. The repository
        controls ordering and page semantics; no total or completeness is inferred.

        Example:
            >>> page = CoreApplicationAPI().catalog_entity_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the selected Catalog repository.
        :param query: Query with repository and optional limit/offset fields.
        :return: Repository name, entities list, and the bounds passed to list.
        """
        payload = _payload(query)
        repository = self._repository(runtime, payload)
        limit = int(_optional_int(payload, "limit", default=100) or 0)
        offset = int(_optional_int(payload, "offset", default=0) or 0)
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
        """
        Retrieve the Catalog bundle rooted at one Work, Expression, Manifestation, or Item.

        The exact lowercase level selects retrieval.bundles.for_<level>. Bundle
        shape, absence, and relationship loading remain the retriever's responsibility.

        Example:
            >>> bundle = CoreApplicationAPI().catalog_bundle_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose Catalog exposes WEMI bundle retrieval.
        :param query: Query with level and integer-convertible entity_id.
        :return: Retriever's bundle without an additional adapter projection.
        """
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
        """
        Request a Work-rooted graph with independently supplied descendant limits.

        Defaults are 100 Expressions, 500 Manifestations, and 1000 Items. Limits
        must be nonnegative; no additional upper cap is imposed. Explicit None
        reaches the non-None assertions rather than selecting the default. The
        retriever owns truncation and the interpretation of each limit.

        Example:
            >>> graph = CoreApplicationAPI().catalog_graph_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing Catalog graph retrieval.
        :param query: Query with work_id and optional max_expressions, max_manifestations, and max_items.
        :return: Graph returned by retrieval.graph.for_work, unchanged.
        :raises AssertionError: If a limit is explicitly None while assertions are enabled.
        """
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
        """
        Copy the Catalog's Item-summary projection into a plain dictionary.

        Example:
            >>> summary = CoreApplicationAPI().catalog_item_summary(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime exposing Catalog retrieval projections.
        :param query: Query containing a required integer-convertible item_id.
        :return: Shallow Item summary with projection-defined fields and nested values.
        """
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
        """
        Obtain a repository's explained candidate-match decision without invoking creation.

        Titles are excluded. Expressions, Manifestations, Items, and Item identifiers
        require parent_id; Item identifier matching receives it as keyword item_id,
        while other scoped repositories receive it positionally. The decision is
        not collapsed to a boolean or an arbitrarily selected ID.

        Example:
            >>> decision = CoreApplicationAPI().catalog_match(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying the matchable Catalog repository.
        :param query: Query with repository, Mapping candidate, optional source/hints, and scope-dependent parent_id.
        :return: Repository's matching decision, including its evidence and ambiguity information.
        """
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
        """
        Resolve an Agent name and optional role using the Agent repository's matching policy.

        Name is required and stripped. Role is None or stringified without stripping;
        this adapter does not independently choose among ambiguous matches.

        Example:
            >>> result = CoreApplicationAPI().catalog_agent_resolve(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose Catalog supplies the Agent repository.
        :param query: Query containing name and optional role.
        :return: Dictionary containing the repository's resolved agent value.
        """
        payload = _payload(query)
        return {
            "agent": runtime.services.catalog.repositories.agents.resolve(
                name=_required_text(payload, "name"),
                role=(
                    None if payload.get("role") is None else str(payload.get("role"))
                ),
            )
        }

    def catalog_annotations_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        List all Item annotations matching optional user and kind filters without paging.

        User IDs are None or nonnegative integers, excluding bool. Kind must
        already be str or None and is not stripped. The repository result must be
        sized as well as iterable, because count is taken from that original value.

        Example:
            >>> annotations = CoreApplicationAPI().catalog_annotations_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying the annotations repository.
        :param query: Query with required item_id and optional user_id/kind filters.
        :return: Filter values, materialized annotations, and the repository result's count.
        :raises CoreDispatchError: If IDs or the kind field fail adapter validation.
        """
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
        annotations = runtime.services.catalog.repositories.annotations.list_for_item(
            item_id,
            user_id=user_id,
            kind=kind,
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
        """
        Construct a fresh WEMI hydrator over the selected read source and load one Item's metadata.

        Example:
            >>> metadata = CoreApplicationAPI._hydrated_metadata(runtime, 7)  # doctest: +SKIP


        :param runtime: Runtime providing the application read source used for hydration.
        :param item_id: Item identifier passed unchanged to the hydrator.
        :return: Hydrated LiuXin WEMI metadata container; loading failures propagate.
        """
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
        """
        Hydrate Item metadata and expose its mapping with optional related and legacy fields.

        Both inclusion switches default to True and use truth-value conversion,
        not strict boolean validation. Hydration precedes projection.

        Example:
            >>> metadata = CoreApplicationAPI().metadata_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose selected read source supplies WEMI metadata.
        :param query: Query with item_id and optional include_related/include_legacy switches.
        :return: Shallow dictionary copy of the metadata container's selected mapping.
        """
        payload = _payload(query)
        metadata = self._hydrated_metadata(
            runtime,
            _required_int(payload, "item_id"),
        )
        return dict(
            metadata.to_mapping(
                include_related=bool(payload.get("include_related", True)),
                include_legacy=bool(payload.get("include_legacy", True)),
            )
        )

    def metadata_opf_export(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Hydrate an Item and serialize its metadata to OPF bytes without writing a file.

        default_lang is passed as None or unstripped text to the serializer. Content
        remains bytes here; transport encoding belongs to the outer Core wire layer.

        Example:
            >>> opf = CoreApplicationAPI().metadata_opf_export(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the metadata hydration read source.
        :param query: Query with item_id and optional default_lang for OPF serialization.
        :return: Item ID and serialized OPF content; hydration/serialization errors propagate.
        """
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
        """
        Return the cache section of the composed services' description without reloading the cache.

        Describing all services may resolve other lazy service metadata; this is
        not a direct cache-only capability probe.

        Example:
            >>> status = CoreApplicationAPI().cache_status(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose services supply the description mapping.
        :param query: Unused query envelope; no payload fields are consumed.
        :return: Shallow cache-description dictionary, including unconfigured-cache status.
        """
        del query
        return dict(runtime.services.describe()["cache"])

    @staticmethod
    def _semantic_receipt(
        runtime: "CoreRuntime",
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        """
        Reconcile read-side state after a semantic write and return the service-shaped receipt.

        This is a post-operation step, not a transaction around the preceding
        mutation. Reconciliation failure can therefore follow an already applied write.

        Example:
            >>> result = CoreApplicationAPI._semantic_receipt(runtime, {"entity_id": 7})  # doctest: +SKIP


        :param runtime: Runtime whose services own cache/read-source reconciliation.
        :param receipt: Mutation outcome to pass to the service reconciler.
        :return: Reconciled receipt returned by runtime.services.reconcile.
        """
        return runtime.services.reconcile(receipt)

    def catalog_entity_create(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Create an entity through its Catalog repository, read it back, and reconcile read-side state.

        The repository validates entity data. ID conversion, required readback, or
        reconciliation can fail after creation; the adapter adds no rollback scope.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_entity_create(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying repository creation and service reconciliation.
        :param command: Command with allowlisted repository name and Mapping data.
        :return: Reconciled receipt containing repository name, integer entity_id, and required entity readback.
        """
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
        """
        Reuse or create a candidate according to repository policy, then read back and reconcile.

        Titles are excluded. Parent-scoped repositories, including Item identifiers,
        receive parent_id as the first argument. Readback and reconciliation run
        even when an existing entity was reused; no created-versus-matched flag is
        inferred. Repository ambiguity/conflict errors remain visible.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_entity_match_or_create(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing matching repositories and post-operation reconciliation.
        :param command: Command with repository, candidate, optional source/hints, and parent_id when scoped.
        :return: Reconciled repository/entity_id/entity receipt without a creation-status claim.
        """
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
        """
        Create a Person Agent with optional details, identifiers, languages, and notes, then reconcile.

        Data and identifier entries must be Mappings; details may also be None.
        Optional arrays default empty but reject explicit None. Language IDs use
        plain int conversion, accepting bool unlike ordinary request ID helpers.
        Notes are passed through without element validation. Required Agent readback
        and reconciliation follow the repository call outside any adapter transaction.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_agent_create_person(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying the Agent repository and reconciliation services.
        :param command: Command with data and optional details, identifiers, language_ids, and notes.
        :return: Reconciled receipt with integer agent_id, required Agent readback, and kind="person".
        """
        payload = _payload(command)
        details = payload.get("details")
        if details is not None and not isinstance(details, Mapping):
            raise CoreDispatchError("`details` must be an object or null.")
        identifiers = self._array(payload, "identifiers")
        if any(not isinstance(value, Mapping) for value in identifiers):
            raise CoreDispatchError("Every `identifiers` entry must be an object.")
        agent_id = runtime.services.catalog.repositories.agents.create_person(
            _mapping(payload, "data"),
            details=None if details is None else dict(details),
            identifiers=tuple(dict(value) for value in identifiers),
            language_ids=tuple(
                int(value) for value in self._array(payload, "language_ids")
            ),
            notes=self._array(payload, "notes"),
        )
        return self._semantic_receipt(
            runtime,
            {
                "agent_id": int(agent_id),
                "agent": runtime.services.catalog.repositories.agents.require(agent_id),
                "kind": "person",
            },
        )

    def catalog_agent_create_organisation(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Create an Organisation Agent and optional parent relation, then read back and reconcile.

        Mapping/array handling matches Person creation, with additional synopses.
        Parent ID may be None; otherwise it rejects bool and uses int conversion.
        relation_type defaults to "imprint_of" but explicit None becomes "None";
        relation_note is None or unstripped text. Language IDs use plain int, while
        note/synopsis entries remain unvalidated by this adapter.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_agent_create_organisation(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing Agent creation, required readback, and reconciliation.
        :param command: Command with data and optional details, parent_id, relation_type, relation_note, identifiers, language_ids, notes, and synopses.
        :return: Reconciled receipt with agent_id, Agent readback, and kind="organisation"; later failures do not roll back creation here.
        """
        payload = _payload(command)
        details = payload.get("details")
        if details is not None and not isinstance(details, Mapping):
            raise CoreDispatchError("`details` must be an object or null.")
        identifiers = self._array(payload, "identifiers")
        if any(not isinstance(value, Mapping) for value in identifiers):
            raise CoreDispatchError("Every `identifiers` entry must be an object.")
        agent_id = runtime.services.catalog.repositories.agents.create_organisation(
            _mapping(payload, "data"),
            details=None if details is None else dict(details),
            parent_id=(
                None
                if payload.get("parent_id") is None
                else _required_int(payload, "parent_id")
            ),
            relation_type=str(payload.get("relation_type", "imprint_of")),
            relation_note=(
                None
                if payload.get("relation_note") is None
                else str(payload.get("relation_note"))
            ),
            identifiers=tuple(dict(value) for value in identifiers),
            language_ids=tuple(
                int(value) for value in self._array(payload, "language_ids")
            ),
            notes=self._array(payload, "notes"),
            synopses=self._array(payload, "synopses"),
        )
        return self._semantic_receipt(
            runtime,
            {
                "agent_id": int(agent_id),
                "agent": runtime.services.catalog.repositories.agents.require(agent_id),
                "kind": "organisation",
            },
        )

    def catalog_entity_update(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Apply a repository-defined entity update, read the resulting entity, and reconcile.

        The update method's return value is ignored; required readback supplies
        the receipt. Readback or reconciliation can fail after the update succeeds.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_entity_update(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime exposing the selected Catalog repository and reconciliation.
        :param command: Command with repository, entity_id, and Mapping data describing the update.
        :return: Reconciled repository/entity_id/entity receipt built from post-update readback.
        """
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
        """
        Require an entity before repository deletion and retain that pre-delete value in the receipt.

        deleted contains the former entity, not a boolean. The delete method's
        return value is ignored, and reconciliation follows deletion without an
        adapter-owned transaction or additional confirmation field.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_entity_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing required entity lookup, deletion, and reconciliation.
        :param command: Command identifying an allowlisted repository and entity_id.
        :return: Reconciled receipt containing repository, entity_id, and the pre-delete entity as deleted.
        """
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
        """
        Delegate creation of a WEMI stack to the semantic writer and reconcile its assigned IDs.

        Work, Expression, and Manifestation mappings are required even when work_id
        selects an existing Work. Items default empty and must be a non-string
        Sequence of Mappings. Origin is None or unstripped text. The writer owns
        atomicity; converting its returned IDs and reconciling happen afterwards.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_wemi_create(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime exposing the Catalog's semantic WEMI writer.
        :param command: Command with work, expression, manifestation, optional items, origin, and work_id.
        :return: Reconciled integer work_id/expression_id/manifestation_id and list of integer item_ids.
        """
        payload = _payload(command)
        raw_items = payload.get("items", ())
        if not isinstance(raw_items, Sequence) or isinstance(
            raw_items,
            (str, bytes),
        ):
            raise CoreDispatchError("`items` must be an array.")
        if any(not isinstance(item, Mapping) for item in raw_items):
            raise CoreDispatchError("Every `items` entry must be an object.")
        created = runtime.services.catalog.mutations.writer.create_wemi_stack(
            work=_mapping(payload, "work"),
            expression=_mapping(payload, "expression"),
            manifestation=_mapping(payload, "manifestation"),
            items=tuple(dict(item) for item in raw_items),
            origin=(
                None if payload.get("origin") is None else str(payload.get("origin"))
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
        """
        Link two WEMI entities using semantic writer policy and reconcile its receipt.

        Levels must be exact lowercase WEMI names; legal adjacency and endpoint
        existence belong to the writer. primary requires bool or None, priority
        is None or a nonnegative integer, and origin is None or unstripped text.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_wemi_link(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying semantic relationship mutation and reconciliation.
        :param command: Command with parent_level/parent_id, child_level/child_id, and optional primary, priority, and origin.
        :return: Shallow copy of the writer's link receipt passed through service reconciliation.
        """
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
            origin=(None if payload.get("origin") is None else str(payload["origin"])),
        )
        return self._semantic_receipt(runtime, dict(receipt))

    def catalog_wemi_unlink(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Request semantic WEMI unlinking and reconcile the writer's unmodified outcome value.

        The adapter validates level names and integer-convertible endpoint IDs;
        relationship legality, absence, and mutation behavior belong to the writer.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_wemi_unlink(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing semantic relationship removal and reconciliation.
        :param command: Command specifying parent_level/parent_id and child_level/child_id.
        :return: Reconciled endpoint identifiers and writer result as unlinked, without boolean coercion.
        """
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
        """
        Attach metadata through the WEMI writer and reconcile after the call returns.

        Merge/attachment policy belongs to the writer. Its return value is ignored;
        attached=True means the call returned, not that a separate readback verified
        each requested field or established that anything changed.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_metadata_attach(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime exposing the semantic metadata writer and reconciliation.
        :param command: Command with WEMI level, entity_id, and Mapping data to attach.
        :return: Reconciled level/entity_id receipt with attached=True after successful delegation.
        """
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
        """
        Replace metadata according to the WEMI writer's field policy, then reconcile.

        Replacement semantics and mutation scope belong to the writer. Its return
        value is ignored, with no field readback before reporting replaced=True.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_metadata_replace(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying semantic metadata replacement and reconciliation.
        :param command: Command with WEMI level, entity_id, and Mapping replacement data.
        :return: Reconciled level/entity_id receipt with replaced=True after the writer returns.
        """
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
        """
        Merge source into target at one WEMI level using the semantic writer, then reconcile.

        The writer owns conflict, self-merge, and source-retention policies. Its
        return value is ignored; merged=True reports completed delegation rather
        than an independent inspection of the resulting entities.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_metadata_merge(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose Catalog supplies entity merging and read-side reconciliation.
        :param command: Command with level and required integer-convertible source_id/target_id.
        :return: Reconciled source/target identifiers and merged=True after the writer call.
        """
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
        """
        Prefer a configured cache as the field-write facade, otherwise use the Catalog.

        Selection depends only on cache being non-None, not its capabilities or
        health. Failures on a selected cache must not trigger a Catalog retry.

        Example:
            >>> from types import SimpleNamespace
            >>> catalog = object()
            >>> runtime = SimpleNamespace(services=SimpleNamespace(cache=None, catalog=catalog))
            >>> target, cache_aware = CoreApplicationAPI._writer_target(runtime)
            >>> target is catalog, cache_aware
            (True, False)


        :param runtime: Runtime exposing cache and Catalog services.
        :return: Chosen facade and whether it is the configured cache.
        """
        if runtime.services.cache is not None:
            return runtime.services.cache, True
        return runtime.services.catalog, False

    def catalog_field_write(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Forward a multi-value field write to the cache-aware facade or uncached Catalog.

        args must be a non-string Sequence and kwargs a Mapping. force_refresh is
        truth-tested; destination_owned passes through unchanged. Keyword collisions
        with those explicit arguments raise TypeError rather than overriding them.
        No separate service reconciliation runs: the reconciled flag records facade
        selection, not an independent refresh check.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_field_write(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying the selected field writer and cache-configuration flag.
        :param command: Command with src_table, dst_column, optional args/kwargs, force_refresh, and destination_owned.
        :return: Writer result and cache configured/reconciled flags; backend failures propagate.
        """
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
        """
        Forward one source/destination field pair to the selected cache-aware or Catalog writer.

        dst_value must be present but may be None. src_id is passed through and
        defaults to None here despite its required public metadata declaration.
        kwargs must be a Mapping; collisions with explicit writer keywords raise.
        Cache flags describe selected routing, with no separate reconciliation call.

        Example:
            >>> receipt = CoreApplicationAPI().catalog_field_write_one(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying the selected field-write facade.
        :param command: Command with src_table, dst_column, dst_value, and optional src_id, kwargs, force_refresh, and destination_owned.
        :return: write_one result and cache configured/reconciled routing flags.
        :raises CoreDispatchError: If dst_value is absent or request text/kwargs validation fails.
        """
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
        """
        Create a raw database Row from idless values, project it, and reconcile read-side state.

        This is administrative row creation, not a Catalog entity operation.
        Database validation applies; the schema description's advisory writable
        flag is not checked here. Projection or reconciliation may fail after creation.

        Example:
            >>> receipt = CoreApplicationAPI().admin_row_create(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing the database, ID introspection, and reconciliation.
        :param command: Command with table text and Mapping values for Row.from_idless_row_dict.
        :return: Reconciled table and newly created row record.
        """
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
        """
        Delegate raw field updates to the library and reconcile its returned row value.

        The library result is placed directly in record.values without _row_record
        projection. Library validation and update policy remain authoritative;
        reconciliation is a separate post-write operation.

        Example:
            >>> receipt = CoreApplicationAPI().admin_row_update(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose library updates raw row fields.
        :param command: Command with table, row_id, and Mapping updates.
        :return: Reconciled table/row_id and a record wrapping the library's update result.
        """
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
        """
        Delegate raw row deletion to the library and reconcile its returned deletion outcome.

        No confirmation field or separate impact query is required by this
        handler. Dependency handling and deletion policy belong to the library;
        reconciliation failure does not introduce an adapter-level rollback.

        Example:
            >>> receipt = CoreApplicationAPI().admin_row_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing library deletion and post-write reconciliation.
        :param command: Command with required table and integer-convertible row_id.
        :return: Reconciled identifiers and the library result as deleted, without coercion.
        """
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
        """
        Resolve both raw endpoint rows, delegate linking to the database, and reconcile.

        Both rows must exist. Priority defaults to "highest", including explicit
        None, and is otherwise passed through without the advertised integer-type
        check. Type and extra Mapping values pass through too; collisions with
        explicit keywords raise TypeError. linked=True means interlink_rows returned,
        not that an additional existence check verified a newly created link.

        Example:
            >>> receipt = CoreApplicationAPI().admin_relation_link(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying raw database row lookup, linking, and reconciliation.
        :param command: Command with table/row_id, related_table/related_row_id, and optional priority, type, and Mapping extra.
        :return: Reconciled endpoint identifiers and linked=True after successful delegation.
        :raises CoreDispatchError: If either endpoint is absent or request validation fails.
        """
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
            raise CoreDispatchError("Both relationship endpoint rows must exist.")
        extra = payload.get("extra", {})
        if not isinstance(extra, Mapping):
            raise CoreDispatchError("`extra` must be an object.")
        priority = payload.get("priority", "highest")
        if priority is None:
            priority = "highest"
        database.interlink_rows(
            primary_row=primary,
            secondary_row=secondary,
            priority=priority,
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
        """
        Require both raw endpoint rows and delegate their unlinking before reconciliation.

        The database unlink result is ignored; unlinked=True reports that the call
        returned, not a removed-link count or an independent absence check.

        Example:
            >>> receipt = CoreApplicationAPI().admin_relation_unlink(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime supplying database row lookup, unlinking, and reconciliation.
        :param command: Command with table/row_id and related_table/related_row_id.
        :return: Reconciled endpoint identifiers and unlinked=True after successful delegation.
        :raises CoreDispatchError: If either endpoint is absent or request validation fails.
        """
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
            raise CoreDispatchError("Both relationship endpoint rows must exist.")
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
    def _store_record(store: Any, *, refresh: bool) -> dict[str, Any]:
        """
        Snapshot a Store's configuration reference and status for an application response.

        Configuration is fetched before status. Errors propagate; neither value
        is converted to wire form or copied deeply in this helper.

        Example:
            >>> record = CoreApplicationAPI._store_record(store, refresh=False)  # doctest: +SKIP


        :param store: Store exposing configuration and a status(refresh=...) method.
        :param refresh: Whether to request refreshed status from the Store implementation.
        :return: UUID/name/kind/root labels plus the original configuration and returned status objects.
        """
        configuration = store.configuration
        status = store.status(refresh=refresh)
        return {
            "store_uuid": configuration.store_uuid,
            "store_name": configuration.store_name,
            "store_kind": configuration.store_kind,
            "store_root_uri": configuration.store_root_uri,
            "configuration": configuration,
            "status": status,
        }

    @staticmethod
    def _location_record(library: Any, location: Any) -> dict[str, Any]:
        """
        Describe a storage location using independent best-effort Store-name and stat lookups.

        Ordinary name-lookup errors yield store_name=None; ordinary stat errors
        yield exists=False and unknown size/digest/version. Thus exists=False is
        not proof of absence. Attributes on a returned non-None stat object are
        accessed outside the error handler and may still raise.

        Example:
            >>> from types import SimpleNamespace
            >>> from unittest.mock import Mock
            >>> library = Mock()
            >>> library.get_store.side_effect = LookupError("unavailable")
            >>> library.storage.stat.return_value = None
            >>> location = SimpleNamespace(store_ref="store-1", key="book.epub")
            >>> record = CoreApplicationAPI._location_record(library, location)
            >>> record["store_name"], record["exists"], record["size"]
            (None, False, None)


        :param library: Library supporting Store resolution and storage.stat(location).
        :param location: Object exposing store_ref and key; these attributes are not validated or suppressed.
        :return: Location labels and best-effort file observations, without reading file content.
        """
        try:
            store = library.get_store(location.store_ref)
            store_name = store.configuration.store_name
        except Exception:
            store_name = None
        try:
            info = library.storage.stat(location)
        except Exception:
            info = None
        return {
            "store_uuid": location.store_ref,
            "store_name": store_name,
            "key": location.key,
            "exists": info is not None,
            "size": None if info is None else info.size,
            "digest": None if info is None else info.digest,
            "version": None if info is None else info.version,
        }

    @staticmethod
    def _store_argument(library: Any, value: Any) -> Any:
        """
        Resolve an optional Store UUID, falling back to a unique exact name on type/value errors.

        None or empty text selects no Store. UUID conversion and get_store share
        the same TypeError/ValueError handler; other lookup errors propagate without
        a name retry. Names are stringified but not stripped or case-normalized.
        Multiple name matches are rejected just like no match.

        Example:
            >>> CoreApplicationAPI._store_argument(object(), None) is None
            True


        :param library: Library exposing get_store(UUID) and iter_stores() for name fallback.
        :param value: Optional UUID-like value or exact Store name, not a numeric database row selector.
        :return: Resolved Store or None when no preference was supplied.
        :raises CoreDispatchError: If name fallback finds zero or multiple matching Stores.
        """
        if value in (None, ""):
            return None
        try:
            return library.get_store(UUID(str(value)))
        except (TypeError, ValueError):
            matches = [
                store
                for store in library.iter_stores()
                if store.configuration.store_name == str(value)
            ]
            if len(matches) == 1:
                return matches[0]
            raise CoreDispatchError(f"Unknown Store: {value!r}.")

    def storage_stores_list(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Describe all library Stores in iteration order, optionally refreshing each status.

        Status lookup is not best-effort: failure on one Store aborts the list.
        No sorting or pagination is added, and raw configuration/status values are
        retained for the outer wire layer.

        Example:
            >>> stores = CoreApplicationAPI().storage_stores_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose library enumerates configured Stores.
        :param query: Query with optional truth-tested refresh switch, defaulting to False.
        :return: Store description list and its count after all descriptions succeed.
        """
        payload = _payload(query)
        records = [
            self._store_record(
                store,
                refresh=bool(payload.get("refresh", False)),
            )
            for store in runtime.services.library.iter_stores()
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
        """
        Materialize all managed files and return an iteration-ordered page with the full count.

        Limit defaults to 100 and offset to zero, with nonnegative values and no
        upper cap. Explicit None becomes zero. A zero limit still enumerates all
        files before producing the count; no extra sorting or asset projection occurs.

        Example:
            >>> page = CoreApplicationAPI().storage_files_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose library provides iter_files().
        :param query: Query carrying optional limit and offset page bounds.
        :return: Raw file/asset values in files, pre-page total_count, limit, and offset.
        """
        payload = _payload(query)
        limit = int(_optional_int(payload, "limit", default=100) or 0)
        offset = int(_optional_int(payload, "offset", default=0) or 0)
        assets = list(runtime.services.library.iter_files())
        return {
            "files": [asset for asset in assets[offset : offset + limit]],
            "total_count": len(assets),
            "limit": limit,
            "offset": offset,
        }

    def storage_file_locate(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Locate one managed Asset with an optional Store preference and describe its selected location.

        Store selection accepts UUID or a unique exact name despite the store_uuid
        field label. Resolution errors propagate; subsequent location stat errors
        are represented by the helper's best-effort exists=False observation.

        Example:
            >>> result = CoreApplicationAPI().storage_file_locate(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime whose library resolves managed Assets and Store preferences.
        :param query: Query with required asset_id and optional store_uuid selector.
        :return: Dictionary containing the selected location's labels and best-effort stat fields.
        """
        payload = _payload(query)
        library = runtime.services.library
        store = self._store_argument(library, payload.get("store_uuid"))
        location = library.locate_file(
            _required_int(payload, "asset_id"),
            store=store,
        )
        return {
            "location": self._location_record(library, location),
        }

    def storage_file_read(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Locate a managed Asset, describe that location, and separately read its complete content.

        Location/stat and reading are separate observations, not an atomic snapshot.
        A failed stat can report exists=False even when the subsequent read succeeds.
        The adapter imposes no content-size cap and does not encode the returned bytes.

        Example:
            >>> result = CoreApplicationAPI().storage_file_read(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing library location and content-reading operations.
        :param query: Query with asset_id and optional UUID/exact-name store_uuid preference.
        :return: Best-effort location record and the library's read_file content value.
        """
        payload = _payload(query)
        library = runtime.services.library
        store = self._store_argument(library, payload.get("store_uuid"))
        asset_id = _required_int(payload, "asset_id")
        location = library.locate_file(
            asset_id,
            store=store,
        )
        return {
            "location": self._location_record(library, location),
            "content": library.read_file(asset_id, store=store),
        }

    def storage_store_save(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Save a Store row through the library and reconcile the returned administrative receipt.

        The library owns Store validation and persistence. No additional Store
        startup or status refresh is performed by this handler after saving.

        Example:
            >>> receipt = CoreApplicationAPI().storage_store_save(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime exposing Store-row persistence and read-side reconciliation.
        :param command: Command containing a required Mapping store payload.
        :return: Reconciled receipt containing the library's saved Store-row value.
        """
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
        """
        Rebuild or extend library storage registration according to truth-tested refresh switches.

        startup_on_add, include_offline, and strict default False; clear_existing
        defaults True. refreshed=True means refresh_storage returned, even when
        its report describes partial failures. No separate cache reconciliation runs.

        Example:
            >>> result = CoreApplicationAPI().storage_refresh(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose library owns Store discovery and refresh lifecycle.
        :param command: Command with optional startup_on_add, include_offline, clear_existing, and strict switches.
        :return: Library refresh report and refreshed=True after successful delegation.
        """
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
        """
        Decode strict base64 content, add a managed file, and report its resolved replica and location.

        Encoded text is stripped and must be nonempty, so the empty base64 encoding
        is rejected. No decoded-size cap is imposed. Metadata is None or a shallow
        Mapping copy; name/original_name/media_type are None for absent or empty
        values, otherwise unstripped text. Store selection supports UUID or exact name.

        Addition precedes replica resolution and location reporting. Failure in
        those later steps can leave an already stored file; this adapter performs
        no compensating deletion or separate cache reconciliation.

        Example:
            >>> receipt = CoreApplicationAPI().storage_file_put(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing library file addition and storage replica resolution.
        :param command: Command with content_base64 and optional metadata, store_uuid, name, original_name, and media_type.
        :return: Created asset value, selected replica_id, best-effort location, and decoded content size in bytes.
        :raises CoreDispatchError: If base64, metadata, or Store selector validation fails.
        """
        payload = _payload(command)
        encoded = _required_text(payload, "content_base64")
        try:
            content = base64.b64decode(encoded, validate=True)
        except Exception as exc:
            raise CoreDispatchError("`content_base64` is not valid base64.") from exc
        metadata = payload.get("metadata")
        if metadata is not None and not isinstance(metadata, Mapping):
            raise CoreDispatchError("`metadata` must be an object or null.")
        library = runtime.services.library
        store = self._store_argument(library, payload.get("store_uuid"))
        asset = library.add_file(
            content,
            metadata=None if metadata is None else dict(metadata),
            store=store,
            name=(None if payload.get("name") in (None, "") else str(payload["name"])),
            original_name=(
                None
                if payload.get("original_name") in (None, "")
                else str(payload["original_name"])
            ),
            media_type=(
                None
                if payload.get("media_type") in (None, "")
                else str(payload["media_type"])
            ),
        )
        resolution = library.storage.resolve_digital_asset(
            asset.digital_asset_id,
            preferred_store_ref=(None if store is None else store.store_ref),
        )
        return {
            "asset": asset,
            "replica_id": resolution.replica_record.replica_id,
            "location": self._location_record(library, resolution.location),
            "size": len(content),
        }

    def storage_file_delete(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Delete a managed replica through the library and report whether bytes or registration were removed.

        The selector is replica_id, not an Asset ID or arbitrary filesystem path.
        deleted is true when either bytes_deleted or replica_forgotten is truthy;
        callers need the retained report to distinguish these outcomes. No extra
        confirmation field or service reconciliation is applied here.

        Example:
            >>> receipt = CoreApplicationAPI().storage_file_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose library owns managed replica deletion policy.
        :param command: Command containing a required integer-convertible replica_id.
        :return: Replica ID, combined deleted flag, and the library's detailed deletion report.
        """
        payload = _payload(command)
        replica_id = _required_int(payload, "replica_id")
        report = runtime.services.library.delete_file(replica_id)
        return {
            "replica_id": replica_id,
            "deleted": bool(report.bytes_deleted or report.replica_forgotten),
            "report": report,
        }

    def cache_reload(
        self,
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        """
        Reload the configured cache and then describe its current service status.

        Status description is a separate operation and can fail after reload has
        completed. An absent cache is an error, not a successful no-op.

        Example:
            >>> status = CoreApplicationAPI().cache_reload(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing the optional cache and composed service description.
        :param command: Unused command envelope; no payload fields are consumed.
        :return: Shallow cache-description dictionary after reload returns.
        :raises CoreDispatchError: If no cache is configured.
        """
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
        """
        Ask services to refresh the selected read source and report its resulting type name.

        The refresh outcome is truth-tested without inferring success from source
        presence. Reading the source for its type occurs afterwards and may fail
        after the refresh operation has run.

        Example:
            >>> result = CoreApplicationAPI().read_source_refresh(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime whose services own read-source refresh and selection.
        :param command: Unused command envelope; refresh accepts no payload options here.
        :return: Boolean refreshed outcome and the current read source's class name.
        """
        del command
        return {
            "refreshed": bool(runtime.services.refresh_read_source()),
            "source": type(runtime.services.read_source).__name__,
        }


def install_application_api(runtime: "CoreRuntime") -> CoreApplicationAPI:
    """
    Create an application API owner, register its stable named handlers, and return it.

    The runtime retains bound handlers; the owner acquires no independent service
    lifecycle. Registration errors propagate without removing earlier bindings.

    Example:
        >>> from unittest.mock import Mock
        >>> runtime = Mock()
        >>> owner = install_application_api(runtime)
        >>> isinstance(owner, CoreApplicationAPI)
        True
        >>> runtime.register_query_handler.call_count, runtime.register_command_handler.call_count
        (21, 25)


    :param runtime: Core-compatible registrar receiving all application query/command handlers and metadata.
    :return: Newly installed CoreApplicationAPI owner after every registration succeeds.
    """

    api = CoreApplicationAPI()
    api.install(runtime)
    return api


__all__ = [
    "CoreApplicationAPI",
    "install_application_api",
]
