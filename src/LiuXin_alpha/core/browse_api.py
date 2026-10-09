"""
Project Works, related categories, legacy files, managed Replicas, and cover candidates for Core clients.

Read-source compatibility fallbacks can turn missing capabilities or selected
lookup failures into empty collections; these projections are not atomic database
snapshots. Paging happens after full reads and projection. Acquisition trusts stored
paths/URLs and uses redirects or whole-byte reads without a size cap, path-confinement
policy, or independent Replica verification. Outer Core dispatch supplies wire
encoding; this module does not render a UI or start a transport server.
"""

# pyright: reportImportCycles=false

from __future__ import annotations

import mimetypes

from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin

from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.utils.store_configuration import store_configuration_from_row

if TYPE_CHECKING:
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


_CATEGORY_TABLES: dict[str, tuple[str, ...]] = {
    "authors": ("agents", "human_agents", "org_agents"),
    "tags": ("tags", "labels"),
    "series": ("series",),
}


def _field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
) -> CorePayloadFieldDescription:
    """
    Build a browse/acquisition payload-field declaration for introspection, not runtime validation.

    Example:
        >>> _field("work_id", required=True, field_type="integer").name
        'work_id'


    :param name: Public payload key described by the declaration.
    :param required: Whether clients should see the field as mandatory.
    :param field_type: Optional transport-facing type label, not an executable validator.
    :return: New field-description record retaining the supplied values.
    """
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    """
    Shallow-copy a Mapping payload, treating an absent or None payload as an empty request.

    Example:
        >>> from types import SimpleNamespace
        >>> _payload(SimpleNamespace(payload={"work_id": 7}))
        {'work_id': 7}


    :param envelope: Object whose optional payload attribute supplies request data.
    :return: New dictionary preserving keys and nested value references.
    :raises CoreDispatchError: If a non-None payload is not a Mapping.
    """
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    """
    Convert a required non-None, non-boolean field using int without imposing an ID range.

    Fractions may truncate. TypeError/ValueError are wrapped; overflow or other
    custom conversion errors propagate without translation.

    Example:
        >>> _required_int({"work_id": "7"}, "work_id")
        7


    :param payload: Mapping carrying the required integer-convertible value.
    :param name: Exact request key, also included in the validation error text.
    :return: Converted integer, including zero or negative values if supplied.
    :raises CoreDispatchError: For missing/None/bool values or conversion TypeError/ValueError.
    """
    value = payload.get(name)
    if value is None or isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CoreDispatchError("`{}` must be an integer.".format(name)) from exc


def _row_mapping(row: Any) -> dict[str, Any]:
    """
    Copy a Mapping row or its Mapping-valued row_dict attribute, rejecting other read-source values.

    Mapping support takes precedence over row_dict. Neither column names nor
    nested values are normalized; attribute/iteration errors remain visible.

    Example:
        >>> from types import SimpleNamespace
        >>> _row_mapping(SimpleNamespace(row_dict={"work_id": 7}))
        {'work_id': 7}


    :param row: Mapping or row-like object exposing a Mapping row_dict.
    :return: Shallow dictionary copy of the selected row representation.
    :raises CoreDispatchError: With invalid_read_source_row if no supported row representation exists.
    """
    if isinstance(row, Mapping):
        return dict(row)
    values = getattr(row, "row_dict", None)
    if isinstance(values, Mapping):
        return dict(values)
    raise CoreDispatchError(
        "The read source returned a non-row value.",
        code="invalid_read_source_row",
    )


def _tables(runtime: "CoreRuntime") -> set[str]:
    """
    Ask the read source for table names without forced refresh, retrying the call without keywords on TypeError.

    The retry also applies to internal TypeErrors, not just unsupported signatures.
    Missing/noncallable get_tables returns empty; final call and iteration failures
    propagate. Names are stringified without stripping or schema validation.

    Example:
        >>> from types import SimpleNamespace
        >>> _tables(SimpleNamespace(services=SimpleNamespace(read_source=object())))
        set()


    :param runtime: Runtime whose services supply the read source.
    :return: Set of advertised table names, or an empty set when enumeration is unsupported.
    """
    source = runtime.services.read_source
    method = getattr(source, "get_tables", None)
    if not callable(method):
        return set()
    try:
        values = cast(Iterable[object], method(force_refresh=False))
    except TypeError:
        values = cast(Iterable[object], method())
    return {str(value) for value in values}


def _id_column(runtime: "CoreRuntime", table: str) -> str:
    """
    Prefer the driver's ID-column method, falling back to a trailing-s-stripped table name plus _id.

    Method call/text-conversion Exceptions are suppressed, but attribute access
    can still raise. Successful values are trusted, including empty text or "None".
    The fallback removes every trailing s and is not a primary-key guarantee.

    Example:
        >>> from types import SimpleNamespace
        >>> _id_column(SimpleNamespace(database=object()), "works")
        'work_id'


    :param runtime: Runtime whose database may expose driver_wrapper.get_id_column.
    :param table: Table name passed to the driver and used for heuristic fallback.
    :return: Driver-supplied ID-column text or the unvalidated naming heuristic.
    """
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    method = getattr(wrapper, "get_id_column", None)
    if callable(method):
        try:
            return str(method(table))
        except Exception:
            pass
    return "{}_id".format(table.rstrip("s"))


def _all_rows(runtime: "CoreRuntime", table: str) -> list[dict[str, Any]]:
    """
    Materialize every read-source row for a table, retrying without iterator_return on TypeError.

    Missing/noncallable get_all_rows yields empty. The keyword retry can mask an
    internal TypeError from the first call; final-call, iteration, and row-shape
    errors propagate. No filtering or row limit is imposed here.

    Example:
        >>> from types import SimpleNamespace
        >>> _all_rows(SimpleNamespace(services=SimpleNamespace(read_source=object())), "works")
        []


    :param runtime: Runtime whose read source may provide get_all_rows.
    :param table: Table name forwarded to both supported call shapes.
    :return: List of shallow row dictionaries in read-source order, or empty when unsupported.
    """
    source = runtime.services.read_source
    method = getattr(source, "get_all_rows", None)
    if not callable(method):
        return []
    try:
        rows = method(table, iterator_return=False)
    except TypeError:
        rows = method(table)
    return [_row_mapping(row) for row in cast(Iterable[object], rows)]


def _interlinked_tables(
    runtime: "CoreRuntime",
    table: str,
) -> list[str]:
    """
    Use driver's interlinked-table names when possible, otherwise return a fixed set of available candidate tables.

    Successful driver results are stringified, deduplicated, sorted, and exclude
    the source table. Call/iteration Exceptions trigger fallback. The fallback
    preserves its fixed order and does not exclude table itself or prove a link exists.

    Example:
        >>> tables = _interlinked_tables(runtime, "works")  # doctest: +SKIP


    :param runtime: Runtime providing driver relationship introspection or read-source table enumeration.
    :param table: Source table passed to driver introspection.
    :return: Linked table names, or available names from the fixed agents/expressions/files/images/items/labels/series/tags fallback.
    """
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    method = getattr(wrapper, "get_interlinked_tables", None)
    if callable(method):
        try:
            values = cast(Iterable[object], method(table))
            return sorted({str(value) for value in values if str(value) != table})
        except Exception:
            pass
    return [
        value
        for value in (
            "agents",
            "expressions",
            "files",
            "images",
            "items",
            "labels",
            "series",
            "tags",
        )
        if value in _tables(runtime)
    ]


def _get_row(
    runtime: "CoreRuntime",
    table: str,
    row_id: int,
) -> dict[str, Any] | None:
    """
    Read one row through the read source, returning None for an absent row or unsupported lookup capability.

    Backend and invalid-row errors propagate; this helper does not try the direct
    database or macro reader when the read source cannot supply a row.

    Example:
        >>> from types import SimpleNamespace
        >>> _get_row(SimpleNamespace(services=SimpleNamespace(read_source=object())), "works", 7) is None
        True


    :param runtime: Runtime whose read source may implement get_row_from_id.
    :param table: Table name forwarded without validation.
    :param row_id: Identity forwarded without further conversion.
    :return: Shallow row dictionary or None when lookup is unsupported or reports absence.
    """
    method = getattr(runtime.services.read_source, "get_row_from_id", None)
    if not callable(method):
        return None
    row = method(table, row_id)
    return None if row is None else _row_mapping(row)


def _search_rows(
    runtime: "CoreRuntime",
    table: str,
    column: str,
    value: Any,
) -> list[dict[str, Any]]:
    """
    Try read-source search, falling back on any ordinary call/iteration/row-projection failure to an equality scan.

    A successful empty search is final. The fallback fully reads the table and
    compares row.get(column) == value, which need not match backend search semantics.
    Fallback failures propagate; a missing all-rows capability can instead yield empty.

    Example:
        >>> rows = _search_rows(runtime, "items", "item_manifestation_id", 7)  # doctest: +SKIP


    :param runtime: Runtime providing read-source search and optional full-table access.
    :param table: Table searched or scanned.
    :param column: Exact row key and backend search-column argument.
    :param value: Search value, forwarded unchanged and compared directly in the fallback.
    :return: Shallow matching row dictionaries in successful search or fallback scan order.
    """
    method = getattr(runtime.services.read_source, "search", None)
    if callable(method):
        try:
            rows = cast(Iterable[object], method(table, column, value))
            return [_row_mapping(row) for row in rows]
        except Exception:
            pass
    return [row for row in _all_rows(runtime, table) if row.get(column) == value]


def _related_rows(
    runtime: "CoreRuntime",
    row: Mapping[str, Any],
    table: str,
) -> list[dict[str, Any]]:
    """
    Resolve related rows, optionally reconstructing a database row through heuristic identity/column matching first.

    Candidate tables use their shortest ID-like heading and rank by matching row-key
    prefixes, then reverse tuple order. Heading/direct-row lookup failures are
    skipped. A relation-call Exception retries the original mapping if reconstruction
    changed the target; otherwise failure or missing capability yields empty.
    Result iteration and row projection occur outside those catches and can raise.

    Example:
        >>> rows = _related_rows(runtime, work_row, "series")  # doctest: +SKIP


    :param runtime: Runtime providing read-source relationship access and optional database-row reconstruction.
    :param row: Source mapping whose key/ID patterns may identify a richer database row.
    :param table: Secondary table passed to get_interlinked_rows.
    :return: Shallow related-row dictionaries in source order, possibly empty after suppressed lookup failures.
    """
    method = getattr(
        runtime.services.read_source,
        "get_interlinked_rows",
        None,
    )
    if not callable(method):
        return []
    target: Any = row
    getter = getattr(runtime.database, "get_row_from_id", None)
    if callable(getter):
        ranked: list[tuple[int, str, Any]] = []
        heading_getter = getattr(
            runtime.services.read_source,
            "get_column_headings",
            None,
        )
        for candidate in _tables(runtime):
            try:
                headings = (
                    {
                        str(value)
                        for value in cast(
                            Iterable[object],
                            heading_getter(candidate),
                        )
                    }
                    if callable(heading_getter)
                    else set()
                )
            except Exception:
                headings = set()
            id_candidates = sorted(
                (value for value in headings if value == "id" or value.endswith("_id")),
                key=len,
            )
            if not id_candidates:
                continue
            id_column = id_candidates[0]
            row_id = row.get(id_column)
            if row_id is None:
                continue
            prefix = candidate.rstrip("s") + "_"
            rank = sum(1 for key in row if str(key).startswith(prefix))
            ranked.append((rank, candidate, row_id))
        for _rank, candidate, row_id in sorted(ranked, reverse=True):
            try:
                candidate_row = getter(candidate, int(row_id))
            except Exception:
                continue
            if candidate_row is not None:
                target = candidate_row
                break
    try:
        rows = method(target_row=target, secondary_table=table)
    except Exception:
        if target is row:
            return []
        try:
            rows = method(target_row=row, secondary_table=table)
        except Exception:
            return []
    return [_row_mapping(value) for value in cast(Iterable[object], rows)]


def _primary_text(table: str, row: Mapping[str, Any]) -> str:
    """
    Choose a display label from preferred title/name/label/value fields, then suffix matches, then a table/ID fallback.

    Only None and the empty string are skipped; zero, whitespace, and other
    non-string values are stringified. Generic suffix matches follow row order.
    Table-derived names remove all trailing s characters, not linguistic plurals.

    Example:
        >>> _primary_text("works", {"work_title": "A Book", "name": "fallback"})
        'A Book'


    :param table: Table name used to derive preferred field prefixes and the fallback label.
    :param row: Mapping containing possible label and ID columns.
    :return: Selected value as text, or a title-cased table stem and optional heuristic ID.
    """
    stem = table.rstrip("s")
    preferred = (
        "{}_title".format(stem),
        "{}_name".format(stem),
        "{}_label".format(stem),
        "{}_value".format(stem),
        "title",
        "name",
        "label",
        "tag",
        "series",
    )
    for key in preferred:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    for key, value in row.items():
        if key.endswith(("_title", "_name", "_label")) and value not in (None, ""):
            return str(value)
    return "{} {}".format(
        table.rstrip("s").replace("_", " ").title(), row.get("{}_id".format(stem), "")
    )


def _flatten(value: Any) -> str:
    """
    Flatten Mapping values and non-string Sequences into space-joined, casefolded search text.

    Keys are excluded. Sets/generators and bytes are stringified rather than
    iterated; None contributes empty text. No cycle or output-size guard is present.

    Example:
        >>> _flatten({"title": "Straße", "tags": ["SCIENCE", None]})
        'strasse science '


    :param value: Row or nested scalar/container data whose values should be searched.
    :return: Casefolded textual projection with sequence/mapping boundaries represented by spaces.
    """
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return " ".join(_flatten(item) for item in value)
    return str(value).casefold()


def _work_summary(
    runtime: "CoreRuntime",
    row: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Project a Work's title, raw record, authors, series, and the first nonempty tags/labels relationship family.

    Author rows from agents/human_agents/org_agents are concatenated without
    deduplication. Tags take precedence over labels rather than being merged.
    Every relation lookup follows the compatibility helper's fallback/error policy.

    Example:
        >>> summary = _work_summary(runtime, work_row)  # doctest: +SKIP


    :param runtime: Runtime providing relationship reads and ID-column discovery for the participating tables.
    :param row: Work row mapping, copied shallowly into the record field.
    :return: work_id/title/authors/series/tags/record dictionary; relationship order follows source iteration.
    """
    work_id = row.get(_id_column(runtime, "works"))
    authors: list[dict[str, Any]] = []
    for table in _CATEGORY_TABLES["authors"]:
        for author_row in _related_rows(runtime, row, table):
            authors.append(
                {
                    "id": author_row.get(_id_column(runtime, table)),
                    "table": table,
                    "name": _primary_text(table, author_row),
                }
            )
    series = [
        {
            "id": value.get(_id_column(runtime, "series")),
            "name": _primary_text("series", value),
        }
        for value in _related_rows(runtime, row, "series")
    ]
    tag_rows: list[dict[str, Any]] = []
    for table in _CATEGORY_TABLES["tags"]:
        linked_tags = _related_rows(runtime, row, table)
        if linked_tags:
            tag_rows = [
                {
                    "id": value.get(_id_column(runtime, table)),
                    "table": table,
                    "name": _primary_text(table, value),
                }
                for value in linked_tags
            ]
            break
    return {
        "work_id": work_id,
        "title": _primary_text("works", row),
        "authors": authors,
        "series": series,
        "tags": tag_rows,
        "record": dict(row),
    }


def _work_item_ids(
    runtime: "CoreRuntime",
    work: Mapping[str, Any],
) -> list[int]:
    """
    Collect Items reached through Work/Expression/Manifestation links and direct Work/Item links.

    Manifestation IDs drive an item_manifestation_id search. Missing identities
    are skipped; non-None Item IDs are int-converted, deduplicated, and sorted.
    Relation/search fallbacks can omit links, and conversion errors propagate.

    Example:
        >>> item_ids = _work_item_ids(runtime, work_row)  # doctest: +SKIP


    :param runtime: Runtime providing relation reads, Item search, and ID-column lookup.
    :param work: Source Work mapping used for both indirect and direct relationship traversal.
    :return: Sorted unique integer Item IDs across the observed relationship paths.
    """
    item_ids: set[int] = set()
    for expression in _related_rows(runtime, work, "expressions"):
        manifestations = _related_rows(
            runtime,
            expression,
            "manifestations",
        )
        for manifestation in manifestations:
            manifestation_id = manifestation.get(_id_column(runtime, "manifestations"))
            if manifestation_id is None:
                continue
            for item in _search_rows(
                runtime,
                "items",
                "item_manifestation_id",
                manifestation_id,
            ):
                item_id = item.get(_id_column(runtime, "items"))
                if item_id is not None:
                    item_ids.add(int(item_id))
    for item in _related_rows(runtime, work, "items"):
        item_id = item.get(_id_column(runtime, "items"))
        if item_id is not None:
            item_ids.add(int(item_id))
    return sorted(item_ids)


def _legacy_file_record(row: Mapping[str, Any]) -> dict[str, Any]:
    """
    Project a legacy files row into an acquisition listing without checking its location or availability.

    Name prefers file_name, original name, storage key, then download.bin. Extension
    prefers stored values, then filename suffix, and is lowercased but not stripped.
    MIME falls back through name inference to octet-stream. A zero size_bytes is
    falsey and therefore falls back to file_size rather than necessarily remaining zero.

    Example:
        >>> record = _legacy_file_record({"file_id": 7, "file_name": "BOOK.EPUB", "file_size_bytes": 4})
        >>> record["kind"], record["extension"], record["size"]
        ('legacy-file', 'epub', 4)


    :param row: Legacy file mapping containing optional identity, naming, MIME, size, and Store fields.
    :return: New listing dictionary with kind, IDs, name/extension/MIME, size, and Store/key hints.
    """
    name = str(
        row.get("file_name")
        or row.get("file_original_name")
        or row.get("file_storage_key")
        or "download.bin"
    )
    extension = str(
        row.get("file_extension")
        or row.get("file_original_extension")
        or Path(name).suffix.lstrip(".")
        or ""
    ).lower()
    return {
        "kind": "legacy-file",
        "id": row.get("file_id"),
        "item_id": row.get("file_item_id"),
        "name": name,
        "extension": extension,
        "mime_type": (
            row.get("file_mime_type")
            or mimetypes.guess_type(name)[0]
            or "application/octet-stream"
        ),
        "size": row.get("file_size_bytes") or row.get("file_size"),
        "store_id": row.get("file_store_id"),
        "storage_key": row.get("file_storage_key"),
    }


def _asset_records(
    runtime: "CoreRuntime",
    item_ids: Iterable[int],
) -> list[dict[str, Any]]:
    """
    List all Replica rows linked to Items directly or through Composite Digital Asset members.

    Items follow input order; direct links precede composite members per Item,
    and macro queries request link/Replica ID order, not member sequence order.
    Replica IDs are deduplicated globally, retaining the first Item/composite context.
    No health, mode, or availability filter is applied. Only get_rows capability is
    checked initially; later missing get_row support or query failures can raise.

    Example:
        >>> records = _asset_records(runtime, [7])  # doctest: +SKIP


    :param runtime: Runtime whose database macros can read Asset, Replica, Item-link, and composite-member rows.
    :param item_ids: Item identities consumed in supplied order without independent normalization or deduplication.
    :return: Replica listing dictionaries, or empty when portable get_rows capability is unavailable.
    """
    macros = getattr(runtime.database, "macros", None)
    if macros is None or not callable(getattr(macros, "get_rows", None)):
        return []
    results: list[dict[str, Any]] = []
    seen_replicas: set[int] = set()

    def add_asset(
        asset_id: Any,
        *,
        item_id: int,
        composite_id: Any = None,
        member_sequence: Any = None,
    ) -> None:
        """
        Append previously unseen Replicas for one linked Asset, using the first observed relationship context.

        None or absent Assets add nothing. Replica IDs are marked seen before
        listing fields are built. Name/extension/MIME use stored values and filename
        fallbacks; zero observed size falls back to Asset size. Exceptions propagate
        and can leave the enclosing accumulator partially populated.

        Example:
            >>> add_asset(7, item_id=3, composite_id=2, member_sequence=0)  # doctest: +SKIP


        :param asset_id: Asset lookup identity; None skips the operation.
        :param item_id: Originating Item identity attached to every newly appended Replica listing.
        :param composite_id: Optional Composite Digital Asset context, omitted for direct Item links.
        :param member_sequence: Optional stored member sequence copied to the listing, not used to sort it.
        :return: None after extending the enclosing results and seen-ID collections.
        """
        if asset_id is None:
            return
        asset = macros.get_row(
            "digital_assets",
            asset_id,
            id_column="digital_asset_id",
        )
        if asset is None:
            return
        replicas = macros.get_rows(
            "asset_replicas",
            where={"asset_replica_digital_asset_id": asset_id},
            order_by=("asset_replica_id",),
        )
        for replica in replicas:
            replica_id = int(replica["asset_replica_id"])
            if replica_id in seen_replicas:
                continue
            seen_replicas.add(replica_id)
            name = str(
                replica.get("asset_replica_name")
                or asset.get("digital_asset_name")
                or replica.get("asset_replica_storage_key")
                or "asset.bin"
            )
            extension = str(
                replica.get("asset_replica_extension")
                or asset.get("digital_asset_extension")
                or Path(name).suffix.lstrip(".")
                or ""
            ).lower()
            results.append(
                {
                    "kind": "replica",
                    "id": replica_id,
                    "asset_id": asset_id,
                    "composite_id": composite_id,
                    "member_sequence": member_sequence,
                    "item_id": item_id,
                    "name": name,
                    "extension": extension,
                    "mime_type": (
                        asset.get("digital_asset_mime_type")
                        or mimetypes.guess_type(name)[0]
                        or "application/octet-stream"
                    ),
                    "size": (
                        replica.get("asset_replica_observed_size_bytes")
                        or asset.get("digital_asset_size_bytes")
                    ),
                    "store_id": replica.get("asset_replica_store_id"),
                    "storage_key": replica.get("asset_replica_storage_key"),
                    "mode": replica.get("asset_replica_mode"),
                }
            )

    for item_id in item_ids:
        links = macros.get_rows(
            "digital_asset_item_links",
            where={"digital_asset_item_link_item_id": item_id},
            order_by=("digital_asset_item_link_id",),
        )
        for link in links:
            add_asset(
                link.get("digital_asset_item_link_digital_asset_id"),
                item_id=item_id,
            )
        composite_links = macros.get_rows(
            "composite_digital_asset_item_links",
            where={
                "composite_digital_asset_item_link_item_id": item_id,
            },
            order_by=("composite_digital_asset_item_link_id",),
        )
        for composite_link in composite_links:
            composite_id = composite_link.get(
                "composite_digital_asset_item_link_composite_digital_asset_id"
            )
            if composite_id is None:
                continue
            members = macros.get_rows(
                "composite_digital_asset_digital_asset_links",
                where={
                    "composite_digital_asset_digital_asset_link_composite_digital_asset_id": composite_id,
                },
                order_by=("composite_digital_asset_digital_asset_link_id",),
            )
            for member in members:
                add_asset(
                    member.get(
                        "composite_digital_asset_digital_asset_link_digital_asset_id"
                    ),
                    item_id=item_id,
                    composite_id=composite_id,
                    member_sequence=member.get(
                        "composite_digital_asset_digital_asset_link_sequence_number"
                    ),
                )
    return results


def _work_files(
    runtime: "CoreRuntime",
    work: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """
    Combine direct/Item-linked legacy files with Item-linked managed Replica listings for a Work.

    Direct legacy rows are appended without deduplicating each other. Their IDs
    suppress later Item-linked duplicates; rows without IDs remain undeduplicated.
    Managed listings follow all legacy listings and have separate Replica-ID
    deduplication. No resource resolution or byte read occurs here.

    Example:
        >>> formats = _work_files(runtime, work_row)  # doctest: +SKIP


    :param runtime: Runtime providing Work-to-Item relationships, file search, and managed-asset macros.
    :param work: Work mapping whose direct files and associated Items are inspected.
    :return: Legacy-file listings followed by managed-Replica listings, without a global format sort.
    """
    item_ids = _work_item_ids(runtime, work)
    legacy: list[dict[str, Any]] = []
    seen: set[int] = set()
    for row in _related_rows(runtime, work, "files"):
        file_id = row.get("file_id")
        if file_id is not None:
            seen.add(int(file_id))
        legacy.append(_legacy_file_record(row))
    for item_id in item_ids:
        for row in _search_rows(runtime, "files", "file_item_id", item_id):
            file_id = row.get("file_id")
            if file_id is not None and int(file_id) in seen:
                continue
            if file_id is not None:
                seen.add(int(file_id))
            legacy.append(_legacy_file_record(row))
    return legacy + _asset_records(runtime, item_ids)


def _store_row(
    runtime: "CoreRuntime",
    store_id: Any,
) -> dict[str, Any] | None:
    """
    Prefer a portable stores-row macro lookup, then fall back to the read source if unsupported or absent.

    Macro exceptions propagate rather than triggering fallback. The macro receives
    store_id unchanged; only the read-source fallback int-converts it.

    Example:
        >>> from unittest.mock import Mock
        >>> _store_row(Mock(), None) is None
        True


    :param runtime: Runtime providing optional macro get_row and read-source row lookup; unused for store_id=None.
    :param store_id: Store row identity, or None to return absence without touching the runtime.
    :return: Shallow Store row dictionary, or None when no source returns a row.
    """
    if store_id is None:
        return None
    macros = getattr(runtime.database, "macros", None)
    if macros is not None and callable(getattr(macros, "get_row", None)):
        row = macros.get_row("stores", store_id, id_column="store_id")
        if row is not None:
            return dict(row)
    return _get_row(runtime, "stores", int(store_id))


def _resolution(
    runtime: "CoreRuntime",
    *,
    kind: str,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Choose a redirect, Core delivery hint, or unavailable result from a resource row's stored location fields.

    Legacy/image HTTP(S) sources take precedence over existing local paths, then
    Store/key resolution. Replicas use Store/key only. HTTP(S) Store roots use
    urljoin, which can let an absolute key replace the root; URLs are not fetched
    or validated. A non-HTTP Store/key pair is marked readable without a backend
    probe, so readable=True does not guarantee a subsequent byte read succeeds.

    Example:
        >>> from unittest.mock import Mock
        >>> result = _resolution(Mock(), kind="legacy-file", row={"file_id": 7, "file_source": "https://example.test/book.epub"})
        >>> result["delivery"], result["readable"]
        ('redirect', False)


    :param runtime: Runtime used for Store lookup if direct URL/local-path hints do not resolve the resource.
    :param kind: Exact legacy-file, replica, or image token; this helper does not normalize it.
    :param row: Resource mapping supplying naming and trusted path/URL/Store-key hints.
    :return: Kind/ID/name and delivery/readable fields, plus redirect location or Store/key details where applicable.
    :raises CoreDispatchError: If kind is unsupported; local-path or Store lookup errors may also propagate.
    """
    local_paths: tuple[Any, ...]
    remote_urls: tuple[Any, ...]
    if kind == "legacy-file":
        file_id = row.get("file_id")
        name = _legacy_file_record(row)["name"]
        store_id = row.get("file_store_id")
        storage_key = row.get("file_storage_key")
        local_paths = (row.get("file_original_path"), row.get("file_path"))
        remote_urls = (row.get("file_source"), row.get("file_original_path"))
    elif kind == "replica":
        file_id = row.get("asset_replica_id")
        name = (
            row.get("asset_replica_name")
            or row.get("asset_replica_storage_key")
            or "asset.bin"
        )
        store_id = row.get("asset_replica_store_id")
        storage_key = row.get("asset_replica_storage_key")
        local_paths = ()
        remote_urls = ()
    elif kind == "image":
        file_id = row.get("image_id")
        name = (
            row.get("image_name")
            or row.get("image_original_name")
            or row.get("image_storage_key")
            or "cover.bin"
        )
        store_id = row.get("image_store_id")
        storage_key = row.get("image_storage_key")
        local_paths = (row.get("image_original_path"), row.get("image_path"))
        remote_urls = (row.get("image_source"), row.get("image_original_path"))
    else:
        raise CoreDispatchError("Unknown acquisition kind `{}`.".format(kind))

    for value in remote_urls:
        url = str(value or "").strip()
        if url.startswith(("http://", "https://")):
            return {
                "kind": kind,
                "id": file_id,
                "name": str(name),
                "delivery": "redirect",
                "location": url,
                "readable": False,
            }
    for value in local_paths:
        path = Path(str(value or "").strip())
        if str(value or "").strip() and path.is_file():
            return {
                "kind": kind,
                "id": file_id,
                "name": str(name),
                "delivery": "core",
                "readable": True,
            }
    store = _store_row(runtime, store_id)
    if store is not None and storage_key:
        root = str(store.get("store_root_uri") or store.get("store_url") or "").strip()
        if root.startswith(("http://", "https://")):
            base = root if root.endswith("/") else root + "/"
            return {
                "kind": kind,
                "id": file_id,
                "name": str(name),
                "delivery": "redirect",
                "location": urljoin(base, str(storage_key)),
                "readable": False,
            }
        return {
            "kind": kind,
            "id": file_id,
            "name": str(name),
            "delivery": "core",
            "readable": True,
            "store_id": store_id,
            "storage_key": storage_key,
        }
    return {
        "kind": kind,
        "id": file_id,
        "name": str(name),
        "delivery": "unavailable",
        "readable": False,
    }


def _resource_row(
    runtime: "CoreRuntime",
    *,
    kind: str,
    resource_id: int,
) -> dict[str, Any] | None:
    """
    Read a supported acquisition row from the read source for legacy files/images or macros for Replicas.

    Missing capability is treated as absence, while call and row-conversion errors
    propagate. Kind selection does not check availability or ownership of the bytes.

    Example:
        >>> row = _resource_row(runtime, kind="replica", resource_id=7)  # doctest: +SKIP


    :param runtime: Runtime providing read-source row access and optional Replica-table macros.
    :param kind: Exact legacy-file, image, or replica token.
    :param resource_id: Identity forwarded to the selected lookup without further conversion.
    :return: Shallow resource row or None for absent rows/unsupported lookup capability.
    :raises CoreDispatchError: If the kind token is unknown.
    """
    if kind == "legacy-file":
        return _get_row(runtime, "files", resource_id)
    if kind == "image":
        return _get_row(runtime, "images", resource_id)
    if kind == "replica":
        macros = getattr(runtime.database, "macros", None)
        if macros is None or not callable(getattr(macros, "get_row", None)):
            return None
        value = macros.get_row(
            "asset_replicas",
            resource_id,
            id_column="asset_replica_id",
        )
        return None if value is None else dict(value)
    raise CoreDispatchError("Unknown acquisition kind `{}`.".format(kind))


def _resource_bytes(
    runtime: "CoreRuntime",
    *,
    kind: str,
    row: Mapping[str, Any],
) -> bytes:
    """
    Read whole bytes from a direct local path, managed Store location, or legacy Store-root path fallback.

    Direct local-file read failures propagate. Managed configuration/read/type-check
    Exceptions are suppressed before trying the root/key path. The fallback strips
    a literal file:// prefix without URL decoding and joins trusted, unconfined
    keys; absolute keys or parent components can escape the root. No size limit,
    digest verification, or HTTP fetching is implemented here. Non-file/image kinds
    use Replica fields; callers must validate kind before reaching this helper.

    Example:
        >>> content = _resource_bytes(runtime, kind="replica", row=replica_row)  # doctest: +SKIP


    :param runtime: Runtime providing Store rows and services.library.storage.read_bytes for managed locations.
    :param kind: Legacy-file/image selects direct-path fields; other tokens select Replica Store/key fields.
    :param row: Resource location mapping; paths and keys are consumed as trusted metadata.
    :return: Entire resource as bytes, loaded into memory; managed readers must return bytes rather than bytearray.
    :raises CoreDispatchError: With acquisition_unavailable when no read path succeeds or remains available.
    """
    local_paths: tuple[Any, ...]
    if kind == "legacy-file":
        local_paths = (row.get("file_original_path"), row.get("file_path"))
        store_id = row.get("file_store_id")
        storage_key = row.get("file_storage_key")
    elif kind == "image":
        local_paths = (row.get("image_original_path"), row.get("image_path"))
        store_id = row.get("image_store_id")
        storage_key = row.get("image_storage_key")
    else:
        local_paths = ()
        store_id = row.get("asset_replica_store_id")
        storage_key = row.get("asset_replica_storage_key")
    for value in local_paths:
        path_text = str(value or "").strip()
        if path_text and Path(path_text).is_file():
            return Path(path_text).read_bytes()
    store = _store_row(runtime, store_id)
    if store is not None and storage_key:
        try:
            configuration = store_configuration_from_row(
                store,
                fallback_store_id=int(store_id),
            )
            location = Location(
                configuration.store_uuid,
                str(storage_key),
            )
            value = runtime.services.library.storage.read_bytes(location)
            if not isinstance(value, bytes):
                raise TypeError("Storage byte reader returned a non-byte value.")
            return value
        except Exception:
            pass
        root = str(store.get("store_root_uri") or store.get("store_url") or "").strip()
        if root.startswith("file://"):
            root = root[7:]
        if root and not root.startswith(("http://", "https://")):
            path = Path(root) / str(storage_key)
            if path.is_file():
                return path.read_bytes()
    raise CoreDispatchError(
        "The acquisition resource is not readable by this Core.",
        code="acquisition_unavailable",
    )


class CoreBrowseAPI:
    """
    Register display-neutral browse and acquisition queries without owning the library or transport.

    Work/category projections join legacy read sources and managed-storage rows.
    Direct handler calls bypass runtime dispatch locking/events and final wire
    encoding. Acquisition results describe trusted stored locations, not verified
    availability, and full reads/pagination have no streaming interface here.

    Example:
        >>> adapter = CoreBrowseAPI()
        >>> adapter.install(runtime)  # doctest: +SKIP
    """

    def install(self, runtime: "CoreRuntime") -> None:
        """
        Register four browse and four acquisition queries with explicit summaries, payload metadata, and tags.

        Registration reads no library content and probes no acquisition location.
        Later registration errors leave earlier bindings in place; duplicate-name
        handling belongs to the runtime.

        Example:
            >>> CoreBrowseAPI().install(runtime)  # doctest: +SKIP


        :param runtime: Registrar receiving all eight query handlers and their transport descriptions.
        :return: None after all query registrations finish; no commands are registered here.
        """
        query = runtime.register_query_handler
        query(
            "browse.categories",
            self.categories,
            summary="List display-neutral top-level library browse categories.",
            tags=("browse", "catalog", "read"),
        )
        query(
            "browse.category.items",
            self.category_items,
            summary="List entities within one browse category.",
            payload_fields=(
                _field("category", required=True, field_type="string"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
                _field("sort", field_type="string"),
                _field("ascending", field_type="boolean"),
            ),
            tags=("browse", "catalog", "read"),
        )
        query(
            "browse.works",
            self.works,
            summary="Page works by category entity, search text, and sort order.",
            payload_fields=(
                _field("category", field_type="string"),
                _field("category_id", field_type="integer"),
                _field("text", field_type="string"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
                _field("sort", field_type="string"),
                _field("ascending", field_type="boolean"),
            ),
            tags=("browse", "catalog", "read"),
        )
        query(
            "browse.work",
            self.work,
            summary="Return one work projection with related entities and formats.",
            payload_fields=(_field("work_id", required=True, field_type="integer"),),
            tags=("browse", "catalog", "acquisition", "read"),
        )
        query(
            "acquisition.formats",
            self.acquisition_formats,
            summary="List downloadable legacy files and managed replicas for one work.",
            payload_fields=(_field("work_id", required=True, field_type="integer"),),
            tags=("acquisition", "storage", "read"),
        )
        query(
            "acquisition.resolve",
            self.acquisition_resolve,
            summary="Resolve one acquisition resource to Core delivery or redirect.",
            payload_fields=(
                _field("kind", required=True, field_type="string"),
                _field("id", required=True, field_type="integer"),
            ),
            tags=("acquisition", "storage", "read"),
        )
        query(
            "acquisition.read",
            self.acquisition_read,
            summary="Read one Core-accessible acquisition resource.",
            payload_fields=(
                _field("kind", required=True, field_type="string"),
                _field("id", required=True, field_type="integer"),
            ),
            tags=("acquisition", "storage", "read"),
        )
        query(
            "acquisition.cover",
            self.acquisition_cover,
            summary="Return cover candidates for one work.",
            payload_fields=(_field("work_id", required=True, field_type="integer"),),
            tags=("acquisition", "images", "read"),
        )

    @staticmethod
    def categories(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Return all/newest plus author/tag/series category declarations with fully read row counts.

        Work counts are zero if works is not advertised. Entity counts sum every
        available candidate table, not distinct people/labels or related Work counts.
        Empty or unavailable categories remain present with zero counts.

        Example:
            >>> result = CoreBrowseAPI.categories(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing table enumeration and full-row reads from its read source.
        :param query: Query envelope, ignored; no filter/pagination options are consumed.
        :return: Ordered category records for all, newest, authors, tags, and series, including entity-table lists.
        """
        del query
        available = _tables(runtime)
        works_count = len(_all_rows(runtime, "works")) if "works" in available else 0
        records: list[dict[str, Any]] = [
            {
                "category": "all",
                "label": "All works",
                "count": works_count,
                "entity_category": False,
            },
            {
                "category": "newest",
                "label": "Newest",
                "count": works_count,
                "entity_category": False,
            },
        ]
        for category, candidates in _CATEGORY_TABLES.items():
            selected = [table for table in candidates if table in available]
            count = sum(len(_all_rows(runtime, table)) for table in selected)
            records.append(
                {
                    "category": category,
                    "label": category.title(),
                    "count": count,
                    "entity_category": True,
                    "tables": selected,
                }
            )
        return {"categories": records}

    @staticmethod
    def category_items(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Page fully projected Works or category entities after reading, counting relationships, and sorting in memory.

        limit/offset default to 100/0, use int coercion, and clamp to 0..10,000/nonnegative.
        ascending defaults True. For all/newest, recent/newest sorting uses reverse=
        ascending, so True puts larger Work IDs first; this differs from works().
        Other Work sorts use casefolded titles. Entity popularity sorts by Work count
        then label; other sorts use labels. Unknown sort tokens fall back to text sort.

        Example:
            >>> result = CoreBrowseAPI.category_items(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing full category/Work reads, relationship counts, and summary projection.
        :param query: Query with required category (all/newest/authors/tags/series), optional limit/offset/sort, and truth-converted ascending.
        :return: Category, paged records, full projected total_count, normalized pagination, and end-of-result complete flag.
        :raises CoreDispatchError: If the stripped/lowercased category token is unknown; conversion/backend failures may also propagate.
        """
        payload = _payload(query)
        category = str(payload.get("category") or "").strip().lower()
        if category not in {*_CATEGORY_TABLES, "all", "newest"}:
            raise CoreDispatchError("Unknown browse category `{}`.".format(category))
        limit = max(0, min(int(payload.get("limit", 100)), 10_000))
        offset = max(0, int(payload.get("offset", 0)))
        ascending = bool(payload.get("ascending", True))
        sort = str(payload.get("sort") or "name").strip().lower()
        if category in {"all", "newest"}:
            works = _all_rows(runtime, "works")
            records = [_work_summary(runtime, row) for row in works]
            if category == "newest" or sort == "recent":
                records.sort(
                    key=lambda row: int(row.get("work_id") or 0),
                    reverse=ascending,
                )
            else:
                records.sort(
                    key=lambda row: str(row["title"]).casefold(),
                    reverse=not ascending,
                )
        else:
            available = _tables(runtime)
            records = []
            for table in _CATEGORY_TABLES[category]:
                if table not in available:
                    continue
                for row in _all_rows(runtime, table):
                    row_id = row.get(_id_column(runtime, table))
                    works = _related_rows(runtime, row, "works")
                    records.append(
                        {
                            "category": category,
                            "table": table,
                            "id": row_id,
                            "label": _primary_text(table, row),
                            "work_count": len(works),
                            "record": row,
                        }
                    )
            if sort == "popularity":
                records.sort(
                    key=lambda row: (
                        int(row.get("work_count") or 0),
                        str(row.get("label") or "").casefold(),
                    ),
                    reverse=not ascending,
                )
            else:
                records.sort(
                    key=lambda row: str(row.get("label") or "").casefold(),
                    reverse=not ascending,
                )
        visible = records[offset : offset + limit]
        return {
            "category": category,
            "records": visible,
            "total_count": len(records),
            "limit": limit,
            "offset": offset,
            "complete": offset + len(visible) >= len(records),
        }

    @staticmethod
    def works(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Page Work summaries selected by category identity and raw-row text, then sorted by title or numeric ID.

        Category defaults all. Entity categories choose the first candidate table
        containing category_id and return empty if none does; matching is not merged
        across tables. Text searches flattened raw row values before author/tag
        enrichment. Recent sorting uses Work ID, not timestamps. Newest defaults
        to descending recent order; otherwise defaults are ascending title order.
        All matching rows are projected before pagination.

        Example:
            >>> result = CoreBrowseAPI.works(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing Work/category reads, relationship lookup, and summary projection.
        :param query: Query with optional category/category_id/text/sort/ascending and int-coerced limit/offset (defaults 100/0, clamped to 0..10,000/nonnegative).
        :return: Paged Work records, full post-text-filter count, pagination values, and end-of-result complete flag.
        :raises CoreDispatchError: For unknown category or invalid required entity ID; lower-level read/conversion errors propagate.
        """
        payload = _payload(query)
        category = str(payload.get("category") or "all").strip().lower()
        text = str(payload.get("text") or "").strip().casefold()
        limit = max(0, min(int(payload.get("limit", 100)), 10_000))
        offset = max(0, int(payload.get("offset", 0)))
        ascending = bool(payload.get("ascending", category != "newest"))
        sort = (
            str(payload.get("sort") or ("recent" if category == "newest" else "title"))
            .strip()
            .lower()
        )
        if category in _CATEGORY_TABLES:
            category_id = _required_int(payload, "category_id")
            table = next(
                (
                    value
                    for value in _CATEGORY_TABLES[category]
                    if _get_row(runtime, value, category_id) is not None
                ),
                None,
            )
            category_row = (
                None if table is None else _get_row(runtime, table, category_id)
            )
            rows = (
                []
                if category_row is None
                else _related_rows(runtime, category_row, "works")
            )
        elif category in {"all", "newest"}:
            rows = _all_rows(runtime, "works")
        else:
            raise CoreDispatchError("Unknown browse category `{}`.".format(category))
        if text:
            rows = [row for row in rows if text in _flatten(row)]
        records = [_work_summary(runtime, row) for row in rows]
        if sort == "recent":
            records.sort(
                key=lambda row: int(row.get("work_id") or 0),
                reverse=not ascending,
            )
        else:
            records.sort(
                key=lambda row: str(row["title"]).casefold(),
                reverse=not ascending,
            )
        visible = records[offset : offset + limit]
        return {
            "records": visible,
            "total_count": len(records),
            "limit": limit,
            "offset": offset,
            "complete": offset + len(visible) >= len(records),
        }

    @staticmethod
    def work(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Project one Work with nonempty related-table rows, associated Item IDs, and unresolved file/Replica listings.

        A missing row or unsupported read-source lookup returns only work=None.
        Related tables use driver discovery or the fixed compatibility fallback.
        Independent relationship/format reads are not an atomic snapshot, and
        formats here lack the added resolution field supplied by acquisition_formats.

        Example:
            >>> result = CoreBrowseAPI.work(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing Work lookup, relationship introspection, and legacy/managed file metadata.
        :param query: Query with required integer-convertible work_id, excluding None/bool.
        :return: work=None alone for absence; otherwise work summary, related mapping, sorted Item IDs, and format listings.
        """
        work_id = _required_int(_payload(query), "work_id")
        row = _get_row(runtime, "works", work_id)
        if row is None:
            return {"work": None}
        related: dict[str, list[dict[str, Any]]] = {}
        for table in _interlinked_tables(runtime, "works"):
            values = _related_rows(runtime, row, table)
            if values:
                related[table] = values
        return {
            "work": _work_summary(runtime, row),
            "related": related,
            "item_ids": _work_item_ids(runtime, row),
            "formats": _work_files(runtime, row),
        }

    @staticmethod
    def acquisition_formats(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        List a Work's legacy files and managed Replicas, adding a current resolution hint for each resource.

        Missing Works return an empty list. Each resource is looked up separately;
        vanished resources keep their listing with resolution=None. Listings are
        not filtered to readable/healthy resources and no bytes are fetched here.
        Invalid listing IDs or resolution failures can abort the entire request.

        Example:
            >>> result = CoreBrowseAPI.acquisition_formats(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying Work/file relationships, managed resource rows, and Store/path resolution.
        :param query: Query with required integer-convertible work_id.
        :return: work_id and ordered format dictionaries augmented in place with resolution hints or None.
        """
        work_id = _required_int(_payload(query), "work_id")
        row = _get_row(runtime, "works", work_id)
        if row is None:
            return {"work_id": work_id, "formats": []}
        records = _work_files(runtime, row)
        for record in records:
            resource = _resource_row(
                runtime,
                kind=str(record["kind"]),
                resource_id=int(record["id"]),
            )
            record["resolution"] = (
                None
                if resource is None
                else _resolution(
                    runtime,
                    kind=str(record["kind"]),
                    row=resource,
                )
            )
        return {"work_id": work_id, "formats": records}

    @staticmethod
    def acquisition_resolve(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Normalize kind/ID, require the resource row, and return its redirect/Core/unavailable delivery hint.

        Resolution trusts stored location metadata and does not fetch redirect
        URLs or verify backend bytes. A Core-readable hint can still fail on read.

        Example:
            >>> result = CoreBrowseAPI.acquisition_resolve(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing resource-row lookup and Store/local-path resolution.
        :param query: Query with kind (legacy-file/image/replica, stripped/lowercased) and required integer-convertible id.
        :return: Resource identity/name and delivery/readable fields, plus location or Store/key details when applicable.
        :raises CoreDispatchError: For unknown kinds or acquisition_not_found when the row/lookup capability is absent.
        """
        payload = _payload(query)
        kind = str(payload.get("kind") or "").strip().lower()
        resource_id = _required_int(payload, "id")
        row = _resource_row(runtime, kind=kind, resource_id=resource_id)
        if row is None:
            raise CoreDispatchError(
                "Unknown {} {}.".format(kind, resource_id),
                code="acquisition_not_found",
            )
        return _resolution(runtime, kind=kind, row=row)

    @staticmethod
    def acquisition_read(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Resolve one resource and, when marked readable, load its entire content for outer Core wire encoding.

        Any readable=False result raises acquisition_redirect, including unavailable
        resources with no redirect URL. Stored HTTP sources take precedence over
        local paths, so they can block a local read even when such bytes exist.
        Resolution and reading are separate observations without a shared snapshot.

        Example:
            >>> result = CoreBrowseAPI.acquisition_read(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing resource resolution and whole-byte acquisition reads.
        :param query: Query with normalized supported kind and required integer-convertible id.
        :return: Resource resolution plus raw bytes content; no byte cap or streaming iterator is supplied here.
        :raises CoreDispatchError: For missing/unknown resources, unreadable resolution, or exhausted acquisition read paths.
        """
        payload = _payload(query)
        kind = str(payload.get("kind") or "").strip().lower()
        resource_id = _required_int(payload, "id")
        row = _resource_row(runtime, kind=kind, resource_id=resource_id)
        if row is None:
            raise CoreDispatchError(
                "Unknown {} {}.".format(kind, resource_id),
                code="acquisition_not_found",
            )
        resolved = _resolution(runtime, kind=kind, row=row)
        if not resolved["readable"]:
            raise CoreDispatchError(
                "The acquisition resource must be followed as a redirect.",
                code="acquisition_redirect",
                details=resolved,
            )
        return {
            "resource": resolved,
            "content": _resource_bytes(runtime, kind=kind, row=row),
        }

    @staticmethod
    def acquisition_cover(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        List directly related image rows as cover candidates with MIME and acquisition-resolution hints.

        Missing Works and absent relationships yield empty lists. Images without
        IDs are skipped; others remain in relationship order without deduplication,
        ranking, resizing, or a readability filter. No image content is read here.

        Example:
            >>> result = CoreBrowseAPI.acquisition_cover(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing Work/image relationships and stored-location resolution.
        :param query: Query with required integer-convertible work_id.
        :return: work_id and cover candidate dictionaries containing image ID, display name, MIME, and resolution.
        """
        work_id = _required_int(_payload(query), "work_id")
        work = _get_row(runtime, "works", work_id)
        if work is None:
            return {"work_id": work_id, "covers": []}
        covers = []
        for image in _related_rows(runtime, work, "images"):
            image_id = image.get("image_id")
            if image_id is None:
                continue
            covers.append(
                {
                    "kind": "image",
                    "id": image_id,
                    "name": _primary_text("images", image),
                    "mime_type": (
                        image.get("image_mime_type")
                        or mimetypes.guess_type(str(image.get("image_name") or ""))[0]
                        or "application/octet-stream"
                    ),
                    "resolution": _resolution(
                        runtime,
                        kind="image",
                        row=image,
                    ),
                }
            )
        return {"work_id": work_id, "covers": covers}


def install_browse_api(runtime: "CoreRuntime") -> CoreBrowseAPI:
    """
    Construct a stateless browse adapter, register its eight queries, and return it without taking resource ownership.

    Partial registration remains if a later registration raises.

    Example:
        >>> from unittest.mock import Mock
        >>> runtime = Mock()
        >>> isinstance(install_browse_api(runtime), CoreBrowseAPI)
        True
        >>> runtime.register_query_handler.call_count
        8


    :param runtime: Runtime registrar receiving display-neutral browse/acquisition handlers and metadata.
    :return: Newly constructed adapter after every query registration succeeds.
    """

    api = CoreBrowseAPI()
    api.install(runtime)
    return api


__all__ = ["CoreBrowseAPI", "install_browse_api"]
