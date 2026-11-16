"""
Native custom-field identities, typed values, write results and change plans.

Records freeze attribute assignment, not nested dictionaries or payloads.
Construction alone performs no database lookup or semantic validation; the
service validates intent against persisted definitions and transaction state.
FieldRef is either a numeric definition ID or an (attachment, label) pair.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

type Scalar = str | int | float | bool | datetime
type CollectionMode = Literal["scalar", "set", "list"]
type FieldRef = int | tuple[str, str]


class CustomFieldError(ValueError):
    """
    Report a custom-field request that cannot satisfy its contract.

    Example:
        >>> isinstance(CustomFieldError("Unknown field"), ValueError)
        True
    """


class UnsupportedStorage(CustomFieldError):
    """
    Reject a catalogue format unsupported by the native custom-field service.

    The service does not convert or provide runtime access to v1 storage.

    Example:
        >>> error = UnsupportedStorage("Unsupported custom-field storage")
    """


class StalePlan(CustomFieldError):
    """
    Reject a change plan whose source fingerprint no longer matches storage.

    Prepare and review a fresh plan before applying the change again.

    Example:
        >>> error = StalePlan("Source changed after planning")
    """


@dataclass(frozen=True, order=True)
class OwnerRef:
    """
    Identify a stored owner by physical attachment table and row ID.

    The same numeric ID in another table names a different owner. Creating this
    record does not establish existence; native read/write operations check it.

    Example:
        >>> OwnerRef("works", 7) == OwnerRef("agents", 7)
        False


    :param table: Physical owner table, matching the field attachment.
    :param id: Persistent row identity within that table.
    """

    table: str
    id: int


@dataclass(frozen=True)
class FieldDefinition:
    """
    Describe a native field at one observed revision.

    Scalar fields have at most one membership per owner. Sets preserve order
    while deduplicating canonical identities; lists preserve repeated values.
    Revisions cover the entire field, so another owner's write can invalidate a
    revision check. Retirement hides the field from ordinary lookup but retains
    its definition and values until an explicit purge.

    Service reads return detached display metadata. The dataclass itself neither
    copies a supplied dictionary nor persists edits to it; use a migration plan
    to change stored definitions. Composite evaluation belongs to Catalog.

    Example:
        >>> definition = FieldDefinition(1, "works", "genre", "Genre", mode="set")
        >>> definition.key, definition.normalized
        ('#works.genre', True)


    :param id: Persistent field identity.
    :param attachment: Physical table whose rows can own values.
    :param label: Lookup label scoped to the attachment.
    :param name: Display name scoped to the attachment.
    :param datatype: Native datatype validated by the service.
    :param mode: scalar, ordered set, or ordered list.
    :param editable: Whether ordinary value writes are allowed; composites are
        read-only.
    :param display: Datatype and presentation options; mutable even on a frozen
        definition.
    :param revision: Observed field-wide revision for conflict checks.
    :param retired: Whether normal field lookup should hide the definition.
    """

    id: int
    attachment: str
    label: str
    name: str
    datatype: str = "text"
    mode: CollectionMode = "scalar"
    editable: bool = True
    display: dict[str, Any] = field(default_factory=dict)
    revision: int = 0
    retired: bool = False

    @property
    def normalized(self) -> bool:
        """
        Report whether values share a separate vocabulary within this field.

        Example:
            >>> FieldDefinition(1, "works", "score", "Score", datatype="int").normalized
            False


        :return: True for text, enumeration, series and rating; False for inline or
            computed datatypes.
        """
        return self.datatype in {"text", "enumeration", "series", "rating"}

    @property
    def key(self) -> str:
        """
        Build the native metadata key from attachment and label.

        Example:
            >>> FieldDefinition(1, "works", "genre", "Genre").key
            '#works.genre'


        :return: The #attachment.label spelling for this definition.
        """
        return f"#{self.attachment}.{self.label}"


@dataclass(frozen=True)
class SeriesValue:
    """
    Pair a Series name with the position of one owner membership.

    Position belongs to the membership, rather than the shared Series identity.

    Example:
        >>> SeriesValue("Example cycle", 2.5).position
        2.5


    :param value: Series name subject to the field identity policy.
    :param position: Numeric Series position, defaulting to 1.0; unrelated to
        collection ordinal.
    """

    value: str
    position: float = 1.0


@dataclass(frozen=True)
class ValueEntry:
    """
    Describe a stored membership and its separate vocabulary identity, if any.

    id is scoped to this field's membership space; value_id names its vocabulary
    space. Native migrations preserve surviving memberships and do not recycle
    deleted IDs. Supplying this record in a replacement can retain id only while
    that membership still belongs to the selected owner and field. Replacement
    order determines persisted ordinals.

    Example:
        >>> entry = ValueEntry(9, "Example cycle", 0, position=2.5, value_id=4)
        >>> entry.id, entry.value_id
        (9, 4)


    :param id: Stable membership identity within the field.
    :param value: Decoded typed value; a Series entry carries its name here.
    :param ordinal: Zero-based position within the owner collection at the time
        of reading.
    :param position: Series position, or None for other datatypes.
    :param value_id: Shared vocabulary identity, or None for inline storage.
    """

    id: int
    value: Scalar
    ordinal: int
    position: float | None = None
    value_id: int | None = None


@dataclass(frozen=True)
class FieldPatch:
    """
    Replace one owner/field value, optionally checking its field-wide revision.

    None clears the value. Collections replace the complete membership sequence;
    raw entries receive new identities, while valid ValueEntry records can retain
    their owned IDs. The service applies the batch in one managed write unit.

    Example:
        >>> patch = FieldPatch(OwnerRef("works", 7), ("works", "genre"), ["Fiction"])
        >>> patch.expected_revision is None
        True


    :param owner: Existing row receiving the replacement.
    :param field: Numeric field ID or (attachment, label) pair.
    :param value: Typed scalar/collection replacement, or None to clear it.
    :param expected_revision: Required revision of the whole field, or None to
        omit the check.
    """

    owner: OwnerRef
    field: FieldRef
    value: Any
    expected_revision: int | None = None


@dataclass(frozen=True)
class WriteReceipt:
    """
    Report affected identities and revisions for a native write.

    Inside an outer managed transaction, a returned receipt is provisional until
    that transaction commits. Subscribers receive it only after outer commit;
    rollback discards delivery. Listener failure occurs after commit and cannot
    undo SQL effects. Receipts do not refresh caches or metadata projections.

    Migration mappings use old IDs as keys and surviving/new IDs or None as values.
    Ordinary writes may leave these mappings empty. Frozen attributes do not
    protect dictionaries from mutation; consumers should treat them as results.

    Example:
        >>> receipt = WriteReceipt((OwnerRef("works", 7),), (3,), {3: 2})
        >>> receipt.revisions[3]
        2


    :param owners: Owner identities reported as affected by this operation.
    :param fields: Field IDs reported as affected.
    :param revisions: Resulting revisions for fields that still have revision
        records.
    :param entry_ids: Migration mapping from old membership IDs to retained IDs
        or None.
    :param value_ids: Migration mapping from old vocabulary IDs to resulting IDs
        or None.
    """

    owners: tuple[OwnerRef, ...]
    fields: tuple[int, ...]
    revisions: dict[int, int]
    entry_ids: dict[int, int | None] = field(default_factory=dict)
    value_ids: dict[int, int | None] = field(default_factory=dict)


@dataclass(frozen=True)
class Predicate:
    """
    Filter stored field values using canonical equality or typed comparisons.

    eq compares one canonical identity; in accepts candidate values. contains and
    prefix are literal, case-sensitive text comparisons. range takes inclusive
    lower/upper numeric or datetime bounds. unset matches absence of memberships
    and ignores value and quantifier. For other operators, any requires a matching
    membership; all requires a nonempty collection with every membership matching.
    Composite fields must be evaluated through Catalog instead.

    Example:
        >>> predicate = Predicate(("works", "score"), "range", (3, 5))
        >>> predicate.quantifier
        'any'


    :param field: Stored field on the queried attachment, by ID or scoped label.
    :param operator: eq, in, contains, prefix, range or unset.
    :param value: Operand appropriate to the operator; unused for unset.
    :param quantifier: any or non-vacuous all for membership comparisons.
    """

    field: FieldRef
    operator: Literal["eq", "in", "contains", "prefix", "range", "unset"]
    value: Any = None
    quantifier: Literal["any", "all"] = "any"


@dataclass(frozen=True)
class Page:
    """
    Carry a bounded native query result and its exclusive continuation cursor.

    find_owners returns OwnerRef items; inventory returns dictionaries with id,
    value and distinct-owner count under owners. Pass a non-None next_cursor as
    after_id to the same query. None marks the end observed in this snapshot;
    hold an outer service read context to keep later pages on that snapshot.

    Example:
        >>> Page((OwnerRef("works", 7),), None).next_cursor is None
        True


    :param items: Ordered results with the shape defined by the query method.
    :param next_cursor: Exclusive continuation ID, or None when no further
        result was observed.
    """

    items: tuple[Any, ...]
    next_cursor: int | None


@dataclass(frozen=True)
class Issue:
    """
    Locate a custom-field integrity fault without repairing persisted data.

    Example:
        >>> issue = Issue("ordering", 3, "Ordinals need repair", owner_id=7)
        >>> issue.field_id, issue.owner_id
        (3, 7)


    :param code: Machine-readable issue category.
    :param field_id: Affected definition ID, or None for a catalogue-level
        issue.
    :param detail: Human-readable description of the observed fault.
    :param owner_id: Affected owner ID when known.
    :param entry_id: Affected membership ID when known.
    """

    code: str
    field_id: int | None
    detail: str
    owner_id: int | None = None
    entry_id: int | None = None


@dataclass(frozen=True)
class ChangePlan:
    """
    Carry proposed intent, blockers and source state for explicit application.

    Planning alone does not mutate storage. The matching apply operation checks
    the plan kind, source fingerprint and current validity under a writer
    reservation. Issues block application; effects describe the preview rather
    than completed work. A plan is neither a lock nor a promise that later apply
    will succeed. Its payload remains mutable despite frozen attributes.

    Example:
        >>> plan = service.plan_purge(field_id)  # doctest: +SKIP
        >>> blockers = plan.issues  # doctest: +SKIP


    :param kind: migration, purge or repair, selecting the matching apply
        operation.
    :param fingerprint: Observed source state checked again before mutation.
    :param payload: Operation-specific intent interpreted and revalidated by the
        service.
    :param issues: Previewed blockers that prevent application.
    :param effects: Human-readable descriptions of anticipated changes.
    """

    kind: Literal["migration", "purge", "repair"]
    fingerprint: str
    payload: dict[str, Any]
    issues: tuple[Issue, ...] = ()
    effects: tuple[str, ...] = ()
