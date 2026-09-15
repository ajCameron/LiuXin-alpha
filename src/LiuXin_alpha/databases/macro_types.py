"""
Define backend-neutral link, identity and migration records for portable macros.

Frozen slotted dataclasses prevent field reassignment but do not validate values or deep-freeze supplied mappings. Default mappings are new per instance. LINK_TYPE_UNSET is the identity-tested sentinel for an omitted filter; None can instead select a SQL NULL link type.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


class UnsetLinkType:
    """
    Provide the display form for the exported omitted-link-type sentinel.

    Use LINK_TYPE_UNSET rather than constructing another instance: consumers compare the exported singleton by identity. This class does not enforce singleton construction or equality between instances.

    Example:
        >>> repr(LINK_TYPE_UNSET)
        'LINK_TYPE_UNSET'
        >>> UnsetLinkType() is LINK_TYPE_UNSET
        False
    """

    __slots__ = ()

    def __repr__(self) -> str:
        """
        Return the sentinel’s stable diagnostic label.

        Example:
            >>> repr(LINK_TYPE_UNSET)
            'LINK_TYPE_UNSET'


        :return: The text LINK_TYPE_UNSET, without distinguishing separate instances.
        """

        return "LINK_TYPE_UNSET"


LINK_TYPE_UNSET = UnsetLinkType()


@dataclass(frozen=True, slots=True)
class LinkValue:
    """
    Describe the desired secondary row and writable properties of one link.

    secondary_id identifies the target; link_type and priority default to None and are interpreted by the chosen macro/spec. extra supplies nonstandard column values. This frozen record performs no database validation and retains supplied mappings by reference.

    Example:
        >>> desired = LinkValue(7, link_type="author", priority=1)
        >>> (desired.secondary_id, desired.extra)
        (7, {})
    """

    secondary_id: Any
    link_type: Any = None
    priority: int | float | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LinkRow:
    """
    Carry a complete link result with both endpoints and additional columns.

    primary_id and secondary_id identify the relation endpoints. link_type/priority are None when absent from the link shape; extra holds other returned columns, potentially including the link row’s own key. Frozen fields do not make extra deeply immutable.

    Example:
        >>> row = LinkRow(1, 7, link_type="author", extra={"note": "source"})
        >>> row.extra["note"]
        'source'
    """

    primary_id: Any
    secondary_id: Any
    link_type: Any = None
    priority: int | float | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class UnreferencedRowsSpec:
    """
    Describe one table’s orphan-pruning request for a bulk macro.

    table selects candidate rows and link_specs supplies relations used to establish references. id_column can override key discovery; protected_ids exempts rows from deletion. Construction validates neither schema membership nor a nonempty link list; the executing macro applies those checks.

    Example:
        spec = UnreferencedRowsSpec("agents", (work_agent_links,), protected_ids=(1,))
        deleted = macros.delete_unreferenced_rows_bulk((spec,))
    """

    table: str
    link_specs: tuple[StorageLinkSpec, ...]
    id_column: str | None = None
    protected_ids: tuple[Any, ...] = ()


@dataclass(frozen=True, slots=True)
class CanonicalIdentity:
    """
    Pair one stored canonical value with its normalized identity and scope.

    table/row_id locate the stored row; value_column/canonical_value hold its display value and identity_column/identity_value its derived lookup key. scope_values records any additional identity scope. The record does not derive or validate a key and does not copy the supplied scope mapping.

    Example:
        >>> identity = CanonicalIdentity("tags", 1, "tag", "History", "tag_phash", "history")
        >>> identity.canonical_value
        'History'
    """

    table: str
    row_id: Any
    value_column: str
    canonical_value: Any
    identity_column: str
    identity_value: Any
    scope_values: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NormalizedIdentityCollision:
    """
    Describe rows that would share a declared unique identity within one scope.

    table, value_column and identity_column locate the declaration; identity_value and scope_values identify the conflicting group. row_ids and canonical_values are corresponding tuples produced by the audit. Construction does not check their lengths, normalize values or resolve collisions.

    Example:
        >>> collision = NormalizedIdentityCollision("tags", "tag", "tag_phash", "history", {}, (1, 2), ("History", "history"))
        >>> collision.row_ids
        (1, 2)
    """

    table: str
    value_column: str
    identity_column: str
    identity_value: Any
    scope_values: Mapping[str, Any]
    row_ids: tuple[Any, ...]
    canonical_values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class NormalizedIdentityMigrationReport:
    """
    Report inspected identity declarations, pending changes and migration effects.

    declarations_checked and rows_examined count audit work; rows_needing_update records stale/missing derived values and rows_updated records applied updates. columns_added and indexes_created name schema additions. collisions contains conflicting groups. clean only checks collisions, so a clean audit can still require updates. Counts and containers are accepted without validation.

    Example:
        >>> report = NormalizedIdentityMigrationReport(1, 3, 2, 0)
        >>> (report.clean, report.rows_needing_update)
        (True, 2)
    """

    declarations_checked: int
    rows_examined: int
    rows_needing_update: int
    rows_updated: int
    columns_added: tuple[str, ...] = ()
    indexes_created: tuple[str, ...] = ()
    collisions: tuple[NormalizedIdentityCollision, ...] = ()

    @property
    def clean(self) -> bool:
        """
        Check whether the report contains no identity collisions.

        Example:
            >>> NormalizedIdentityMigrationReport(1, 3, 2, 0).clean
            True


        :return: True when collisions is empty, even if updates remain pending.
        """

        return not self.collisions


__all__ = [
    "LINK_TYPE_UNSET",
    "CanonicalIdentity",
    "LinkRow",
    "LinkValue",
    "NormalizedIdentityCollision",
    "NormalizedIdentityMigrationReport",
    "UnreferencedRowsSpec",
    "UnsetLinkType",
]
