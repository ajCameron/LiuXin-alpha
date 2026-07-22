"""Portable SQL macro contract shared by every relational backend."""

from __future__ import annotations

import abc
from contextlib import AbstractContextManager
from typing import Any, Iterable, Mapping

from LiuXin_alpha.databases.macro_types import (
    CanonicalIdentity,
    LINK_TYPE_UNSET,
    LinkRow,
    LinkValue,
    UnreferencedRowsSpec,
    NormalizedIdentityMigrationReport,
)
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


class PortableMacrosAPI(abc.ABC):
    """Backend-optimised compound operations with portable semantics."""

    @abc.abstractmethod
    def get_link_rows(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> tuple[LinkRow, ...]:
        """Return complete rows for one primary id, highest priority first."""

    @abc.abstractmethod
    def get_link_rows_bulk(
        self,
        link_spec: StorageLinkSpec,
        primary_ids: Iterable[Any] | None = None,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """Return rows grouped by primary id; ``None`` reads every primary id."""

    @abc.abstractmethod
    def upsert_link(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        link: LinkValue,
    ) -> LinkRow:
        """Insert a link or update the properties of its logical identity.

        Omitted extra values are preserved when the link already exists.
        """

    @abc.abstractmethod
    def upsert_links(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: Iterable[LinkValue],
    ) -> tuple[LinkRow, ...]:
        """Upsert several distinct logical links atomically for one primary id."""

    @abc.abstractmethod
    def replace_links(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: Iterable[LinkValue],
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> tuple[LinkRow, ...]:
        """Atomically synchronise one primary id's desired link set.

        Missing priorities are assigned from the iterable order, highest first.
        A type scope is valid for non-exclusive links whose type is part of
        their identity; links of other types are then left untouched.
        """

    @abc.abstractmethod
    def replace_links_bulk(
        self,
        link_spec: StorageLinkSpec,
        replacements: Mapping[Any, Iterable[LinkValue]],
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """Synchronise several desired link sets in one all-or-nothing change."""

    @abc.abstractmethod
    def replace_owned_one_to_one_values_bulk(
        self,
        link_spec: StorageLinkSpec,
        value_column: str,
        replacements: Mapping[Any, Any | None],
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """Replace values stored in rows owned by one-to-one links.

        Existing linked rows are updated in place. A missing link causes a
        destination row to be created and linked atomically. ``None`` removes
        the link without deleting the destination row.
        """

    @abc.abstractmethod
    def find_table_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """Return an existing logical value's id without creating a row.

        Matching follows the same column metadata, normalization, comparison,
        and identity-scope policy as :meth:`ensure_table_value`.
        """

    @abc.abstractmethod
    def ensure_table_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> Any:
        """Return the id of an existing or newly inserted logical value.

        Comparison follows the database-owned case, normalization, comparison
        column, and empty-value policies while preserving display text.
        """

    @abc.abstractmethod
    def ensure_table_values(
        self,
        table: str,
        value_column: str,
        values: Iterable[Any],
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> dict[Any, Any]:
        """Ensure several values atomically, mapping each input value to its id."""

    @abc.abstractmethod
    def derive_identity_value(
        self,
        table: str,
        value_column: str,
        value: Any,
    ) -> Any:
        """Derive the declared normalized identity for a display value."""

    @abc.abstractmethod
    def get_canonical_identity(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """Resolve a display value to the complete stored canonical identity."""

    @abc.abstractmethod
    def get_canonical_identity_by_key(
        self,
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """Resolve an already-derived identity to its canonical stored row."""

    @abc.abstractmethod
    def get_canonical_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """Resolve a display value and return its canonical stored spelling."""

    @abc.abstractmethod
    def get_canonical_value_by_identity(
        self,
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """Resolve a derived identity and return its canonical stored spelling."""

    @abc.abstractmethod
    def audit_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """Report stale keys and collisions without changing the database."""

    @abc.abstractmethod
    def migrate_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """Install, backfill, and index normalized identities atomically."""

    @abc.abstractmethod
    def temporary_value_table(
        self,
        values: Iterable[Any],
        *,
        column: str = "value",
        declared_type: str = "TEXT",
        prefix: str = "liuxin_values",
    ) -> AbstractContextManager[str]:
        """Yield a populated temporary-table name and remove it on context exit.

        The portable declared types are BLOB, INTEGER, NUMERIC, REAL, and TEXT.
        """

    @abc.abstractmethod
    def temporary_id_table(
        self,
        values: Iterable[Any],
        *,
        prefix: str = "liuxin_ids",
    ) -> AbstractContextManager[str]:
        """Yield a temporary table with one INTEGER column named ``id``."""

    @abc.abstractmethod
    def delete_unreferenced_rows(
        self,
        table: str,
        link_specs: Iterable[StorageLinkSpec],
        *,
        id_column: str | None = None,
        protected_ids: Iterable[Any] = (),
    ) -> tuple[Any, ...]:
        """Delete and return ids unreferenced by every supplied link spec.

        At least one supplied spec must genuinely reference the target table.
        """

    @abc.abstractmethod
    def delete_unreferenced_rows_bulk(
        self,
        specs: Iterable[UnreferencedRowsSpec],
    ) -> dict[str, tuple[Any, ...]]:
        """Prune several distinct tables in one all-or-nothing transaction."""

    @abc.abstractmethod
    def fingerprint_table(
        self,
        target_table: str,
        columns: Iterable[str] | None = None,
        *,
        order_by: Iterable[str] | None = None,
        where: Mapping[str, Any] | None = None,
        algorithm: str = "sha256",
    ) -> str:
        """Return a typed, deterministic digest of selected table content."""


__all__ = ["PortableMacrosAPI"]
