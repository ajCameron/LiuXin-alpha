"""Database-backed foundations shared by catalog repositories."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Sequence
import unicodedata

from LiuXin_alpha.databases.macro_types import LINK_TYPE_UNSET, LinkRow, LinkValue
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec

from ..api.common import (
    CatalogMutationError,
    CatalogNotFoundError,
    DatabaseHandle,
    EntityId,
    RowInput,
    RowMapping,
    WemiLevel,
)

if TYPE_CHECKING:
    from ..matching.policy import MatchingPolicy


WEMI_TABLES: Mapping[WemiLevel, str] = {
    "work": "works",
    "expression": "expressions",
    "manifestation": "manifestations",
    "item": "items",
}


def normalise_text(value: object) -> str:
    """Return a stable comparison form for human-readable metadata."""

    text = unicodedata.normalize("NFKC", str(value))
    return " ".join(text.casefold().split())


class BaseRepository:
    """Provide validated generic CRUD over one writable catalog table.

    Concrete repositories supply their table and ID column and may declare
    caller-facing aliases for storage columns. SQL and transaction handling
    remain in the database driver and portable macro layers.

    :param db: Database handle used for persistence.
    """

    table_name: ClassVar[str] = ""
    id_column: ClassVar[str] = "id"
    input_aliases: ClassVar[Mapping[str, str]] = {}

    def __init__(self, db: DatabaseHandle) -> None:
        """Store the database dependency.

        :param db: Database handle used for persistence.
        :return: None.
        """

        self.db = db
        self._repositories: Any = None
        self._matching_policy: MatchingPolicy | None = None

    def bind_repositories(self, repositories: Any) -> None:
        """Bind the completed repository group for cross-repository services.

        :param repositories: Group containing every catalog repository.
        :return: None.
        """

        self._repositories = repositories

    def bind_matching_policy(self, policy: MatchingPolicy) -> None:
        """Bind the identity policy selected by the catalog composition root.

        :param policy: Shared catalog matching policy.
        :return: None.
        """

        self._matching_policy = policy

    @property
    def repositories(self) -> Any:
        """Return the bound repository group.

        :return: Catalog repository group.
        :raises RuntimeError: If the repository has not been composed by
            :class:`Catalog`.
        """

        if self._repositories is None:
            raise RuntimeError("repository group has not been bound")
        return self._repositories

    @property
    def matching_policy(self) -> MatchingPolicy:
        """Return the bound identity policy or the standalone default.

        :return: Catalog matching policy.
        """

        if self._matching_policy is None:
            from ..matching.policy import DEFAULT_MATCHING_POLICY

            return DEFAULT_MATCHING_POLICY
        return self._matching_policy

    @property
    def _wrapper(self) -> Any:
        wrapper = getattr(self.db, "driver_wrapper", None)
        if wrapper is None:
            raise TypeError("catalog database must provide driver_wrapper")
        return wrapper

    @property
    def _macros(self) -> Any:
        macros = getattr(self.db, "macros", None)
        if macros is None:
            raise TypeError("catalog database must provide portable macros")
        return macros

    @staticmethod
    def _validate_entity_id(entity_id: EntityId) -> None:
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            raise TypeError("entity_id must be an integer")
        if entity_id < 0:
            raise ValueError("entity_id cannot be negative")

    @property
    def columns(self) -> tuple[str, ...]:
        """Return the current storage columns for this repository.

        :return: Database column names in declared order.
        """

        return tuple(self._wrapper.get_column_headings(self.table_name))

    def normalise_input(
        self,
        data: RowInput,
        *,
        allow_id: bool = False,
        ignore_unknown: bool = False,
    ) -> dict[str, Any]:
        """Map public aliases to validated storage columns.

        :param data: Caller-supplied row values.
        :param allow_id: Permit the repository ID column in the result.
        :param ignore_unknown: Omit unknown keys instead of rejecting them.
        :return: Plain storage-column mapping.
        :raises TypeError: If ``data`` is not a string-keyed mapping.
        :raises CatalogMutationError: If a key is unknown, read-only, or maps
            ambiguously.
        """

        if not isinstance(data, Mapping):
            raise TypeError("repository data must be a mapping")
        available = set(self.columns)
        result: dict[str, Any] = {}
        for raw_key, value in data.items():
            if not isinstance(raw_key, str):
                raise TypeError("repository data keys must be strings")
            key = self.input_aliases.get(raw_key, raw_key)
            if key not in available:
                if ignore_unknown:
                    continue
                raise CatalogMutationError(
                    f"{raw_key!r} is not writable through {type(self).__name__}"
                )
            if key == self.id_column and not allow_id:
                raise CatalogMutationError(
                    f"{self.id_column!r} cannot be changed through repository data"
                )
            if key in result and result[key] != value:
                raise CatalogMutationError(
                    f"multiple inputs specify conflicting values for {key!r}"
                )
            result[key] = value
        return result

    @staticmethod
    def _as_mapping(row: object) -> dict[str, Any]:
        if isinstance(row, Mapping):
            return dict(row)
        keys = getattr(row, "keys", None)
        if callable(keys):
            return {key: row[key] for key in keys()}  # type: ignore[index]
        raise TypeError("database rows must provide a mapping interface")

    def get(self, entity_id: EntityId) -> RowMapping | None:
        """Return one entity by ID.

        :param entity_id: Repository entity ID.
        :return: Plain row mapping, or ``None`` when absent.
        """

        self._validate_entity_id(entity_id)
        row = self._macros.get_row(
            self.table_name,
            entity_id,
            id_column=self.id_column,
        )
        return None if not row else self._as_mapping(row)

    def require(self, entity_id: EntityId) -> RowMapping:
        """Return one entity or raise a catalog-specific not-found error.

        :param entity_id: Repository entity ID.
        :return: Existing row mapping.
        :raises CatalogNotFoundError: If the row does not exist.
        """

        row = self.get(entity_id)
        if row is None:
            raise CatalogNotFoundError(f"{self.table_name} row not found: {entity_id}")
        return row

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        """Return a stable ID-ordered page of entities.

        :param limit: Maximum rows to return.
        :param offset: Number of rows to skip.
        :return: Tuple of plain row mappings.
        """

        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("limit must be an integer")
        if not isinstance(offset, int) or isinstance(offset, bool):
            raise TypeError("offset must be an integer")
        if limit < 0 or offset < 0:
            raise ValueError("limit and offset cannot be negative")
        if limit == 0:
            return ()
        materialised = self._all_rows()
        return materialised[offset : offset + limit]

    def _all_rows(self) -> tuple[RowMapping, ...]:
        rows = self._macros.get_rows(
            self.table_name,
            order_by=(self.id_column,),
        )
        return tuple(self._as_mapping(row) for row in rows)

    def create(self, data: RowInput) -> EntityId:
        """Create an entity and return its database-assigned ID.

        :param data: Public aliases or storage-column values.
        :return: New entity ID.
        :raises CatalogMutationError: If the payload is empty or insertion does
            not return an ID.
        """

        payload = self.normalise_input(data)
        if not payload:
            raise CatalogMutationError(
                f"{type(self).__name__}.create requires at least one value"
            )
        new_id = self._macros.insert_row(
            self.table_name,
            payload,
            id_column=self.id_column,
        )
        if not isinstance(new_id, int) or isinstance(new_id, bool):
            raise CatalogMutationError(
                f"database did not return an ID for new {self.table_name} row"
            )
        return new_id

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """Update writable values on an existing entity.

        :param entity_id: Entity to update.
        :param data: Public aliases or storage-column values.
        :return: None.
        """

        self.require(entity_id)
        changes = self.normalise_input(data)
        if not changes:
            return
        self._macros.update_row(
            self.table_name,
            entity_id,
            changes,
            id_column=self.id_column,
        )

    def delete(self, entity_id: EntityId) -> None:
        """Delete an existing entity by ID.

        :param entity_id: Entity to delete.
        :return: None.
        """

        self.require(entity_id)
        self._macros.delete_row(
            self.table_name,
            entity_id,
            id_column=self.id_column,
        )

    def _require_table_row(self, table: str, entity_id: EntityId) -> RowMapping:
        self._validate_entity_id(entity_id)
        row = self._macros.get_row(table, entity_id)
        if not row:
            raise CatalogNotFoundError(f"{table} row not found: {entity_id}")
        return self._as_mapping(row)

    def _link_spec(self, primary_table: str, secondary_table: str) -> StorageLinkSpec:
        spec = self._wrapper.get_link_spec(primary_table, secondary_table)
        if not isinstance(spec, StorageLinkSpec):
            raise CatalogMutationError(
                f"no catalog link exists from {primary_table!r} to {secondary_table!r}"
            )
        return spec

    def _link(
        self,
        primary_table: str,
        primary_id: EntityId,
        secondary_table: str,
        secondary_id: EntityId,
        *,
        link_type: str | None = None,
        priority: int | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> LinkRow:
        with self._macros.transaction():
            self._require_table_row(primary_table, primary_id)
            self._require_table_row(secondary_table, secondary_id)
            spec = self._link_spec(primary_table, secondary_table)
            if spec.ordered and priority is None:
                rows = self._macros.get_link_rows(spec, primary_id)
                existing = next(
                    (
                        row
                        for row in rows
                        if row.secondary_id == secondary_id
                        and (
                            not spec.type_part_of_identity
                            or row.link_type == link_type
                        )
                    ),
                    None,
                )
                if existing is not None:
                    priority = existing.priority
                else:
                    assigned = tuple(
                        row.priority
                        for row in rows
                        if isinstance(row.priority, (int, float))
                        and not isinstance(row.priority, bool)
                    )
                    priority = int(max(assigned, default=0)) + 1
            return self._macros.upsert_link(
                spec,
                primary_id,
                LinkValue(
                    secondary_id,
                    link_type=link_type,
                    priority=priority,
                    extra=dict(extra or {}),
                ),
            )

    @staticmethod
    def _link_metadata(row: LinkRow) -> dict[str, Any]:
        return {
            "primary_id": row.primary_id,
            "secondary_id": row.secondary_id,
            "type": row.link_type,
            "priority": row.priority,
            "extra": dict(row.extra),
        }

    def _linked_rows(
        self,
        primary_table: str,
        primary_id: EntityId,
        secondary_table: str,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[RowMapping, ...]:
        self._require_table_row(primary_table, primary_id)
        spec = self._link_spec(primary_table, secondary_table)
        result: list[RowMapping] = []
        for link in self._macros.get_link_rows(
            spec,
            primary_id,
            link_type=link_type,
        ):
            row = self._macros.get_row(secondary_table, link.secondary_id)
            if not row:
                raise CatalogNotFoundError(
                    f"{secondary_table} row linked from {primary_table}:"
                    f"{primary_id} is missing: {link.secondary_id}"
                )
            rendered = self._as_mapping(row)
            rendered["_catalog_link"] = self._link_metadata(link)
            result.append(rendered)
        return tuple(result)

__all__ = ["BaseRepository", "WEMI_TABLES", "normalise_text"]
