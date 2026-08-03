"""Repository for identifiers owned by catalog entities."""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import (
    CatalogMutationError,
    EntityId,
    IdentifierCandidate,
    MatchResult,
    RowMapping,
    WemiLevel,
)
from .base import BaseRepository, WEMI_TABLES


class IdentifierRepository(BaseRepository):
    """Store and resolve identifiers in the polymorphic ownership table."""

    table_name = "entity_identifiers"
    id_column = "entity_identifier_id"
    input_aliases: ClassVar[Mapping[str, str]] = {
        "id": "entity_identifier_id",
        "identifier_type": "entity_identifier_scheme",
        "scheme": "entity_identifier_scheme",
        "value": "entity_identifier_value",
        "source": "entity_identifier_provenance",
        "provenance": "entity_identifier_provenance",
        "is_primary": "entity_identifier_is_primary",
        "level": "entity_identifier_entity_type",
        "entity_type": "entity_identifier_entity_type",
        "entity_id": "entity_identifier_entity_id",
    }

    def normalise(self, candidate: IdentifierCandidate) -> IdentifierCandidate:
        """Return a stable identifier comparison form.

        :param candidate: Raw identifier candidate.
        :return: Candidate with normalized scheme and value.
        """

        from ..matching.policy import normalise_identifier

        return normalise_identifier(candidate)

    def find(self, *, identifier_type: str, value: str) -> RowMapping | None:
        """Find an identifier by normalized scheme and value.

        :param identifier_type: Identifier scheme, for example ``isbn13``.
        :param value: Identifier value.
        :return: First matching identifier row, or ``None``.
        """

        if not isinstance(identifier_type, str) or not isinstance(value, str):
            raise TypeError("identifier_type and value must be strings")
        wanted = self.normalise(IdentifierCandidate(identifier_type, value))
        for row in self._all_rows():
            try:
                existing = self.normalise(
                    IdentifierCandidate(
                        str(row.get("entity_identifier_scheme") or ""),
                        str(row.get("entity_identifier_value") or ""),
                    )
                )
            except (TypeError, ValueError):
                continue
            if existing.identifier_type == wanted.identifier_type and (
                existing.normalised_value == wanted.normalised_value
            ):
                return row
        return None

    def match(self, candidate: IdentifierCandidate) -> MatchResult:
        """Return the exact existing identifier for ``candidate``.

        :param candidate: Identifier candidate to match.
        :return: Explained exact match or non-match result.
        """

        from ..matching.identifier_matcher import IdentifierMatcher

        return IdentifierMatcher(self.db, self.repositories).best(candidate)

    def match_or_create(self, candidate: IdentifierCandidate) -> EntityId:
        """Return an exact identifier row or create a new logical value.

        :param candidate: Identifier to normalize and persist when absent.
        :return: Existing or newly created identifier row ID.
        """

        normalised = self.normalise(candidate)
        match = self.match(normalised)
        if match.is_match:
            assert match.entity_id is not None
            return match.entity_id
        return self.create(
            {
                "identifier_type": normalised.identifier_type,
                "value": normalised.value,
                "source": normalised.source,
            }
        )

    def link_to_wemi(
        self,
        *,
        identifier_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        priority: int | None = None,
    ) -> EntityId:
        """Assign an identifier row to a WEMI entity.

        Identifier ownership is represented directly on ``entity_identifiers``.
        Assigning an already-owned identifier to a different entity creates a
        second owned row, preserving the original entity's identifier.

        :return: The assigned identifier row ID.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._assign(
            identifier_id=identifier_id,
            entity_type=level,
            entity_table=WEMI_TABLES[level],
            entity_id=entity_id,
            priority=priority,
        )

    def link_to_agent(
        self,
        *,
        identifier_id: EntityId,
        agent_id: EntityId,
        priority: int | None = None,
    ) -> EntityId:
        """Assign an identifier row to an Agent.

        Assigning an identifier already owned by another entity copies its
        logical value, matching :meth:`link_to_wemi` ownership semantics.

        :param identifier_id: Existing identifier row ID.
        :param agent_id: Existing Agent ID.
        :param priority: Optional priority, where zero marks the primary value.
        :return: Assigned identifier row ID.
        """

        return self._assign(
            identifier_id=identifier_id,
            entity_type="agent",
            entity_table="agents",
            entity_id=agent_id,
            priority=priority,
        )

    def _assign(
        self,
        *,
        identifier_id: EntityId,
        entity_type: str,
        entity_table: str,
        entity_id: EntityId,
        priority: int | None,
    ) -> EntityId:
        self._require_table_row(entity_table, entity_id)
        row = dict(self.require(identifier_id))
        current_level = row.get("entity_identifier_entity_type")
        current_id = row.get("entity_identifier_entity_id")
        changes = {
            "entity_identifier_entity_type": entity_type,
            "entity_identifier_entity_id": entity_id,
        }
        if priority is not None:
            if not isinstance(priority, int) or isinstance(priority, bool):
                raise TypeError("priority must be an integer or None")
            changes["entity_identifier_is_primary"] = int(priority == 0)
        if current_level in (None, entity_type) and current_id in (None, entity_id):
            self.update(identifier_id, changes)
            return identifier_id
        copied = {
            key: value
            for key, value in row.items()
            if key != self.id_column and not key.endswith("_timestamp_ep_k")
        }
        copied.update(changes)
        return self.create(copied)

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """Return identifier rows owned by one WEMI entity."""

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        return self._list_for_owner(
            entity_type=level,
            entity_table=WEMI_TABLES[level],
            entity_id=entity_id,
        )

    def primary_values_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Mapping[str, str]:
        """Return the primary Identifier value for each normalized scheme."""

        result: dict[str, str] = {}
        for row in self.list_for_wemi(level=level, entity_id=entity_id):
            if not row.get("entity_identifier_is_primary"):
                continue
            scheme = row.get("entity_identifier_scheme")
            value = row.get("entity_identifier_value")
            if not isinstance(scheme, str) or not isinstance(value, str):
                raise CatalogMutationError(
                    "primary identifier rows require string scheme and value"
                )
            if scheme in result:
                raise CatalogMutationError(
                    f"multiple primary identifiers exist for scheme {scheme!r}"
                )
            result[scheme] = value
        return result

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        identifiers: Mapping[str, str],
    ) -> Mapping[str, EntityId]:
        """Replace the complete identifier mapping for one WEMI entity.

        :param level: WEMI level which owns the identifiers.
        :param entity_id: Existing WEMI entity ID.
        :param identifiers: Identifier values keyed by scheme.
        :return: Assigned identifier IDs keyed by normalized scheme.
        """

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(identifiers, Mapping):
            raise TypeError("identifiers must be a string mapping")
        candidates: list[IdentifierCandidate] = []
        normalised_schemes: set[str] = set()
        for scheme, value in identifiers.items():
            if not isinstance(scheme, str) or not scheme.strip():
                raise TypeError("identifier schemes must be non-empty strings")
            if not isinstance(value, str) or not value.strip():
                raise TypeError("identifier values must be non-empty strings")
            candidate = self.normalise(IdentifierCandidate(scheme, value))
            if candidate.identifier_type in normalised_schemes:
                raise CatalogMutationError(
                    "identifiers contain a duplicate normalized scheme: "
                    f"{candidate.identifier_type!r}"
                )
            normalised_schemes.add(candidate.identifier_type)
            candidates.append(candidate)

        with self._macros.transaction():
            existing_ids = {
                row[self.id_column]
                for row in self.list_for_wemi(level=level, entity_id=entity_id)
            }
            assigned: dict[str, EntityId] = {}
            assigned_ids: set[EntityId] = set()
            for candidate in candidates:
                identifier_id = self.match_or_create(candidate)
                assigned_id = self.link_to_wemi(
                    identifier_id=identifier_id,
                    level=level,
                    entity_id=entity_id,
                    priority=0,
                )
                assigned[candidate.identifier_type] = assigned_id
                assigned_ids.add(assigned_id)
            for identifier_id in existing_ids - assigned_ids:
                self.delete(identifier_id)
        return assigned

    def list_for_agent(self, agent_id: EntityId) -> Sequence[RowMapping]:
        """Return identifier rows owned by one Agent.

        :param agent_id: Existing Agent ID.
        :return: Agent-owned identifier rows in stable ID order.
        """

        return self._list_for_owner(
            entity_type="agent",
            entity_table="agents",
            entity_id=agent_id,
        )

    def _list_for_owner(
        self,
        *,
        entity_type: str,
        entity_table: str,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        self._require_table_row(entity_table, entity_id)
        return tuple(
            row
            for row in self._all_rows()
            if row.get("entity_identifier_entity_type") == entity_type
            and row.get("entity_identifier_entity_id") == entity_id
        )


__all__ = ["IdentifierRepository"]
