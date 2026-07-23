"""Coordinated semantic catalog mutations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, cast

from LiuXin_alpha.databases.macro_types import LinkValue

from ..api.common import (
    CatalogMutationError,
    DatabaseHandle,
    EntityId,
    IdentifierCandidate,
    MetadataCandidate,
    RowInput,
    RowMapping,
    WemiLevel,
)
from ..repositories.base import WEMI_TABLES
from .mutation_policy import MutationPolicy


@dataclass(frozen=True, slots=True)
class CreatedWemiStack:
    """IDs created by one coordinated WEMI-stack mutation."""

    work_id: EntityId
    expression_id: EntityId
    manifestation_id: EntityId
    item_ids: tuple[EntityId, ...] = ()


class MetadataWriter:
    """Coordinated writes that may touch multiple catalog repositories."""

    def __init__(self, db: DatabaseHandle, repositories: Any, policy: MutationPolicy) -> None:
        self.db = db
        self.repositories = repositories
        self.policy = policy

    def create_wemi_stack(
        self,
        *,
        work: RowInput,
        expression: RowInput,
        manifestation: RowInput,
        items: Sequence[RowInput] = (),
        origin: str | None = None,
        work_id: EntityId | None = None,
    ) -> CreatedWemiStack:
        """Atomically create and link one Work-to-Items WEMI path.

        The method owns graph coordination, not input-format interpretation.
        Callers provide repository payloads for each level; every Item is
        assigned to the newly created Manifestation.

        :param work: New Work repository payload.
        :param expression: New preferred or alternate Expression payload.
        :param manifestation: New Manifestation payload.
        :param items: Zero or more new Item payloads.
        :param origin: Optional provenance stored on both graph links.
        :param work_id: Optional Work ID to update or explicitly create. When
            supplied, the new preferred Expression replaces that Work's
            existing Work-to-Expression links.
        :return: IDs of every created WEMI entity.
        :raises CatalogMutationError: If a payload is empty, an Item targets a
            different Manifestation, or link metadata cannot be represented.
        """

        payloads = {
            "work": work,
            "expression": expression,
            "manifestation": manifestation,
        }
        for level, payload in payloads.items():
            if not self.policy.can_create(level=cast(WemiLevel, level), data=payload):
                raise CatalogMutationError(
                    f"WEMI stack requires a non-empty {level} payload"
                )
        raw_items: object = items
        if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
            raise TypeError("items must be a sequence of mappings")
        item_payloads: list[dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, Mapping) or not item:
                raise CatalogMutationError("every WEMI stack Item must be a non-empty mapping")
            item_payloads.append(dict(item))
        if origin is not None and not isinstance(origin, str):
            raise TypeError("origin must be a string or None")
        if work_id is not None and (
            not isinstance(work_id, int) or isinstance(work_id, bool) or work_id < 0
        ):
            raise TypeError("work_id must be a non-negative integer or None")

        with self.db.macros.transaction():
            if work_id is None:
                created_work_id = self.repositories.works.create(work)
            elif self.repositories.works.get(work_id) is None:
                payload = self.repositories.works.normalise_input(work)
                payload["work_id"] = work_id
                inserted_work_id = self.db.macros.insert_row(
                    "works",
                    payload,
                    id_column="work_id",
                )
                if inserted_work_id != work_id:
                    raise CatalogMutationError(
                        "database did not preserve the requested Work ID"
                    )
                created_work_id = work_id
            else:
                self.repositories.works.update(work_id, work)
                created_work_id = work_id
            expression_id = self.repositories.expressions.create(expression)
            work_expression_extra = self._wemi_link_extra(
                "works",
                "expressions",
                primary=True,
                origin=origin,
            )
            if work_id is None:
                self.repositories.works._link(
                    "works",
                    created_work_id,
                    "expressions",
                    expression_id,
                    priority=0,
                    extra=work_expression_extra,
                )
            else:
                work_expression_spec = self.repositories.works._link_spec(
                    "works",
                    "expressions",
                )
                self.db.macros.replace_links(
                    work_expression_spec,
                    created_work_id,
                    (
                        LinkValue(
                            expression_id,
                            priority=0,
                            extra=work_expression_extra,
                        ),
                    ),
                )
            manifestation_id = self.repositories.manifestations.create(manifestation)
            self.repositories.expressions._link(
                "expressions",
                expression_id,
                "manifestations",
                manifestation_id,
                priority=0,
                extra=self._wemi_link_extra(
                    "expressions",
                    "manifestations",
                    primary=True,
                    origin=origin,
                ),
            )
            item_ids: list[EntityId] = []
            for item_payload in item_payloads:
                supplied_manifestation = item_payload.get(
                    "manifestation_id",
                    item_payload.get("item_manifestation_id"),
                )
                if supplied_manifestation not in (None, manifestation_id):
                    raise CatalogMutationError(
                        "WEMI stack Item cannot target a different Manifestation"
                    )
                item_payload.pop("manifestation_id", None)
                item_payload["item_manifestation_id"] = manifestation_id
                item_ids.append(self.repositories.items.create(item_payload))
        return CreatedWemiStack(
            work_id=created_work_id,
            expression_id=expression_id,
            manifestation_id=manifestation_id,
            item_ids=tuple(item_ids),
        )

    def _wemi_link_extra(
        self,
        primary_table: str,
        secondary_table: str,
        *,
        primary: bool,
        origin: str | None,
    ) -> dict[str, object]:
        spec = self.repositories.works._link_spec(primary_table, secondary_table)
        writable = {
            column.name
            for column in spec.extra_link_columns
            if not column.is_primary_key
        }
        result: dict[str, object] = {}
        primary_column = next(
            (name for name in writable if name.endswith("_primary")),
            None,
        )
        origin_column = next(
            (name for name in writable if name.endswith("_origin")),
            None,
        )
        if primary_column is None:
            raise CatalogMutationError(
                f"{primary_table}-to-{secondary_table} link has no primary marker"
            )
        result[primary_column] = int(primary)
        if origin is not None:
            if origin_column is None:
                raise CatalogMutationError(
                    f"{primary_table}-to-{secondary_table} link has no origin field"
                )
            result[origin_column] = origin
        return result

    def attach_metadata(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> None:
        """Attach direct fields, titles, Agents, identifiers, and notes.

        Structured values use the keys ``fields``, ``title``/``titles``,
        ``agents``, ``identifiers``, and ``notes``. Unreserved top-level keys
        are treated as direct fields on the selected WEMI entity.
        """

        transaction = getattr(self.db.macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            self._attach_metadata(level=level, entity_id=entity_id, data=data)

    def _attach_metadata(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> None:

        if not self.policy.can_update(level=level, entity_id=entity_id, data=data):
            raise CatalogMutationError(f"metadata attachment rejected for {level}:{entity_id}")
        payload = dict(data)
        reserved = {"fields", "title", "titles", "agents", "identifiers", "notes"}
        direct = {key: value for key, value in payload.items() if key not in reserved}
        fields = payload.get("fields", {})
        if not isinstance(fields, Mapping):
            raise CatalogMutationError("fields must be a mapping")
        direct.update(fields)

        title_values = self._as_sequence(payload.get("titles", ()))
        if "title" in payload:
            title_values = (payload["title"], *title_values)
        agent_values = self._as_sequence(payload.get("agents", ()))
        identifier_values = self._as_sequence(payload.get("identifiers", ()))
        note_values = self._as_sequence(payload.get("notes", ()))
        self._preflight_attachments(
            level=level,
            entity_id=entity_id,
            titles=title_values,
            agents=agent_values,
            identifiers=identifier_values,
            notes=note_values,
        )

        repository = getattr(self.repositories, f"{level}s")
        if direct:
            repository.update(entity_id, direct)
        for title in title_values:
            title_data = (
                {"title": title}
                if isinstance(title, str)
                else dict(cast(Mapping[str, Any], title))
            )
            self.repositories.titles.add_for_wemi(
                level=level,
                entity_id=entity_id,
                data=title_data,
            )
        for value in agent_values:
            record = dict(cast(Mapping[str, Any], value))
            agent_id = record.get("agent_id")
            if agent_id is None:
                agent_data = record.get("data")
                if agent_data is None:
                    agent_data = {
                        key: item
                        for key, item in record.items()
                        if key not in {"role", "priority"}
                    }
                agent_id = self.repositories.agents.match_or_create(
                    MetadataCandidate(agent_data)
                )
            self.repositories.agents.link_to_wemi(
                agent_id=agent_id,
                level=level,
                entity_id=entity_id,
                role=record["role"],
                priority=record.get("priority"),
            )
        for value in identifier_values:
            record = dict(cast(Mapping[str, Any], value))
            identifier_id = record.get("identifier_id")
            if identifier_id is None:
                identifier_type = record.get(
                    "identifier_type",
                    record.get("scheme"),
                )
                identifier_value = record.get("value")
                normalised_value = record.get("normalised_value")
                source = record.get("source", record.get("provenance"))
                if not isinstance(identifier_type, str) or not identifier_type:
                    raise CatalogMutationError(
                        "identifier scheme/type must be a non-empty string"
                    )
                if not isinstance(identifier_value, str) or not identifier_value:
                    raise CatalogMutationError(
                        "identifier value must be a non-empty string"
                    )
                if normalised_value is not None and not isinstance(
                    normalised_value,
                    str,
                ):
                    raise CatalogMutationError(
                        "normalised identifier value must be a string or None"
                    )
                if source is not None and not isinstance(source, str):
                    raise CatalogMutationError(
                        "identifier source must be a string or None"
                    )
                identifier_id = self.repositories.identifiers.match_or_create(
                    IdentifierCandidate(
                        identifier_type=identifier_type,
                        value=identifier_value,
                        normalised_value=normalised_value,
                        source=source,
                    )
                )
            self.repositories.identifiers.link_to_wemi(
                identifier_id=identifier_id,
                level=level,
                entity_id=entity_id,
                priority=record.get("priority"),
            )
        for value in note_values:
            note_data = (
                {"note": value}
                if isinstance(value, str)
                else dict(cast(Mapping[str, Any], value))
            )
            self.repositories.notes.add_for_wemi(
                level=level,
                entity_id=entity_id,
                data=note_data,
            )

    def merge_entities(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> None:
        """Merge one entity into another while preserving metadata and links."""

        transaction = getattr(self.db.macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            self._merge_entities(
                level=level,
                source_id=source_id,
                target_id=target_id,
            )

    def _merge_entities(
        self,
        *,
        level: WemiLevel,
        source_id: EntityId,
        target_id: EntityId,
    ) -> None:

        if not self.policy.can_merge(level=level, source_id=source_id, target_id=target_id):
            raise CatalogMutationError(f"merge rejected for {level}:{source_id}->{target_id}")
        repository = getattr(self.repositories, f"{level}s")
        source = repository.require(source_id)
        target = repository.require(target_id)
        changes = self._missing_target_values(repository, source, target)
        if changes:
            repository.update(target_id, changes)

        table = WEMI_TABLES[level]
        for secondary in self._relationship_targets(level):
            self._transfer_links(table, source_id, target_id, secondary)
        self._transfer_links(table, source_id, target_id, "agents")
        self._transfer_links(table, source_id, target_id, "notes")
        self._transfer_identifiers(level, source_id, target_id)

        if level == "item":
            pass
        elif level == "manifestation":
            for item in self.repositories.items.list_for_manifestation(source_id):
                self.repositories.items.update(
                    item["item_id"],
                    {"item_manifestation_id": target_id},
                )
        repository.delete(source_id)

    @staticmethod
    def _as_sequence(value: object) -> tuple[object, ...]:
        if value is None:
            return ()
        if isinstance(value, (str, bytes, Mapping)):
            return (value,)
        if isinstance(value, Sequence):
            return tuple(value)
        raise CatalogMutationError("metadata attachment groups must be sequences")

    def _preflight_attachments(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        titles: tuple[object, ...],
        agents: tuple[object, ...],
        identifiers: tuple[object, ...],
        notes: tuple[object, ...],
    ) -> None:
        self.repositories.titles._require_table_row(WEMI_TABLES[level], entity_id)
        if titles and not self.repositories.titles._TITLE_COLUMNS[level]:
            raise CatalogMutationError("Items do not own title columns")
        for title in titles:
            if not isinstance(title, (str, Mapping)):
                raise CatalogMutationError("titles must be strings or mappings")
        for agent in agents:
            if not isinstance(agent, Mapping):
                raise CatalogMutationError("agents must be mappings")
            role = agent.get("role")
            if not isinstance(role, str) or not role.strip():
                raise CatalogMutationError("every Agent attachment requires a role")
            if "agent_id" in agent:
                self.repositories.agents.require(agent["agent_id"])
        for identifier in identifiers:
            if not isinstance(identifier, Mapping):
                raise CatalogMutationError("identifiers must be mappings")
            if "identifier_id" in identifier:
                self.repositories.identifiers.require(identifier["identifier_id"])
            elif not (
                identifier.get("identifier_type", identifier.get("scheme"))
                and identifier.get("value")
            ):
                raise CatalogMutationError(
                    "identifiers require identifier_id or scheme/type and value"
                )
        for note in notes:
            if not isinstance(note, (str, Mapping)):
                raise CatalogMutationError("notes must be strings or mappings")

    @staticmethod
    def _missing_target_values(
        repository: Any,
        source: RowMapping,
        target: RowMapping,
    ) -> dict[str, object]:
        return {
            column: source[column]
            for column in repository.columns
            if column != repository.id_column
            and not column.endswith(("_timestamp_ep_k", "_scratch"))
            and target.get(column) in (None, "")
            and source.get(column) not in (None, "")
        }

    @staticmethod
    def _relationship_targets(level: WemiLevel) -> tuple[str, ...]:
        return {
            "work": ("expressions",),
            "expression": ("works", "manifestations"),
            "manifestation": ("expressions",),
            "item": (),
        }[level]

    def _transfer_links(
        self,
        table: str,
        source_id: EntityId,
        target_id: EntityId,
        secondary_table: str,
    ) -> None:
        repository = self.repositories.works
        try:
            spec = repository._link_spec(table, secondary_table)
        except CatalogMutationError:
            return
        rows = self.db.macros.get_link_rows(spec, source_id)
        writable_extras = {
            column.name
            for column in spec.extra_link_columns
            if not column.is_primary_key
        }
        try:
            writable_extras.discard(
                repository._wrapper.get_id_column(spec.link_table)
            )
        except Exception:
            pass
        if not rows:
            return
        target_rows = self.db.macros.get_link_rows(spec, target_id)
        desired: dict[tuple[object, ...], LinkValue] = {}
        for row in (*target_rows, *rows):
            identity: tuple[object, ...] = (row.secondary_id,)
            if spec.type_part_of_identity:
                identity += (row.link_type,)
            desired.setdefault(
                identity,
                LinkValue(
                    row.secondary_id,
                    link_type=row.link_type,
                    priority=row.priority,
                    extra={
                        key: value
                        for key, value in row.extra.items()
                        if key in writable_extras
                    },
                ),
            )
        transaction = getattr(self.db.macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            self.db.macros.replace_links(spec, source_id, ())
            self.db.macros.replace_links(spec, target_id, desired.values())

    def _transfer_identifiers(
        self,
        level: WemiLevel,
        source_id: EntityId,
        target_id: EntityId,
    ) -> None:
        target_primary_schemes = {
            row.get("entity_identifier_scheme")
            for row in self.repositories.identifiers.list_for_wemi(
                level=level,
                entity_id=target_id,
            )
            if row.get("entity_identifier_is_primary")
        }
        for row in self.repositories.identifiers.list_for_wemi(
            level=level,
            entity_id=source_id,
        ):
            changes: dict[str, object] = {
                "entity_identifier_entity_id": target_id,
            }
            if (
                row.get("entity_identifier_is_primary")
                and row.get("entity_identifier_scheme") in target_primary_schemes
            ):
                changes["entity_identifier_is_primary"] = 0
            self.repositories.identifiers.update(
                row["entity_identifier_id"],
                changes,
            )
