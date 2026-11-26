"""Coordinate typed WEMI credits and Work memberships through Catalog."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from math import isfinite
from typing import TYPE_CHECKING, Literal, cast, overload

from LiuXin_alpha.caches.metadata.read_operations import complete_read
from LiuXin_alpha.caches.metadata.reads import MetadataReadSource
from LiuXin_alpha.caches.metadata.record_reads import SharedMetadataReader
from LiuXin_alpha.catalog.api.common import WemiLevel
from LiuXin_alpha.catalog.api.metadata_relationships import (
    MetadataCredit,
    MetadataMembership,
)
from LiuXin_alpha.catalog.api.metadata_revisions import MetadataRevision
from LiuXin_alpha.catalog.api.metadata_search import MetadataReference
from LiuXin_alpha.catalog.repositories.base import WEMI_TABLES, BaseRepository
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI
    from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt


class EntityRelationshipMethods:
    """Coordinate credit operations using the bound WEMI service's owner and level."""

    _owner: MetadataAPI
    _level: WemiLevel

    def _relationship_expected(
        self, entity_id: int, token: MetadataRevision | None
    ) -> tuple[MetadataRevision, ...]:
        """Validate that an optional precondition belongs to the WEMI owner."""
        if token is None:
            return ()
        if not isinstance(
            token, MetadataRevision
        ) or token.reference != MetadataReference(self._level, entity_id):
            raise ValueError("Relationship precondition belongs to a different owner")
        return (token,)

    def _members(
        self, entity_id: int, kind: Literal["agents", "tags", "series"]
    ) -> tuple[MetadataMembership, ...]:
        """Read association and target records under one complete captured source."""
        BaseRepository._validate_entity_id(entity_id)
        repository = getattr(self._owner._catalog, kind)
        reader = SharedMetadataReader(self._owner, repository, kind)

        def read(source: MetadataReadSource) -> tuple[MetadataMembership, ...]:
            """Preserve typed associations and reject dangling or malformed links."""
            source.require_rows = True
            table = WEMI_TABLES[self._level]
            owner_row = source.get_row_from_id(table, entity_id)
            spec = source.driver_wrapper.get_link_spec(table, kind)
            if not isinstance(spec, StorageLinkSpec):
                raise ValueError(f"No declared metadata relationship: {table}/{kind}")
            result = []
            for link in source.get_interlink_rows(owner_row, kind):
                values = link if isinstance(link, Mapping) else link.row_dict
                target_id = values[spec.secondary_link_col]
                if not isinstance(target_id, int) or isinstance(target_id, bool):
                    raise TypeError("Metadata relationship target must be an integer")
                BaseRepository._validate_entity_id(target_id)
                target = source.get_row_from_id(kind, target_id)
                assert target is not None
                link_type = values[spec.type_link_col] if spec.type_link_col else None
                priority = (
                    values[spec.priority_link_col] if spec.priority_link_col else None
                )
                if link_type is not None and not isinstance(link_type, str):
                    raise TypeError("Metadata relationship type must be text or None")
                if priority is not None and (
                    not isinstance(priority, (int, float))
                    or isinstance(priority, bool)
                    or not isfinite(priority)
                ):
                    raise TypeError(
                        "Metadata relationship priority must be numeric or None"
                    )
                result.append(
                    MetadataMembership(
                        reader._record(cast(Mapping[str, object], target.row_dict)),
                        link_type,
                        priority,
                    )
                )
            result.sort(
                key=lambda entry: (
                    entry.priority is None,
                    -(entry.priority or 0),
                    entry.member.entity_id,
                    entry.link_type or "",
                )
            )
            return tuple(result)

        return complete_read(self._owner, read)

    def list_credits(self, entity_id: int) -> tuple[MetadataCredit, ...]:
        """Return detached Agent credits, including repeated Agents in different roles."""
        result = []
        for member in self._members(entity_id, "agents"):
            if not member.link_type:
                raise ValueError("Agent credit has no role")
            result.append(
                MetadataCredit(member.member, member.link_type, member.priority)
            )
        return tuple(result)

    def add_credit(
        self,
        entity_id: int,
        *,
        agent_id: int,
        role: str,
        priority: int | None = None,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Upsert an explicit Agent credit with one persistence/publication receipt."""

        def operation() -> tuple[MetadataReference, bool]:
            """Compare the authoritative selected credit while holding the writer unit."""
            repository = self._owner._catalog.agents
            before = repository.list_for_wemi(level=self._level, entity_id=entity_id)
            repository.link_to_wemi(
                agent_id=agent_id,
                level=self._level,
                entity_id=entity_id,
                role=role,
                priority=priority,
            )
            after = repository.list_for_wemi(level=self._level, entity_id=entity_id)
            return MetadataReference(self._level, entity_id), before != after

        return self._owner._write(
            operation,
            expected=self._relationship_expected(entity_id, expected_revision),
        )

    def remove_credit(
        self,
        entity_id: int,
        *,
        agent_id: int,
        role: str,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Remove only this role's credit, keeping the Agent and its other roles."""
        return self._owner._write(
            lambda: (
                MetadataReference(self._level, entity_id),
                self._owner._catalog.agents.unlink_from_wemi(
                    agent_id=agent_id, level=self._level, entity_id=entity_id, role=role
                ),
            ),
            expected=self._relationship_expected(entity_id, expected_revision),
        )

    def set_credits(
        self,
        entity_id: int,
        *,
        role: str,
        agent_ids: Sequence[int],
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Replace one role in display order, preserving every other role."""
        if isinstance(agent_ids, (str, bytes)):
            raise TypeError("agent_ids must be an integer sequence")
        ids = tuple(agent_ids)
        for identity in ids:
            BaseRepository._validate_entity_id(identity)
        if len(set(ids)) != len(ids):
            raise ValueError("Agent credits must be unique within each role")
        if not isinstance(role, str) or not role.strip():
            raise ValueError("Credit role must be nonblank text")
        role = role.strip()

        def operation() -> tuple[MetadataReference, bool]:
            """Avoid rewriting an already identical role ordering."""
            repository = self._owner._catalog.agents
            before = repository.list_for_wemi(level=self._level, entity_id=entity_id)
            selected = tuple(
                row["agent_id"]
                for row in before
                if row["_catalog_link"]["type"] == role
            )
            changed = selected != ids
            if changed:
                repository.replace_for_wemi(
                    level=self._level, entity_id=entity_id, role=role, agent_ids=ids
                )
            return MetadataReference(self._level, entity_id), changed

        return self._owner._write(
            operation,
            expected=self._relationship_expected(entity_id, expected_revision),
        )


class WorkRelationshipMethods(EntityRelationshipMethods):
    """Add Work-specific Tag/Series conveniences without extending other WEMI levels."""

    def list_tags(self, work_id: int) -> tuple[MetadataMembership, ...]:
        """Read every Tag membership from a single complete captured source."""
        return self._members(work_id, "tags")

    def list_series(self, work_id: int) -> tuple[MetadataMembership, ...]:
        """Read typed Series memberships without interpreting their order as volumes."""
        return self._members(work_id, "series")

    def _membership_write(
        self,
        work_id: int,
        entity_id: int,
        kind: Literal["tags", "series"],
        *,
        remove: bool,
        expected_revision: MetadataRevision | None,
    ) -> MetadataWriteReceipt[int]:
        """Delegate authoritative membership policy and report true no-op requests."""
        repository = (
            self._owner._catalog.tags if kind == "tags" else self._owner._catalog.series
        )

        def operation() -> tuple[int, bool]:
            """Check and modify the pair within the same serialized transaction."""
            present = repository.is_linked_to_work(entity_id=entity_id, work_id=work_id)
            method = repository.unlink_from_work if remove else repository.link_to_work
            method(entity_id=entity_id, work_id=work_id)
            return entity_id, present if remove else not present

        return self._owner._write(
            operation, expected=self._relationship_expected(work_id, expected_revision)
        )

    @overload
    def add_tag(
        self,
        work_id: int,
        *,
        tag_id: int,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]: ...
    @overload
    def add_tag(
        self,
        work_id: int,
        *,
        text: str,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]: ...
    def add_tag(
        self,
        work_id: int,
        *,
        tag_id: int | None = None,
        text: str | None = None,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Attach one existing ID or atomically ensure and attach one literal Tag."""
        if (tag_id is None) == (text is None):
            raise ValueError("Supply exactly one of tag_id or text")
        if tag_id is not None:
            return self._membership_write(
                work_id,
                tag_id,
                "tags",
                remove=False,
                expected_revision=expected_revision,
            )
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Tag text must be nonblank")

        def operation() -> tuple[int, bool]:
            """Require the owner and ensure against authoritative storage before linking."""
            self._owner._catalog.works.require(work_id)
            repository = self._owner._catalog.tags
            identity, created = repository.ensure({"name": text})
            present = repository.is_linked_to_work(entity_id=identity, work_id=work_id)
            repository.link_to_work(entity_id=identity, work_id=work_id)
            return identity, created or not present

        return self._owner._write(
            operation,
            expected=self._relationship_expected(work_id, expected_revision),
            present=lambda identity: (("tags", identity),),
        )

    def remove_tag(
        self,
        work_id: int,
        tag_id: int,
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Remove a Tag membership while retaining the Tag and Work."""
        return self._membership_write(
            work_id, tag_id, "tags", remove=True, expected_revision=expected_revision
        )

    def add_series(
        self,
        work_id: int,
        series_id: int,
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Attach an existing Series while preserving retained association metadata."""
        return self._membership_write(
            work_id,
            series_id,
            "series",
            remove=False,
            expected_revision=expected_revision,
        )

    def remove_series(
        self,
        work_id: int,
        series_id: int,
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Remove all types for one Work/Series pair while keeping both entities."""
        return self._membership_write(
            work_id,
            series_id,
            "series",
            remove=True,
            expected_revision=expected_revision,
        )

    def _set_members(
        self,
        work_id: int,
        ids: Sequence[int],
        kind: Literal["tags", "series"],
        expected_revision: MetadataRevision | None,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Replace membership under Catalog policy without reordering retained links."""
        if isinstance(ids, (str, bytes)):
            raise TypeError("Membership IDs must be an integer sequence")
        values = tuple(ids)
        repository = (
            self._owner._catalog.tags if kind == "tags" else self._owner._catalog.series
        )

        def operation() -> tuple[tuple[int, ...], bool]:
            """Validate and replace the set, comparing authoritative membership identities."""
            previous = {
                row[repository.id_column] for row in repository.list_for_work(work_id)
            }
            repository.replace_for_work(work_id=work_id, entity_ids=values)
            return values, previous != set(values)

        return self._owner._write(
            operation, expected=self._relationship_expected(work_id, expected_revision)
        )

    def set_tags(
        self,
        work_id: int,
        tag_ids: Sequence[int],
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Replace Tags with unique existing IDs; an empty sequence clears membership."""
        return self._set_members(work_id, tag_ids, "tags", expected_revision)

    def set_series(
        self,
        work_id: int,
        series_ids: Sequence[int],
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Replace Series membership, retaining every surviving typed association."""
        return self._set_members(work_id, series_ids, "series", expected_revision)
