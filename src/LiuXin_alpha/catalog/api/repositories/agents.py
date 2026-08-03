"""Agent repository contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.common import (
    EntityId,
    MetadataCandidate,
    MatchResult,
    RowInput,
    RowMapping,
    WemiLevel,
)
from LiuXin_alpha.catalog.api.repositories.base import BaseRepositoryAPI


@runtime_checkable
class AgentRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage, identity, and WEMI-credit API for people/organisations.

    ``resolve`` is a conservative exact lookup. ``match`` exposes the full
    identity decision. ``match_or_create`` automates only safe matches and
    genuine non-matches.

    Example::

        agent_id = catalog.agents.match_or_create(
            MetadataCandidate({
                "canonical_name": "Mary Shelley",
                "type": "person",
            })
        )
        catalog.agents.link_to_wemi(
            agent_id=agent_id,
            level="work",
            entity_id=work_id,
            role="author",
            priority=1,
        )
    """

    def create_person(
        self,
        data: RowInput,
        *,
        details: RowInput | None = None,
        identifiers: Sequence[Mapping[str, object]] = (),
        language_ids: Sequence[EntityId] = (),
        notes: Sequence[str | RowInput] = (),
    ) -> EntityId:
        """Atomically create a person Agent and subtype metadata.

        :param data: Core Agent values.
        :param details: Optional ``human_agents`` subtype values.
        :param identifiers: Identifier records to assign.
        :param language_ids: Existing native-language IDs to link.
        :param notes: Text or Note payloads to create and link.
        :return: New Agent ID.
        """

    def create_organisation(
        self,
        data: RowInput,
        *,
        details: RowInput | None = None,
        parent_id: EntityId | None = None,
        relation_type: str = "imprint_of",
        relation_note: str | None = None,
        identifiers: Sequence[Mapping[str, object]] = (),
        language_ids: Sequence[EntityId] = (),
        notes: Sequence[str | RowInput] = (),
        synopses: Sequence[str | RowInput] = (),
    ) -> EntityId:
        """Atomically create an organisation and its related metadata.

        :return: New organisation Agent ID.
        """

    def resolve(self, *, name: str, role: str | None = None) -> RowMapping | None:
        """
        Resolve an agent by name and optional role.

        Name comparison is Unicode/case/whitespace normalized. ``role`` scopes
        resolution to Agents already credited in that role where supported.

        :param name: Canonical name or known alias.
        :param role: Optional relationship role such as ``"author"``.
        :return: One uniquely resolved Agent mapping, otherwise ``None``.
        """

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate to an existing agent if possible.

        :param candidate: Agent identity fields and optional aliases/hints.
        :return: Explained match, no-match, ambiguity, or conflict decision.
        """

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched agent id, or create a new agent.

        :param candidate: Agent values accepted by :meth:`create`.
        :return: Existing safely matched or newly created Agent ID.
        :raises CatalogAmbiguousMatchError: If multiple Agents remain plausible.
        :raises CatalogMatchConflictError: If decisive evidence conflicts.
        """

    def match_or_create_person(
        self,
        candidate: MetadataCandidate,
        *,
        details: RowInput | None = None,
    ) -> EntityId:
        """Match a person or atomically create its complete aggregate."""

    def match_or_create_organisation(
        self,
        candidate: MetadataCandidate,
        *,
        details: RowInput | None = None,
    ) -> EntityId:
        """Match an organisation or atomically create its aggregate."""

    def link_to_wemi(
        self,
        *,
        agent_id: EntityId,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        priority: int | None = None,
    ) -> None:
        """
        Link an agent to a WEMI entity.

        Existing identical credits are updated rather than duplicated.

        :param agent_id: Existing Agent ID.
        :param level: ``"work"``, ``"expression"``, ``"manifestation"``, or
            ``"item"``.
        :param entity_id: Existing entity ID at ``level``.
        :param role: Non-empty credit role such as ``"author"``,
            ``"translator"``, or ``"narrator"``.
        :param priority: Optional ordering value; lower values sort first.
        :return: ``None``.
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Return agents linked to a WEMI entity.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Existing entity ID.
        :return: Priority-ordered Agent mappings with role/priority data in
            ``"_catalog_link"``.
        """

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        role: str,
        agent_ids: Sequence[EntityId],
    ) -> None:
        """Replace one role-scoped ordered credit set.

        Credits with other roles remain unchanged. An empty ``agent_ids``
        sequence clears the selected role.
        """
