"""Agent repository contract."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, MetadataCandidate, MatchResult, RowMapping, WemiLevel
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
