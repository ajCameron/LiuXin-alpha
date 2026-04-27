"""Read-side participation views for agents across the W/E/M/I graph.

Category: read-side snapshot/view.
These dataclasses model joined query results. They are not editable metadata
bundles and they are not identity objects.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from LiuXin_alpha.metadata.metadata_types import (
    AgentID,
    AgentTypes,
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
    WorkAgentRole,
    ExpressionAgentRole,
    ManifestationAgentRole,
    ItemAgentRole,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_credit_containers import (
    AgentCreditBase,
    WorkAgentCredit,
    ExpressionAgentCredit,
    ManifestationAgentCredit,
    ItemAgentCredit,
)

CreditT = TypeVar('CreditT', bound=AgentCreditBase)
TargetSummaryT = TypeVar('TargetSummaryT')


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentProfileSummary:
    """Lightweight read model for an agent plus intrinsic profile fields."""

    agent_id: AgentID
    agent_type: AgentTypes
    display_name: str
    sort_name: str | None = None
    legal_name: str | None = None
    birth_name: str | None = None
    canonical_name_ep_k: int | None = None
    biography: str | None = None
    notes: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class WorkSummary:
    work_id: WorkID
    title: str
    sort_title: str | None = None
    primary_language_id: LanguageID | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ExpressionSummary:
    expression_id: ExpressionID
    work_id: WorkID
    title: str
    language_id: LanguageID | None = None
    expression_type: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ManifestationSummary:
    manifestation_id: ManifestationID
    expression_id: ExpressionID
    title: str
    publisher_name: str | None = None
    publication_year: int | None = None
    format_name: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ItemSummary:
    item_id: ItemID
    manifestation_id: ManifestationID
    shelfmark: str | None = None
    barcode: str | None = None
    current_location: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationEntry(Generic[CreditT, TargetSummaryT]):
    credit: CreditT
    target: TargetSummaryT
    display_label: str | None = None
    source_label: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationsByRole:
    work_roles: dict[WorkAgentRole, tuple[AgentParticipationEntry[WorkAgentCredit, WorkSummary], ...]] = field(default_factory=dict)
    expression_roles: dict[ExpressionAgentRole, tuple[AgentParticipationEntry[ExpressionAgentCredit, ExpressionSummary], ...]] = field(default_factory=dict)
    manifestation_roles: dict[ManifestationAgentRole, tuple[AgentParticipationEntry[ManifestationAgentCredit, ManifestationSummary], ...]] = field(default_factory=dict)
    item_roles: dict[ItemAgentRole, tuple[AgentParticipationEntry[ItemAgentCredit, ItemSummary], ...]] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationSnapshot:
    """Read-side container for 'this agent, and everything they are involved in'."""

    agent: AgentProfileSummary
    works: tuple[AgentParticipationEntry[WorkAgentCredit, WorkSummary], ...] = ()
    expressions: tuple[AgentParticipationEntry[ExpressionAgentCredit, ExpressionSummary], ...] = ()
    manifestations: tuple[AgentParticipationEntry[ManifestationAgentCredit, ManifestationSummary], ...] = ()
    items: tuple[AgentParticipationEntry[ItemAgentCredit, ItemSummary], ...] = ()
    participations_by_role: AgentParticipationsByRole = field(default_factory=AgentParticipationsByRole)

    def all_entries(self) -> tuple[object, ...]:
        return self.works + self.expressions + self.manifestations + self.items

    def is_empty(self) -> bool:
        return not (self.works or self.expressions or self.manifestations or self.items)

    def counts_by_level(self) -> dict[str, int]:
        return {
            'works': len(self.works),
            'expressions': len(self.expressions),
            'manifestations': len(self.manifestations),
            'items': len(self.items),
        }


__all__ = [
    "AgentProfileSummary",
    "WorkSummary",
    "ExpressionSummary",
    "ManifestationSummary",
    "ItemSummary",
    "AgentParticipationEntry",
    "AgentParticipationsByRole",
    "AgentParticipationSnapshot",
]
