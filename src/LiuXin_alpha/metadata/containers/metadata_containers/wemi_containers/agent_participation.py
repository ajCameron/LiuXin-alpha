"""
Represent joined agent participation results as read-side summaries and snapshots.

These frozen dataclasses are snapshots rather than editable metadata bundles.
Freezing prevents field reassignment; it does not freeze contained dictionaries or
credit objects.

Example:
    >>> snapshot = AgentParticipationSnapshot(agent=AgentProfileSummary(agent_id=7, agent_type='person', display_name='Ada'))
    >>> snapshot.is_empty()
    True
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
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_credit_containers import (
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
    """
    Hold an agent identity and optional intrinsic profile fields for a query result.

    Example:
        >>> agent = AgentProfileSummary(agent_id=7, agent_type='person', display_name='Ada')
        >>> agent.display_name
        'Ada'
    """

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
    """
    Describe a work using its id, title and optional sort title and primary language.

    Example:
        >>> work = WorkSummary(work_id=1, title='Notes')
        >>> work.work_id, work.title
        (1, 'Notes')
    """
    work_id: WorkID
    title: str
    sort_title: str | None = None
    primary_language_id: LanguageID | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ExpressionSummary:
    """
    Describe an expression together with its parent work, title, language and type.

    Example:
        >>> expression = ExpressionSummary(expression_id=2, work_id=1, title='Notes')
        >>> expression.work_id
        1
    """
    expression_id: ExpressionID
    work_id: WorkID
    title: str
    language_id: LanguageID | None = None
    expression_type: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ManifestationSummary:
    """
    Describe a manifestation with its parent expression and optional publication details.

    Example:
        >>> manifestation = ManifestationSummary(manifestation_id=3, expression_id=2, title='Notes')
        >>> manifestation.publication_year is None
        True
    """
    manifestation_id: ManifestationID
    expression_id: ExpressionID
    title: str
    publisher_name: str | None = None
    publication_year: int | None = None
    format_name: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ItemSummary:
    """
    Describe an individual copy with its parent manifestation and optional location identifiers.

    Example:
        >>> item = ItemSummary(item_id=4, manifestation_id=3, shelfmark='A1')
        >>> item.shelfmark
        'A1'
    """
    item_id: ItemID
    manifestation_id: ManifestationID
    shelfmark: str | None = None
    barcode: str | None = None
    current_location: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationEntry(Generic[CreditT, TargetSummaryT]):
    """
    Pair a shared credit object with a target summary and optional display/source labels.

    The frozen entry retains the supplied objects; it does not validate their ids or
    make the credit immutable.

    Example:
        >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
        >>> entry = AgentParticipationEntry(credit=credit, target=WorkSummary(work_id=1, title='Notes'))
        >>> entry.credit is credit
        True
    """
    credit: CreditT
    target: TargetSummaryT
    display_label: str | None = None
    source_label: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationsByRole:
    """
    Hold separate role-to-entry-tuple dictionaries for works, expressions, manifestations and items.

    Each instance starts with fresh mutable dictionaries. This view does not derive or
    synchronize entries from a snapshot.

    Example:
        >>> roles = AgentParticipationsByRole()
        >>> roles.work_roles == {} and roles.work_roles is not AgentParticipationsByRole().work_roles
        True
    """
    work_roles: dict[WorkAgentRole, tuple[AgentParticipationEntry[WorkAgentCredit, WorkSummary], ...]] = field(default_factory=dict)
    expression_roles: dict[ExpressionAgentRole, tuple[AgentParticipationEntry[ExpressionAgentCredit, ExpressionSummary], ...]] = field(default_factory=dict)
    manifestation_roles: dict[ManifestationAgentRole, tuple[AgentParticipationEntry[ManifestationAgentCredit, ManifestationSummary], ...]] = field(default_factory=dict)
    item_roles: dict[ItemAgentRole, tuple[AgentParticipationEntry[ItemAgentCredit, ItemSummary], ...]] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationSnapshot:
    """
    Collect one agent summary and participation entries at each WEMI level.

    Level tuples and the role-grouped view are supplied independently; neither is
    inferred from the other. Frozen fields still contain shared credit objects and
    mutable role dictionaries.

    Example:
        >>> snapshot = AgentParticipationSnapshot(agent=AgentProfileSummary(agent_id=7, agent_type='person', display_name='Ada'))
        >>> snapshot.counts_by_level()
        {'works': 0, 'expressions': 0, 'manifestations': 0, 'items': 0}
    """

    agent: AgentProfileSummary
    works: tuple[AgentParticipationEntry[WorkAgentCredit, WorkSummary], ...] = ()
    expressions: tuple[AgentParticipationEntry[ExpressionAgentCredit, ExpressionSummary], ...] = ()
    manifestations: tuple[AgentParticipationEntry[ManifestationAgentCredit, ManifestationSummary], ...] = ()
    items: tuple[AgentParticipationEntry[ItemAgentCredit, ItemSummary], ...] = ()
    participations_by_role: AgentParticipationsByRole = field(default_factory=AgentParticipationsByRole)

    def all_entries(self) -> tuple[object, ...]:
        """
        Concatenate the four level tuples in work, expression, manifestation and item order.

        Entries are retained by reference and are not deduplicated.

        Example:
            >>> snapshot = AgentParticipationSnapshot(agent=AgentProfileSummary(agent_id=7, agent_type='person', display_name='Ada'))
            >>> snapshot.all_entries()
            ()


        :return: Tuple containing every level entry; the role-grouped view is not consulted.
        """
        return self.works + self.expressions + self.manifestations + self.items

    def is_empty(self) -> bool:
        """
        Check whether all four level-entry tuples are empty.

        The independently supplied participations_by_role view does not affect this result.

        Example:
            >>> snapshot = AgentParticipationSnapshot(agent=AgentProfileSummary(agent_id=7, agent_type='person', display_name='Ada'))
            >>> snapshot.is_empty()
            True


        :return: True when no level tuple contains an entry.
        """
        return not (self.works or self.expressions or self.manifestations or self.items)

    def counts_by_level(self) -> dict[str, int]:
        """
        Count entries in each level tuple without consulting the role-grouped view.

        Example:
            >>> snapshot = AgentParticipationSnapshot(agent=AgentProfileSummary(agent_id=7, agent_type='person', display_name='Ada'))
            >>> snapshot.counts_by_level()['works']
            0


        :return: New dictionary keyed by works, expressions, manifestations and items.
        """
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
