"""
Containers for agent credits attached to W/E/M/I entities.

These are metadata value objects and editing containers.
They are not row or database proxies.

This module also includes read-side snapshot containers for the results of
querying "what is this agent involved in?".
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, TypeVar

from LiuXin_alpha.metadata.metadata_types import (
    AgentTypes,
    AgentID,
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
    CreditSource,
    WorkAgentRole,
    ExpressionAgentRole,
    ManifestationAgentRole,
    ItemAgentRole,
)


RoleT = TypeVar("RoleT")
CreditT = TypeVar("CreditT", bound="AgentCreditBase")
RoleContainerT = TypeVar("RoleContainerT", bound="RoleCreditsContainer")
TargetSummaryT = TypeVar("TargetSummaryT")


# ---------------------------------------------------------------------------
# Agent identity surface
# ---------------------------------------------------------------------------


class AgentIdentityAPI(abc.ABC):
    """
    Lightweight identity surface for a canonical agent.

    This should stay narrow. Querying "what is this agent credited for?"
    belongs on a repository / database API, not on the identity object itself.
    """



    @property
    @abc.abstractmethod
    def agent_id(self) -> AgentID:
        """
        Canonical agent ID.
        """

    @property
    @abc.abstractmethod
    def agent_type(self) -> AgentTypes:
        """
        Human / organisation / etc.
        """

    @property
    @abc.abstractmethod
    def display_name(self) -> str:
        """
        Best display name for the agent.
        """

    @property
    def sort_name(self) -> str | None:
        """
        Optional canonical sort form.
        """
        return None

    def __str__(self) -> str:
        return self.display_name


# ---------------------------------------------------------------------------
# Credit value objects
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AgentCreditBase(abc.ABC):
    """
    Shared relation data for an agent attached to a bibliographic entity.

    This models the link, not the agent row itself.
    """

    agent_id: AgentID | None = None
    credited_as: str
    sort_as: str | None = None

    # Ordering / display
    position: int | None = None
    priority: float = 0.0
    is_primary: bool = False
    join_before: str = ""
    join_after: str = ""

    # Provenance / editorial state
    source: CreditSource = CreditSource.USER_SET
    confidence: float | None = None
    notes: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the W/E/M/I entity this credit attaches to.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """
        One of: work / expression / manifestation / item.
        """

    @property
    @abc.abstractmethod
    def role_key(self) -> object:
        """
        Enum value describing this credit's role.
        """

    def validate(self) -> None:
        """
        Validate shared credit state.
        """
        if not self.credited_as.strip():
            raise ValueError("credited_as cannot be blank")

        if self.confidence is not None and not (0.0 <= self.confidence <= 1.0):
            raise ValueError("confidence must be between 0.0 and 1.0")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        """
        Shared serialisable payload for the write layer.
        """
        return {
            "agent_id": self.agent_id,
            "credited_as": self.credited_as,
            "sort_as": self.sort_as,
            "position": self.position,
            "priority": self.priority,
            "is_primary": self.is_primary,
            "join_before": self.join_before,
            "join_after": self.join_after,
            "source": self.source,
            "confidence": self.confidence,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Serialise to a write-layer payload.
        """


@dataclass(slots=True, kw_only=True)
class WorkAgentCredit(AgentCreditBase):
    """
    Link between an agent and a work.
    """

    work_id: WorkID
    role: WorkAgentRole
    contribution_summary: str | None = None
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> str:
        return "work"

    @property
    def role_key(self) -> WorkAgentRole:
        return self.role

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "work_id": self.work_id,
                "role": self.role,
                "contribution_summary": self.contribution_summary,
                "canonical_for_work": self.canonical_for_work,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionAgentCredit(AgentCreditBase):
    """
    Link between an agent and an expression.
    """

    expression_id: ExpressionID
    role: ExpressionAgentRole
    language_id: LanguageID | None = None
    based_on_expression_id: ExpressionID | None = None
    abridged: bool = False

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> str:
        return "expression"

    @property
    def role_key(self) -> ExpressionAgentRole:
        return self.role

    def validate(self) -> None:
        super().validate()

        if self.role == ExpressionAgentRole.TRANSLATOR and self.language_id is None:
            raise ValueError("translator credits should carry a language_id")

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "expression_id": self.expression_id,
                "role": self.role,
                "language_id": self.language_id,
                "based_on_expression_id": self.based_on_expression_id,
                "abridged": self.abridged,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationAgentCredit(AgentCreditBase):
    """
    Link between an agent and a manifestation.
    """

    manifestation_id: ManifestationID
    role: ManifestationAgentRole
    imprint_name: str | None = None
    publication_statement: str | None = None
    appears_in_imprint: bool = True

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        return "manifestation"

    @property
    def role_key(self) -> ManifestationAgentRole:
        return self.role

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "manifestation_id": self.manifestation_id,
                "role": self.role,
                "imprint_name": self.imprint_name,
                "publication_statement": self.publication_statement,
                "appears_in_imprint": self.appears_in_imprint,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemAgentCredit(AgentCreditBase):
    """
    Link between an agent and an individual item / copy.
    """

    item_id: ItemID
    role: ItemAgentRole
    provenance_note: str | None = None
    association_start_ep_k: int | None = None
    association_end_ep_k: int | None = None
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> str:
        return "item"

    @property
    def role_key(self) -> ItemAgentRole:
        return self.role

    def validate(self) -> None:
        super().validate()

        if (
            self.association_start_ep_k is not None
            and self.association_end_ep_k is not None
            and self.association_end_ep_k < self.association_start_ep_k
        ):
            raise ValueError(
                "association_end_ep_k cannot be earlier than association_start_ep_k"
            )

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "item_id": self.item_id,
                "role": self.role,
                "provenance_note": self.provenance_note,
                "association_start_ep_k": self.association_start_ep_k,
                "association_end_ep_k": self.association_end_ep_k,
                "copy_specific": self.copy_specific,
            }
        )
        return payload


# ---------------------------------------------------------------------------
# Edit-side containers
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class RoleCreditsContainer(Generic[CreditT, RoleT], abc.ABC):
    """
    Ordered editable container for all credits of one role on one target entity.

    Example:
    - all author credits for work 123
    - all translator credits for expression 456
    """

    role: RoleT
    target_id: int
    _credits: list[CreditT] = field(default_factory=list)

    target_kind: ClassVar[str]

    def __iter__(self) -> Iterator[CreditT]:
        return iter(self._credits)

    def __len__(self) -> int:
        return len(self._credits)

    def __getitem__(self, index: int) -> CreditT:
        return self._credits[index]

    def credits(self) -> tuple[CreditT, ...]:
        return tuple(self._credits)

    def ids(self) -> tuple[AgentID | None, ...]:
        return tuple(credit.agent_id for credit in self._credits)

    def display_names(self) -> tuple[str, ...]:
        return tuple(credit.credited_as for credit in self._credits)

    def to_display_string(self, sep: str = " & ") -> str:
        return sep.join(self.display_names())

    def add_credit(self, credit: CreditT) -> None:
        self._validate_credit_shape(credit)
        self._credits.append(credit)
        self.normalize_positions()

    def replace_credit(self, index: int, credit: CreditT) -> None:
        self._validate_credit_shape(credit)
        self._credits[index] = credit
        self.normalize_positions()

    def remove_credit_at(self, index: int) -> CreditT:
        removed = self._credits.pop(index)
        self.normalize_positions()
        return removed

    def remove_agent(self, agent_id: AgentID) -> int:
        """
        Remove all credits for a given agent ID.
        Returns the number removed.
        """
        before = len(self._credits)
        self._credits = [
            credit for credit in self._credits if credit.agent_id != agent_id
        ]
        removed = before - len(self._credits)
        if removed:
            self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._credits.clear()

    def move_credit(self, old_index: int, new_index: int) -> None:
        credit = self._credits.pop(old_index)
        self._credits.insert(new_index, credit)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, credit in enumerate(self._credits):
            credit.is_primary = i == index

    def normalize_positions(self) -> None:
        for index, credit in enumerate(self._credits):
            credit.position = index

    def validate(self) -> None:
        primary_count = 0

        for expected_index, credit in enumerate(self._credits):
            self._validate_credit_shape(credit)
            credit.validate()

            if credit.position != expected_index:
                raise ValueError(
                    f"Credit position mismatch for {self.target_kind} {self.target_id}: "
                    f"expected {expected_index}, got {credit.position}"
                )

            if credit.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary credit is allowed for "
                f"{self.target_kind} {self.target_id} role {self.role}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [credit.as_write_payload() for credit in self._credits]

    def _validate_credit_shape(self, credit: CreditT) -> None:
        if credit.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {credit.target_kind} credit to {self.target_kind} container"
            )

        if credit.target_id != self.target_id:
            raise ValueError(
                f"Credit target_id {credit.target_id} does not match container "
                f"target_id {self.target_id}"
            )

        if credit.role_key != self.role:
            raise ValueError(
                f"Credit role {credit.role_key} does not match container role {self.role}"
            )


@dataclass(slots=True, kw_only=True)
class WorkRoleCreditsContainer(RoleCreditsContainer[WorkAgentCredit, WorkAgentRole]):
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionRoleCreditsContainer(
    RoleCreditsContainer[ExpressionAgentCredit, ExpressionAgentRole]
):
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationRoleCreditsContainer(
    RoleCreditsContainer[ManifestationAgentCredit, ManifestationAgentRole]
):
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemRoleCreditsContainer(RoleCreditsContainer[ItemAgentCredit, ItemAgentRole]):
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetAgentCreditsContainer(
    Generic[RoleT, CreditT, RoleContainerT],
    abc.ABC,
):
    """
    Top-level editable credit container for one target entity.
    Holds one RoleCreditsContainer per role.
    """

    _by_role: dict[RoleT, RoleContainerT] = field(default_factory=dict)

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the target object.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """
        work / expression / manifestation / item
        """

    @abc.abstractmethod
    def _make_role_container(self, role: RoleT) -> RoleContainerT:
        """
        Build the correct per-role container for this target type.
        """

    def roles(self) -> tuple[RoleT, ...]:
        return tuple(self._by_role.keys())

    def has_role(self, role: RoleT) -> bool:
        return role in self._by_role

    def get_role(self, role: RoleT) -> RoleContainerT | None:
        return self._by_role.get(role)

    def ensure_role(self, role: RoleT) -> RoleContainerT:
        container = self._by_role.get(role)
        if container is None:
            container = self._make_role_container(role)
            self._by_role[role] = container
        return container

    def add_credit(self, credit: CreditT) -> None:
        if credit.target_id != self.target_id:
            raise ValueError(
                f"Credit target_id {credit.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_role(credit.role_key).add_credit(credit)

    def iter_all_credits(self) -> Iterator[CreditT]:
        for container in self._by_role.values():
            yield from container

    def all_agent_ids(self) -> set[AgentID]:
        return {
            credit.agent_id
            for credit in self.iter_all_credits()
            if credit.agent_id is not None
        }

    def role_ids(self, role: RoleT) -> tuple[AgentID | None, ...]:
        container = self.get_role(role)
        if container is None:
            return tuple()
        return container.ids()

    def role_to_display_string(self, role: RoleT, sep: str = " & ") -> str:
        container = self.get_role(role)
        if container is None:
            return ""
        return container.to_display_string(sep=sep)

    def validate(self) -> None:
        for container in self._by_role.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_role.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkAgentCreditsContainer(
    BaseTargetAgentCreditsContainer[
        WorkAgentRole,
        WorkAgentCredit,
        WorkRoleCreditsContainer,
    ]
):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> str:
        return "work"

    def _make_role_container(
        self,
        role: WorkAgentRole,
    ) -> WorkRoleCreditsContainer:
        return WorkRoleCreditsContainer(role=role, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionAgentCreditsContainer(
    BaseTargetAgentCreditsContainer[
        ExpressionAgentRole,
        ExpressionAgentCredit,
        ExpressionRoleCreditsContainer,
    ]
):
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> str:
        return "expression"

    def _make_role_container(
        self,
        role: ExpressionAgentRole,
    ) -> ExpressionRoleCreditsContainer:
        return ExpressionRoleCreditsContainer(role=role, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationAgentCreditsContainer(
    BaseTargetAgentCreditsContainer[
        ManifestationAgentRole,
        ManifestationAgentCredit,
        ManifestationRoleCreditsContainer,
    ]
):
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        return "manifestation"

    def _make_role_container(
        self,
        role: ManifestationAgentRole,
    ) -> ManifestationRoleCreditsContainer:
        return ManifestationRoleCreditsContainer(
            role=role,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemAgentCreditsContainer(
    BaseTargetAgentCreditsContainer[
        ItemAgentRole,
        ItemAgentCredit,
        ItemRoleCreditsContainer,
    ]
):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> str:
        return "item"

    def _make_role_container(
        self,
        role: ItemAgentRole,
    ) -> ItemRoleCreditsContainer:
        return ItemRoleCreditsContainer(role=role, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Read-side query result containers
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentSummary:
    """
    Lightweight read model for an agent row.

    Keep this limited to the fields you expect to expose frequently.
    """

    agent_id: AgentID
    agent_type: AgentTypes
    display_name: str
    sort_name: str | None = None

    legal_name: str | None = None
    birth_name: str | None = None
    canonical_name_ep_k: int | None = None
    notes: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class WorkSummary:
    """
    Read-side summary for a work.
    """

    work_id: WorkID
    title: str
    sort_title: str | None = None
    primary_language_id: LanguageID | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ExpressionSummary:
    """
    Read-side summary for an expression.
    """

    expression_id: ExpressionID
    work_id: WorkID
    title: str
    language_id: LanguageID | None = None
    expression_type: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class ManifestationSummary:
    """
    Read-side summary for a manifestation.
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
    Read-side summary for an item.
    """

    item_id: ItemID
    manifestation_id: ManifestationID
    shelfmark: str | None = None
    barcode: str | None = None
    current_location: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationEntry(Generic[CreditT, TargetSummaryT]):
    """
    A single read-side participation entry for one agent on one target.
    """

    credit: CreditT
    target: TargetSummaryT
    display_label: str | None = None
    source_label: str | None = None


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationsByRole:
    """
    Grouped participations for convenience when browsing an agent.
    """

    work_roles: dict[
        WorkAgentRole,
        tuple[AgentParticipationEntry[WorkAgentCredit, WorkSummary], ...],
    ] = field(default_factory=dict)
    expression_roles: dict[
        ExpressionAgentRole,
        tuple[AgentParticipationEntry[ExpressionAgentCredit, ExpressionSummary], ...],
    ] = field(default_factory=dict)
    manifestation_roles: dict[
        ManifestationAgentRole,
        tuple[AgentParticipationEntry[ManifestationAgentCredit, ManifestationSummary], ...],
    ] = field(default_factory=dict)
    item_roles: dict[
        ItemAgentRole,
        tuple[AgentParticipationEntry[ItemAgentCredit, ItemSummary], ...],
    ] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True, frozen=True)
class AgentParticipationSnapshot:
    """
    Read-side container for "this agent, and everything they are involved in".
    """

    agent: AgentSummary

    works: tuple[AgentParticipationEntry[WorkAgentCredit, WorkSummary], ...] = ()
    expressions: tuple[
        AgentParticipationEntry[ExpressionAgentCredit, ExpressionSummary], ...
    ] = ()
    manifestations: tuple[
        AgentParticipationEntry[ManifestationAgentCredit, ManifestationSummary], ...
    ] = ()
    items: tuple[AgentParticipationEntry[ItemAgentCredit, ItemSummary], ...] = ()

    participations_by_role: AgentParticipationsByRole = field(
        default_factory=AgentParticipationsByRole
    )

    def all_entries(self) -> tuple[object, ...]:
        return self.works + self.expressions + self.manifestations + self.items

    def is_empty(self) -> bool:
        return not (self.works or self.expressions or self.manifestations or self.items)

    def counts_by_level(self) -> dict[str, int]:
        return {
            "works": len(self.works),
            "expressions": len(self.expressions),
            "manifestations": len(self.manifestations),
            "items": len(self.items),
        }


# ---------------------------------------------------------------------------
# MARC convenience layer
# ---------------------------------------------------------------------------


def _default_role_stem(role: object) -> str:
    """
    Convert enum member names into property stems.

    AUTHOR -> authors
    EDITOR -> editors
    TRANSLATOR -> translators
    EDITORIAL_DIRECTOR -> editorial_directors
    """
    name = role.name.lower()  # type: ignore[attr-defined]

    if name.endswith("y"):
        return name[:-1] + "ies"
    if name.endswith("s"):
        return name
    return name + "s"



def _install_role_convenience_properties(
    cls: type[BaseTargetAgentCreditsContainer],
    roles: type,
    *,
    stem_overrides: dict[object, str] | None = None,
) -> None:
    """
    Install per-role convenience properties and methods on a container class.

    For a role stem of 'authors', this creates:
    - .authors -> RoleCreditsContainer
    - .authors_ids -> tuple[AgentID | None, ...]
    - .authors_str -> str   (default " & " separator)
    - .authors_to_string(sep=" & ") -> str
    """
    stem_overrides = stem_overrides or {}

    for role in roles:
        stem = stem_overrides.get(role, _default_role_stem(role))

        def role_container_getter(self, _role=role):
            return self.ensure_role(_role)

        def role_ids_getter(self, _role=role):
            return self.role_ids(_role)

        def role_string_getter(self, _role=role):
            return self.role_to_display_string(_role)

        def role_string_method(self, sep: str = " & ", _role=role) -> str:
            return self.role_to_display_string(_role, sep=sep)

        setattr(cls, stem, property(role_container_getter))
        setattr(cls, f"{stem}_ids", property(role_ids_getter))
        setattr(cls, f"{stem}_str", property(role_string_getter))
        setattr(cls, f"{stem}_to_string", role_string_method)


# Optional overrides for ugly plurals / legacy compatibility names.
WORK_ROLE_STEM_OVERRIDES: dict[WorkAgentRole, str] = {}
EXPRESSION_ROLE_STEM_OVERRIDES: dict[ExpressionAgentRole, str] = {}
MANIFESTATION_ROLE_STEM_OVERRIDES: dict[ManifestationAgentRole, str] = {}
ITEM_ROLE_STEM_OVERRIDES: dict[ItemAgentRole, str] = {}


_install_role_convenience_properties(
    WorkAgentCreditsContainer,
    WorkAgentRole,
    stem_overrides=WORK_ROLE_STEM_OVERRIDES,
)
_install_role_convenience_properties(
    ExpressionAgentCreditsContainer,
    ExpressionAgentRole,
    stem_overrides=EXPRESSION_ROLE_STEM_OVERRIDES,
)
_install_role_convenience_properties(
    ManifestationAgentCreditsContainer,
    ManifestationAgentRole,
    stem_overrides=MANIFESTATION_ROLE_STEM_OVERRIDES,
)
_install_role_convenience_properties(
    ItemAgentCreditsContainer,
    ItemAgentRole,
    stem_overrides=ITEM_ROLE_STEM_OVERRIDES,
)
