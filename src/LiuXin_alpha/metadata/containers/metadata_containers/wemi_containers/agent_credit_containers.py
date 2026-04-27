"""Agent credit metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These containers model agent-to-target credit records. They are editable
metadata value objects, not agent identity objects and not read-side views.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import ClassVar, Generic, Iterator, TypeVar

from LiuXin_alpha.metadata.metadata_types import (
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

RoleT = TypeVar('RoleT')
CreditT = TypeVar('CreditT', bound='AgentCreditBase')
RoleContainerT = TypeVar('RoleContainerT', bound='RoleCreditsContainer')


@dataclass(slots=True, kw_only=True)
class AgentCreditBase(abc.ABC):
    """Shared relation data for an agent attached to a bibliographic entity."""

    agent_id: AgentID | None = None
    credited_as: str
    sort_as: str | None = None
    position: int | None = None
    priority: float = 0.0
    is_primary: bool = False
    join_before: str = ''
    join_after: str = ''
    source: CreditSource = CreditSource.USER_SET
    confidence: float | None = None
    notes: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the W/E/M/I entity this credit attaches to."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """One of: work / expression / manifestation / item."""

    @property
    @abc.abstractmethod
    def role_key(self) -> object:
        """Enum value describing this credit's role."""

    def validate(self) -> None:
        if not self.credited_as.strip():
            raise ValueError('credited_as cannot be blank')
        if self.confidence is not None and not (0.0 <= self.confidence <= 1.0):
            raise ValueError('confidence must be between 0.0 and 1.0')
        if self.position is not None and self.position < 0:
            raise ValueError('position cannot be negative')

    def _common_write_payload(self) -> dict[str, object]:
        return {
            'agent_id': self.agent_id,
            'credited_as': self.credited_as,
            'sort_as': self.sort_as,
            'position': self.position,
            'priority': self.priority,
            'is_primary': self.is_primary,
            'join_before': self.join_before,
            'join_after': self.join_after,
            'source': self.source,
            'confidence': self.confidence,
            'notes': self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """Serialise to a write-layer payload."""


@dataclass(slots=True, kw_only=True)
class WorkAgentCredit(AgentCreditBase):
    work_id: WorkID
    role: WorkAgentRole
    contribution_summary: str | None = None
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> str:
        return 'work'

    @property
    def role_key(self) -> WorkAgentRole:
        return self.role

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({
            'work_id': self.work_id,
            'role': self.role,
            'contribution_summary': self.contribution_summary,
            'canonical_for_work': self.canonical_for_work,
        })
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionAgentCredit(AgentCreditBase):
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
        return 'expression'

    @property
    def role_key(self) -> ExpressionAgentRole:
        return self.role

    def validate(self) -> None:
        super().validate()
        if self.role == ExpressionAgentRole.TRANSLATOR and self.language_id is None:
            raise ValueError('translator credits should carry a language_id')

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({
            'expression_id': self.expression_id,
            'role': self.role,
            'language_id': self.language_id,
            'based_on_expression_id': self.based_on_expression_id,
            'abridged': self.abridged,
        })
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationAgentCredit(AgentCreditBase):
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
        return 'manifestation'

    @property
    def role_key(self) -> ManifestationAgentRole:
        return self.role

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({
            'manifestation_id': self.manifestation_id,
            'role': self.role,
            'imprint_name': self.imprint_name,
            'publication_statement': self.publication_statement,
            'appears_in_imprint': self.appears_in_imprint,
        })
        return payload


@dataclass(slots=True, kw_only=True)
class ItemAgentCredit(AgentCreditBase):
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
        return 'item'

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
            raise ValueError('association_end_ep_k cannot be earlier than association_start_ep_k')

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({
            'item_id': self.item_id,
            'role': self.role,
            'provenance_note': self.provenance_note,
            'association_start_ep_k': self.association_start_ep_k,
            'association_end_ep_k': self.association_end_ep_k,
            'copy_specific': self.copy_specific,
        })
        return payload


@dataclass(slots=True, kw_only=True)
class RoleCreditsContainer(Generic[CreditT, RoleT], abc.ABC):
    """Ordered editable container for all credits of one role on one target entity."""

    role: RoleT
    target_id: int
    _credits: list[CreditT]

    target_kind: ClassVar[str]

    def __init__(self, *, role: RoleT, target_id: int, credits: list[CreditT] | None = None) -> None:
        self.role = role
        self.target_id = target_id
        self._credits = list(credits or [])

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

    def to_text(self, sep: str = ' & ') -> str:
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
        before = len(self._credits)
        self._credits = [credit for credit in self._credits if credit.agent_id != agent_id]
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
            credit.is_primary = (i == index)

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
                    f'Credit position mismatch for {self.target_kind} {self.target_id}: '
                    f'expected {expected_index}, got {credit.position}'
                )
            if credit.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(
                f'Only one primary credit is allowed for {self.target_kind} {self.target_id} role {self.role}'
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [credit.as_write_payload() for credit in self._credits]

    def _validate_credit_shape(self, credit: CreditT) -> None:
        if credit.target_kind != self.target_kind:
            raise ValueError(f'Cannot add {credit.target_kind} credit to {self.target_kind} container')
        if credit.target_id != self.target_id:
            raise ValueError(
                f'Credit target_id {credit.target_id} does not match container target_id {self.target_id}'
            )
        if credit.role_key != self.role:
            raise ValueError(f'Credit role {credit.role_key} does not match container role {self.role}')


class WorkRoleCreditsContainer(RoleCreditsContainer[WorkAgentCredit, WorkAgentRole]):
    target_kind: ClassVar[str] = 'work'

    @property
    def work_id(self) -> WorkID:
        return self.target_id


class ExpressionRoleCreditsContainer(RoleCreditsContainer[ExpressionAgentCredit, ExpressionAgentRole]):
    target_kind: ClassVar[str] = 'expression'

    @property
    def expression_id(self) -> ExpressionID:
        return self.target_id


class ManifestationRoleCreditsContainer(RoleCreditsContainer[ManifestationAgentCredit, ManifestationAgentRole]):
    target_kind: ClassVar[str] = 'manifestation'

    @property
    def manifestation_id(self) -> ManifestationID:
        return self.target_id


class ItemRoleCreditsContainer(RoleCreditsContainer[ItemAgentCredit, ItemAgentRole]):
    target_kind: ClassVar[str] = 'item'

    @property
    def item_id(self) -> ItemID:
        return self.target_id


class BaseTargetAgentCreditsContainer(Generic[RoleT, CreditT, RoleContainerT], abc.ABC):
    """Top-level editable credit container for one target entity."""

    def __init__(self) -> None:
        self._by_role: dict[RoleT, RoleContainerT] = {}

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """work / expression / manifestation / item"""

    @abc.abstractmethod
    def _make_role_container(self, role: RoleT) -> RoleContainerT:
        """Build the correct per-role container for this target type."""

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
                f'Credit target_id {credit.target_id} does not match {self.target_kind} target_id {self.target_id}'
            )
        self.ensure_role(credit.role_key).add_credit(credit)

    def iter_all_credits(self) -> Iterator[CreditT]:
        for container in self._by_role.values():
            yield from container

    def all_agent_ids(self) -> set[AgentID]:
        return {credit.agent_id for credit in self.iter_all_credits() if credit.agent_id is not None}

    def role_ids(self, role: RoleT) -> tuple[AgentID | None, ...]:
        container = self.get_role(role)
        if container is None:
            return tuple()
        return container.ids()

    def role_text(self, role: RoleT, sep: str = ' & ') -> str:
        container = self.get_role(role)
        if container is None:
            return ''
        return container.to_text(sep=sep)

    def validate(self) -> None:
        for container in self._by_role.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_role.values():
            payload.extend(container.as_write_payload())
        return payload


class WorkAgentCreditsContainer(BaseTargetAgentCreditsContainer[WorkAgentRole, WorkAgentCredit, WorkRoleCreditsContainer]):
    def __init__(self, *, work_id: WorkID) -> None:
        super().__init__()
        self.work_id = work_id

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> str:
        return 'work'

    def _make_role_container(self, role: WorkAgentRole) -> WorkRoleCreditsContainer:
        return WorkRoleCreditsContainer(role=role, target_id=self.work_id)


class ExpressionAgentCreditsContainer(BaseTargetAgentCreditsContainer[ExpressionAgentRole, ExpressionAgentCredit, ExpressionRoleCreditsContainer]):
    def __init__(self, *, expression_id: ExpressionID) -> None:
        super().__init__()
        self.expression_id = expression_id

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> str:
        return 'expression'

    def _make_role_container(self, role: ExpressionAgentRole) -> ExpressionRoleCreditsContainer:
        return ExpressionRoleCreditsContainer(role=role, target_id=self.expression_id)


class ManifestationAgentCreditsContainer(BaseTargetAgentCreditsContainer[ManifestationAgentRole, ManifestationAgentCredit, ManifestationRoleCreditsContainer]):
    def __init__(self, *, manifestation_id: ManifestationID) -> None:
        super().__init__()
        self.manifestation_id = manifestation_id

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        return 'manifestation'

    def _make_role_container(self, role: ManifestationAgentRole) -> ManifestationRoleCreditsContainer:
        return ManifestationRoleCreditsContainer(role=role, target_id=self.manifestation_id)


class ItemAgentCreditsContainer(BaseTargetAgentCreditsContainer[ItemAgentRole, ItemAgentCredit, ItemRoleCreditsContainer]):
    def __init__(self, *, item_id: ItemID) -> None:
        super().__init__()
        self.item_id = item_id

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> str:
        return 'item'

    def _make_role_container(self, role: ItemAgentRole) -> ItemRoleCreditsContainer:
        return ItemRoleCreditsContainer(role=role, target_id=self.item_id)


def _default_role_stem(role: object) -> str:
    name = role.name.lower()  # type: ignore[attr-defined]
    if name.endswith('y'):
        return name[:-1] + 'ies'
    if name.endswith('s'):
        return name
    return name + 's'


def _install_role_convenience_properties(
    cls: type[BaseTargetAgentCreditsContainer],
    roles: type,
    *,
    stem_overrides: dict[object, str] | None = None,
) -> None:
    """
    Install per-role convenience properties and methods on an agent-credit
    container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.
    """
    stem_overrides = stem_overrides or {}
    for role in roles:
        stem = stem_overrides.get(role, _default_role_stem(role))

        def role_container_getter(self, _role=role):
            return self.ensure_role(_role)

        def role_ids_getter(self, _role=role):
            return self.role_ids(_role)

        def role_text_getter(self, _role=role):
            return self.role_text(_role)

        def role_text_method(self, sep: str = ' & ', _role=role) -> str:
            return self.role_text(_role, sep=sep)

        setattr(cls, stem, property(role_container_getter))
        setattr(cls, f'{stem}_ids', property(role_ids_getter))
        setattr(cls, f'{stem}_text', property(role_text_getter))
        setattr(cls, f'{stem}_to_text', role_text_method)


WORK_ROLE_STEM_OVERRIDES: dict[WorkAgentRole, str] = {}
EXPRESSION_ROLE_STEM_OVERRIDES: dict[ExpressionAgentRole, str] = {}
MANIFESTATION_ROLE_STEM_OVERRIDES: dict[ManifestationAgentRole, str] = {}
ITEM_ROLE_STEM_OVERRIDES: dict[ItemAgentRole, str] = {}

_install_role_convenience_properties(WorkAgentCreditsContainer, WorkAgentRole, stem_overrides=WORK_ROLE_STEM_OVERRIDES)
_install_role_convenience_properties(ExpressionAgentCreditsContainer, ExpressionAgentRole, stem_overrides=EXPRESSION_ROLE_STEM_OVERRIDES)
_install_role_convenience_properties(ManifestationAgentCreditsContainer, ManifestationAgentRole, stem_overrides=MANIFESTATION_ROLE_STEM_OVERRIDES)
_install_role_convenience_properties(ItemAgentCreditsContainer, ItemAgentRole, stem_overrides=ITEM_ROLE_STEM_OVERRIDES)


__all__ = [
    "AgentCreditBase",
    "WorkAgentCredit",
    "ExpressionAgentCredit",
    "ManifestationAgentCredit",
    "ItemAgentCredit",
    "RoleCreditsContainer",
    "WorkRoleCreditsContainer",
    "ExpressionRoleCreditsContainer",
    "ManifestationRoleCreditsContainer",
    "ItemRoleCreditsContainer",
    "BaseTargetAgentCreditsContainer",
    "WorkAgentCreditsContainer",
    "ExpressionAgentCreditsContainer",
    "ManifestationAgentCreditsContainer",
    "ItemAgentCreditsContainer",
]
