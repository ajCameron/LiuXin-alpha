"""
Model editable agent credits grouped by role on work, expression, manifestation and item targets.

Credits contain relationship metadata and optional agent ids. Role containers
preserve list order, and target containers group those lists in role insertion
order. Construction and serialization do not automatically perform full validation.

Example:
    >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
    >>> credits = WorkAgentCreditsContainer(work_id=1)
    >>> credits.add_credit(credit)
    >>> credits.validate()
    >>> credits.role_text(WorkAgentRole.AUTHOR)
    'Ada'
"""
from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import ClassVar, Generic, Iterator, TypeVar

from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
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
class AgentCreditBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold shared editable credit fields and require target-specific identity and serialization.

    Concrete keyword-only dataclasses add the target id, role and family fields. Call
    validate explicitly to check display text, confidence and position; the agent id may
    remain unresolved.

    Example:
        >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
        >>> credit.agent_id is None
        True
    """

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
    STRING_DISPLAY_KEYS: ClassVar[tuple[str, ...]] = ("credited_as", "role", "agent_id")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the id of the WEMI entity receiving this credit.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.target_id
            1


        :return: Concrete target id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """
        Require the WEMI level receiving this credit.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @property
    @abc.abstractmethod
    def role_key(self) -> object:
        """
        Require the role used to group this credit within its target.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.role_key == WorkAgentRole.AUTHOR
            True


        :return: Target-specific role enum.
        """

    def validate(self) -> None:
        """
        Reject blank credited text, confidence outside zero through one, or a negative position.

        Raise ValueError for an invalid value. This check does not resolve the agent id or
        normalize fields.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.validate()
            >>> credit.confidence = 2.0
            >>> credit.validate()
            Traceback (most recent call last):
            ...
            ValueError: confidence must be between 0.0 and 1.0


        :return: None.
        """
        if not self.credited_as.strip():
            raise ValueError('credited_as cannot be blank')
        if self.confidence is not None and not (0.0 <= self.confidence <= 1.0):
            raise ValueError('confidence must be between 0.0 and 1.0')
        if self.position is not None and self.position < 0:
            raise ValueError('position cannot be negative')

    def _common_write_payload(self) -> dict[str, object]:
        """
        Collect shared credit fields without validation or target-specific additions.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit._common_write_payload()['credited_as']
            'Ada'


        :return: New dictionary retaining the stored field values, including the source
            enum.
        """
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
        """
        Require serialization of shared and target-specific credit fields.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.as_write_payload()['work_id']
            1


        :return: New write-layer dictionary; implementations retain enum values.
        """


@dataclass(slots=True, kw_only=True)
class WorkAgentCredit(AgentCreditBase):
    """
    Represent an editable agent credit on a work, including contribution summary and canonical-for-work flag.

    Construction stores the supplied values. Call validate explicitly before relying on
    the field constraints.

    Example:
        >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
        >>> credit.target_id, credit.target_kind
        (1, 'work')
    """
    work_id: WorkID
    role: WorkAgentRole
    contribution_summary: str | None = None
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id stored on this credit.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.target_id
            1


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> str:
        """
        Identify this credit as attached to a work.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.target_kind
            'work'


        :return: The literal 'work'.
        """
        return 'work'

    @property
    def role_key(self) -> WorkAgentRole:
        """
        Return the work role used as the credit bucket key.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.role_key == WorkAgentRole.AUTHOR
            True


        :return: Stored WorkAgentRole value.
        """
        return self.role

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared credit data together with the work id, role and contribution summary and canonical-for-work flag.

        This method neither validates the credit nor writes to a database. Enum values
        remain enum objects.

        Example:
            >>> credit = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada')
            >>> credit.as_write_payload()['work_id']
            1


        :return: New dictionary containing the credit fields for a work write.
        """
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
    """
    Represent an editable agent credit on a expression, including language, source expression and abridgement context.

    Construction stores the supplied values. Call validate explicitly before relying on
    the field constraints.

    Example:
        >>> credit = ExpressionAgentCredit(expression_id=1, role=ExpressionAgentRole.TRANSLATOR, credited_as='Ada', language_id=2)
        >>> credit.target_id, credit.target_kind
        (1, 'expression')
    """
    expression_id: ExpressionID
    role: ExpressionAgentRole
    language_id: LanguageID | None = None
    based_on_expression_id: ExpressionID | None = None
    abridged: bool = False

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id stored on this credit.

        Example:
            >>> credit = ExpressionAgentCredit(expression_id=1, role=ExpressionAgentRole.TRANSLATOR, credited_as='Ada', language_id=2)
            >>> credit.target_id
            1


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> str:
        """
        Identify this credit as attached to a expression.

        Example:
            >>> credit = ExpressionAgentCredit(expression_id=1, role=ExpressionAgentRole.TRANSLATOR, credited_as='Ada', language_id=2)
            >>> credit.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return 'expression'

    @property
    def role_key(self) -> ExpressionAgentRole:
        """
        Return the expression role used as the credit bucket key.

        Example:
            >>> credit = ExpressionAgentCredit(expression_id=1, role=ExpressionAgentRole.TRANSLATOR, credited_as='Ada', language_id=2)
            >>> credit.role_key == ExpressionAgentRole.TRANSLATOR
            True


        :return: Stored ExpressionAgentRole value.
        """
        return self.role

    def validate(self) -> None:
        """
        Check common credit constraints and require a language id for translator credits.

        Raise ValueError on a failed constraint; no language lookup is performed.

        Example:
            >>> credit = ExpressionAgentCredit(expression_id=1, role=ExpressionAgentRole.TRANSLATOR, credited_as='Ada', language_id=2)
            >>> credit.validate()
            >>> credit.language_id = None
            >>> credit.validate()
            Traceback (most recent call last):
            ...
            ValueError: translator credits should carry a language_id


        :return: None.
        """
        AgentCreditBase.validate(self)
        if self.role == ExpressionAgentRole.TRANSLATOR and self.language_id is None:
            raise ValueError('translator credits should carry a language_id')

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared credit data together with the expression id, role and language, source expression and abridgement context.

        This method neither validates the credit nor writes to a database. Enum values
        remain enum objects.

        Example:
            >>> credit = ExpressionAgentCredit(expression_id=1, role=ExpressionAgentRole.TRANSLATOR, credited_as='Ada', language_id=2)
            >>> credit.as_write_payload()['expression_id']
            1


        :return: New dictionary containing the credit fields for a expression write.
        """
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
    """
    Represent an editable agent credit on a manifestation, including imprint name, publication statement and imprint-presence flag.

    Construction stores the supplied values. Call validate explicitly before relying on
    the field constraints.

    Example:
        >>> credit = ManifestationAgentCredit(manifestation_id=1, role=ManifestationAgentRole.PUBLISHER, credited_as='Ada')
        >>> credit.target_id, credit.target_kind
        (1, 'manifestation')
    """
    manifestation_id: ManifestationID
    role: ManifestationAgentRole
    imprint_name: str | None = None
    publication_statement: str | None = None
    appears_in_imprint: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id stored on this credit.

        Example:
            >>> credit = ManifestationAgentCredit(manifestation_id=1, role=ManifestationAgentRole.PUBLISHER, credited_as='Ada')
            >>> credit.target_id
            1


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        """
        Identify this credit as attached to a manifestation.

        Example:
            >>> credit = ManifestationAgentCredit(manifestation_id=1, role=ManifestationAgentRole.PUBLISHER, credited_as='Ada')
            >>> credit.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return 'manifestation'

    @property
    def role_key(self) -> ManifestationAgentRole:
        """
        Return the manifestation role used as the credit bucket key.

        Example:
            >>> credit = ManifestationAgentCredit(manifestation_id=1, role=ManifestationAgentRole.PUBLISHER, credited_as='Ada')
            >>> credit.role_key == ManifestationAgentRole.PUBLISHER
            True


        :return: Stored ManifestationAgentRole value.
        """
        return self.role

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared credit data together with the manifestation id, role and imprint name, publication statement and imprint-presence flag.

        This method neither validates the credit nor writes to a database. Enum values
        remain enum objects.

        Example:
            >>> credit = ManifestationAgentCredit(manifestation_id=1, role=ManifestationAgentRole.PUBLISHER, credited_as='Ada')
            >>> credit.as_write_payload()['manifestation_id']
            1


        :return: New dictionary containing the credit fields for a manifestation write.
        """
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
    """
    Represent an editable agent credit on a item, including provenance, association interval and copy-specific flag.

    Construction stores the supplied values. Call validate explicitly before relying on
    the field constraints.

    Example:
        >>> credit = ItemAgentCredit(item_id=1, role=ItemAgentRole.OWNER, credited_as='Ada')
        >>> credit.target_id, credit.target_kind
        (1, 'item')
    """
    item_id: ItemID
    role: ItemAgentRole
    provenance_note: str | None = None
    association_start_ep_k: int | None = None
    association_end_ep_k: int | None = None
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id stored on this credit.

        Example:
            >>> credit = ItemAgentCredit(item_id=1, role=ItemAgentRole.OWNER, credited_as='Ada')
            >>> credit.target_id
            1


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> str:
        """
        Identify this credit as attached to a item.

        Example:
            >>> credit = ItemAgentCredit(item_id=1, role=ItemAgentRole.OWNER, credited_as='Ada')
            >>> credit.target_kind
            'item'


        :return: The literal 'item'.
        """
        return 'item'

    @property
    def role_key(self) -> ItemAgentRole:
        """
        Return the item role used as the credit bucket key.

        Example:
            >>> credit = ItemAgentCredit(item_id=1, role=ItemAgentRole.OWNER, credited_as='Ada')
            >>> credit.role_key == ItemAgentRole.OWNER
            True


        :return: Stored ItemAgentRole value.
        """
        return self.role

    def validate(self) -> None:
        """
        Check common credit constraints and reject a reversed association interval.

        The interval is checked only when both endpoints are present; equal endpoints are
        allowed. Invalid values raise ValueError.

        Example:
            >>> credit = ItemAgentCredit(item_id=1, role=ItemAgentRole.OWNER, credited_as='Ada')
            >>> credit.association_start_ep_k = 20
            >>> credit.association_end_ep_k = 10
            >>> credit.validate()
            Traceback (most recent call last):
            ...
            ValueError: association_end_ep_k cannot be earlier than association_start_ep_k


        :return: None.
        """
        AgentCreditBase.validate(self)
        if (
            self.association_start_ep_k is not None
            and self.association_end_ep_k is not None
            and self.association_end_ep_k < self.association_start_ep_k
        ):
            raise ValueError('association_end_ep_k cannot be earlier than association_start_ep_k')

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared credit data together with the item id, role and provenance, association interval and copy-specific flag.

        This method neither validates the credit nor writes to a database. Enum values
        remain enum objects.

        Example:
            >>> credit = ItemAgentCredit(item_id=1, role=ItemAgentRole.OWNER, credited_as='Ada')
            >>> credit.as_write_payload()['item_id']
            1


        :return: New dictionary containing the credit fields for a item write.
        """
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
class RoleCreditsContainer(
    MetadataSequenceStringMixin,
    Generic[CreditT, RoleT],
    abc.ABC,
):
    """
    Maintain an editable ordered list of credits for one role and target.

    The list is owned by the container, but credit objects are shared with callers.
    Mutations that change ordering renumber positions; full validation is a separate
    operation.

    Example:
        >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
        >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
        >>> credits.add_credit(ada)
        >>> credits.display_names()
        ('Ada',)
    """

    role: RoleT
    target_id: int
    _credits: list[CreditT]

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL: ClassVar[str] = "credits"

    def __init__(self, *, role: RoleT, target_id: int, credits: list[CreditT] | None = None) -> None:
        """
        Copy the initial credit list without validating shapes or normalizing positions.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> len(credits)
            0


        :param role: Role shared by credits in this bucket.
        :param target_id: WEMI row id shared by credits in this bucket.
        :param credits: Initial credit objects, shallow-copied into a new list; None starts
            empty.
        :return: None.
        """
        self.role = role
        self.target_id = target_id
        self._credits = list(credits or [])

    def __iter__(self) -> Iterator[CreditT]:
        """
        Iterate over live credit objects in their current list order.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> next(iter(credits)) is ada
            True


        :return: Iterator over the stored credit references.
        """
        return iter(self._credits)

    def __len__(self) -> int:
        """
        Count credits currently stored in this role bucket.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> len(credits)
            1


        :return: Number of stored credits.
        """
        return len(self._credits)

    def __getitem__(self, index: int) -> CreditT:
        """
        Read a credit by list index, allowing negative indices.

        An out-of-range index raises IndexError.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits[-1] is ada
            True


        :param index: Zero-based or negative list index.
        :return: Stored credit object at the requested index.
        """
        return self._credits[index]

    def credits(self) -> tuple[CreditT, ...]:
        """
        Take a tuple snapshot of the current credit order.

        The tuple contains shared mutable credit objects.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.credits()[0] is ada
            True


        :return: Tuple of stored credit references.
        """
        return tuple(self._credits)

    def ids(self) -> tuple[AgentID | None, ...]:
        """
        Collect agent ids in credit order, retaining unresolved ids and duplicates.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.ids()
            (7,)


        :return: Tuple of agent ids, including None values.
        """
        return tuple(credit.agent_id for credit in self._credits)

    def display_names(self) -> tuple[str, ...]:
        """
        Collect credited-as text in list order.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.display_names()
            ('Ada',)


        :return: Tuple of stored display names.
        """
        return tuple(credit.credited_as for credit in self._credits)

    def to_text(self, sep: str = ' & ') -> str:
        """
        Join the credited-as names with a caller-selected separator.

        Per-credit join_before and join_after values are not used here.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.to_text(sep='; ')
            'Ada'


        :param sep: Separator inserted between adjacent display names.
        :return: Joined text, or an empty string for an empty bucket.
        """
        return sep.join(self.display_names())

    def add_credit(self, credit: CreditT) -> None:
        """
        Check target and role, append the shared credit, and renumber all positions.

        Shape mismatches raise ValueError before insertion. Other credit fields are checked
        only by validate.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> ada.position
            0


        :param credit: Credit object with matching target kind, target id and role.
        :return: None.
        """
        self._validate_credit_shape(credit)
        self._credits.append(credit)
        self.normalize_positions()

    def replace_credit(self, index: int, credit: CreditT) -> None:
        """
        Check shape, replace the indexed credit, and renumber positions.

        Shape mismatches raise ValueError; an invalid list index raises IndexError.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> replacement = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Bea')
            >>> credits.replace_credit(0, replacement)
            >>> credits.display_names(), replacement.position
            (('Bea',), 0)


        :param index: List index of the credit to replace.
        :param credit: Replacement credit with matching target kind, id and role.
        :return: None.
        """
        self._validate_credit_shape(credit)
        self._credits[index] = credit
        self.normalize_positions()

    def remove_credit_at(self, index: int) -> CreditT:
        """
        Pop the indexed credit and renumber the remaining positions.

        An invalid list index raises IndexError.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.remove_credit_at(0) is ada
            True
            >>> len(credits)
            0


        :param index: List index to remove, including negative indices.
        :return: Removed credit object; its own position is not reset.
        """
        removed = self._credits.pop(index)
        self.normalize_positions()
        return removed

    def remove_agent(self, agent_id: AgentID) -> int:
        """
        Remove every credit with the given agent id and renumber survivors if any were removed.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.remove_agent(7)
            1
            >>> credits.remove_agent(7)
            0


        :param agent_id: Agent id to match across all stored credits.
        :return: Number of removed credits.
        """
        before = len(self._credits)
        self._credits = [credit for credit in self._credits if credit.agent_id != agent_id]
        removed = before - len(self._credits)
        if removed:
            self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Remove all stored references from this bucket.

        Previously returned credit objects retain their field values.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.clear()
            >>> credits.credits()
            ()


        :return: None.
        """
        self._credits.clear()

    def move_credit(self, old_index: int, new_index: int) -> None:
        """
        Pop a credit, insert it at the new list position, and renumber all credits.

        The source follows list.pop rules and the destination follows list.insert rules,
        including clipping out-of-range destinations.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.move_credit(0, 99)
            >>> credits[0] is ada and ada.position == 0
            True


        :param old_index: Index to remove; an invalid index raises IndexError.
        :param new_index: Insertion index applied after removing the source credit.
        :return: None.
        """
        credit = self._credits.pop(old_index)
        self._credits.insert(new_index, credit)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set the primary flag only on the credit whose enumerated index matches.

        Negative and out-of-range indices clear every primary flag; this method does not use
        negative list-index semantics.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.set_primary(0)
            >>> ada.is_primary
            True
            >>> credits.set_primary(-1)
            >>> ada.is_primary
            False


        :param index: Nonnegative index to designate as primary, or an unmatched index to
            clear all flags.
        :return: None.
        """
        for i, credit in enumerate(self._credits):
            credit.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Overwrite each shared credit position with its zero-based list index.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> ada.position = 8
            >>> credits.normalize_positions()
            >>> ada.position
            0


        :return: None.
        """
        for index, credit in enumerate(self._credits):
            credit.position = index

    def validate(self) -> None:
        """
        Check every credit shape and value, contiguous positions, and at most one primary credit.

        Raise ValueError on the first failed constraint. An empty bucket is valid; no values
        are repaired.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.validate()
            >>> ada.position = 4
            >>> credits.validate()
            Traceback (most recent call last):
            ...
            ValueError: Credit position mismatch for work 1: expected 0, got 4


        :return: None.
        """
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
        """
        Serialize credits in list order without validating or persisting them.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits.as_write_payload()[0]['position']
            0


        :return: New list of per-credit write dictionaries.
        """
        return [credit.as_write_payload() for credit in self._credits]

    def _validate_credit_shape(self, credit: CreditT) -> None:
        """
        Require the bucket target kind, target id and role on a candidate credit.

        Raise ValueError for the first mismatch. Position and other credit fields are not
        inspected.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=1)
            >>> ada = WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7)
            >>> credits.add_credit(ada)
            >>> credits._validate_credit_shape(ada)


        :param credit: Credit object whose target and role must match this bucket.
        :return: None.
        """
        if credit.target_kind != self.target_kind:
            raise ValueError(f'Cannot add {credit.target_kind} credit to {self.target_kind} container')
        if credit.target_id != self.target_id:
            raise ValueError(
                f'Credit target_id {credit.target_id} does not match container target_id {self.target_id}'
            )
        if credit.role_key != self.role:
            raise ValueError(f'Credit role {credit.role_key} does not match container role {self.role}')


class WorkRoleCreditsContainer(RoleCreditsContainer[WorkAgentCredit, WorkAgentRole]):
    """
    Collect ordered agent credits for one role on a work.

    The inherited constructor copies the credit list; add_credit checks shape and
    normalizes positions. Call validate for full credit constraints.

    Example:
        >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=3)
        >>> credits.work_id
        3
    """
    target_kind: ClassVar[str] = 'work'

    @property
    def work_id(self) -> WorkID:
        """
        Expose the bucket target id under its work-specific name.

        Example:
            >>> credits = WorkRoleCreditsContainer(role=WorkAgentRole.AUTHOR, target_id=3)
            >>> credits.work_id
            3


        :return: Stored work row id.
        """
        return self.target_id


class ExpressionRoleCreditsContainer(RoleCreditsContainer[ExpressionAgentCredit, ExpressionAgentRole]):
    """
    Collect ordered agent credits for one role on a expression.

    The inherited constructor copies the credit list; add_credit checks shape and
    normalizes positions. Call validate for full credit constraints.

    Example:
        >>> credits = ExpressionRoleCreditsContainer(role=ExpressionAgentRole.TRANSLATOR, target_id=3)
        >>> credits.expression_id
        3
    """
    target_kind: ClassVar[str] = 'expression'

    @property
    def expression_id(self) -> ExpressionID:
        """
        Expose the bucket target id under its expression-specific name.

        Example:
            >>> credits = ExpressionRoleCreditsContainer(role=ExpressionAgentRole.TRANSLATOR, target_id=3)
            >>> credits.expression_id
            3


        :return: Stored expression row id.
        """
        return self.target_id


class ManifestationRoleCreditsContainer(RoleCreditsContainer[ManifestationAgentCredit, ManifestationAgentRole]):
    """
    Collect ordered agent credits for one role on a manifestation.

    The inherited constructor copies the credit list; add_credit checks shape and
    normalizes positions. Call validate for full credit constraints.

    Example:
        >>> credits = ManifestationRoleCreditsContainer(role=ManifestationAgentRole.PUBLISHER, target_id=3)
        >>> credits.manifestation_id
        3
    """
    target_kind: ClassVar[str] = 'manifestation'

    @property
    def manifestation_id(self) -> ManifestationID:
        """
        Expose the bucket target id under its manifestation-specific name.

        Example:
            >>> credits = ManifestationRoleCreditsContainer(role=ManifestationAgentRole.PUBLISHER, target_id=3)
            >>> credits.manifestation_id
            3


        :return: Stored manifestation row id.
        """
        return self.target_id


class ItemRoleCreditsContainer(RoleCreditsContainer[ItemAgentCredit, ItemAgentRole]):
    """
    Collect ordered agent credits for one role on a item.

    The inherited constructor copies the credit list; add_credit checks shape and
    normalizes positions. Call validate for full credit constraints.

    Example:
        >>> credits = ItemRoleCreditsContainer(role=ItemAgentRole.OWNER, target_id=3)
        >>> credits.item_id
        3
    """
    target_kind: ClassVar[str] = 'item'

    @property
    def item_id(self) -> ItemID:
        """
        Expose the bucket target id under its item-specific name.

        Example:
            >>> credits = ItemRoleCreditsContainer(role=ItemAgentRole.OWNER, target_id=3)
            >>> credits.item_id
            3


        :return: Stored item row id.
        """
        return self.target_id


class BaseTargetAgentCreditsContainer(
    MetadataSequenceStringMixin,
    Generic[RoleT, CreditT, RoleContainerT],
    abc.ABC,
):
    """
    Group editable credit buckets by role for one WEMI target.

    Buckets retain role insertion order; each bucket retains its credit order. Looking
    up a role is nonmutating unless ensure_role or a generated bucket property is used.

    Example:
        >>> credits = WorkAgentCreditsContainer(work_id=1)
        >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
        >>> credits.role_text(WorkAgentRole.AUTHOR)
        'Ada'
    """

    STRING_COUNT_LABEL: ClassVar[str] = "credits"

    def __init__(self) -> None:
        """
        Initialize an empty role-to-bucket mapping.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.roles()
            ()


        :return: None.
        """
        self._by_role: dict[RoleT, RoleContainerT] = {}

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the WEMI row id shared by all role buckets.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """
        Require the WEMI level represented by this target container.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    @abc.abstractmethod
    def _make_role_container(self, role: RoleT) -> RoleContainerT:
        """
        Require construction of an empty role bucket bound to this target.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> bucket = credits._make_role_container(WorkAgentRole.AUTHOR)
            >>> bucket.target_id, len(bucket), credits.roles()
            (1, 0, ())


        :param role: Role enum selecting a bucket for this target.
        :return: New target-specific role container; registration is handled by ensure_role.
        """

    def roles(self) -> tuple[RoleT, ...]:
        """
        Return registered roles in bucket insertion order.

        Empty buckets remain represented.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
            >>> credits.roles() == (WorkAgentRole.AUTHOR,)
            True


        :return: Tuple of role keys.
        """
        return tuple(self._by_role.keys())

    def has_role(self, role: RoleT) -> bool:
        """
        Check whether a bucket is registered, even if it contains no credits.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.has_role(WorkAgentRole.AUTHOR)
            False


        :param role: Role enum selecting a bucket for this target.
        :return: True when the role key exists.
        """
        return role in self._by_role

    def get_role(self, role: RoleT) -> RoleContainerT | None:
        """
        Look up a role bucket without creating one.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.get_role(WorkAgentRole.AUTHOR) is None
            True
            >>> credits.roles()
            ()


        :param role: Role enum selecting a bucket for this target.
        :return: Stored live bucket, or None when absent.
        """
        return self._by_role.get(role)

    def ensure_role(self, role: RoleT) -> RoleContainerT:
        """
        Return the registered role bucket, creating and storing an empty one if absent.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> bucket = credits.ensure_role(WorkAgentRole.AUTHOR)
            >>> credits.ensure_role(WorkAgentRole.AUTHOR) is bucket
            True


        :param role: Role enum selecting a bucket for this target.
        :return: Live bucket bound to this target.
        """
        container = self._by_role.get(role)
        if container is None:
            container = self._make_role_container(role)
            self._by_role[role] = container
        return container

    def add_credit(self, credit: CreditT) -> None:
        """
        Check the target id, ensure the role bucket, and delegate shape checking and insertion.

        An id mismatch raises ValueError before creating a bucket. A later bucket shape
        error may leave a newly created empty bucket. Successful insertion renumbers that
        bucket; full validation remains explicit.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
            >>> credits.role_ids(WorkAgentRole.AUTHOR)
            (7,)


        :param credit: Credit object to attach to its role on this target.
        :return: None.
        """
        if credit.target_id != self.target_id:
            raise ValueError(
                f'Credit target_id {credit.target_id} does not match {self.target_kind} target_id {self.target_id}'
            )
        self.ensure_role(credit.role_key).add_credit(credit)

    def iter_all_credits(self) -> Iterator[CreditT]:
        """
        Yield shared credit objects in role insertion order and then bucket order.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
            >>> [credit.credited_as for credit in credits.iter_all_credits()]
            ['Ada']


        :return: Iterator over every stored credit.
        """
        for container in self._by_role.values():
            yield from container

    def all_agent_ids(self) -> set[AgentID]:
        """
        Collect distinct resolved agent ids across every bucket.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
            >>> credits.all_agent_ids()
            {7}


        :return: Set of agent ids, excluding None.
        """
        return {credit.agent_id for credit in self.iter_all_credits() if credit.agent_id is not None}

    def role_ids(self, role: RoleT) -> tuple[AgentID | None, ...]:
        """
        Read ordered agent ids for a role without creating its bucket.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.role_ids(WorkAgentRole.AUTHOR), credits.roles()
            ((), ())


        :param role: Role enum selecting a bucket for this target.
        :return: Tuple retaining duplicates and None ids; empty tuple for an absent role.
        """
        container = self.get_role(role)
        if container is None:
            return tuple()
        return container.ids()

    def role_text(self, role: RoleT, sep: str = ' & ') -> str:
        """
        Join credited names for a role without creating its bucket.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
            >>> credits.role_text(WorkAgentRole.AUTHOR, sep='; ')
            'Ada'


        :param role: Role enum selecting a bucket for this target.
        :param sep: Separator inserted between credited names.
        :return: Joined display names, or an empty string for an absent or empty role.
        """
        container = self.get_role(role)
        if container is None:
            return ''
        return container.to_text(sep=sep)

    def validate(self) -> None:
        """
        Validate each registered role bucket without normalizing or repairing credits.

        The first bucket validation error propagates.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
            >>> credits.validate()


        :return: None.
        """
        for container in self._by_role.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        """
        Flatten per-role write payloads in bucket insertion order.

        Serialization neither validates credits nor persists the returned data.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=1)
            >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
            >>> credits.as_write_payload()[0]['agent_id']
            7


        :return: New list of credit dictionaries.
        """
        payload: list[dict[str, object]] = []
        for container in self._by_role.values():
            payload.extend(container.as_write_payload())
        return payload


class WorkAgentCreditsContainer(BaseTargetAgentCreditsContainer[WorkAgentRole, WorkAgentCredit, WorkRoleCreditsContainer]):
    """
    Group agent contributions by role for a single work.

    The explicit role methods are the core API. Runtime-installed role properties and
    text methods provide convenience access to the same buckets.

    Example:
        >>> credits = WorkAgentCreditsContainer(work_id=3)
        >>> credits.target_id, credits.roles()
        (3, ())
    """
    def __init__(self, *, work_id: WorkID) -> None:
        """
        Store the work id and start with no registered roles.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=3)
            >>> credits.roles()
            ()


        :param work_id: Work row id retained by this container.
        :return: None.
        """
        super().__init__()
        self.work_id = work_id

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id used by this container.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=3)
            >>> credits.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> str:
        """
        Identify the target as a work.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=3)
            >>> credits.target_kind
            'work'


        :return: The literal 'work'.
        """
        return 'work'

    def _make_role_container(self, role: WorkAgentRole) -> WorkRoleCreditsContainer:
        """
        Build an empty work role bucket without registering it.

        Example:
            >>> credits = WorkAgentCreditsContainer(work_id=3)
            >>> bucket = credits._make_role_container(WorkAgentRole.AUTHOR)
            >>> bucket.target_id, len(bucket), credits.roles()
            (3, 0, ())


        :param role: Role enum selecting a bucket for this target.
        :return: New WorkRoleCreditsContainer sharing the target id.
        """
        return WorkRoleCreditsContainer(role=role, target_id=self.work_id)


class ExpressionAgentCreditsContainer(BaseTargetAgentCreditsContainer[ExpressionAgentRole, ExpressionAgentCredit, ExpressionRoleCreditsContainer]):
    """
    Group agent contributions by role for a single expression.

    The explicit role methods are the core API. Runtime-installed role properties and
    text methods provide convenience access to the same buckets.

    Example:
        >>> credits = ExpressionAgentCreditsContainer(expression_id=3)
        >>> credits.target_id, credits.roles()
        (3, ())
    """
    def __init__(self, *, expression_id: ExpressionID) -> None:
        """
        Store the expression id and start with no registered roles.

        Example:
            >>> credits = ExpressionAgentCreditsContainer(expression_id=3)
            >>> credits.roles()
            ()


        :param expression_id: Expression row id retained by this container.
        :return: None.
        """
        super().__init__()
        self.expression_id = expression_id

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id used by this container.

        Example:
            >>> credits = ExpressionAgentCreditsContainer(expression_id=3)
            >>> credits.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> str:
        """
        Identify the target as a expression.

        Example:
            >>> credits = ExpressionAgentCreditsContainer(expression_id=3)
            >>> credits.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return 'expression'

    def _make_role_container(self, role: ExpressionAgentRole) -> ExpressionRoleCreditsContainer:
        """
        Build an empty expression role bucket without registering it.

        Example:
            >>> credits = ExpressionAgentCreditsContainer(expression_id=3)
            >>> bucket = credits._make_role_container(ExpressionAgentRole.TRANSLATOR)
            >>> bucket.target_id, len(bucket), credits.roles()
            (3, 0, ())


        :param role: Role enum selecting a bucket for this target.
        :return: New ExpressionRoleCreditsContainer sharing the target id.
        """
        return ExpressionRoleCreditsContainer(role=role, target_id=self.expression_id)


class ManifestationAgentCreditsContainer(BaseTargetAgentCreditsContainer[ManifestationAgentRole, ManifestationAgentCredit, ManifestationRoleCreditsContainer]):
    """
    Group agent contributions by role for a single manifestation.

    The explicit role methods are the core API. Runtime-installed role properties and
    text methods provide convenience access to the same buckets.

    Example:
        >>> credits = ManifestationAgentCreditsContainer(manifestation_id=3)
        >>> credits.target_id, credits.roles()
        (3, ())
    """
    def __init__(self, *, manifestation_id: ManifestationID) -> None:
        """
        Store the manifestation id and start with no registered roles.

        Example:
            >>> credits = ManifestationAgentCreditsContainer(manifestation_id=3)
            >>> credits.roles()
            ()


        :param manifestation_id: Manifestation row id retained by this container.
        :return: None.
        """
        super().__init__()
        self.manifestation_id = manifestation_id

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id used by this container.

        Example:
            >>> credits = ManifestationAgentCreditsContainer(manifestation_id=3)
            >>> credits.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        """
        Identify the target as a manifestation.

        Example:
            >>> credits = ManifestationAgentCreditsContainer(manifestation_id=3)
            >>> credits.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return 'manifestation'

    def _make_role_container(self, role: ManifestationAgentRole) -> ManifestationRoleCreditsContainer:
        """
        Build an empty manifestation role bucket without registering it.

        Example:
            >>> credits = ManifestationAgentCreditsContainer(manifestation_id=3)
            >>> bucket = credits._make_role_container(ManifestationAgentRole.PUBLISHER)
            >>> bucket.target_id, len(bucket), credits.roles()
            (3, 0, ())


        :param role: Role enum selecting a bucket for this target.
        :return: New ManifestationRoleCreditsContainer sharing the target id.
        """
        return ManifestationRoleCreditsContainer(role=role, target_id=self.manifestation_id)


class ItemAgentCreditsContainer(BaseTargetAgentCreditsContainer[ItemAgentRole, ItemAgentCredit, ItemRoleCreditsContainer]):
    """
    Group agent contributions by role for a single item.

    The explicit role methods are the core API. Runtime-installed role properties and
    text methods provide convenience access to the same buckets.

    Example:
        >>> credits = ItemAgentCreditsContainer(item_id=3)
        >>> credits.target_id, credits.roles()
        (3, ())
    """
    def __init__(self, *, item_id: ItemID) -> None:
        """
        Store the item id and start with no registered roles.

        Example:
            >>> credits = ItemAgentCreditsContainer(item_id=3)
            >>> credits.roles()
            ()


        :param item_id: Item row id retained by this container.
        :return: None.
        """
        super().__init__()
        self.item_id = item_id

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id used by this container.

        Example:
            >>> credits = ItemAgentCreditsContainer(item_id=3)
            >>> credits.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> str:
        """
        Identify the target as a item.

        Example:
            >>> credits = ItemAgentCreditsContainer(item_id=3)
            >>> credits.target_kind
            'item'


        :return: The literal 'item'.
        """
        return 'item'

    def _make_role_container(self, role: ItemAgentRole) -> ItemRoleCreditsContainer:
        """
        Build an empty item role bucket without registering it.

        Example:
            >>> credits = ItemAgentCreditsContainer(item_id=3)
            >>> bucket = credits._make_role_container(ItemAgentRole.OWNER)
            >>> bucket.target_id, len(bucket), credits.roles()
            (3, 0, ())


        :param role: Role enum selecting a bucket for this target.
        :return: New ItemRoleCreditsContainer sharing the target id.
        """
        return ItemRoleCreditsContainer(role=role, target_id=self.item_id)


def _default_role_stem(role: object) -> str:
    """
    Lowercase a role name and apply the convenience-property plural spelling.

    A terminal y becomes ies, a terminal s is retained, and other names gain s. This is
    a naming heuristic, not general English inflection.

    Example:
        >>> _default_role_stem(WorkAgentRole.AUTHOR)
        'authors'


    :param role: Enum-like object exposing a string name attribute.
    :return: Pluralized property stem.
    """
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
    Install role bucket, id, text and configurable text accessors on a credit container class.

    Each role receives stem, stem_ids, stem_text and stem_to_text attributes. Bucket
    properties call ensure_role; id and text accessors leave absent roles unregistered.
    Existing attributes with those names are overwritten. The explicit generic methods
    remain the canonical API under metadata_container_dynamic_convenience_policy.md.

    Example:
        >>> credits = WorkAgentCreditsContainer(work_id=1)
        >>> credits.authors_ids, credits.roles()
        ((), ())
        >>> bucket = credits.authors
        >>> credits.get_role(WorkAgentRole.AUTHOR) is bucket
        True


    :param cls: Target container class to receive the generated descriptors and methods.
    :param roles: Iterable enum type whose members identify available roles.
    :param stem_overrides: Optional role-to-stem replacements for the default plural
        naming rule.
    :return: None.
    """
    stem_overrides = stem_overrides or {}
    for role in roles:
        stem = stem_overrides.get(role, _default_role_stem(role))

        def role_container_getter(self, _role=role):
            """
            Return the captured role bucket, registering an empty bucket when necessary.

            Example:
                >>> credits = WorkAgentCreditsContainer(work_id=1)
                >>> bucket = credits.authors
                >>> credits.has_role(WorkAgentRole.AUTHOR)
                True


            :param self: Target container instance supplied when the generated accessor is
                bound.
            :param _role: Role captured at installation time as the default argument.
            :return: Live per-role credit container.
            """
            return self.ensure_role(_role)

        def role_ids_getter(self, _role=role):
            """
            Read the captured role ids without creating a missing bucket.

            Example:
                >>> credits = WorkAgentCreditsContainer(work_id=1)
                >>> credits.authors_ids, credits.roles()
                ((), ())


            :param self: Target container instance supplied when the generated accessor is
                bound.
            :param _role: Role captured at installation time as the default argument.
            :return: Ordered tuple of agent ids, including unresolved None values.
            """
            return self.role_ids(_role)

        def role_text_getter(self, _role=role):
            """
            Read the captured role text with the default name separator.

            Example:
                >>> credits = WorkAgentCreditsContainer(work_id=1)
                >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
                >>> credits.authors_text
                'Ada'


            :param self: Target container instance supplied when the generated accessor is
                bound.
            :param _role: Role captured at installation time as the default argument.
            :return: Joined names, or an empty string for an absent or empty bucket.
            """
            return self.role_text(_role)

        def role_text_method(self, sep: str = ' & ', _role=role) -> str:
            """
            Render the captured role names with a caller-selected separator.

            Example:
                >>> credits = WorkAgentCreditsContainer(work_id=1)
                >>> credits.add_credit(WorkAgentCredit(work_id=1, role=WorkAgentRole.AUTHOR, credited_as='Ada', agent_id=7))
                >>> credits.authors_to_text(sep='; ')
                'Ada'


            :param self: Target container instance supplied when the generated accessor is
                bound.
            :param sep: Separator passed to the target container role_text method.
            :param _role: Role captured at installation time as the default argument.
            :return: Joined names, or an empty string for an absent or empty bucket.
            """
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
