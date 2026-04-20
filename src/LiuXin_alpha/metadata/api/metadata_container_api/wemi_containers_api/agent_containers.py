
"""
Container for the agents involved in a work - includes methods allowing re-prioritization of agents.

Containers are not intended to be row or database proxies.
You have to hand these back off to a write, if you've made changes, to be written out to the database.
"""

from __future__ import annotations


import abc

from dataclasses import dataclass
from typing import Literal, Optional


from typing import TYPE_CHECKING, Iterable, OrderedDict

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.metadata_types import AgentTypes, AgentID, WorkID, ExpressionID, ManifestationID, ItemID, \
    LanguageID, CreditSource, WorkAgentRole, ExpressionAgentRole, ManifestationAgentRole, ItemAgentRole
    from LiuXin_alpha.databases.db_types import AgentID


class AgentIdentityAPI(abc.ABC):
    """
    Container for information concerning an individual agent on the system.
    """
    # PROPERTIES FOR ALL THE ROW PROPERTIES GO HERE

    @property
    @abc.abstractmethod
    def agent_id(self) -> AgentID:
        """
        Return the ID for the creator.

        :return:
        """

    @property
    @abc.abstractmethod
    def agent_type(self) -> "AgentTypes":
        """
        What type is this agent?

        Current options are "human" or "organization"
        :return:
        """

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Name of the creator.

        :return:
        """

    @abc.abstractmethod
    def works_credited_for(self, type_filter: Optional[str] = None) -> Iterable["WorkAgentCredit"]:
        """
        Get the works this agent is credited for.

        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def expressions_credited_for(self, type_filter: Optional[str] = None) -> Iterable["ExpressionAgentCredit"]:
        """
        Get the expressions this agent is credited for.

        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def manifestations_credited_for(self, type_filter: Optional[str] = None) -> Iterable["ManifestationAgentCredit"]:
        """
        Get the manifestations this agent is credited for.

        :param type_filter:
        :return:
        """

    @abc.abstractmethod
    def items_credited_for(self, type_filter: Optional[str] = None) -> Iterable["ItemAgentCredit"]:
        """
        Get the items this agent is credited for.

        :param type_filter:
        :return:
        """


@dataclass(slots=True, kw_only=True)
class AgentCreditBase:
    """
    Shared relation data for an agent attached to some bibliographic entity.

    Base info for any link of an agent to
    - work
    - expression
    - manifestation
    - item
    """
    agent_id: AgentID | None = None
    credited_as: str
    sort_as: str | None = None

    # Relation/display/order state
    position: int | None = None
    priority: float = 0.0
    is_primary: bool = False

    # Provenance/editorial state
    source: CreditSource = CreditSource.USER_SET
    confidence: float | None = None
    notes: str | None = None

    # Optional glue for display rendering if you need it later
    join_before: str = ""
    join_after: str = ""

    def validate(self) -> None:
        """
        Check that the link is valid.

        :return:
        """
        if not self.credited_as.strip():
            raise ValueError("credited_as cannot be blank")
        if self.confidence is not None and not (0.0 <= self.confidence <= 1.0):
            raise ValueError("confidence must be between 0.0 and 1.0")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")


@dataclass(slots=True, kw_only=True)
class WorkAgentCredit(AgentCreditBase):
    """
    Dataclass representing an agents role in a work.

    An agent can have multiple roles in a work, so a single agent can have many links like this to a work.
    """
    work_id: WorkID
    role: WorkAgentRole
    target_kind: Literal["work"] = "work"

    # Optional work-level semantics
    contribution_summary: str | None = None
    canonical_for_work: bool = False

    def validate(self) -> None:
        super().validate()


@dataclass(slots=True, kw_only=True)
class ExpressionAgentCredit(AgentCreditBase):
    """
    Responsible for getting details of how an agent should be credited for an expression.
    """
    expression_id: ExpressionID
    role: ExpressionAgentRole
    target_kind: Literal["expression"] = "expression"

    # Expression-specific detail
    language_id: LanguageID | None = None
    based_on_expression_id: ExpressionID | None = None
    abridged: bool = False

    def validate(self) -> None:
        """
        Check the relation is valid.

        :return:
        """
        super().validate()
        if (
            self.role == ExpressionAgentRole.TRANSLATOR
            and self.language_id is None
        ):
            # Optional rule; keep/remove depending on your model
            raise ValueError("translator credits should usually carry a language_id")


@dataclass(slots=True, kw_only=True)
class ManifestationAgentCredit(AgentCreditBase):
    manifestation_id: ManifestationID
    role: ManifestationAgentRole
    target_kind: Literal["manifestation"] = "manifestation"

    # Manifestation-specific detail
    imprint_name: str | None = None
    publication_statement: str | None = None
    appears_in_imprint: bool = True

    def validate(self) -> None:
        super().validate()


@dataclass(slots=True, kw_only=True)
class ItemAgentCredit(AgentCreditBase):
    """
    Link between an agent and an item.
    """
    item_id: ItemID
    role: ItemAgentRole
    target_kind: Literal["item"] = "item"

    # Item/provenance-specific detail
    provenance_note: str | None = None
    association_start_ep_k: int | None = None
    association_end_ep_k: int | None = None
    copy_specific: bool = True

    def validate(self) -> None:
        super().validate()
        if (
            self.association_start_ep_k is not None
            and self.association_end_ep_k is not None
            and self.association_end_ep_k < self.association_start_ep_k
        ):
            raise ValueError("association_end_ep_k cannot be earlier than association_start_ep_k")





class TypedAgentsContainerBaseAPI(abc.ABC):
    """
    Container for an agents contribution to a work.
    """
    @property
    @abc.abstractmethod
    def type(self) -> str:
        """
        The type of agents this container represents.

        :return:
        """

    def __iter__(self) -> Iterable["AgentContainerAPI"]:
        """
        Iterate over all the agents in the container.

        :return:
        """

    @abc.abstractmethod
    def __len__(self) -> int:
        """
        Number of agents linked to this object.

        :return:
        """

    def to_string(self, sep: str = "&") -> str:
        """
        Take the types creators and render it as a string.

        :param sep: The seperator for the string.
        :return:
        """

    def from_string(self, creators_str: str, sep: str = "&") -> None:
        """
        Take a string and set the creators from it.

        :param creators_str:
        :param sep:
        :return:
        """

    def to_dict(self) -> OrderedDict[str, AgentID]:
        """
        Render the creator content as an ordered dictionary.

        You have to actually write the dict back to this object for changes to register with from_dict.
        :return:
        """

    def from_dict(self, creator_dict: OrderedDict[str, AgentID]) -> None:
        """
        Update this metadata object with a creator dictionary.

        :param creator_dict:
        :return:
        """


class TypedAgentContainerWorkAPI(TypedAgentsContainerBaseAPI, abc.ABC):
    """
    Contains all the agents of a single type linked to a work.
    """
    @property
    @abc.abstractmethod
    def work_id(self) -> "WorkID":
        """
        The ID of the work these agents are credited for.

        :return:
        """


class TypedAgentContainerExpressionAPI(TypedAgentsContainerBaseAPI, abc.ABC):
    """
    Contains all the agents of a single type linked to an expression.
    """

    @property
    @abc.abstractmethod
    def expression_id(self) -> "ExpressionID":
        """
        The ID of the expression these agents are credited for.

        :return:
        """


class TypedAgentContainerManifestationAPI(TypedAgentsContainerBaseAPI, abc.ABC):
    """
    Contains of the agents of a single type linked to an expression.
    """

    @property
    @abc.abstractmethod
    def manifestation_id(self) -> "ManifestationID":
        """
        The ID of the manifestation these agents are credited for.

        :return:
        """


class TypedAgentContainerItemAPI(TypedAgentsContainerBaseAPI, abc.ABC):
    """
    Contains of the agents of a single type linked to an expression.
    """

    @property
    @abc.abstractmethod
    def item_id(self) -> "ItemID":
        """
        The ID of the manifestation these agents are credited for.

        :return:
        """



class AgentCreditsContainerBaseAPI(abc.ABC):
    """
    Container for all the agents linked to an object.
    """

    def as_dict(self) -> dict[str, OrderedDict[str, AgentID]]:
        """
        Return all the agents in the container as a dict.

        :return:
        """

    def from_dict(self, agent_dict: dict[str, OrderedDict[str, AgentID]]) -> None:
        """
        Load from a dict.

        :param agent_dict:
        :return:
        """

    # - ACCESS METHOD PER MARC ROLE START HERE

    @property
    @abc.abstractmethod
    def authors_ids(self) -> Iterable["AgentID"]:
        """

        :return:
        """

    @property
    @abc.abstractmethod
    def authors_str(self, sep: str = "&") -> list[AgentID]:
        """
        Return the authors as a string.

        :param sep:
        :return:
        """

    # SAME FOR THE REST OF THE MARC ROLES



class AgentWorkCreditsContainerAPI(AgentCreditsContainerBaseAPI, abc.ABC):
    """
    Base API for the agents responsible for a work.
    """

    @property
    @abc.abstractmethod
    def work_id(self) -> "WorkID":
        """
        The ID for the work these agents are responsible for.

        :return:
        """

    # - ACCESS METHOD PER MARC DICT START HERE

    @property
    @abc.abstractmethod
    def authors(self) -> "TypedAgentContainerWorkAPI":
        """

        :return:
        """

    # SAME FOR THE REST OF THE MARC ROLES


class AgentExpressionCreditsContainerAPI(AgentCreditsContainerBaseAPI, abc.ABC):
    """
    Base API for the agents responsible for an expression.
    """

    @property
    @abc.abstractmethod
    def expression_id(self) -> "ExpressionID":
        """
        ID for the expression these creators are credited for.

        :return:
        """

    # - ACCESS METHOD PER MARC DICT START HERE

    @property
    @abc.abstractmethod
    def authors(self) -> "TypedAgentContainerExpressionAPI":
        """

        :return:
        """

    # SAME FOR THE REST OF THE MARC ROLES


class AgentManifestationCreditsContainerAPI(AgentCreditsContainerBaseAPI, abc.ABC):
    """
    Base API for the agents responsible for an expression.
    """

    @property
    @abc.abstractmethod
    def manifestation_id(self) -> "ManifestationID":
        """
        ID for the expression these creators are credited for.

        :return:
        """

    # - ACCESS METHOD PER MARC DICT START HERE

    @property
    @abc.abstractmethod
    def authors(self) -> "TypedAgentContainerManifestationAPI":
        """

        :return:
        """

    # SAME FOR THE REST OF THE MARC ROLES




class AgentItemCreditsContainerAPI(AgentCreditsContainerBaseAPI, abc.ABC):
    """
    Base API for the agents responsible for an expression.
    """

    @property
    @abc.abstractmethod
    def item_id(self) -> "ItemID":
        """
        ID for the expression these creators are credited for.

        :return:
        """

    # - ACCESS METHOD PER MARC DICT START HERE

    @property
    @abc.abstractmethod
    def authors(self) -> "TypedAgentContainerItemAPI":
        """

        :return:
        """

    # SAME FOR THE REST OF THE MARC ROLES