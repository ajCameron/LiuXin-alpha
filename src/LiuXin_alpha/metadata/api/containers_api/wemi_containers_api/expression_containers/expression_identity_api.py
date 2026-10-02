"""
Define the core expression identity contract and normalized flag type.

The contract covers the expression row itself; related metadata and read-side
projections live in separate APIs.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
    >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
    >>> expression.WEMI_LEVEL
    'expression'
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self, TypeAlias

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.identity_api import (
    WemiIdentityAPI,
)


ExpressionFlags: TypeAlias = tuple[str, ...]
"""Normalized expression flag tokens. Empty tuple means no flags."""


class ExpressionIdentityPropertiesAPI(WemiIdentityAPI, metaclass=abc.ABCMeta):
    """
    Require the stable row-level surface for one expression identity.

    Concrete implementations define id assignment, flag normalization and mapping
    ownership. The common id alias delegates directly to expression_id.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
        >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
        >>> expression.id
        2
    """

    WEMI_LEVEL: ClassVar[str] = "expression"
    SOURCE_TABLE: ClassVar[str] = "expressions"
    ID_FIELD: ClassVar[str] = "expression_id"

    @property
    def id(self) -> Optional[int]:
        """
        Return expression_id through the level-independent WEMI id alias.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
            >>> expression.id
            2


        :return: Current expression id, or None.
        """
        return self.expression_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Assign expression_id through the level-independent WEMI id alias.

        Concrete expression identities may reject reassignment once a non-None id is stored.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.id = 2
            >>> expression.expression_id
            2


        :param value: New expression id, or None.
        :return: None.
        """
        self.expression_id = value

    @property
    @abc.abstractmethod
    def expression_id(self) -> Optional[int]:
        """
        Require access to the expression row id for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_id=2)
            >>> expression.expression_id
            2


        :return: Current expression row id, or None where optional.
        """

    @expression_id.setter
    @abc.abstractmethod
    def expression_id(self, expression_id: Optional[int]) -> None:
        """
        Require assignment of the expression row id under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_id = 2
            >>> expression.expression_id
            2


        :param expression_id: New expression row id.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def expression_type(self) -> Optional[str]:
        """
        Require access to the expression type for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_type='text')
            >>> expression.expression_type
            'text'


        :return: Current expression type, or None where optional.
        """

    @expression_type.setter
    @abc.abstractmethod
    def expression_type(self, expression_type: Optional[str]) -> None:
        """
        Require assignment of the expression type under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_type = 'text'
            >>> expression.expression_type
            'text'


        :param expression_type: New expression type.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def expression_language_id(self) -> Optional[int]:
        """
        Require access to the language row id for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_language_id=3)
            >>> expression.expression_language_id
            3


        :return: Current language row id, or None where optional.
        """

    @expression_language_id.setter
    @abc.abstractmethod
    def expression_language_id(self, expression_language_id: Optional[int]) -> None:
        """
        Require assignment of the language row id under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_language_id = 3
            >>> expression.expression_language_id
            3


        :param expression_language_id: New language row id.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def expression_label(self) -> Optional[str]:
        """
        Require access to the display label for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_label='English text')
            >>> expression.expression_label
            'English text'


        :return: Current display label, or None where optional.
        """

    @expression_label.setter
    @abc.abstractmethod
    def expression_label(self, expression_label: Optional[str]) -> None:
        """
        Require assignment of the display label under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_label = 'English text'
            >>> expression.expression_label
            'English text'


        :param expression_label: New display label.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def expression_title_override(self) -> Optional[str]:
        """
        Require access to the title override for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_title_override='Special edition')
            >>> expression.expression_title_override
            'Special edition'


        :return: Current title override, or None where optional.
        """

    @expression_title_override.setter
    @abc.abstractmethod
    def expression_title_override(self, expression_title_override: Optional[str]) -> None:
        """
        Require assignment of the title override under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_title_override = 'Special edition'
            >>> expression.expression_title_override
            'Special edition'


        :param expression_title_override: New title override.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def expression_subtitle(self) -> Optional[str]:
        """
        Require access to the subtitle for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_subtitle='Annotated')
            >>> expression.expression_subtitle
            'Annotated'


        :return: Current subtitle, or None where optional.
        """

    @expression_subtitle.setter
    @abc.abstractmethod
    def expression_subtitle(self, expression_subtitle: Optional[str]) -> None:
        """
        Require assignment of the subtitle under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_subtitle = 'Annotated'
            >>> expression.expression_subtitle
            'Annotated'


        :param expression_subtitle: New subtitle.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def expression_flags(self) -> ExpressionFlags:
        """
        Require access to the normalized flag tokens for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_flags='draft,reviewed')
            >>> expression.expression_flags
            ('draft', 'reviewed')


        :return: Current normalized flag tokens, or None where optional.
        """

    @expression_flags.setter
    @abc.abstractmethod
    def expression_flags(self, expression_flags: ExpressionFlags | None) -> None:
        """
        Require assignment of the normalized flag tokens under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_flags = ('draft', 'reviewed')
            >>> expression.expression_flags
            ('draft', 'reviewed')


        :param expression_flags: New normalized flag tokens.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def expression_status(self) -> Optional[str]:
        """
        Require access to the status for this expression.

        Concrete implementations determine normalization and assignment policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_status='complete')
            >>> expression.expression_status
            'complete'


        :return: Current status, or None where optional.
        """

    @expression_status.setter
    @abc.abstractmethod
    def expression_status(self, expression_status: Optional[str]) -> None:
        """
        Require assignment of the status under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity()
            >>> expression.expression_status = 'complete'
            >>> expression.expression_status
            'complete'


        :param expression_status: New status.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """
        Require serialization to canonical expression-column keys.

        Field inclusion, flag encoding and copy depth belong to the concrete implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
            >>> expression.to_mapping()['expression_id']
            2


        :return: Mutable metadata record describing the expression.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when a concrete identity does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
            >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
            >>> ExpressionIdentityPropertiesAPI.__str__(expression)
            'ExpressionIdentity()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"


class ExpressionIdentityAPI(ExpressionIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """
    Mark a concrete expression identity that provides every row-level property.

    The marker adds no behavior beyond ExpressionIdentityPropertiesAPI and remains
    useful as the public annotation boundary.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
        >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
        >>> isinstance(expression, ExpressionIdentityAPI)
        True
    """

__all__ = ["ExpressionFlags", "ExpressionIdentityPropertiesAPI", "ExpressionIdentityAPI"]
