"""
Core WEMI identity API contract for expression entities.

Category: core WEMI identity object.

This module defines the smallest stable API for the expression entity itself,
not the editable metadata bundle and not a read-side query result.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
)

# Todo: Should have a title or subtitle field
class ExpressionIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """Row-level API for one expression."""

    @property
    def id(self) -> Optional[int]:
        """
        ID for this expression.

        :return:
        """
        return self.expression_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Set the ID for this expression.

        :return:
        """
        self.expression_id = value

    @property
    @abc.abstractmethod
    def expression_id(self) -> Optional[int]:
        """
        The id of the expression this container represents.

        :return:
        """

    @expression_id.setter
    @abc.abstractmethod
    def expression_id(self, expression_id: Optional[int]) -> None:
        """
        Set the id of the expression this container represents.

        :param expression_id:
        :return:
        """

    @property
    @abc.abstractmethod
    def expression_type(self) -> Optional[str]:
        """
        The type of the expression.

        :return:
        """

    @expression_type.setter
    @abc.abstractmethod
    def expression_type(self, expression_type: Optional[str]) -> None:
        """
        Set the type of the expression.

        :param expression_type:
        :return:
        """

    @property
    @abc.abstractmethod
    def expression_language_id(self) -> Optional[int]:
        """
        Language ID for this expression.

        :return:
        """

    @expression_language_id.setter
    @abc.abstractmethod
    def expression_language_id(self, expression_language_id: Optional[int]) -> None:
        """
        Set the Language ID for this expression.

        :param expression_language_id:
        :return:
        """

    @property
    @abc.abstractmethod
    def expression_label(self) -> Optional[str]:
        """
        Get the label of this expression.

        :return:
        """

    @expression_label.setter
    @abc.abstractmethod
    def expression_label(self, expression_label: Optional[str]) -> None:
        """
        Set the label for this expression.

        :param expression_label:
        :return:
        """

    @property
    @abc.abstractmethod
    def expression_title_override(self) -> Optional[str]:
        """
        An expression can provide a title override.

        Otherwise, it's built out of the work title and the expression subtitle.
        :return:
        """

    @expression_title_override.setter
    @abc.abstractmethod
    def expression_title_override(self, expression_title_override: Optional[str]) -> None:
        """
        Set the expression title override for the given expression.

        :param expression_title_override:
        :return:
        """

    @property
    @abc.abstractmethod
    def expression_subtitle(self) -> Optional[str]:
        """
        Get the expression subtitle for this expression - if there is one.

        :return:
        """

    @expression_subtitle.setter
    @abc.abstractmethod
    def expression_subtitle(self, expression_subtitle: Optional[str]) -> None:
        """
        Set the expression subtitle for this expression.

        :param expression_subtitle:
        :return:
        """

    # Todo: Not sure what this should be, but it shouldn't be a string.
    @property
    @abc.abstractmethod
    def expression_flags(self) -> Optional[str]:
        """
        Flags associated with the expression.

        :return:
        """

    @expression_flags.setter
    @abc.abstractmethod
    def expression_flags(self, expression_flags: Optional[str]) -> None:
        """
        Set the flags for the expression.

        :param expression_flags:
        :return:
        """

    @property
    @abc.abstractmethod
    def expression_status(self) -> Optional[str]:
        """
        Get the status of this expression.

        :return:
        """

    @expression_status.setter
    @abc.abstractmethod
    def expression_status(self, expression_status: Optional[str]) -> None:
        """
        Set the status of the expression.

        :param expression_status:
        :return:
        """

    @property
    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """
        Transform the record to a mapping.

        :return:
        """

    def __str__(self) -> str:
        """
        String representation of this expression.

        :return:
        """
        return f"{self.__class__.__name__}()"


class ExpressionIdentityAPI(ExpressionIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """Marker ABC for a concrete expression identity container."""

__all__ = ["ExpressionIdentityPropertiesAPI", "ExpressionIdentityAPI"]
