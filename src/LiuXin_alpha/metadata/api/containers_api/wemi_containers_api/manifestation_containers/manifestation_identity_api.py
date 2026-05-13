
"""
Core WEMI identity API contract for manifestation entities.

Category: core WEMI identity object.
This module defines the smallest stable API for the manifestation entity itself,
not the editable metadata bundle and not a read-side query result.
"""

from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
)

class ManifestationIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """
    Row-level API for one manifestation.
    """

    @property
    def id(self) -> Optional[int]:
        """
        ID of the manifestation.

        :return:
        """
        return self.manifestation_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Set the ID of the manifestation.

        :param value:
        :return:
        """
        self.manifestation_id = value

    @property
    @abc.abstractmethod
    def manifestation_id(self) -> Optional[int]:
        """
        Front end of the manifestation id.

        :return:
        """

    @manifestation_id.setter
    @abc.abstractmethod
    def manifestation_id(self, manifestation_id: Optional[int]) -> None:
        """
        Front end of the manifestation id.

        :param manifestation_id:
        :return:
        """

    @property
    def expression_id(self) -> Optional[int]:
        """
        ID for the expression this is a manifestation of.

        :return:
        """
        return self.manifestation_expression_id

    # Todo: Again, should be something like primary e.t.c
    @expression_id.setter
    def expression_id(self, value: Optional[int]) -> None:
        """
        Get the ID of the expression this is a manifestation of.

        :param value:
        :return:
        """
        self.manifestation_expression_id = value

    @property
    @abc.abstractmethod
    def manifestation_expression_id(self) -> Optional[int]:
        """
        Get the ID of the expression this is a manifestation of.

        :return:
        """

    @manifestation_expression_id.setter
    @abc.abstractmethod
    def manifestation_expression_id(self, manifestation_expression_id: Optional[int]) -> None:
        """
        Set the ID of the expression this is a manifestation of.

        :param manifestation_expression_id:
        :return:
        """

    # Todo: Not... sure what this is/is for?
    @property
    @abc.abstractmethod
    def manifestation_format_detail(self) -> Optional[str]:
        ...

    @manifestation_format_detail.setter
    @abc.abstractmethod
    def manifestation_format_detail(self, manifestation_format_detail: Optional[str]) -> None:
        ...

    @property
    @abc.abstractmethod
    def manifestation_carrier_type(self) -> Optional[str]:
        """
        Get the manifestation carrier type.

        :return:
        """

    @manifestation_carrier_type.setter
    @abc.abstractmethod
    def manifestation_carrier_type(self, manifestation_carrier_type: Optional[str]) -> None:
        """
        Set the manifestation carrier type.

        :param manifestation_carrier_type:
        :return:
        """

    @property
    @abc.abstractmethod
    def manifestation_edition_statement(self) -> Optional[str]:
        """
        Get the edition statement for the manifestation.

        :return:
        """

    @manifestation_edition_statement.setter
    @abc.abstractmethod
    def manifestation_edition_statement(self, manifestation_edition_statement: Optional[str]) -> None:
        """
        Set the edition statement for the manifestation.

        :param manifestation_edition_statement:
        :return:
        """

    @property
    @abc.abstractmethod
    def manifestation_pub_year(self) -> Optional[int]:
        """
        Get the manifestation publication year.

        :return:
        """

    @manifestation_pub_year.setter
    @abc.abstractmethod
    def manifestation_pub_year(self, manifestation_pub_year: Optional[int]) -> None:
        """
        Set the manifestation publication year.

        :param manifestation_pub_year:
        :return:
        """

    @property
    @abc.abstractmethod
    def manifestation_status(self) -> Optional[str]:
        """
        Get the manifestation status.

        :return:
        """

    @manifestation_status.setter
    @abc.abstractmethod
    def manifestation_status(self, manifestation_status: Optional[str]) -> None:
        """
        Set the manifestation status.

        :param manifestation_status:
        :return:
        """

    @property
    @abc.abstractmethod
    def manifestation_flags(self) -> Optional[str]:
        """
        Get the manifestation flags.

        :return:
        """

    @manifestation_flags.setter
    @abc.abstractmethod
    def manifestation_flags(self, manifestation_flags: Optional[str]) -> None:
        """
        Set the manifestation flags, in the form of a string.

        :param manifestation_flags:
        :return:
        """

    @property
    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """
        Return this metadata object as a mapping.

        :return:
        """

    def __str__(self) -> str:
        """
        Render this object as a string.

        :return:
        """
        return f"{self.__class__.__name__}()"


class ManifestationIdentityAPI(ManifestationIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """Marker ABC for a concrete manifestation identity container."""

__all__ = ["ManifestationIdentityPropertiesAPI", "ManifestationIdentityAPI"]
