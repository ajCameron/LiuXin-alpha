"""Core WEMI identity API contract for manifestation entities.

Category: core WEMI identity object.
This module defines the smallest stable API for the manifestation entity itself,
not the editable metadata bundle and not a read-side query result.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import Any, ClassVar, Iterable, Mapping, Optional, Self


class ManifestationIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """Row-level API for one manifestation."""

    @property
    def id(self) -> Optional[int]:
        return self.manifestation_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.manifestation_id = value

    @property
    @abc.abstractmethod
    def manifestation_id(self) -> Optional[int]:
        raise NotImplementedError

    @manifestation_id.setter
    @abc.abstractmethod
    def manifestation_id(self, manifestation_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def expression_id(self) -> Optional[int]:
        return self.manifestation_expression_id

    @expression_id.setter
    def expression_id(self, value: Optional[int]) -> None:
        self.manifestation_expression_id = value

    @property
    @abc.abstractmethod
    def manifestation_expression_id(self) -> Optional[int]:
        raise NotImplementedError

    @manifestation_expression_id.setter
    @abc.abstractmethod
    def manifestation_expression_id(self, manifestation_expression_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_format_detail(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_format_detail.setter
    @abc.abstractmethod
    def manifestation_format_detail(self, manifestation_format_detail: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_carrier_type(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_carrier_type.setter
    @abc.abstractmethod
    def manifestation_carrier_type(self, manifestation_carrier_type: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_edition_statement(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_edition_statement.setter
    @abc.abstractmethod
    def manifestation_edition_statement(self, manifestation_edition_statement: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_pub_year(self) -> Optional[int]:
        raise NotImplementedError

    @manifestation_pub_year.setter
    @abc.abstractmethod
    def manifestation_pub_year(self, manifestation_pub_year: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_status(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_status.setter
    @abc.abstractmethod
    def manifestation_status(self, manifestation_status: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def manifestation_flags(self) -> Optional[str]:
        raise NotImplementedError

    @manifestation_flags.setter
    @abc.abstractmethod
    def manifestation_flags(self, manifestation_flags: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def to_mapping(self) -> Any:
        raise NotImplementedError


class ManifestationIdentityAPI(ManifestationIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """Marker ABC for a concrete manifestation identity container."""

__all__ = ["ManifestationIdentityPropertiesAPI", "ManifestationIdentityAPI"]
