"""Core WEMI identity API contract for expression entities.

Category: core WEMI identity object.
This module defines the smallest stable API for the expression entity itself,
not the editable metadata bundle and not a read-side query result.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import Any, ClassVar, Iterable, Mapping, Optional, Self


class ExpressionIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """Row-level API for one expression."""

    @property
    def id(self) -> Optional[int]:
        return self.expression_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.expression_id = value

    @property
    @abc.abstractmethod
    def expression_id(self) -> Optional[int]:
        raise NotImplementedError

    @expression_id.setter
    @abc.abstractmethod
    def expression_id(self, expression_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def work_id(self) -> Optional[int]:
        return self.expression_work_id

    @work_id.setter
    def work_id(self, value: Optional[int]) -> None:
        self.expression_work_id = value

    @property
    @abc.abstractmethod
    def expression_work_id(self) -> Optional[int]:
        raise NotImplementedError

    @expression_work_id.setter
    @abc.abstractmethod
    def expression_work_id(self, expression_work_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_type(self) -> Optional[str]:
        raise NotImplementedError

    @expression_type.setter
    @abc.abstractmethod
    def expression_type(self, expression_type: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_language_id(self) -> Optional[int]:
        raise NotImplementedError

    @expression_language_id.setter
    @abc.abstractmethod
    def expression_language_id(self, expression_language_id: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_label(self) -> Optional[str]:
        raise NotImplementedError

    @expression_label.setter
    @abc.abstractmethod
    def expression_label(self, expression_label: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_title_override(self) -> Optional[str]:
        raise NotImplementedError

    @expression_title_override.setter
    @abc.abstractmethod
    def expression_title_override(self, expression_title_override: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_subtitle(self) -> Optional[str]:
        raise NotImplementedError

    @expression_subtitle.setter
    @abc.abstractmethod
    def expression_subtitle(self, expression_subtitle: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_flags(self) -> Optional[str]:
        raise NotImplementedError

    @expression_flags.setter
    @abc.abstractmethod
    def expression_flags(self, expression_flags: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def expression_status(self) -> Optional[str]:
        raise NotImplementedError

    @expression_status.setter
    @abc.abstractmethod
    def expression_status(self, expression_status: Optional[str]) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def to_mapping(self) -> Any:
        raise NotImplementedError


class ExpressionIdentityAPI(ExpressionIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """Marker ABC for a concrete expression identity container."""

__all__ = ["ExpressionIdentityPropertiesAPI", "ExpressionIdentityAPI"]
