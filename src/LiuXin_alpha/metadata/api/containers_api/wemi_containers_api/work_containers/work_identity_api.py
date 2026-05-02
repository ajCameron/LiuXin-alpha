"""Core WEMI identity API contract for work entities.

Category: core WEMI identity object.
This module defines the smallest stable API for the work entity itself, not the
editable metadata bundle around that entity and not any read-side query view.
"""
from __future__ import annotations

import abc
import dataclasses
from abc import abstractmethod

from typing import ClassVar, Iterable, Mapping, Optional, Self

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
)

class WorkIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """
    Provides a full interface to the properties of a work row.
    """
    # ------------------------------------------------------------------
    # Primary key
    # ------------------------------------------------------------------

    @property
    def id(self) -> Optional[int]:
        """
        id for the underlying works row.

        :return:
        """
        return self.work_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Set the id for the underlying works row.

        :param value:
        :return:
        """
        self.work_id = value

    @property
    @abstractmethod
    def work_id(self) -> Optional[int]:
        """
        Proxy for the work_id.

        :return:
        """
        ...

    @work_id.setter
    @abstractmethod
    def work_id(self, work_id: Optional[int]) -> None:
        ...

    # ------------------------------------------------------------------
    # Core work identity
    # ------------------------------------------------------------------

    @property
    def type(self) -> Optional[str]:
        """
        Return the type of the underlying works row.

        :return:
        """
        return self.work_type

    @type.setter
    def type(self, value: Optional[str]) -> None:
        """
        Validate and set the type of the underlying works row.

        :param value:
        :return:
        """
        self.work_type = value

    @property
    @abstractmethod
    def work_type(self) -> Optional[str]:
        """
        Return the type of the work type.

        :return:
        """
        ...

    @work_type.setter
    @abstractmethod
    def work_type(self, work_type: Optional[str]) -> None:
        """
        Validate and set the work type.

        :param work_type:
        :return:
        """
        ...

    @property
    def medium(self) -> Optional[str]:
        """
        Set the medium of the underlying works row.

        :return:
        """
        return self.work_medium

    @medium.setter
    def medium(self, value: Optional[str]) -> None:
        self.work_medium = value

    @property
    @abstractmethod
    def work_medium(self) -> Optional[str]:
        """
        Porxy to set the medium type of the underlying work.

        :return:
        """
        ...

    @work_medium.setter
    @abstractmethod
    def work_medium(self, work_medium: Optional[str]) -> None:
        ...

    # - Title methods

    @property
    def title(self) -> Optional[str]:
        """
        The raw title of the work - alias of work_title.

        :return:
        """
        return self.work_title

    @title.setter
    def title(self, value: Optional[str]) -> None:

        self.work_title = value

    @property
    @abstractmethod
    def work_title(self) -> Optional[str]:
        ...

    @work_title.setter
    @abstractmethod
    def work_title(self, work_title: Optional[str]) -> None:
        ...

    @property
    def name(self) -> Optional[str]:
        return self.work_name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        self.work_name = value

    @property
    @abstractmethod
    def work_name(self) -> Optional[str]:
        ...

    @work_name.setter
    @abstractmethod
    def work_name(self, work_name: Optional[str]) -> None:
        ...

    @property
    def canonical_title(self) -> Optional[str]:
        return self.work_canonical_title

    @canonical_title.setter
    def canonical_title(self, value: Optional[str]) -> None:
        self.work_canonical_title = value

    @property
    @abstractmethod
    def work_canonical_title(self) -> Optional[str]:
        ...

    @work_canonical_title.setter
    @abstractmethod
    def work_canonical_title(self, work_canonical_title: Optional[str]) -> None:
        ...

    @property
    def sort_title(self) -> Optional[str]:
        return self.work_sort_title

    @sort_title.setter
    def sort_title(self, value: Optional[str]) -> None:
        self.work_sort_title = value

    @property
    @abstractmethod
    def work_sort_title(self) -> Optional[str]:
        ...

    @work_sort_title.setter
    @abstractmethod
    def work_sort_title(self, work_sort_title: Optional[str]) -> None:
        ...

    # ------------------------------------------------------------------
    # High-level classification
    # ------------------------------------------------------------------

    @property
    def is_fiction(self) -> Optional[int]:
        return self.work_is_fiction

    @is_fiction.setter
    def is_fiction(self, value: Optional[int]) -> None:
        self.work_is_fiction = value

    @property
    @abstractmethod
    def work_is_fiction(self) -> Optional[int]:
        """Stored as SQLite-ish 1/0/NULL."""
        ...

    @work_is_fiction.setter
    @abstractmethod
    def work_is_fiction(self, work_is_fiction: Optional[int]) -> None:
        ...

    @property
    def audience(self) -> Optional[str]:
        return self.work_audience

    @audience.setter
    def audience(self, value: Optional[str]) -> None:
        self.work_audience = value

    @property
    @abstractmethod
    def work_audience(self) -> Optional[str]:
        ...

    @work_audience.setter
    @abstractmethod
    def work_audience(self, work_audience: Optional[str]) -> None:
        ...

    @property
    def completion_status(self) -> Optional[str]:
        return self.work_completion_status

    @completion_status.setter
    def completion_status(self, value: Optional[str]) -> None:
        self.work_completion_status = value

    @property
    @abstractmethod
    def work_completion_status(self) -> Optional[str]:
        ...

    @work_completion_status.setter
    @abstractmethod
    def work_completion_status(self, work_completion_status: Optional[str]) -> None:
        ...

    @property
    def original_language_id(self) -> Optional[int]:
        return self.work_original_language_id

    @original_language_id.setter
    def original_language_id(self, value: Optional[int]) -> None:
        self.work_original_language_id = value

    @property
    @abstractmethod
    def work_original_language_id(self) -> Optional[int]:
        ...

    @work_original_language_id.setter
    @abstractmethod
    def work_original_language_id(self, work_original_language_id: Optional[int]) -> None:
        ...

    # ------------------------------------------------------------------
    # Notes / provenance
    # ------------------------------------------------------------------

    @property
    def discovery_note(self) -> Optional[str]:
        return self.work_discovery_note

    @discovery_note.setter
    def discovery_note(self, value: Optional[str]) -> None:
        self.work_discovery_note = value

    @property
    @abstractmethod
    def work_discovery_note(self) -> Optional[str]:
        ...

    @work_discovery_note.setter
    @abstractmethod
    def work_discovery_note(self, work_discovery_note: Optional[str]) -> None:
        ...

    # ------------------------------------------------------------------
    # Timestamps
    # ------------------------------------------------------------------

    @property
    def created_timestamp_ep_k(self) -> Optional[int]:
        return self.work_created_timestamp_ep_k

    @created_timestamp_ep_k.setter
    def created_timestamp_ep_k(self, value: Optional[int]) -> None:
        self.work_created_timestamp_ep_k = value

    @property
    def modified_timestamp_ep_k(self) -> Optional[int]:
        return self.work_modified_timestamp_ep_k

    @modified_timestamp_ep_k.setter
    def modified_timestamp_ep_k(self, value: Optional[int]) -> None:
        self.work_modified_timestamp_ep_k = value

    @property
    @abstractmethod
    def work_created_timestamp_ep_k(self) -> Optional[int]:
        ...

    @work_created_timestamp_ep_k.setter
    @abstractmethod
    def work_created_timestamp_ep_k(self, work_created_timestamp_ep_k: Optional[int]) -> None:
        ...

    @property
    @abstractmethod
    def work_modified_timestamp_ep_k(self) -> Optional[int]:
        ...

    @work_modified_timestamp_ep_k.setter
    @abstractmethod
    def work_modified_timestamp_ep_k(self, work_modified_timestamp_ep_k: Optional[int]) -> None:
        ...

    @property
    def original_year(self) -> Optional[int]:
        return self.work_original_year

    @original_year.setter
    def original_year(self, value: Optional[int]) -> None:
        self.work_original_year = value

    @property
    @abstractmethod
    def work_original_year(self) -> Optional[int]:
        ...

    @work_original_year.setter
    @abstractmethod
    def work_original_year(self, work_original_year: Optional[int]) -> None:
        ...

    # ------------------------------------------------------------------
    # Scratch / misc
    # ------------------------------------------------------------------

    @property
    def scratch(self) -> Optional[str]:
        return self.work_scratch

    @scratch.setter
    def scratch(self, value: Optional[str]) -> None:
        self.work_scratch = value

    @property
    @abstractmethod
    def work_scratch(self) -> Optional[str]:
        ...

    @work_scratch.setter
    @abstractmethod
    def work_scratch(self, work_scratch: Optional[str]) -> None:
        ...

    # ------------------------------------------------------------------
    # Mapping helpers
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def from_mapping(cls, row: MetadataRecord) -> Self:
        ...

    @abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        ...

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class WorkIdentityAPI(WorkIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """Typing interface for a Work container.

    This exists so container implementations can inherit a single ABC with the
    full property + mapping surface.
    """

    pass


# Todo: This feels like it could be a generic thing

__all__ = ["WorkIdentityPropertiesAPI", "WorkIdentityAPI"]
