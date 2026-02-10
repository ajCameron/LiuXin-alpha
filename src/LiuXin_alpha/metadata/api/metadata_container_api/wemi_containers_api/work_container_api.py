
import abc
from abc import abstractmethod

from typing import Optional, Mapping, Any, Self, Dict


class WorkContainerPropertiesApi(metaclass=abc.ABCMeta):
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
        raise NotImplementedError

    @work_id.setter
    @abstractmethod
    def work_id(self, work_id: Optional[int]) -> None:
        raise NotImplementedError

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
        raise NotImplementedError

    @work_type.setter
    @abstractmethod
    def work_type(self, work_type: Optional[str]) -> None:
        """
        Validate and set the work type.

        :param work_type:
        :return:
        """
        raise NotImplementedError

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
        raise NotImplementedError

    @work_medium.setter
    @abstractmethod
    def work_medium(self, work_medium: Optional[str]) -> None:
        raise NotImplementedError

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
        raise NotImplementedError

    @work_title.setter
    @abstractmethod
    def work_title(self, work_title: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def name(self) -> Optional[str]:
        return self.work_name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        self.work_name = value

    @property
    @abstractmethod
    def work_name(self) -> Optional[str]:
        raise NotImplementedError

    @work_name.setter
    @abstractmethod
    def work_name(self, work_name: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def canonical_title(self) -> Optional[str]:
        return self.work_canonical_title

    @canonical_title.setter
    def canonical_title(self, value: Optional[str]) -> None:
        self.work_canonical_title = value

    @property
    @abstractmethod
    def work_canonical_title(self) -> Optional[str]:
        raise NotImplementedError

    @work_canonical_title.setter
    @abstractmethod
    def work_canonical_title(self, work_canonical_title: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def sort_title(self) -> Optional[str]:
        return self.work_sort_title

    @sort_title.setter
    def sort_title(self, value: Optional[str]) -> None:
        self.work_sort_title = value

    @property
    @abstractmethod
    def work_sort_title(self) -> Optional[str]:
        raise NotImplementedError

    @work_sort_title.setter
    @abstractmethod
    def work_sort_title(self, work_sort_title: Optional[str]) -> None:
        raise NotImplementedError

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
        raise NotImplementedError

    @work_is_fiction.setter
    @abstractmethod
    def work_is_fiction(self, work_is_fiction: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def audience(self) -> Optional[str]:
        return self.work_audience

    @audience.setter
    def audience(self, value: Optional[str]) -> None:
        self.work_audience = value

    @property
    @abstractmethod
    def work_audience(self) -> Optional[str]:
        raise NotImplementedError

    @work_audience.setter
    @abstractmethod
    def work_audience(self, work_audience: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def completion_status(self) -> Optional[str]:
        return self.work_completion_status

    @completion_status.setter
    def completion_status(self, value: Optional[str]) -> None:
        self.work_completion_status = value

    @property
    @abstractmethod
    def work_completion_status(self) -> Optional[str]:
        raise NotImplementedError

    @work_completion_status.setter
    @abstractmethod
    def work_completion_status(self, work_completion_status: Optional[str]) -> None:
        raise NotImplementedError

    @property
    def original_language_id(self) -> Optional[int]:
        return self.work_original_language_id

    @original_language_id.setter
    def original_language_id(self, value: Optional[int]) -> None:
        self.work_original_language_id = value

    @property
    @abstractmethod
    def work_original_language_id(self) -> Optional[int]:
        raise NotImplementedError

    @work_original_language_id.setter
    @abstractmethod
    def work_original_language_id(self, work_original_language_id: Optional[int]) -> None:
        raise NotImplementedError

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
        raise NotImplementedError

    @work_discovery_note.setter
    @abstractmethod
    def work_discovery_note(self, work_discovery_note: Optional[str]) -> None:
        raise NotImplementedError

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
        raise NotImplementedError

    @work_created_timestamp_ep_k.setter
    @abstractmethod
    def work_created_timestamp_ep_k(self, work_created_timestamp_ep_k: Optional[int]) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def work_modified_timestamp_ep_k(self) -> Optional[int]:
        raise NotImplementedError

    @work_modified_timestamp_ep_k.setter
    @abstractmethod
    def work_modified_timestamp_ep_k(self, work_modified_timestamp_ep_k: Optional[int]) -> None:
        raise NotImplementedError

    @property
    def original_year(self) -> Optional[int]:
        return self.work_original_year

    @original_year.setter
    def original_year(self, value: Optional[int]) -> None:
        self.work_original_year = value

    @property
    @abstractmethod
    def work_original_year(self) -> Optional[int]:
        raise NotImplementedError

    @work_original_year.setter
    @abstractmethod
    def work_original_year(self, work_original_year: Optional[int]) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Mapping helpers
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> Self:
        raise NotImplementedError

    @abstractmethod
    def to_mapping(self) -> Dict[str, Any]:
        raise NotImplementedError




