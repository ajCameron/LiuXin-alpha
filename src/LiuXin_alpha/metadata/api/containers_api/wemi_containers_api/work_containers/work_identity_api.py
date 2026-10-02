"""
Define canonical work identity fields and their short compatibility aliases.

The identity represents one work row; relation bundles and query projections use
separate contracts.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
    >>> work = WorkIdentity(work_id=3, work_title='Example')
    >>> work.WEMI_LEVEL
    'work'
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
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.identity_api import (
    WemiIdentityAPI,
)

class WorkIdentityPropertiesAPI(WemiIdentityAPI, metaclass=abc.ABCMeta):
    """
    Require canonical work-prefixed properties, short aliases and mapping conversion for one work.

    Aliases delegate directly. Concrete identities decide id guards, fiction-flag
    conversion and value ownership.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
        >>> work = WorkIdentity(work_id=3, work_title='Example')
        >>> work.id, work.title
        (3, 'Example')
    """
    WEMI_LEVEL: ClassVar[str] = "work"
    SOURCE_TABLE: ClassVar[str] = "works"
    ID_FIELD: ClassVar[str] = "work_id"

    # ------------------------------------------------------------------
    # Primary key
    # ------------------------------------------------------------------

    @property
    def id(self) -> Optional[int]:
        """
        Return work_id through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_id=3)
            >>> work.id
            3


        :return: Current work id, or None.
        """
        return self.work_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Assign work_id through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.id = 3
            >>> work.work_id
            3


        :param value: New work id, or None.
        :return: None.
        """
        self.work_id = value

    @property
    @abstractmethod
    def work_id(self) -> Optional[int]:
        """
        Return work_id through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_id=3)
            >>> work.work_id
            3


        :return: Current work id, or None.
        """
        ...

    @work_id.setter
    @abstractmethod
    def work_id(self, work_id: Optional[int]) -> None:
        """
        Assign work_id through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_id = 3
            >>> work.work_id
            3


        :param work_id: New work id, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # Core work identity
    # ------------------------------------------------------------------

    @property
    def type(self) -> Optional[str]:
        """
        Return work_type through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_type='novel')
            >>> work.type
            'novel'


        :return: Current work type, or None.
        """
        return self.work_type

    @type.setter
    def type(self, value: Optional[str]) -> None:
        """
        Assign work_type through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.type = 'novel'
            >>> work.work_type
            'novel'


        :param value: New work type, or None.
        :return: None.
        """
        self.work_type = value

    @property
    @abstractmethod
    def work_type(self) -> Optional[str]:
        """
        Return work_type through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_type='novel')
            >>> work.work_type
            'novel'


        :return: Current work type, or None.
        """
        ...

    @work_type.setter
    @abstractmethod
    def work_type(self, work_type: Optional[str]) -> None:
        """
        Assign work_type through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_type = 'novel'
            >>> work.work_type
            'novel'


        :param work_type: New work type, or None.
        :return: None.
        """
        ...

    @property
    def medium(self) -> Optional[str]:
        """
        Return work_medium through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_medium='text')
            >>> work.medium
            'text'


        :return: Current work medium, or None.
        """
        return self.work_medium

    @medium.setter
    def medium(self, value: Optional[str]) -> None:
        """
        Assign work_medium through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.medium = 'text'
            >>> work.work_medium
            'text'


        :param value: New work medium, or None.
        :return: None.
        """
        self.work_medium = value

    @property
    @abstractmethod
    def work_medium(self) -> Optional[str]:
        """
        Return work_medium through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_medium='text')
            >>> work.work_medium
            'text'


        :return: Current work medium, or None.
        """
        ...

    @work_medium.setter
    @abstractmethod
    def work_medium(self, work_medium: Optional[str]) -> None:
        """
        Assign work_medium through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_medium = 'text'
            >>> work.work_medium
            'text'


        :param work_medium: New work medium, or None.
        :return: None.
        """
        ...

    # - Title methods

    @property
    def title(self) -> Optional[str]:
        """
        Return work_title through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_title='Example')
            >>> work.title
            'Example'


        :return: Current work title, or None.
        """
        return self.work_title

    @title.setter
    def title(self, value: Optional[str]) -> None:

        """
        Assign work_title through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.title = 'Example'
            >>> work.work_title
            'Example'


        :param value: New work title, or None.
        :return: None.
        """
        self.work_title = value

    @property
    @abstractmethod
    def work_title(self) -> Optional[str]:
        """
        Return work_title through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_title='Example')
            >>> work.work_title
            'Example'


        :return: Current work title, or None.
        """
        ...

    @work_title.setter
    @abstractmethod
    def work_title(self, work_title: Optional[str]) -> None:
        """
        Assign work_title through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_title = 'Example'
            >>> work.work_title
            'Example'


        :param work_title: New work title, or None.
        :return: None.
        """
        ...

    @property
    def name(self) -> Optional[str]:
        """
        Return work_name through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_title='Example name')
            >>> work.name
            'Example name'


        :return: Current work name, or None.
        """
        return self.work_name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        """
        Assign work_name through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.name = 'Example name'
            >>> work.work_name
            'Example name'


        :param value: New work name, or None.
        :return: None.
        """
        self.work_name = value

    @property
    @abstractmethod
    def work_name(self) -> Optional[str]:
        """
        Return work_name through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_title='Example name')
            >>> work.work_name
            'Example name'


        :return: Current work name, or None.
        """
        ...

    @work_name.setter
    @abstractmethod
    def work_name(self, work_name: Optional[str]) -> None:
        """
        Assign work_name through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_name = 'Example name'
            >>> work.work_name
            'Example name'


        :param work_name: New work name, or None.
        :return: None.
        """
        ...

    @property
    def canonical_title(self) -> Optional[str]:
        """
        Return work_canonical_title through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_canonical_title='Example, The')
            >>> work.canonical_title
            'Example, The'


        :return: Current work canonical title, or None.
        """
        return self.work_canonical_title

    @canonical_title.setter
    def canonical_title(self, value: Optional[str]) -> None:
        """
        Assign work_canonical_title through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.canonical_title = 'Example, The'
            >>> work.work_canonical_title
            'Example, The'


        :param value: New work canonical title, or None.
        :return: None.
        """
        self.work_canonical_title = value

    @property
    @abstractmethod
    def work_canonical_title(self) -> Optional[str]:
        """
        Return work_canonical_title through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_canonical_title='Example, The')
            >>> work.work_canonical_title
            'Example, The'


        :return: Current work canonical title, or None.
        """
        ...

    @work_canonical_title.setter
    @abstractmethod
    def work_canonical_title(self, work_canonical_title: Optional[str]) -> None:
        """
        Assign work_canonical_title through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_canonical_title = 'Example, The'
            >>> work.work_canonical_title
            'Example, The'


        :param work_canonical_title: New work canonical title, or None.
        :return: None.
        """
        ...

    @property
    def sort_title(self) -> Optional[str]:
        """
        Return work_sort_title through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_sort_title='Example')
            >>> work.sort_title
            'Example'


        :return: Current work sort title, or None.
        """
        return self.work_sort_title

    @sort_title.setter
    def sort_title(self, value: Optional[str]) -> None:
        """
        Assign work_sort_title through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.sort_title = 'Example'
            >>> work.work_sort_title
            'Example'


        :param value: New work sort title, or None.
        :return: None.
        """
        self.work_sort_title = value

    @property
    @abstractmethod
    def work_sort_title(self) -> Optional[str]:
        """
        Return work_sort_title through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_sort_title='Example')
            >>> work.work_sort_title
            'Example'


        :return: Current work sort title, or None.
        """
        ...

    @work_sort_title.setter
    @abstractmethod
    def work_sort_title(self, work_sort_title: Optional[str]) -> None:
        """
        Assign work_sort_title through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_sort_title = 'Example'
            >>> work.work_sort_title
            'Example'


        :param work_sort_title: New work sort title, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # High-level classification
    # ------------------------------------------------------------------

    @property
    def is_fiction(self) -> Optional[int]:
        """
        Return work_is_fiction through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_is_fiction=True)
            >>> work.is_fiction
            True


        :return: Current work is fiction, or None.
        """
        return self.work_is_fiction

    @is_fiction.setter
    def is_fiction(self, value: Optional[int]) -> None:
        """
        Assign work_is_fiction through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.is_fiction = True
            >>> work.work_is_fiction
            True


        :param value: New work is fiction, or None.
        :return: None.
        """
        self.work_is_fiction = value

    @property
    @abstractmethod
    def work_is_fiction(self) -> Optional[int]:
        """
        Return work_is_fiction through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_is_fiction=True)
            >>> work.work_is_fiction
            True


        :return: Current work is fiction, or None.
        """
        ...

    @work_is_fiction.setter
    @abstractmethod
    def work_is_fiction(self, work_is_fiction: Optional[int]) -> None:
        """
        Assign work_is_fiction through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_is_fiction = True
            >>> work.work_is_fiction
            True


        :param work_is_fiction: New work is fiction, or None.
        :return: None.
        """
        ...

    @property
    def audience(self) -> Optional[str]:
        """
        Return work_audience through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_audience='adult')
            >>> work.audience
            'adult'


        :return: Current work audience, or None.
        """
        return self.work_audience

    @audience.setter
    def audience(self, value: Optional[str]) -> None:
        """
        Assign work_audience through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.audience = 'adult'
            >>> work.work_audience
            'adult'


        :param value: New work audience, or None.
        :return: None.
        """
        self.work_audience = value

    @property
    @abstractmethod
    def work_audience(self) -> Optional[str]:
        """
        Return work_audience through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_audience='adult')
            >>> work.work_audience
            'adult'


        :return: Current work audience, or None.
        """
        ...

    @work_audience.setter
    @abstractmethod
    def work_audience(self, work_audience: Optional[str]) -> None:
        """
        Assign work_audience through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_audience = 'adult'
            >>> work.work_audience
            'adult'


        :param work_audience: New work audience, or None.
        :return: None.
        """
        ...

    @property
    def completion_status(self) -> Optional[str]:
        """
        Return work_completion_status through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_completion_status='complete')
            >>> work.completion_status
            'complete'


        :return: Current work completion status, or None.
        """
        return self.work_completion_status

    @completion_status.setter
    def completion_status(self, value: Optional[str]) -> None:
        """
        Assign work_completion_status through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.completion_status = 'complete'
            >>> work.work_completion_status
            'complete'


        :param value: New work completion status, or None.
        :return: None.
        """
        self.work_completion_status = value

    @property
    @abstractmethod
    def work_completion_status(self) -> Optional[str]:
        """
        Return work_completion_status through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_completion_status='complete')
            >>> work.work_completion_status
            'complete'


        :return: Current work completion status, or None.
        """
        ...

    @work_completion_status.setter
    @abstractmethod
    def work_completion_status(self, work_completion_status: Optional[str]) -> None:
        """
        Assign work_completion_status through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_completion_status = 'complete'
            >>> work.work_completion_status
            'complete'


        :param work_completion_status: New work completion status, or None.
        :return: None.
        """
        ...

    @property
    def original_language_id(self) -> Optional[int]:
        """
        Return work_original_language_id through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_original_language_id=2)
            >>> work.original_language_id
            2


        :return: Current work original language id, or None.
        """
        return self.work_original_language_id

    @original_language_id.setter
    def original_language_id(self, value: Optional[int]) -> None:
        """
        Assign work_original_language_id through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.original_language_id = 2
            >>> work.work_original_language_id
            2


        :param value: New work original language id, or None.
        :return: None.
        """
        self.work_original_language_id = value

    @property
    @abstractmethod
    def work_original_language_id(self) -> Optional[int]:
        """
        Return work_original_language_id through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_original_language_id=2)
            >>> work.work_original_language_id
            2


        :return: Current work original language id, or None.
        """
        ...

    @work_original_language_id.setter
    @abstractmethod
    def work_original_language_id(self, work_original_language_id: Optional[int]) -> None:
        """
        Assign work_original_language_id through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_original_language_id = 2
            >>> work.work_original_language_id
            2


        :param work_original_language_id: New work original language id, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # Notes / provenance
    # ------------------------------------------------------------------

    @property
    def discovery_note(self) -> Optional[str]:
        """
        Return work_discovery_note through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_discovery_note='Imported')
            >>> work.discovery_note
            'Imported'


        :return: Current work discovery note, or None.
        """
        return self.work_discovery_note

    @discovery_note.setter
    def discovery_note(self, value: Optional[str]) -> None:
        """
        Assign work_discovery_note through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.discovery_note = 'Imported'
            >>> work.work_discovery_note
            'Imported'


        :param value: New work discovery note, or None.
        :return: None.
        """
        self.work_discovery_note = value

    @property
    @abstractmethod
    def work_discovery_note(self) -> Optional[str]:
        """
        Return work_discovery_note through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_discovery_note='Imported')
            >>> work.work_discovery_note
            'Imported'


        :return: Current work discovery note, or None.
        """
        ...

    @work_discovery_note.setter
    @abstractmethod
    def work_discovery_note(self, work_discovery_note: Optional[str]) -> None:
        """
        Assign work_discovery_note through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_discovery_note = 'Imported'
            >>> work.work_discovery_note
            'Imported'


        :param work_discovery_note: New work discovery note, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # Timestamps
    # ------------------------------------------------------------------

    @property
    def created_timestamp_ep_k(self) -> Optional[int]:
        """
        Return work_created_timestamp_ep_k through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_created_timestamp_ep_k=10)
            >>> work.created_timestamp_ep_k
            10


        :return: Current work created timestamp ep k, or None.
        """
        return self.work_created_timestamp_ep_k

    @created_timestamp_ep_k.setter
    def created_timestamp_ep_k(self, value: Optional[int]) -> None:
        """
        Assign work_created_timestamp_ep_k through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.created_timestamp_ep_k = 10
            >>> work.work_created_timestamp_ep_k
            10


        :param value: New work created timestamp ep k, or None.
        :return: None.
        """
        self.work_created_timestamp_ep_k = value

    @property
    def modified_timestamp_ep_k(self) -> Optional[int]:
        """
        Return work_modified_timestamp_ep_k through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_modified_timestamp_ep_k=11)
            >>> work.modified_timestamp_ep_k
            11


        :return: Current work modified timestamp ep k, or None.
        """
        return self.work_modified_timestamp_ep_k

    @modified_timestamp_ep_k.setter
    def modified_timestamp_ep_k(self, value: Optional[int]) -> None:
        """
        Assign work_modified_timestamp_ep_k through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.modified_timestamp_ep_k = 11
            >>> work.work_modified_timestamp_ep_k
            11


        :param value: New work modified timestamp ep k, or None.
        :return: None.
        """
        self.work_modified_timestamp_ep_k = value

    @property
    @abstractmethod
    def work_created_timestamp_ep_k(self) -> Optional[int]:
        """
        Return work_created_timestamp_ep_k through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_created_timestamp_ep_k=10)
            >>> work.work_created_timestamp_ep_k
            10


        :return: Current work created timestamp ep k, or None.
        """
        ...

    @work_created_timestamp_ep_k.setter
    @abstractmethod
    def work_created_timestamp_ep_k(self, work_created_timestamp_ep_k: Optional[int]) -> None:
        """
        Assign work_created_timestamp_ep_k through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_created_timestamp_ep_k = 10
            >>> work.work_created_timestamp_ep_k
            10


        :param work_created_timestamp_ep_k: New work created timestamp ep k, or None.
        :return: None.
        """
        ...

    @property
    @abstractmethod
    def work_modified_timestamp_ep_k(self) -> Optional[int]:
        """
        Return work_modified_timestamp_ep_k through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_modified_timestamp_ep_k=11)
            >>> work.work_modified_timestamp_ep_k
            11


        :return: Current work modified timestamp ep k, or None.
        """
        ...

    @work_modified_timestamp_ep_k.setter
    @abstractmethod
    def work_modified_timestamp_ep_k(self, work_modified_timestamp_ep_k: Optional[int]) -> None:
        """
        Assign work_modified_timestamp_ep_k through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_modified_timestamp_ep_k = 11
            >>> work.work_modified_timestamp_ep_k
            11


        :param work_modified_timestamp_ep_k: New work modified timestamp ep k, or None.
        :return: None.
        """
        ...

    @property
    def original_year(self) -> Optional[int]:
        """
        Return work_original_year through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_original_year=2026)
            >>> work.original_year
            2026


        :return: Current work original year, or None.
        """
        return self.work_original_year

    @original_year.setter
    def original_year(self, value: Optional[int]) -> None:
        """
        Assign work_original_year through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.original_year = 2026
            >>> work.work_original_year
            2026


        :param value: New work original year, or None.
        :return: None.
        """
        self.work_original_year = value

    @property
    @abstractmethod
    def work_original_year(self) -> Optional[int]:
        """
        Return work_original_year through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_original_year=2026)
            >>> work.work_original_year
            2026


        :return: Current work original year, or None.
        """
        ...

    @work_original_year.setter
    @abstractmethod
    def work_original_year(self, work_original_year: Optional[int]) -> None:
        """
        Assign work_original_year through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_original_year = 2026
            >>> work.work_original_year
            2026


        :param work_original_year: New work original year, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # Scratch / misc
    # ------------------------------------------------------------------

    @property
    def scratch(self) -> Optional[str]:
        """
        Return work_scratch through the short compatibility alias.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_scratch='raw')
            >>> work.scratch
            'raw'


        :return: Current work scratch, or None.
        """
        return self.work_scratch

    @scratch.setter
    def scratch(self, value: Optional[str]) -> None:
        """
        Assign work_scratch through the short compatibility alias under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.scratch = 'raw'
            >>> work.work_scratch
            'raw'


        :param value: New work scratch, or None.
        :return: None.
        """
        self.work_scratch = value

    @property
    @abstractmethod
    def work_scratch(self) -> Optional[str]:
        """
        Return work_scratch through the canonical work property.

        The contract performs no lookup or fallback beyond direct alias delegation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_scratch='raw')
            >>> work.work_scratch
            'raw'


        :return: Current work scratch, or None.
        """
        ...

    @work_scratch.setter
    @abstractmethod
    def work_scratch(self, work_scratch: Optional[str]) -> None:
        """
        Assign work_scratch through the canonical work property under concrete identity policy.

        The concrete fiction setter may normalize bool-like values, and a stored non-None id
        may be write-once.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity()
            >>> work.work_scratch = 'raw'
            >>> work.work_scratch
            'raw'


        :param work_scratch: New work scratch, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # Mapping helpers
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def from_mapping(cls, row: MetadataRecord) -> Self:
        """
        Require construction from canonical work-column keys.

        Concrete identities define recognized keys, normalization and handling of missing
        values.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity.from_mapping({'work_id': 3, 'work_title': 'Example'})
            >>> work.work_id
            3


        :param row: Mapping containing canonical work fields.
        :return: New identity of the requested class.
        """
        ...

    @abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """
        Require serialization to canonical work-column keys.

        Concrete identities define fiction encoding, included fields and copy depth.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> work.to_mapping()['work_id']
            3


        :return: Mutable metadata record describing the work.
        """
        ...

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when the concrete identity does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_id=3, work_title='Example')
            >>> WorkIdentityPropertiesAPI.__str__(work)
            'WorkIdentity()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"


class WorkIdentityAPI(WorkIdentityPropertiesAPI, metaclass=abc.ABCMeta):
    """
    Mark a concrete work identity that provides every row-level property and mapping operation.

    The marker adds no behavior and provides a stable public annotation boundary.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
        >>> work = WorkIdentity(work_id=3, work_title='Example')
        >>> isinstance(work, WorkIdentityAPI)
        True
    """

    pass


__all__ = ["WorkIdentityPropertiesAPI", "WorkIdentityAPI"]
