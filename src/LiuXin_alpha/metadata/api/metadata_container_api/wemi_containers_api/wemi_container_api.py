
"""
LiuXin FRBR: abstract APIs for the `works` table containers.

These ABCs exist purely for typing / contracts.

Concrete implementations (e.g. :class:`~work_container.WorkContainer`) should
inherit from these interfaces.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from typing import Any, Dict, Iterable, Iterator, Mapping, MutableSequence, Optional, Self

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_container_api import WorkContainerPropertiesApi




class WorkContainerAPI(WorkContainerPropertiesApi):
    """
    Abstract API for a single row from the `works` table.

    Provides a full interface to a work row.
    """

    # ------------------------------------------------------------------
    # WEMI stack helpers
    # ------------------------------------------------------------------
    @abstractmethod
    def expressions(self) -> "ExpressionsCollectionAPI":
        """
        Returns the Expressions linked to this Work.

        :return:
        """


class WorksCollectionAPI(ABC):
    """
    Abstract API for a collection of :class:`WorksContainerAPI` rows.

    A collection of rows.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[WorkContainerAPI]:
        """
        Iterate through all the :class:`WorksContainerAPI` rows.

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def __len__(self) -> int:
        """
        How many works are present in the collection.

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, idx: int) -> WorkContainerAPI:
        """
        Return Work at the given index.

        :param idx:
        :return:
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def works(self) -> MutableSequence[WorkContainerAPI]:
        """
        Returns all the

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def add(self, work: WorkContainerAPI) -> None:
        raise NotImplementedError

    @abstractmethod
    def extend(self, works: Iterable[WorkContainerAPI]) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_by_id(self, work_id: int) -> Optional[WorkContainerAPI]:
        raise NotImplementedError

    @abstractmethod
    def remove_by_id(self, work_id: int) -> bool:
        raise NotImplementedError


class ExpressionContainerAPI(ABC):
    """
    Container for an Expression attached to a row.
    """

    @property
    @abstractmethod
    def work(self) -> Optional[WorkContainerAPI]:
        """
        Every Expression must belong to, at most, one Work.

        :return:
        """

    @work.setter
    @abstractmethod
    def work(self, value: Optional[WorkContainerAPI]) -> None:
        """
        Validate and set the Expression for the work.

        :param value:
        :return:
        """

class WorkExpressionsCollectionAPI(ABC):
    """
    Return a collection of Expressions linked to a single work.
    """
    @property
    @abstractmethod
    def work(self) -> Optional[WorkContainerAPI]:
        """
        Every Expression must belong to, at most, one Work.

        :return:
        """

    @work.setter
    @abstractmethod
    def work(self, value: Optional[WorkContainerAPI]) -> None:
        """
        Validate and set the Expression for the work.

        :param value:
        :return:
        """

    @abstractmethod
    def add_expression(self, new_work: ExpressionContainerAPI, sync_now: bool = False) -> None:
        """
        Add a new expression to the collection.

        :param new_work:
        :param sync_now:
        :return:
        """


class ExpressionsCollectionAPI(ABC):
    """
    Return a collection of Expressions which might not be linked to a single work.
    """
    @property
    @abstractmethod
    def works(self) -> Iterable[WorkContainerAPI]:
        """
        Every Expression must belong to, at most, one Work.

        :return:
        """









