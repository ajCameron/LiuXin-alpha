
"""
LiuXin FRBR: abstract APIs for the `works` table containers.

These ABCs exist purely for typing / contracts.

Concrete implementations (e.g. :class:`~work_container.WorkContainer`) should
inherit from these interfaces.
"""

from __future__ import annotations

import abc
from abc import ABC, abstractmethod

from typing import Any, Dict, Iterable, Iterator, Mapping, MutableSequence, Optional, Self

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import WorkContainerPropertiesApi


class WorkContainerAPI(WorkContainerPropertiesApi):
    """
    Abstract API for a single row from the `works` table.

    Provides a full interface to a work row.
    The Work sits at the top of the WEMI stack.
    As such, it's purely fan down from here.
    """

    # ------------------------------------------------------------------
    # WEMI stack helpers
    # ------------------------------------------------------------------
    @abstractmethod
    def expressions(self) -> "ExpressionsCollectionContainerAPI":
        """
        Returns the Expressions linked to this Work.

        :return:
        """

    @abstractmethod
    def manifestations(self) -> "ManifestationsCollectionContainerAPI":
        """
        Returns all the Manifestations linked to this work through Expressions.

        :return:
        """

    @abstractmethod
    def items(self) -> "ItemsCollectionContainerAPI":
        """
        Returns all the Items linked to this work through Manifestations and Expressions.

        :return:
        """

    @abstractmethod
    def files(self) -> "FilesCollectionContainerAPI":
        """
        Returns all the Files linked to this Work's items.

        :return:
        """


class WorksCollectionCollectionAPI(ABC):
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
        Returns all the works in this collection.

        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def add(self, work: WorkContainerAPI) -> None:
        """
        Add a work to the collection.

        :param work:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def extend(self, works: Iterable[WorkContainerAPI]) -> None:
        """
        Extend the collection of works.

        :param works:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def get_work_by_id(self, work_id: int) -> Optional[WorkContainerAPI]:
        """
        Return a work by its ID.

        :param work_id:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def remove_work_by_id(self, work_id: int) -> bool:
        """
        Remove a work by its ID.

        :param work_id:
        :return:
        """
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


class ExpressionsCollectionContainerAPI(ABC):
    """
    Return a collection of Expressions which might not be linked to a single work.
    """
    @property
    @abstractmethod
    def works(self) -> MutableSequence[WorkContainerAPI]:
        """
        Returns all the works in this collection.

        :return:
        """
        raise NotImplementedError

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


    @abstractmethod
    def add(self, work: WorkContainerAPI) -> None:
        """
        Add a work to the collection.

        :param work:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def extend(self, new_expression: Iterable[ExpressionContainerAPI]) -> None:
        """
        Extend the collection of works.

        :param works:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def get_work_by_id(self, work_id: int) -> Optional[WorkContainerAPI]:
        """
        Return a work by its ID.

        :param work_id:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def remove_work_by_id(self, work_id: int) -> bool:
        """
        Remove a work by its ID.

        :param work_id:
        :return:
        """
        raise NotImplementedError


class ManifestationContainerAPI(abc.ABC):
    """
    Container for a Manifestation attached to an Expression attached to an Expression row.

    A Manifestation is part of the WEMI chain.
    This container allows you to request resources from up and down the

    """
    @property
    @abstractmethod
    def works(self) -> Optional["WorksCollectionCollectionAPI"]:
        """
        Manifestations can manifest multiple expressions which can, in turn, be expressions of multiple works.

        :return:
        """

    @property
    @abstractmethod
    def expressions(self) -> Optional["ExpressionsCollectionContainerAPI"]:
        """


        :return:
        """


class ManifestationsCollectionContainerAPI(ABC):
    """
    Collection of Manifestations.
    """











