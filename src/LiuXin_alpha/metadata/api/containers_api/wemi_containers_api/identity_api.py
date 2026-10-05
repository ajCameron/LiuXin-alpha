"""
Define the shared identity contract for work, expression, manifestation and item rows.

Level-specific APIs provide WEMI_LEVEL, SOURCE_TABLE and ID_FIELD metadata alongside
the common id and mapping surface.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
    >>> identity = WorkIdentity(work_id=3)
    >>> isinstance(identity, WemiIdentityAPI)
    True
"""
from __future__ import annotations

import abc
from typing import ClassVar, Self

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
)


class WemiIdentityAPI(abc.ABC):
    """
    Require an id and canonical mapping conversion for a single WEMI identity.

    Implementations define field coercion and id assignment policy. This abstract class
    does not resolve rows or manage related metadata.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
        >>> identity = WorkIdentity(work_id=3)
        >>> identity.WEMI_LEVEL, identity.SOURCE_TABLE, identity.ID_FIELD
        ('work', 'works', 'work_id')
    """

    WEMI_LEVEL: ClassVar[str]
    SOURCE_TABLE: ClassVar[str]
    ID_FIELD: ClassVar[str]

    @property
    @abc.abstractmethod
    def id(self) -> int | None:
        """
        Require the primary row id through a level-independent property.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> identity = WorkIdentity(work_id=3)
            >>> identity.id
            3


        :return: Current integer id, or None.
        """

    @id.setter
    @abc.abstractmethod
    def id(self, value: int | None) -> None:
        """
        Require assignment of the primary row id under the implementation's policy.

        Concrete identities may reject reassignment after the id has been set.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> identity = WorkIdentity(work_id=3)
            >>> fresh = WorkIdentity()
            >>> fresh.id = 7
            >>> fresh.work_id
            7


        :param value: New primary id, or None if the implementation permits it.
        :return: None.
        """

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, row: MetadataRecord) -> Self:
        """
        Require construction from a mapping keyed by database columns.

        Field recognition, defaults and coercion belong to the concrete implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> identity = WorkIdentity(work_id=3)
            >>> WorkIdentity.from_mapping({'work_id': 8}).id
            8


        :param row: Canonical metadata record describing one identity.
        :return: Identity instance of the requested class.
        """

    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """
        Require a mutable mapping representation keyed by database columns.

        Serialization and copying policy belong to the concrete implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> identity = WorkIdentity(work_id=3)
            >>> identity.to_mapping()['work_id']
            3


        :return: Dictionary representation of identity fields.
        """

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when no richer implementation overrides it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> identity = WorkIdentity(work_id=3)
            >>> WemiIdentityAPI.__str__(identity)
            'WorkIdentity()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"


__all__ = ["WemiIdentityAPI"]
