"""Collection-oriented WEMI helper APIs.

These APIs are intentionally small. Row-level identity and rich metadata APIs
live in the sibling modules:
- works_container_api
- expressions_container_api
- manifestations_container_api
- items_container_api
"""

from __future__ import annotations

import abc
from typing import Iterator, Iterable, MutableSequence, Optional

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.works_container_api import WorkIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.expressions_container_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestations_container_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.items_container_api import ItemIdentityAPI


class WorksCollectionAPI(abc.ABC):
    @abc.abstractmethod
    def __iter__(self) -> Iterator[WorkIdentityAPI]:
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def __getitem__(self, idx: int) -> WorkIdentityAPI:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def works(self) -> MutableSequence[WorkIdentityAPI]:
        raise NotImplementedError


class ExpressionsCollectionAPI(abc.ABC):
    @abc.abstractmethod
    def __iter__(self) -> Iterator[ExpressionIdentityAPI]:
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def __getitem__(self, idx: int) -> ExpressionIdentityAPI:
        raise NotImplementedError


class ManifestationsCollectionAPI(abc.ABC):
    @abc.abstractmethod
    def __iter__(self) -> Iterator[ManifestationIdentityAPI]:
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def __getitem__(self, idx: int) -> ManifestationIdentityAPI:
        raise NotImplementedError


class ItemsCollectionAPI(abc.ABC):
    @abc.abstractmethod
    def __iter__(self) -> Iterator[ItemIdentityAPI]:
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def __getitem__(self, idx: int) -> ItemIdentityAPI:
        raise NotImplementedError


class FilesCollectionAPI(abc.ABC):
    @abc.abstractmethod
    def __iter__(self) -> Iterator[object]:
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError


__all__ = [
    "WorksCollectionAPI",
    "ExpressionsCollectionAPI",
    "ManifestationsCollectionAPI",
    "ItemsCollectionAPI",
    "FilesCollectionAPI",
]
