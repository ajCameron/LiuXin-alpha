"""Core WEMI work metadata-bundle implementation containers.

Category: core WEMI metadata bundle.
This module implements the editable metadata surface around a work, not the work
identity object and not a read-side query result.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_containers.work_metadata_api import (
    WorkMetadataAPI,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import (
    WorkIdentity,
)


class WorkMetadata(WorkMetadataAPI):
    """
    Concrete implementation of :class:`WorkMetadataAPI`.

    Targets in relation links are usually live database :class:`Row` objects,
    but plain mappings are also supported for round-tripping/tests.
    """

    def __init__(
        self,
        *,
        work: Optional[WorkIdentityAPI] = None,
        relation_links: Optional[Mapping[str, Iterable[WorkRelationLink]]] = None,
    ) -> None:
        self._work = work
        self._relation_links: dict[str, list[WorkRelationLink]] = {
            relation: [] for relation in self.RELATION_KEYS
        }
        if relation_links:
            for relation, links in relation_links.items():
                self.set_relation_links(relation, links)

    @property
    def work(self) -> Optional[WorkIdentityAPI]:
        return self._work

    @work.setter
    def work(self, value: Optional[WorkIdentityAPI]) -> None:
        self._work = value

    def get_relation_links(self, relation: str) -> list[WorkRelationLink]:
        relation_key = self.validate_relation_name(relation)
        return self._relation_links[relation_key]

    def set_relation_links(self, relation: str, links: Iterable[WorkRelationLink]) -> None:
        relation_key = self.validate_relation_name(relation)
        self._relation_links[relation_key] = self.validate_relation_links(relation_key, links)

    @staticmethod
    def _serialize_target(target: Any) -> Any:
        if target is None:
            return None
        if isinstance(target, Row):
            return dict(target.row_dict)
        to_mapping = getattr(target, "to_mapping", None)
        if callable(to_mapping):
            return to_mapping()
        if isinstance(target, Mapping):
            return dict(target)
        return target

    @staticmethod
    def _deserialize_target(target: Any) -> Any:
        if isinstance(target, Mapping):
            if "work_id" in target or "work_title" in target or "work_canonical_title" in target:
                return WorkIdentity.from_mapping(target)
            return dict(target)
        return target

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "work": self.work.to_mapping() if self.work is not None else None,
        }
        if include_related:
            payload["relations"] = {
                relation: [
                    {
                        "target": self._serialize_target(link.target),
                        "priority": link.priority,
                        "primary": link.primary,
                        "type": link.type,
                        "origin": link.origin,
                        "source": link.source,
                        "policy": link.policy,
                        "data": link.data,
                        "index": link.index,
                        "edge_id": link.edge_id,
                        "cardinality": (
                            link.cardinality.value
                            if link.cardinality is not None
                            else None
                        ),
                        "extra": dict(link.extra),
                    }
                    for link in self.get_relation_links(relation)
                ]
                for relation in self.RELATION_KEYS
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "WorkMetadata":
        work_payload = payload.get("work")
        work: Optional[WorkIdentityAPI]
        if isinstance(work_payload, WorkIdentityAPI):
            work = work_payload
        elif isinstance(work_payload, Mapping):
            work = WorkIdentity.from_mapping(work_payload)
        else:
            work = None

        relation_payload = payload.get("relations") or {}
        relation_links: dict[str, list[WorkRelationLink]] = {}
        for relation in cls.RELATION_KEYS:
            relation_links[relation] = []
            for raw_link in relation_payload.get(relation, []):
                if isinstance(raw_link, WorkRelationLink):
                    relation_links[relation].append(raw_link)
                    continue
                if not isinstance(raw_link, Mapping):
                    continue
                relation_links[relation].append(
                    WorkRelationLink(
                        target=cls._deserialize_target(raw_link.get("target")),
                        priority=raw_link.get("priority"),
                        primary=raw_link.get("primary"),
                        type=raw_link.get("type"),
                        origin=raw_link.get("origin"),
                        source=raw_link.get("source"),
                        policy=raw_link.get("policy"),
                        data=raw_link.get("data"),
                        index=raw_link.get("index"),
                        edge_id=raw_link.get("edge_id"),
                        cardinality=raw_link.get("cardinality"),
                        extra=dict(raw_link.get("extra") or {}),
                    )
                )
        return cls(work=work, relation_links=relation_links)

    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        work_id: Optional[int] = None,
        source_row: Optional[Mapping[str, Any] | Row] = None,
    ) -> "WorkMetadata":
        from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_hydrator import (
            WorkMetadataHydrator,
        )

        hydrator = WorkMetadataHydrator(database)
        if work_id is not None:
            return hydrator.from_work_id(int(work_id))
        if source_row is not None:
            return hydrator.from_source_row(source_row)
        raise ValueError("Provide either work_id or source_row.")

__all__ = ["WorkMetadata"]
