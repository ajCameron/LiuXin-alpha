"""Core WEMI manifestation metadata-bundle implementation containers.

Category: core WEMI metadata bundle.
This module implements the editable metadata surface around a manifestation, not
the manifestation identity object and not a read-side query result.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping

from LiuXin_alpha.databases.row import Row
from typing import Any, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
    ManifestationMetadataAPI,
    ManifestationRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    metadata_bundle_string,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import ManifestationIdentity


class ManifestationMetadata(ManifestationMetadataAPI):
    def __init__(self, *, manifestation: Optional[ManifestationIdentityAPI] = None, relation_links: Optional[Mapping[str, Iterable[ManifestationRelationLink]]] = None) -> None:
        self._manifestation = manifestation
        self._relation_links: dict[str, list[ManifestationRelationLink]] = {relation: [] for relation in self.RELATION_KEYS}
        if relation_links:
            for relation, links in relation_links.items():
                self.set_relation_links(relation, links)

    @property
    def manifestation(self) -> Optional[ManifestationIdentityAPI]:
        return self._manifestation

    @manifestation.setter
    def manifestation(self, value: Optional[ManifestationIdentityAPI]) -> None:
        self._manifestation = value

    def get_relation_links(self, relation: str) -> list[ManifestationRelationLink]:
        return self._relation_links[self.validate_relation_name(relation)]

    def set_relation_links(self, relation: str, links: Iterable[ManifestationRelationLink]) -> None:
        relation_key = self.validate_relation_name(relation)
        self._relation_links[relation_key] = self.validate_relation_links(relation_key, links)

    def __str__(self) -> str:
        return metadata_bundle_string(
            self,
            identity_name="manifestation",
            relation_names=self.RELATION_KEYS,
            get_links=self.get_relation_links,
        )

    @staticmethod
    def _serialize_target(target: Any) -> Any:
        if hasattr(target, 'to_mapping') and callable(target.to_mapping):
            return target.to_mapping()
        if isinstance(target, Mapping):
            return dict(target)
        return target

    @staticmethod
    def _deserialize_target(target: Any) -> Any:
        if isinstance(target, Mapping) and ('manifestation_id' in target or 'manifestation_expression_id' in target):
            return ManifestationIdentity.from_mapping(target)
        if isinstance(target, Mapping):
            return dict(target)
        return target

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        payload = {'manifestation': self.manifestation.to_mapping() if self.manifestation is not None else None}
        if include_related:
            payload['relations'] = {
                relation: [
                    {
                        'target': self._serialize_target(link.target),
                        'priority': link.priority,
                        'primary': link.primary,
                        'type': link.type,
                        'origin': link.origin,
                        'source': link.source,
                        'policy': link.policy,
                        'data': link.data,
                        'index': link.index,
                        'edge_id': link.edge_id,
                        'cardinality': (
                            link.cardinality.value
                            if link.cardinality is not None
                            else None
                        ),
                        'extra': dict(link.extra),
                    }
                    for link in self.get_relation_links(relation)
                ]
                for relation in self.RELATION_KEYS
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> 'ManifestationMetadata':
        manifestation_payload = payload.get('manifestation')
        manifestation = manifestation_payload if isinstance(manifestation_payload, ManifestationIdentityAPI) else (ManifestationIdentity.from_mapping(manifestation_payload) if isinstance(manifestation_payload, Mapping) else None)
        relation_payload = payload.get('relations') or {}
        relation_links: dict[str, list[ManifestationRelationLink]] = {relation: [] for relation in cls.RELATION_KEYS}
        for relation in cls.RELATION_KEYS:
            for raw_link in relation_payload.get(relation, []):
                if isinstance(raw_link, ManifestationRelationLink):
                    relation_links[relation].append(raw_link)
                elif isinstance(raw_link, Mapping):
                    relation_links[relation].append(ManifestationRelationLink(
                        target=cls._deserialize_target(raw_link.get('target')),
                        priority=raw_link.get('priority'),
                        primary=raw_link.get('primary'),
                        type=raw_link.get('type'),
                        origin=raw_link.get('origin'),
                        source=raw_link.get('source'),
                        policy=raw_link.get('policy'),
                        data=raw_link.get('data'),
                        index=raw_link.get('index'),
                        edge_id=raw_link.get('edge_id'),
                        cardinality=raw_link.get('cardinality'),
                        extra=dict(raw_link.get('extra') or {}),
                    ))
        return cls(manifestation=manifestation, relation_links=relation_links)


    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        manifestation_id: Optional[int] = None,
        source_row: Optional[Mapping[str, Any] | Row] = None,
    ) -> "ManifestationMetadata":
        from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_hydrator import (
            ManifestationMetadataHydrator,
        )

        hydrator = ManifestationMetadataHydrator(database)
        if manifestation_id is not None:
            return hydrator.from_manifestation_id(int(manifestation_id))
        if source_row is not None:
            return hydrator.from_source_row(source_row)
        raise ValueError("Provide either manifestation_id or source_row.")

__all__ = ["ManifestationMetadata"]
