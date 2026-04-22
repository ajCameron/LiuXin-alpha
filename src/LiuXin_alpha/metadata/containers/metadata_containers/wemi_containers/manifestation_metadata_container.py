"""Concrete rich metadata bundle for one manifestation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Optional

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestations_container_api import (
    ManifestationIdentityAPI,
    ManifestationMetadataAPI,
    ManifestationRelationLink,
    ManifestationStorageHints,
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
        self._relation_links[self.validate_relation_name(relation)] = list(links)

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
                        'policy': link.policy,
                        'data': link.data,
                        'index': link.index,
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
                        policy=raw_link.get('policy'),
                        data=raw_link.get('data'),
                        index=raw_link.get('index'),
                        extra=dict(raw_link.get('extra') or {}),
                    ))
        return cls(manifestation=manifestation, relation_links=relation_links)

    @staticmethod
    def _display_value(value: Any) -> Optional[str]:
        if value is None:
            return None
        if hasattr(value, 'to_mapping') and callable(value.to_mapping):
            mapping = value.to_mapping()
        elif isinstance(value, Mapping):
            mapping = value
        else:
            return str(value) if value not in (None, '') else None
        for key in ('agent_canonical_name', 'work_canonical_title', 'work_title', 'expression_title_override', 'expression_label', 'manifestation_edition_statement', 'manifestation_format_detail', 'identifier_value', 'file_name', 'language_name', 'language_code', 'note', 'comment'):
            found = mapping.get(key)
            if found not in (None, ''):
                return str(found)
        for key, item in mapping.items():
            if str(key).endswith('_id') or str(key).endswith('_timestamp_ep_k'):
                continue
            if item not in (None, ''):
                return str(item)
        return None

    def storage_hints(self) -> ManifestationStorageHints:
        primary_agents = tuple(filter(None, (self._display_value(link.target) for link in self.get_relation_links('agents') if link.primary or len(self.get_relation_links('agents')) == 1)))
        identifiers = tuple(filter(None, (self._display_value(link.target) for link in self.get_relation_links('identifiers'))))
        file_formats = tuple(filter(None, (self._display_value(link.target) for link in self.get_relation_links('files'))))
        title = None
        title_links = self.get_relation_links('titles')
        if title_links:
            title = self._display_value(title_links[0].target)
        return ManifestationStorageHints(
            manifestation_id=self.manifestation.manifestation_id if self.manifestation is not None else None,
            expression_id=self.manifestation.manifestation_expression_id if self.manifestation is not None else None,
            title=title,
            edition_statement=self.manifestation.manifestation_edition_statement if self.manifestation is not None else None,
            format_detail=self.manifestation.manifestation_format_detail if self.manifestation is not None else None,
            carrier_type=self.manifestation.manifestation_carrier_type if self.manifestation is not None else None,
            publication_year=self.manifestation.manifestation_pub_year if self.manifestation is not None else None,
            primary_agents=primary_agents,
            identifiers=identifiers,
            file_formats=file_formats,
        )


__all__ = ["ManifestationMetadata"]
