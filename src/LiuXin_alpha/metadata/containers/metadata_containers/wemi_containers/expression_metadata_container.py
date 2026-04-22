"""Concrete rich metadata bundle for one expression."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Optional

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.expressions_container_api import (
    ExpressionIdentityAPI,
    ExpressionMetadataAPI,
    ExpressionRelationLink,
    ExpressionStorageHints,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity


class ExpressionMetadata(ExpressionMetadataAPI):
    def __init__(self, *, expression: Optional[ExpressionIdentityAPI] = None, relation_links: Optional[Mapping[str, Iterable[ExpressionRelationLink]]] = None) -> None:
        self._expression = expression
        self._relation_links: dict[str, list[ExpressionRelationLink]] = {relation: [] for relation in self.RELATION_KEYS}
        if relation_links:
            for relation, links in relation_links.items():
                self.set_relation_links(relation, links)

    @property
    def expression(self) -> Optional[ExpressionIdentityAPI]:
        return self._expression

    @expression.setter
    def expression(self, value: Optional[ExpressionIdentityAPI]) -> None:
        self._expression = value

    def get_relation_links(self, relation: str) -> list[ExpressionRelationLink]:
        return self._relation_links[self.validate_relation_name(relation)]

    def set_relation_links(self, relation: str, links: Iterable[ExpressionRelationLink]) -> None:
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
        if isinstance(target, Mapping) and ('expression_id' in target or 'expression_work_id' in target):
            return ExpressionIdentity.from_mapping(target)
        if isinstance(target, Mapping):
            return dict(target)
        return target

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        payload = {'expression': self.expression.to_mapping() if self.expression is not None else None}
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
    def from_mapping(cls, payload: Mapping[str, Any]) -> 'ExpressionMetadata':
        expression_payload = payload.get('expression')
        expression = expression_payload if isinstance(expression_payload, ExpressionIdentityAPI) else (ExpressionIdentity.from_mapping(expression_payload) if isinstance(expression_payload, Mapping) else None)
        relation_payload = payload.get('relations') or {}
        relation_links: dict[str, list[ExpressionRelationLink]] = {relation: [] for relation in cls.RELATION_KEYS}
        for relation in cls.RELATION_KEYS:
            for raw_link in relation_payload.get(relation, []):
                if isinstance(raw_link, ExpressionRelationLink):
                    relation_links[relation].append(raw_link)
                elif isinstance(raw_link, Mapping):
                    relation_links[relation].append(ExpressionRelationLink(
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
        return cls(expression=expression, relation_links=relation_links)

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
        for key in ('agent_canonical_name', 'work_canonical_title', 'work_title', 'expression_title_override', 'expression_label', 'label', 'genre', 'identifier_value', 'language_name', 'language_code', 'note', 'comment'):
            found = mapping.get(key)
            if found not in (None, ''):
                return str(found)
        for key, item in mapping.items():
            if str(key).endswith('_id') or str(key).endswith('_timestamp_ep_k'):
                continue
            if item not in (None, ''):
                return str(item)
        return None

    def storage_hints(self) -> ExpressionStorageHints:
        primary_agents = tuple(filter(None, (self._display_value(link.target) for link in self.get_relation_links('agents') if link.primary or len(self.get_relation_links('agents')) == 1)))
        genres = tuple(filter(None, (self._display_value(link.target) for link in self.get_relation_links('genres'))))
        labels = tuple(filter(None, (self._display_value(link.target) for link in self.get_relation_links('labels'))))
        identifiers = tuple(filter(None, (self._display_value(link.target) for link in self.get_relation_links('identifiers'))))
        language_code = None
        language_links = self.get_relation_links('languages')
        if language_links:
            language_code = self._display_value(language_links[0].target)
        return ExpressionStorageHints(
            expression_id=self.expression.expression_id if self.expression is not None else None,
            work_id=self.expression.expression_work_id if self.expression is not None else None,
            title=self.expression.expression_title_override if self.expression is not None else None,
            label=self.expression.expression_label if self.expression is not None else None,
            expression_type=self.expression.expression_type if self.expression is not None else None,
            language_code=language_code,
            primary_agents=primary_agents,
            genres=genres,
            labels=labels,
            identifiers=identifiers,
        )


__all__ = ["ExpressionMetadata"]
