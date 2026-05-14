"""Core WEMI expression metadata-bundle implementation containers.

Category: core WEMI metadata bundle.
This module implements the editable metadata surface around an expression, not
the expression identity object and not a read-side query result.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping

from LiuXin_alpha.databases.row import Row
from typing import Any, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import (
    ExpressionMetadataAPI,
    ExpressionRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    metadata_bundle_string,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity


class ExpressionMetadata(ExpressionMetadataAPI):
    def __init__(self, *, expression: Optional[ExpressionIdentityAPI] = None, relation_links: Optional[Mapping[str, Iterable[ExpressionRelationLink]]] = None) -> None:
        self._expression = expression
        self._relation_links: dict[str, list[ExpressionRelationLink]] = {relation_key: [] for relation_key in self.RELATION_KEYS}
        if relation_links:
            for relation_key, links in relation_links.items():
                self.set_relation_links(relation_key, links)

    @property
    def expression(self) -> Optional[ExpressionIdentityAPI]:
        return self._expression

    @expression.setter
    def expression(self, value: Optional[ExpressionIdentityAPI]) -> None:
        self._expression = value

    @staticmethod
    def _optional_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _work_id_from_target(cls, target: Any) -> Optional[int]:
        if isinstance(target, Row):
            if target.table == "works":
                return cls._optional_int(target.row_id)
            return cls._optional_int(target.row_dict.get("work_id"))
        if isinstance(target, Mapping):
            return cls._optional_int(
                target.get("work_id")
                or target.get("id")
                or target.get("row_id")
            )
        work_id = getattr(target, "work_id", None)
        if work_id is not None:
            return cls._optional_int(work_id)
        row_dict = getattr(target, "row_dict", None)
        if isinstance(row_dict, Mapping):
            return cls._optional_int(row_dict.get("work_id"))
        return None

    @property
    def expression_work_id(self) -> Optional[int]:
        if self.expression is None:
            return None
        return self.expression.expression_work_id

    @expression_work_id.setter
    def expression_work_id(self, expression_work_id: Optional[int]) -> None:
        if self.expression is None:
            if expression_work_id is None:
                return
            self.expression = ExpressionIdentity(expression_work_id=expression_work_id)
            return
        self.expression.expression_work_id = expression_work_id

    @property
    def work_ids(self) -> Optional[Iterable[int]]:
        ids: list[int] = []
        primary_id = self.expression_work_id
        if primary_id is not None:
            ids.append(primary_id)
        for target in self.works:
            work_id = self._work_id_from_target(target)
            if work_id is not None and work_id not in ids:
                ids.append(work_id)
        if not ids:
            return None
        return tuple(ids)

    @work_ids.setter
    def work_ids(self, work_ids: Optional[Iterable[int]]) -> None:
        ids = tuple(
            work_id
            for value in (work_ids or ())
            if (work_id := self._optional_int(value)) is not None
        )
        self.expression_work_id = ids[0] if ids else None
        self.set_related(
            "works",
            [{"work_id": work_id} for work_id in ids],
        )

    def get_relation_links(self, relation_key: str) -> list[ExpressionRelationLink]:
        return self._relation_links[self.validate_relation_name(relation_key)]

    def set_relation_links(self, relation_key: str, links: Iterable[ExpressionRelationLink]) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self._relation_links[relation_key] = self.validate_relation_links(relation_key, links)

    def __str__(self) -> str:
        return metadata_bundle_string(
            self,
            identity_name="expression",
            relation_names=self.RELATION_KEYS,
            get_links=self.get_relation_links,
        )

    def write_to_database(
        self,
        database: Any,
        *,
        fields: Iterable[str] | None = None,
        item_id: int | None = None,
        target_row: Row | Mapping[str, Any] | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> Any:
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_writer import (
            LiuXinWEMIMetadataWriter,
        )

        return LiuXinWEMIMetadataWriter(database).write(
            self,
            fields=fields,
            target_level="expression",
            item_id=item_id,
            target_row=target_row,
            replace=replace,
            mark_dirty=mark_dirty,
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
        if isinstance(target, Mapping) and ('expression_id' in target or 'expression_work_id' in target):
            return ExpressionIdentity.from_mapping(target)
        if isinstance(target, Mapping):
            return dict(target)
        return target

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        payload = {'expression': self.expression.to_mapping() if self.expression is not None else None}
        if include_related:
            payload['relations'] = {
                relation_key: [
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
                    for link in self.get_relation_links(relation_key)
                ]
                for relation_key in self.RELATION_KEYS
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> 'ExpressionMetadata':
        expression_payload = payload.get('expression')
        expression = expression_payload if isinstance(expression_payload, ExpressionIdentityAPI) else (ExpressionIdentity.from_mapping(expression_payload) if isinstance(expression_payload, Mapping) else None)
        relation_payload = payload.get('relations') or {}
        relation_links: dict[str, list[ExpressionRelationLink]] = {relation_key: [] for relation_key in cls.RELATION_KEYS}
        for relation_key in cls.RELATION_KEYS:
            for raw_link in relation_payload.get(relation_key, []):
                if isinstance(raw_link, ExpressionRelationLink):
                    relation_links[relation_key].append(raw_link)
                elif isinstance(raw_link, Mapping):
                    relation_links[relation_key].append(ExpressionRelationLink(
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
        return cls(expression=expression, relation_links=relation_links)


    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        expression_id: Optional[int] = None,
        source_row: Optional[Mapping[str, Any] | Row] = None,
    ) -> "ExpressionMetadata":
        from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_hydrator import (
            ExpressionMetadataHydrator,
        )

        hydrator = ExpressionMetadataHydrator(database)
        if expression_id is not None:
            return hydrator.from_expression_id(int(expression_id))
        if source_row is not None:
            return hydrator.from_source_row(source_row)
        raise ValueError("Provide either expression_id or source_row.")

__all__ = ["ExpressionMetadata"]
