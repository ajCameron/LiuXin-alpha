"""
Concrete rich work metadata bundle.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.works_container_api import (
    WorkMetadataAPI,
    WorkRelationLink,
    WorkStorageHints, WorkIdentityAPI,
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
        self._relation_links[relation_key] = list(links)

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
                        "type": link.type,
                        "origin": link.origin,
                        "policy": link.policy,
                        "data": link.data,
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
                        type=raw_link.get("type"),
                        origin=raw_link.get("origin"),
                        policy=raw_link.get("policy"),
                        data=raw_link.get("data"),
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

    @classmethod
    def _rowish_to_mapping(cls, value: Any) -> Mapping[str, Any]:
        if value is None:
            return {}
        if isinstance(value, Row):
            return value.row_dict
        if hasattr(value, "to_mapping") and callable(value.to_mapping):
            return value.to_mapping()
        if isinstance(value, Mapping):
            return value
        return {}

    @staticmethod
    def _value_from_mapping(mapping: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
        for key in keys:
            value = mapping.get(key)
            if value not in (None, ""):
                return value
        return None

    @classmethod
    def _display_value(cls, value: Any) -> Optional[str]:
        mapping = cls._rowish_to_mapping(value)
        if mapping:
            candidates = (
                "agent_canonical_name",
                "agent_sort_name",
                "work_canonical_title",
                "work_title",
                "expression_title_override",
                "expression_label",
                "manifestation_format_detail",
                "manifestation_carrier_type",
                "series",
                "series_name",
                "genre",
                "subject",
                "tag",
                "label",
                "language",
                "language_name",
                "language_code",
                "folder_name",
                "folder_relpath",
                "file_name",
                "image_name",
                "identifier_value",
                "note",
                "comment",
                "synopsis",
                "rating_label",
            )
            display = cls._value_from_mapping(mapping, candidates)
            if display not in (None, ""):
                return str(display)
            for key, item in mapping.items():
                key_text = str(key)
                if key_text.endswith("_id") or key_text.endswith("_timestamp_ep_k"):
                    continue
                if item not in (None, ""):
                    return str(item)
        if value in (None, ""):
            return None
        return str(value)

    @classmethod
    def _link_display_values(
        cls,
        links: Iterable[WorkRelationLink],
        *,
        unique: bool = True,
    ) -> tuple[str, ...]:
        values: list[str] = []
        seen: set[str] = set()
        for link in links:
            display = cls._display_value(link.target)
            if not display:
                continue
            if unique and display in seen:
                continue
            seen.add(display)
            values.append(display)
        return tuple(values)

    @classmethod
    def _manifestation_types_from_links(
        cls,
        links: Iterable[WorkRelationLink],
    ) -> tuple[str, ...]:
        values: list[str] = []
        seen: set[str] = set()
        for link in links:
            mapping = cls._rowish_to_mapping(link.target)
            raw = cls._value_from_mapping(
                mapping,
                (
                    "manifestation_carrier_type",
                    "manifestation_type",
                    "manifestation_binding_type",
                ),
            )
            if raw in (None, ""):
                continue
            text = str(raw)
            if text in seen:
                continue
            seen.add(text)
            values.append(text)
        return tuple(values)

    @classmethod
    def _format_candidates_from_links(cls, links: Iterable[WorkRelationLink]) -> tuple[str, ...]:
        values: list[str] = []
        seen: set[str] = set()
        for link in links:
            mapping = cls._rowish_to_mapping(link.target)
            for key in (
                "manifestation_format_detail",
                "file_extension",
                "image_extension",
            ):
                raw = mapping.get(key)
                if raw in (None, ""):
                    continue
                token = str(raw).strip().lower()
                if token in seen:
                    continue
                seen.add(token)
                values.append(token.upper())
        return tuple(values)

    @classmethod
    def _preferred_filename_stem(
        cls,
        title: Optional[str],
        primary_agents: tuple[str, ...],
    ) -> Optional[str]:
        if title and primary_agents:
            return "{} - {}".format(title, " & ".join(primary_agents))
        if title:
            return title
        return None

    def storage_hints(self) -> WorkStorageHints:
        work_map = self.work.to_mapping() if self.work is not None else {}
        expression_links = self.get_relation_links("expressions")
        manifestation_links = self.get_relation_links("manifestations")
        file_links = self.get_relation_links("files")
        image_links = self.get_relation_links("images")
        agent_links = self.get_relation_links("agents")

        title = self._value_from_mapping(work_map, ("work_canonical_title", "work_title"))
        if title in (None, "") and expression_links:
            title = self._display_value(expression_links[0].target)

        canonical_title = self._value_from_mapping(work_map, ("work_canonical_title", "work_title"))
        sort_title = self._value_from_mapping(
            work_map,
            ("work_sort_title", "work_canonical_title", "work_title"),
        )

        primary_agents = self._link_display_values(agent_links)
        series = self._link_display_values(self.get_relation_links("series"))
        genres = self._link_display_values(self.get_relation_links("genres"))
        subjects = self._link_display_values(self.get_relation_links("subjects"))
        languages = self._link_display_values(self.get_relation_links("languages"))
        labels = self._link_display_values(self.get_relation_links("labels"))
        manifestation_types = self._manifestation_types_from_links(manifestation_links)

        file_formats = []
        for token in self._format_candidates_from_links(
            manifestation_links + file_links + image_links
        ):
            if token not in file_formats:
                file_formats.append(token)

        preferred_folder_tokens: list[str] = []
        if primary_agents:
            preferred_folder_tokens.extend(primary_agents)
        elif series:
            preferred_folder_tokens.extend(series)
        if title not in (None, ""):
            preferred_folder_tokens.append(str(title))

        preferred_filename_stem = self._preferred_filename_stem(
            None if title in (None, "") else str(title),
            primary_agents,
        )

        extra = {
            "expression_count": len(expression_links),
            "manifestation_count": len(manifestation_links),
            "item_count": len(self.get_relation_links("items")),
            "file_count": len(file_links),
            "image_count": len(image_links),
            "identifier_count": len(self.get_relation_links("identifiers")),
        }

        return WorkStorageHints(
            work_id=self._value_from_mapping(work_map, ("work_id",)),
            title=None if title in (None, "") else str(title),
            canonical_title=None if canonical_title in (None, "") else str(canonical_title),
            sort_title=None if sort_title in (None, "") else str(sort_title),
            work_type=self._value_from_mapping(work_map, ("work_type",)),
            medium=self._value_from_mapping(work_map, ("work_medium",)),
            primary_agents=primary_agents,
            series=series,
            genres=genres,
            subjects=subjects,
            languages=languages,
            labels=labels,
            manifestation_types=manifestation_types,
            file_formats=tuple(file_formats),
            preferred_folder_tokens=tuple(preferred_folder_tokens),
            preferred_filename_stem=preferred_filename_stem,
            extra=extra,
        )


__all__ = ["WorkMetadata"]
