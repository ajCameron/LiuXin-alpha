"""
Implement editable relation bundles surrounding an optional work identity.

Bundles hold shared relation-link targets, expose structured and text projections,
and delegate database access to their hydrator and writer.

Example:
    >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
    >>> metadata.work.work_id
    3
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_metadata_api import (
    WorkMetadataAPI,
    WorkRelationKey,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    metadata_bundle_string,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import (
    WorkIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.projection_views import (
    MetadataTextView,
    MetadataValuesView,
)


class WorkMetadata(WorkMetadataAPI):
    """
    Collect a work identity and relation-keyed metadata links.

    Targets commonly contain live Rows, while plain mappings support serialization and
    tests. Identity and link objects remain shared. Each relation bucket has its own
    list, exposed live by get_relation_links.

    Example:
        >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
        >>> metadata.get_relation_links('identifiers')
        []
    """

    def __init__(
        self,
        *,
        work: Optional[WorkIdentityAPI] = None,
        relation_links: Optional[Mapping[str, Iterable[WorkRelationLink]]] = None,
    ) -> None:
        """
        Retain the optional work identity and initialize every supported relation bucket.

        Supplied keys are normalized and link collections validated through
        set_relation_links. Unknown relation keys raise KeyError.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> metadata.get_relation_links('identifiers')
            []


        :param work: Optional identity object retained by reference.
        :param relation_links: Optional mapping of supported relation names or aliases to
            link iterables.
        :return: None.
        """
        self._work = work
        self._relation_links: dict[WorkRelationKey, list[WorkRelationLink]] = {
            relation_key: [] for relation_key in self.RELATION_KEYS
        }
        if relation_links:
            for relation_key, links in relation_links.items():
                self.set_relation_links(relation_key, links)

    @property
    def work(self) -> Optional[WorkIdentityAPI]:
        """
        Return the linked work identity by reference.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> metadata.work.work_id
            3


        :return: Stored identity, or None.
        """
        return self._work

    @work.setter
    def work(self, value: Optional[WorkIdentityAPI]) -> None:
        """
        Replace the work identity without modifying relation buckets.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> metadata.work = None
            >>> metadata.work is None
            True


        :param value: New shared identity object, or None to unlink it.
        :return: None.
        """
        self._work = value

    @property
    def values(self) -> MetadataValuesView:
        """
        Create a structured value view backed by this metadata bundle.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> isinstance(metadata.values, MetadataValuesView), metadata.values is metadata.values
            (True, False)


        :return: New MetadataValuesView referencing this bundle.
        """
        return MetadataValuesView(self)

    @property
    def text(self) -> MetadataTextView:
        """
        Create a text view backed by a fresh value projection of this bundle.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> isinstance(metadata.text, MetadataTextView)
            True


        :return: New MetadataTextView.
        """
        return MetadataTextView(self.values)

    def get_relation_links(self, relation_key: WorkRelationKey) -> list[WorkRelationLink]:
        """
        Return the live list for a normalized relation key.

        Unknown keys raise KeyError. Mutating the returned list bypasses setter validation.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> metadata.get_relation_links('identifier') is metadata.get_relation_links('identifiers')
            True


        :param relation_key: Supported relation bucket name or alias, normalized by
            validate_relation_name.
        :return: Stored mutable list of relation links.
        """
        relation_key = self.validate_relation_name(relation_key)
        return self._relation_links[relation_key]

    def set_relation_links(self, relation_key: WorkRelationKey, links: Iterable[WorkRelationLink]) -> None:
        """
        Validate relation name and local cardinality before replacing the stored list.

        Validation materializes a new list while retaining link objects. Validation failures
        leave the existing bucket assigned.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> link = WorkRelationLink(target={'entity_identifier_value': 'local-3'})
            >>> metadata.set_relation_links('identifiers', [link])
            >>> metadata.get_relation_links('identifiers')[0] is link
            True


        :param relation_key: Supported relation bucket name or alias, normalized by
            validate_relation_name.
        :param links: Iterable of relation-link objects to validate and retain.
        :return: None.
        """
        relation_key = self.validate_relation_name(relation_key)
        self._relation_links[relation_key] = self.validate_relation_links(relation_key, links)

    def __str__(self) -> str:
        """
        Render the work identity and populated relation counts as a diagnostic summary.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> 'WorkMetadata' in str(metadata)
            True


        :return: Human-readable bundle summary.
        """
        return metadata_bundle_string(
            self,
            identity_name="work",
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
        """
        Delegate supported metadata writes to the WEMI writer at work level.

        The writer resolves targets and applies incremental changes, returning its write
        report. This wrapper adds no transaction or rollback boundary. Field selection,
        replacement and failures follow writer policy.

        Example:
            Exercise bundle write delegation with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Caller-owned database accepted by LiuXinWEMIMetadataWriter.
        :param fields: Optional field names to write; None uses the writer default
            selection.
        :param item_id: Optional item id supplied for writer target resolution.
        :param target_row: Optional Row or mapping supplied for work target resolution.
        :param replace: Use replacement semantics for supported fields when True; otherwise
            append.
        :param mark_dirty: Request best-effort dirty marking after changes when True.
        :return: Writer report describing applied, skipped and failed work.
        """
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_writer import (
            LiuXinWEMIMetadataWriter,
        )

        return LiuXinWEMIMetadataWriter(database).write(
            self,
            fields=fields,
            target_level="work",
            item_id=item_id,
            target_row=target_row,
            replace=replace,
            mark_dirty=mark_dirty,
        )

    @staticmethod
    def _serialize_target(target: Any) -> Any:
        """
        Serialize a target from Row data, a to_mapping method or a shallow mapping copy.

        A callable to_mapping takes precedence over generic Mapping handling. None and
        unsupported values are returned unchanged.

        Example:
            >>> original = {'expression_id': 2}
            >>> result = WorkMetadata._serialize_target(original)
            >>> result == original and result is not original
            True


        :param target: Relation target to serialize.
        :return: Serialized target, potentially retaining shared nested values.
        """
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
        """
        Reconstruct work identity mappings and shallow-copy other mappings.

        Presence of work_id, work_title or work_canonical_title qualifies, even for a None
        value. Nonmapping targets are retained.

        Example:
            >>> target = WorkMetadata._deserialize_target({'work_canonical_title': 'Example'})
            >>> target.work_canonical_title
            'Example'


        :param target: Target value from a serialized relation link.
        :return: New WorkIdentity, copied mapping or unchanged target.
        """
        if isinstance(target, Mapping):
            if "work_id" in target or "work_title" in target or "work_canonical_title" in target:
                return WorkIdentity.from_mapping(target)
            return dict(target)
        return target

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        """
        Serialize identity and, by default, every supported relation bucket.

        Link cardinality becomes its enum value and extra is shallow-copied. Other link
        fields retain their values; target conversion follows _serialize_target. This is not
        a deep copy or persistence operation.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> metadata.to_mapping(include_related=False)['work']['work_id']
            3
            >>> 'relations' in metadata.to_mapping(include_related=False)
            False


        :param include_related: Include every supported relation bucket and its link
            payloads when True.
        :return: New payload with work and optional relations entries.
        """
        payload: dict[str, Any] = {
            "work": self.work.to_mapping() if self.work is not None else None,
        }
        if include_related:
            payload["relations"] = {
                relation_key: [
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
                        "link_id": link.link_id,
                        "cardinality": (
                            link.cardinality.value
                            if link.cardinality is not None
                            else None
                        ),
                        "extra": dict(link.extra),
                    }
                    for link in self.get_relation_links(relation_key)
                ]
                for relation_key in self.RELATION_KEYS
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "WorkMetadata":
        """
        Reconstruct a work bundle from identity and relation payloads.

        Existing identity and link instances are retained. Link mappings produce new links
        with shallow extra copies; unknown relation keys and unsupported link entries are
        ignored. Recognized buckets undergo constructor validation.

        Example:
            >>> metadata = WorkMetadata(work=WorkIdentity(work_id=3))
            >>> restored = WorkMetadata.from_mapping(metadata.to_mapping())
            >>> restored.work.work_id
            3


        :param payload: Mapping with optional work and relations entries.
        :return: New WorkMetadata instance of the requested class.
        """
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
        for relation_key in cls.RELATION_KEYS:
            relation_links[relation_key] = []
            for raw_link in relation_payload.get(relation_key, []):
                if isinstance(raw_link, WorkRelationLink):
                    relation_links[relation_key].append(raw_link)
                    continue
                if not isinstance(raw_link, Mapping):
                    continue
                relation_links[relation_key].append(
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
                        link_id=raw_link.get("link_id"),
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
        """
        Hydrate work metadata from an explicit id or source row.

        The explicit id takes precedence. The hydrator is constructed before checking
        inputs; if neither entry point is supplied, ValueError is raised. The concrete
        hydrator result is returned even when called on a subclass.

        Example:
            Exercise database hydration with pytest::

                python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py


        :param database: Caller-owned database or compatible metadata read source.
        :param work_id: Optional work id, converted with int and preferred over source_row.
        :param source_row: Optional Row or mapping used when no explicit id is supplied.
        :return: Hydrated WorkMetadata bundle.
        """
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
