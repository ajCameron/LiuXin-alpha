"""
Implement editable relation bundles around an optional expression identity.

Bundles retain relation-link objects, expose value/text projections and delegate
database reads and writes to their hydrator and writer.

Example:
    >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
    >>> metadata.work_ids
    (1,)
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping

from LiuXin_alpha.databases.row import Row
from typing import Any, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import (
    ExpressionMetadataAPI,
    ExpressionRelationKey,
    ExpressionRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    metadata_bundle_string,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import ExpressionIdentity
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.projection_views import (
    MetadataTextView,
    MetadataValuesView,
)


class ExpressionMetadata(ExpressionMetadataAPI):
    """
    Collect expression identity and relation-keyed metadata links.

    The identity and link targets remain shared. Each supported relation has its own
    list; get_relation_links exposes that live list while mapping serialization builds
    new outer containers.

    Example:
        >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
        >>> metadata.expression.expression_id
        2
    """
    def __init__(self, *, expression: Optional[ExpressionIdentityAPI] = None, relation_links: Optional[Mapping[str, Iterable[ExpressionRelationLink]]] = None) -> None:
        """
        Retain the optional identity and initialize every supported relation bucket.

        Supplied relation keys are normalized and link collections validated through
        set_relation_links. Unknown keys raise KeyError.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> metadata.get_relation_links('works')
            []


        :param expression: Optional expression identity retained by reference.
        :param relation_links: Optional mapping of supported relation names or aliases to
            link iterables.
        :return: None.
        """
        self._expression = expression
        self._relation_links: dict[ExpressionRelationKey, list[ExpressionRelationLink]] = {relation_key: [] for relation_key in self.RELATION_KEYS}
        if relation_links:
            for relation_key, links in relation_links.items():
                self.set_relation_links(relation_key, links)

    @property
    def expression(self) -> Optional[ExpressionIdentityAPI]:
        """
        Return the linked expression identity by reference.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> metadata.expression.expression_id
            2


        :return: Stored identity, or None.
        """
        return self._expression

    @expression.setter
    def expression(self, value: Optional[ExpressionIdentityAPI]) -> None:
        """
        Replace the linked identity without changing relation buckets.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> metadata.expression = None
            >>> metadata.expression is None
            True


        :param value: Identity to retain by reference, or None to unlink it.
        :return: None.
        """
        self._expression = value

    @property
    def values(self) -> MetadataValuesView:
        """
        Create a structured value projection backed by this bundle.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> isinstance(metadata.values, MetadataValuesView), metadata.values is metadata.values
            (True, False)


        :return: New MetadataValuesView referencing this bundle.
        """
        return MetadataValuesView(self)

    @property
    def text(self) -> MetadataTextView:
        """
        Create a text projection backed by a fresh value view of this bundle.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> isinstance(metadata.text, MetadataTextView)
            True


        :return: New MetadataTextView.
        """
        return MetadataTextView(self.values)

    @staticmethod
    def _optional_int(value: Any) -> Optional[int]:
        """
        Convert optional input to int, suppressing TypeError and ValueError.

        None and empty strings return None. Other conversion errors, including
        OverflowError, propagate.

        Example:
            >>> ExpressionMetadata._optional_int('7'), ExpressionMetadata._optional_int('bad')
            (7, None)


        :param value: Optional scalar to convert using int.
        :return: Converted integer, or None for missing or rejected input.
        """
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _work_id_from_target(cls, target: Any) -> Optional[int]:
        """
        Extract a work id from a Row, mapping, identity-like object or row_dict fallback.

        Work Rows use row_id; other Rows use their work_id column. Mappings use truthy
        work_id, id, then row_id fallbacks. Objects prefer a non-None work_id attribute over
        row_dict. The selected candidate is converted once.

        Example:
            >>> ExpressionMetadata._work_id_from_target({'work_id': '', 'id': '7'})
            7


        :param target: Relation target whose work identity should be inspected.
        :return: Converted work id, or None when no usable candidate is found.
        """
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
        """
        Read the linked identity parent-work hint.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> metadata.expression_work_id
            1


        :return: Stored work id, or None when the identity is absent or the hint is unset.
        """
        if self.expression is None:
            return None
        return self.expression.expression_work_id

    @expression_work_id.setter
    def expression_work_id(self, expression_work_id: Optional[int]) -> None:
        """
        Set the identity parent-work hint, creating an identity for a non-None value if needed.

        Setting None on an identity-free bundle is a no-op. This setter does not update the
        works relation bucket.

        Example:
            >>> metadata = ExpressionMetadata()
            >>> metadata.expression_work_id = 7
            >>> metadata.expression_work_id, metadata.get_relation_links('works')
            (7, [])


        :param expression_work_id: Optional parent-work id to store without coercion.
        :return: None.
        """
        if self.expression is None:
            if expression_work_id is None:
                return
            self.expression = ExpressionIdentity(expression_work_id=expression_work_id)
            return
        self.expression.expression_work_id = expression_work_id

    @property
    def work_ids(self) -> Optional[Iterable[int]]:
        """
        Collect distinct work ids, placing the identity hint before linked work targets.

        Linked target ids are converted by _work_id_from_target; the identity hint is
        retained as stored. First occurrence order is preserved.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> metadata.works = [{'work_id': 1}, {'work_id': 3}]
            >>> metadata.work_ids
            (1, 3)


        :return: Tuple of work ids, or None when no ids are available.
        """
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
        """
        Convert supplied work ids, set the first as the identity hint and replace work links.

        Values rejected by _optional_int are discarded. Duplicates remain in the new target
        mappings; reading work_ids deduplicates them. Empty input clears the hint and
        relation list. The two updates are not transactional.

        Example:
            >>> metadata = ExpressionMetadata()
            >>> metadata.work_ids = ['7', 'bad', 7, 8]
            >>> metadata.expression_work_id, metadata.work_ids, len(metadata.works)
            (7, (7, 8), 3)


        :param work_ids: Iterable of int-convertible ids, or None to clear the work
            association.
        :return: None.
        """
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

    def get_relation_links(self, relation_key: ExpressionRelationKey) -> list[ExpressionRelationLink]:
        """
        Return the live link list for a normalized relation name.

        Unknown keys raise KeyError. Direct list mutation bypasses set_relation_links
        validation.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> metadata.get_relation_links('work') is metadata.get_relation_links('works')
            True


        :param relation_key: Supported relation bucket name or alias; normalized through
            validate_relation_name.
        :return: Stored mutable list of relation links.
        """
        return self._relation_links[self.validate_relation_name(relation_key)]

    def set_relation_links(self, relation_key: ExpressionRelationKey, links: Iterable[ExpressionRelationLink]) -> None:
        """
        Validate a relation name and link cardinality before replacing its stored list.

        The validator materializes a new list while retaining link objects. Validation failures leave the existing bucket and its link objects unchanged.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> link = ExpressionRelationLink(target={'work_id': 3})
            >>> metadata.set_relation_links('works', [link])
            >>> metadata.get_relation_links('works')[0] is link
            True


        :param relation_key: Supported relation bucket name or alias; normalized through
            validate_relation_name.
        :param links: Iterable of relation-link objects to validate and retain.
        :return: None.
        """
        relation_key = self.validate_relation_name(relation_key)
        self._relation_links[relation_key] = self.validate_relation_links(relation_key, links)

    def __str__(self) -> str:
        """
        Render the identity and populated relation counts as a diagnostic summary.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> 'ExpressionMetadata' in str(metadata)
            True


        :return: Human-readable expression bundle summary.
        """
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
        """
        Delegate supported metadata writes to the WEMI writer at expression level.

        The writer resolves the target and performs incremental changes, returning its write
        report. This wrapper introduces no transaction or rollback boundary. Unsupported
        fields and partial failures follow the writer policy.

        Example:
            Exercise expression bundle writing with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Caller-owned database accepted by LiuXinWEMIMetadataWriter.
        :param fields: Optional field names to write; None uses the writer default field
            selection.
        :param item_id: Optional item id used by writer target-resolution fallbacks.
        :param target_row: Optional Row or mapping supplied for expression target
            resolution.
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
            target_level="expression",
            item_id=item_id,
            target_row=target_row,
            replace=replace,
            mark_dirty=mark_dirty,
        )

    @staticmethod
    def _serialize_target(target: Any) -> Any:
        """
        Serialize a target using Row data, to_mapping, or a shallow mapping copy.

        None and unsupported objects are returned unchanged. A callable to_mapping takes
        precedence over generic Mapping handling.

        Example:
            >>> original = {'work_id': 7}
            >>> result = ExpressionMetadata._serialize_target(original)
            >>> result == original and result is not original
            True


        :param target: Relation target to serialize.
        :return: Serialized target value, potentially sharing nested data.
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
        Convert mappings carrying expression identity keys into ExpressionIdentity objects.

        Presence of expression_id or expression_work_id is enough, including None values.
        Other mappings are shallow-copied and nonmappings are retained.

        Example:
            >>> target = ExpressionMetadata._deserialize_target({'expression_id': 2})
            >>> target.expression_id
            2


        :param target: Target value from a serialized relation link.
        :return: Reconstructed expression identity, copied mapping or unchanged target.
        """
        if isinstance(target, Mapping) and ('expression_id' in target or 'expression_work_id' in target):
            return ExpressionIdentity.from_mapping(target)
        if isinstance(target, Mapping):
            return dict(target)
        return target

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        """
        Serialize the optional identity and, by default, every supported relation bucket.

        Link fields are retained, cardinality becomes its string value, and extra is
        shallow-copied. Target conversion follows _serialize_target; this is not a deep copy
        or persistence operation.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> metadata.to_mapping(include_related=False)['expression']['expression_id']
            2
            >>> 'relations' in metadata.to_mapping(include_related=False)
            False


        :param include_related: Include all relation buckets and their link payloads when
            True.
        :return: New payload with expression and optional relations entries.
        """
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
                        'link_id': link.link_id,
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
        """
        Reconstruct an expression bundle from identity and relation payloads.

        Existing identity and link instances are retained. Link mappings are reconstructed
        with shallow extra copies; unsupported link entries and unknown relation keys are
        ignored. Recognized buckets pass through constructor validation.

        Example:
            >>> metadata = ExpressionMetadata(expression=ExpressionIdentity(expression_id=2, expression_work_id=1))
            >>> restored = ExpressionMetadata.from_mapping(metadata.to_mapping())
            >>> restored.expression.expression_id, restored.work_ids
            (2, (1,))


        :param payload: Mapping with optional expression and relations entries.
        :return: New bundle of the requested class.
        """
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
                        link_id=raw_link.get('link_id'),
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
        """
        Hydrate expression metadata from an explicit id or a source row.

        An explicit id takes precedence. The hydrator is constructed before checking inputs;
        if neither entry point is supplied, ValueError is raised. The result is the concrete
        hydrator bundle, even when invoked on a subclass.

        Example:
            Exercise database hydration with pytest::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param database: Caller-owned database or metadata read source accepted by the
            hydrator.
        :param expression_id: Optional expression id, converted with int and preferred over
            source_row.
        :param source_row: Optional expression Row or mapping used when no explicit id is
            supplied.
        :return: Hydrated ExpressionMetadata instance.
        """
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
