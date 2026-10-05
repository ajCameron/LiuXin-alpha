"""
Project bundle and complete WEMI metadata into structured values and display text.

Views retain their source and compute fresh string tuples or identifier maps on
access. Source records are not rewritten. Bundle views tolerate unsupported
convenience properties; stack views combine legacy fields with item-to-work
relations and report pending lazy dependencies.

Example:
    >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
    >>> from LiuXin_alpha.metadata.api import WorkRelationLink
    >>> bundle = WorkMetadata()
    >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
    >>> MetadataValuesView(bundle).tags
    ('Alpha',)
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Protocol

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
    ProjectionIdentifierMap,
    UnloadedMetadataProjectionError,
)


class _MetadataProjectionBundle(Protocol):
    """
    Describe the relation operations needed by a bundle projection.

    Implementations validate keys, expose target objects and select primary targets.
    Optional identity attributes are discovered separately for title fallback.

    Example:
        >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
        >>> from LiuXin_alpha.metadata.api import WorkRelationLink
        >>> bundle = WorkMetadata()
        >>> bundle.validate_relation_name('tag')
        'tags'
    """
    def validate_relation_name(self, relation_key: str) -> str:
        """
        Require validation and canonicalization of a relation key.

        An unsupported key must raise KeyError so convenience properties can distinguish it
        from other failures.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.validate_relation_name('tag')
            'tags'


        :param relation_key: Relation key or alias to resolve.
        :return: Canonical relation key accepted by this bundle.
        """
        ...

    def get_related(self, relation_key: str) -> list[Any]:
        """
        Require retrieval of relation targets in the order exposed by the bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> bundle.get_related('tags')
            [{'tag': ' Alpha '}]


        :param relation_key: Canonical relation key to read.
        :return: List of target objects; copying and ordering are implementation
            responsibilities.
        """
        ...

    def primary_related(self, relation_key: str) -> Any | None:
        """
        Require selection of the primary target for a relation.

        Projection title fallback is handled by the caller when no usable target text
        exists.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.primary_related('titles') is None
            True


        :param relation_key: Canonical relation key to inspect.
        :return: Selected target, or None when no target is available.
        """
        ...


_TEXT_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "tags": (
        "tag",
        "tag_name",
        "text",
        "name",
        "label_text",
        "genre_full",
        "genre",
        "subject_full",
        "subject",
        "series_full",
        "series",
    ),
    "labels": (
        "label_text",
        "label",
        "text",
        "name",
        "tag",
    ),
    "genres": (
        "genre_full",
        "genre",
        "genre_name",
        "text",
        "name",
    ),
    "subjects": (
        "subject_full",
        "subject",
        "subject_name",
        "text",
        "name",
    ),
    "series": (
        "series_full",
        "series",
        "series_name",
        "text",
        "name",
    ),
    "titles": (
        "text",
        "title",
        "work_canonical_title",
        "work_title",
        "expression_title_override",
        "expression_label",
        "manifestation_title",
        "item_title",
        "item_source_name",
        "name",
    ),
    "languages": (
        "display_name",
        "language",
        "language_code",
        "language_iso639_1",
        "language_iso639_2_t",
        "language_iso639_2_b",
        "language_bcp47_primary",
        "text",
        "name",
    ),
    "ratings": (
        "rating",
        "rating_value",
        "value",
        "text",
    ),
    "agents": (
        "agent_display_name",
        "display_name",
        "agent_canonical_name",
        "agent_name",
        "credited_as",
        "human_agent_preferred_name",
        "human_agent_family_name",
        "org_agent_trading_name",
        "org_agent_legal_name",
        "agent_sort_name",
        "sort_name",
        "name",
        "text",
    ),
}
_GENERIC_TEXT_FIELD_CANDIDATES: tuple[str, ...] = (
    "text",
    "name",
    "title",
    "value",
    "label",
)
_IDENTIFIER_SCHEME_KEYS: tuple[str, ...] = (
    "entity_identifier_scheme",
    "item_identifier_scheme",
    "identifier_scheme",
    "scheme",
    "type",
)
_IDENTIFIER_VALUE_KEYS: tuple[str, ...] = (
    "entity_identifier_value",
    "item_identifier_value",
    "identifier_value",
    "value",
    "identifier",
)
_IDENTITY_TITLE_FIELDS: tuple[str, ...] = (
    "work_canonical_title",
    "work_title",
    "expression_title_override",
    "expression_label",
    "expression_subtitle",
    "item_source_name",
)
_IDENTITY_ATTRIBUTES: tuple[str, ...] = (
    "work",
    "expression",
    "manifestation",
    "item",
)
_MISSING = object()


class MetadataValuesView(MetadataValuesViewAPI):
    """
    Compute structured read-only string projections from one live metadata bundle.

    Each access reads current targets and returns new immutable values. String trimming
    and exact-text deduplication preserve first occurrence order. Unsupported
    convenience relations are empty; explicit relation_values calls raise KeyError.

    Example:
        >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
        >>> from LiuXin_alpha.metadata.api import WorkRelationLink
        >>> bundle = WorkMetadata()
        >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
        >>> values = MetadataValuesView(bundle)
        >>> values.tags
        ('Alpha',)
        >>> bundle.set_relation_links('tags', [])
        >>> values.tags
        ()
    """

    __slots__ = ("_metadata",)

    def __init__(self, metadata: _MetadataProjectionBundle) -> None:
        """
        Retain the source bundle without reading, validating or copying its relations.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> MetadataValuesView(bundle).tags
            ('Alpha',)


        :param metadata: Bundle implementing relation validation, target reads and primary
            selection.
        :return: None.
        """
        self._metadata = metadata

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        """
        Validate a relation key and extract distinct nonblank target strings.

        Aliases follow the bundle validator. Unknown keys raise KeyError. This method
        projects relation targets only; it does not append identity titles or group
        identifier values by scheme.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> MetadataValuesView(bundle).relation_values('tag')
            ('Alpha',)


        :param relation_key: Relation key or alias accepted by the bundle.
        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self._relation_values(relation_key, require_known=True)

    @property
    def tags(self) -> tuple[str, ...]:
        """
        Project distinct nonblank tags strings from current relation targets.

        Target text follows the configured field precedence for this relation. Unsupported
        relations return an empty tuple. Whitespace is stripped and exact duplicates are
        removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> MetadataValuesView(bundle).tags
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("tags", require_known=False)

    @property
    def labels(self) -> tuple[str, ...]:
        """
        Project distinct nonblank labels strings from current relation targets.

        Target text follows the configured field precedence for this relation. Unsupported
        relations return an empty tuple. Whitespace is stripped and exact duplicates are
        removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('labels', [WorkRelationLink(target={'label_text': ' Alpha '})])
            >>> MetadataValuesView(bundle).labels
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("labels", require_known=False)

    @property
    def genres(self) -> tuple[str, ...]:
        """
        Project distinct nonblank genres strings from current relation targets.

        Target text follows the configured field precedence for this relation. Unsupported
        relations return an empty tuple. Whitespace is stripped and exact duplicates are
        removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('genres', [WorkRelationLink(target={'genre_full': ' Alpha '})])
            >>> MetadataValuesView(bundle).genres
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("genres", require_known=False)

    @property
    def subjects(self) -> tuple[str, ...]:
        """
        Project distinct nonblank subjects strings from current relation targets.

        Target text follows the configured field precedence for this relation. Unsupported
        relations return an empty tuple. Whitespace is stripped and exact duplicates are
        removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('subjects', [WorkRelationLink(target={'subject_full': ' Alpha '})])
            >>> MetadataValuesView(bundle).subjects
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("subjects", require_known=False)

    @property
    def series(self) -> tuple[str, ...]:
        """
        Project distinct nonblank series strings from current relation targets.

        Target text follows the configured field precedence for this relation. Unsupported
        relations return an empty tuple. Whitespace is stripped and exact duplicates are
        removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('series', [WorkRelationLink(target={'series_full': ' Alpha '})])
            >>> MetadataValuesView(bundle).series
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("series", require_known=False)

    @property
    def titles(self) -> tuple[str, ...]:
        """
        Append the first usable identity title after projected relation titles.

        Exact duplicates and surrounding whitespace are removed. Identity lookup scans work,
        expression, manifestation and item attributes in that order.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata(work=WorkIdentity(work_title='Identity'))
            >>> bundle.set_relation_links('titles', [WorkRelationLink(target={'text': 'Related'})])
            >>> MetadataValuesView(bundle).titles
            ('Related', 'Identity')


        :return: Tuple of relation titles followed by a distinct identity fallback.
        """
        relation_titles = self._relation_values("titles", require_known=False)
        identity_title = self._identity_title()
        if identity_title is None:
            return relation_titles
        return _dedupe_text((*relation_titles, identity_title))

    @property
    def primary_title(self) -> str | None:
        """
        Prefer primary-related title text, then the first relation title, then identity text.

        A KeyError from primary selection is treated as no primary target; other errors
        propagate. Empty or unusable target text falls through to later choices.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata(work=WorkIdentity(work_title='Identity'))
            >>> bundle.set_relation_links('titles', [WorkRelationLink(target={'text': 'Related'})])
            >>> MetadataValuesView(bundle).primary_title
            'Related'


        :return: Selected title, or None when all sources are empty.
        """
        try:
            primary = self._metadata.primary_related("titles")
        except KeyError:
            primary = None
        title = _target_text(primary, "titles")
        if title is not None:
            return title
        relation_titles = self._relation_values("titles", require_known=False)
        if relation_titles:
            return relation_titles[0]
        return self._identity_title()

    @property
    def identifiers(self) -> ProjectionIdentifierMap:
        """
        Group usable identifier targets by scheme without normalizing scheme case or identifier syntax.

        Trimmed exact duplicates are removed within each scheme; first scheme/value
        occurrence determines order. Unsupported identifier relations are empty. The
        returned mapping is a fresh read-only snapshot.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('identifiers', [WorkRelationLink(target='isbn: 123'), WorkRelationLink(target={'scheme': 'isbn', 'value': '123'})])
            >>> dict(MetadataValuesView(bundle).identifiers)
            {'isbn': ('123',)}


        :return: Mapping proxy from scheme strings to immutable value tuples.
        """
        identifiers: dict[str, list[str]] = {}
        for target in self._related_targets("identifiers", require_known=False):
            pair = _identifier_pair(target)
            if pair is None:
                continue
            scheme, value = pair
            values = identifiers.setdefault(scheme, [])
            if value not in values:
                values.append(value)
        return MappingProxyType(
            {scheme: tuple(values) for scheme, values in identifiers.items()}
        )

    @property
    def languages(self) -> tuple[str, ...]:
        """
        Project distinct nonblank languages strings from current relation targets.

        Language candidate fields prefer display_name before stored codes; no language
        lookup occurs. Unsupported relations return an empty tuple. Whitespace is stripped
        and exact duplicates are removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('languages', [WorkRelationLink(target={'language_code': ' Alpha '})])
            >>> MetadataValuesView(bundle).languages
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("languages", require_known=False)

    @property
    def ratings(self) -> tuple[str, ...]:
        """
        Project distinct nonblank ratings strings from current relation targets.

        Numeric values are converted to text without scale conversion or aggregation.
        Unsupported relations return an empty tuple. Whitespace is stripped and exact
        duplicates are removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('ratings', [WorkRelationLink(target={'rating': ' Alpha '})])
            >>> MetadataValuesView(bundle).ratings
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("ratings", require_known=False)

    @property
    def agents(self) -> tuple[str, ...]:
        """
        Project distinct nonblank agents strings from current relation targets.

        Display and canonical names take precedence over later name candidates. Unsupported
        relations return an empty tuple. Whitespace is stripped and exact duplicates are
        removed without sorting.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('agents', [WorkRelationLink(target={'agent_display_name': ' Alpha '})])
            >>> MetadataValuesView(bundle).agents
            ('Alpha',)


        :return: Tuple of projected strings in first occurrence order.
        """
        return self._relation_values("agents", require_known=False)

    @property
    def agent_names(self) -> tuple[str, ...]:
        """
        Expose the agents value projection under its descriptive alias.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> MetadataValuesView(bundle).agent_names == MetadataValuesView(bundle).agents
            True


        :return: The tuple produced by agents.
        """
        return self.agents

    def _relation_values(
        self,
        relation_key: str,
        *,
        require_known: bool,
    ) -> tuple[str, ...]:
        """
        Resolve a relation key and deduplicate text extracted from its current targets.

        Only unsupported-key handling depends on require_known; target retrieval and
        conversion errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> MetadataValuesView(bundle)._relation_values('unknown', require_known=False)
            ()


        :param relation_key: Relation key or alias to validate.
        :param require_known: True to propagate unknown-key errors; False to return an empty
            tuple.
        :return: Trimmed distinct target strings, or an empty tuple for a tolerated unknown
            key.
        """
        validated_relation_key = self._validated_relation_key(
            relation_key,
            require_known=require_known,
        )
        if validated_relation_key is None:
            return ()
        return _dedupe_text(
            _target_text(target, validated_relation_key)
            for target in self._metadata.get_related(validated_relation_key)
        )

    def _related_targets(
        self,
        relation_key: str,
        *,
        require_known: bool,
    ) -> list[Any]:
        """
        Resolve a relation key and return targets from the bundle without additional copying.

        Only validator KeyError is suppressible; failures while retrieving known relations
        propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> MetadataValuesView(bundle)._related_targets('tag', require_known=True)
            [{'tag': ' Alpha '}]


        :param relation_key: Relation key or alias to validate.
        :param require_known: True to propagate unknown-key errors; False to return an empty
            list.
        :return: Bundle-provided target list, or a new empty list for a tolerated unknown
            key.
        """
        validated_relation_key = self._validated_relation_key(
            relation_key,
            require_known=require_known,
        )
        if validated_relation_key is None:
            return []
        return self._metadata.get_related(validated_relation_key)

    def _validated_relation_key(
        self,
        relation_key: str,
        *,
        require_known: bool,
    ) -> str | None:
        """
        Delegate canonicalization to the bundle and optionally suppress KeyError.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> MetadataValuesView(bundle)._validated_relation_key('tag', require_known=True)
            'tags'


        :param relation_key: Relation key or alias to pass to the bundle validator.
        :param require_known: True to re-raise KeyError; False to return None.
        :return: Canonical relation key, or None for a tolerated unsupported relation.
        """
        try:
            return self._metadata.validate_relation_name(relation_key)
        except KeyError:
            if require_known:
                raise
            return None

    def _identity_title(self) -> str | None:
        """
        Find the first nonblank title candidate across attached identities.

        Scan work, expression, manifestation then item. Within each identity, prefer
        canonical/work title, expression override/label/subtitle, then item source name.
        Candidate fields are trimmed; there is no generic object-string fallback.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata(work=WorkIdentity(work_title='Identity'))
            >>> bundle.set_relation_links('titles', [WorkRelationLink(target={'text': 'Related'})])
            >>> MetadataValuesView(bundle)._identity_title()
            'Identity'


        :return: First usable identity title, or None.
        """
        for identity_attribute in _IDENTITY_ATTRIBUTES:
            identity = getattr(self._metadata, identity_attribute, None)
            title = _first_target_text(identity, _IDENTITY_TITLE_FIELDS)
            if title is not None:
                return title
        return None


class MetadataTextView(MetadataTextViewAPI):
    """
    Render structured bundle projections as display/export text.

    Example:
        >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
        >>> from LiuXin_alpha.metadata.api import WorkRelationLink
        >>> bundle = WorkMetadata()
        >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
        >>> text = MetadataTextView(MetadataValuesView(bundle))
        >>> text.tags
        'Alpha'
    """

    __slots__ = ("_values",)

    def __init__(self, values: MetadataValuesViewAPI) -> None:
        """
        Retain the structured values view without reading or copying its values.

        Later property reads observe the supplied view and propagate its errors.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.tags
            'Alpha'


        :param values: Values-view implementation whose properties supply strings and
            tuples.
        :return: None.
        """
        self._values = values

    def relation_text(self, relation_key: str, separator: str = ", ") -> str:
        """
        Join the named structured relation values with the requested separator.

        Unknown-relation and unloaded-projection errors from the values view propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': ' Alpha '})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.relation_text('tag', separator=' | ')
            'Alpha'


        :param relation_key: Relation key or alias understood by the supplied values view.
        :param separator: Text placed between projected strings.
        :return: Joined relation text, or an empty string for no values.
        """
        return separator.join(self._values.relation_values(relation_key))

    @property
    def tags(self) -> str:
        """
        Join projected tags with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.tags
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.tags)

    @property
    def labels(self) -> str:
        """
        Join projected labels with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('labels', [WorkRelationLink(target={'label_text': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.labels
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.labels)

    @property
    def genres(self) -> str:
        """
        Join projected genres with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('genres', [WorkRelationLink(target={'genre_full': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.genres
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.genres)

    @property
    def subjects(self) -> str:
        """
        Join projected subjects with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('subjects', [WorkRelationLink(target={'subject_full': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.subjects
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.subjects)

    @property
    def series(self) -> str:
        """
        Join projected series with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('series', [WorkRelationLink(target={'series_full': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.series
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.series)

    @property
    def title(self) -> str | None:
        """
        Return the primary title selected by the supplied values view.

        No additional fallback or separator is applied.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata(work=WorkIdentity(work_title='Example'))
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.title
            'Example'


        :return: Selected title string, or None.
        """
        return self._values.primary_title

    @property
    def titles(self) -> str:
        """
        Join all projected titles with a space, semicolon and space.

        Selection, ordering and deduplication belong to the supplied values view.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata(work=WorkIdentity(work_title='Example'))
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.titles
            'Example'


        :return: Joined title text, or an empty string.
        """
        return " ; ".join(self._values.titles)

    @property
    def languages(self) -> str:
        """
        Join projected languages with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('languages', [WorkRelationLink(target={'language_code': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.languages
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.languages)

    @property
    def ratings(self) -> str:
        """
        Join projected ratings with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('ratings', [WorkRelationLink(target={'rating': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.ratings
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.ratings)

    @property
    def agents(self) -> str:
        """
        Join projected agents with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('agents', [WorkRelationLink(target={'agent_display_name': 'Alpha'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.agents
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.agents)

    @property
    def agent_names(self) -> str:
        """
        Expose the agents text projection under its descriptive alias.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('agents', [WorkRelationLink(target={'name': 'Author'})])
            >>> text = MetadataTextView(MetadataValuesView(bundle))
            >>> text.agent_names == text.agents
            True


        :return: The same string returned by agents.
        """
        return self.agents


class LiuXinWEMIValuesView(MetadataValuesViewAPI):
    """
    Combine legacy metadata fields with relations from an item-centred WEMI stack.

    Views retain their source and compute fresh string tuples or read-only identifier
    maps. Ordinary relations put legacy values before item, manifestation, expression
    and work values, then remove exact duplicates. Known pending lazy dependencies raise
    UnloadedMetadataProjectionError instead of being materialized here.

    Example:
        >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
        >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
        >>> metadata.tags = ['Legacy']
        >>> values = LiuXinWEMIValuesView(metadata)
        >>> values.tags
        ('Legacy',)
    """

    __slots__ = ("_metadata",)

    _LEVEL_ORDER = ("item", "manifestation", "expression", "work")
    _LEGACY_FIELD_BY_RELATION: dict[str, tuple[str, ...]] = {
        "tags": ("tags",),
        "labels": ("labels",),
        "genres": ("genre",),
        "subjects": ("subject",),
        "series": ("series",),
        "languages": ("languages", "language"),
        "ratings": ("ratings",),
        "agents": ("authors",),
    }
    _RELATION_ALIASES: dict[str, str] = {
        "agent": "agents",
        "author": "agents",
        "authors": "agents",
        "creator": "agents",
        "creators": "agents",
        "genre": "genres",
        "identifier": "identifiers",
        "label": "labels",
        "language": "languages",
        "rating": "ratings",
        "series_entry": "series",
        "subject": "subjects",
        "tag": "tags",
        "title": "titles",
    }

    def __init__(self, metadata: Any) -> None:
        """
        Retain a metadata object without reading or materializing its values.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Legacy']
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> values.tags
            ('Legacy',)


        :param metadata: Stack-like metadata exposing legacy fields and optional WEMI bundle
            access.
        :return: None.
        """
        self._metadata = metadata

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        """
        Normalize a key and combine its legacy and WEMI values with exact-text deduplication.

        Titles use the title projection; identifiers flatten grouped values. Other relations
        require a supported key and loaded dependencies. Legacy values precede item-to-work
        values. WEMI ratings suppress the legacy calibre rating entry, and legacy languages
        omit und case-insensitively. Unknown keys raise KeyError; pending dependencies raise
        UnloadedMetadataProjectionError.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Legacy']
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> values.relation_values(' TAG ')
            ('Legacy',)


        :param relation_key: Key or alias, stripped and lowercased before alias resolution.
        :return: Distinct trimmed strings in first occurrence order.
        """
        relation_key = self._normalize_relation_key(relation_key)
        if relation_key == "titles":
            return self.titles
        if relation_key == "identifiers":
            return _dedupe_text(
                identifier
                for values in self.identifiers.values()
                for identifier in values
            )

        if not self._stack_supports_relation(relation_key):
            raise KeyError(f"Unknown WEMI stack relation key {relation_key!r}.")
        self._raise_if_projection_unloaded(relation_key)
        wemi_values = self._wemi_relation_values(relation_key)
        legacy_values = self._legacy_values(
            relation_key,
            suppress_calibre_rating=bool(wemi_values),
        )
        values = [*legacy_values, *wemi_values]
        return _dedupe_text(values)

    @property
    def tags(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI tags as distinct strings.

        Legacy field values precede relations from item, manifestation, expression and work.
        Pending dependencies raise UnloadedMetadataProjectionError before values are
        combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Alpha']
            >>> LiuXinWEMIValuesView(metadata).tags
            ('Alpha',)


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("tags")

    @property
    def labels(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI labels as distinct strings.

        Legacy field values precede relations from item, manifestation, expression and work.
        Pending dependencies raise UnloadedMetadataProjectionError before values are
        combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.labels = ['Alpha']
            >>> LiuXinWEMIValuesView(metadata).labels
            ('Alpha',)


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("labels")

    @property
    def genres(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI genres as distinct strings.

        Legacy field values precede relations from item, manifestation, expression and work.
        Pending dependencies raise UnloadedMetadataProjectionError before values are
        combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.genre = 'Alpha'
            >>> LiuXinWEMIValuesView(metadata).genres
            ('Alpha',)


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("genres")

    @property
    def subjects(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI subjects as distinct strings.

        Legacy field values precede relations from item, manifestation, expression and work.
        Pending dependencies raise UnloadedMetadataProjectionError before values are
        combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.subject = ['Alpha']
            >>> LiuXinWEMIValuesView(metadata).subjects
            ('Alpha',)


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("subjects")

    @property
    def series(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI series as distinct strings.

        Legacy field values precede relations from item, manifestation, expression and work.
        Pending dependencies raise UnloadedMetadataProjectionError before values are
        combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.series = 'Alpha'
            >>> LiuXinWEMIValuesView(metadata).series
            ('Alpha',)


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("series")

    @property
    def titles(self) -> tuple[str, ...]:
        """
        Combine metadata-provided titles with item-to-work title relation values.

        Known pending title relation loaders raise UnloadedMetadataProjectionError. Exact
        duplicates and surrounding whitespace are removed; the metadata object supplies its
        own legacy/identity title ordering.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> LiuXinWEMIValuesView(metadata).titles
            ('Example',)


        :return: Distinct title strings in first occurrence order.
        """
        self._raise_if_projection_unloaded("titles")
        return _dedupe_text(
            (
                *tuple(getattr(self._metadata, "titles", ())),
                *self._wemi_relation_values("titles"),
            )
        )

    @property
    def primary_title(self) -> str | None:
        """
        Read and trim the metadata display_title after checking pending title dependencies.

        No independent relation-title fallback is added by this property; the metadata
        object determines display-title precedence.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> LiuXinWEMIValuesView(metadata).primary_title
            'Example'


        :return: Trimmed display title, or None when blank or absent.
        """
        self._raise_if_projection_unloaded("titles")
        title = getattr(self._metadata, "display_title", None)
        return _clean_text(title)

    @property
    def identifiers(self) -> ProjectionIdentifierMap:
        """
        Merge legacy identifier mappings with identifier projections from item through work.

        Known pending identifier dependencies raise UnloadedMetadataProjectionError. Legacy
        get_identifiers is used when callable. Schemes and values are stripped, blank
        entries are omitted and exact values deduplicated within each case-sensitive scheme.
        The returned mapping is a fresh immutable snapshot.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.set_identifier('isbn', '123')
            >>> dict(LiuXinWEMIValuesView(metadata).identifiers)
            {'isbn': ('123',)}


        :return: Mapping proxy from scheme strings to value tuples.
        """
        self._raise_if_projection_unloaded("identifiers")
        identifiers: dict[str, list[str]] = {}
        get_identifiers = getattr(self._metadata, "get_identifiers", None)
        if callable(get_identifiers):
            for scheme, values in get_identifiers().items():
                for value in _iter_text_values(values):
                    _append_identifier(identifiers, str(scheme), value)

        for level in self._LEVEL_ORDER:
            bundle = self._stack_bundle(level)
            if bundle is None or not _bundle_supports_relation(bundle, "identifiers"):
                continue
            for scheme, values in bundle.values.identifiers.items():
                for value in values:
                    _append_identifier(identifiers, scheme, value)

        return MappingProxyType(
            {scheme: tuple(values) for scheme, values in identifiers.items()}
        )

    @property
    def languages(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI languages as distinct strings.

        Legacy und values are removed case-insensitively; WEMI language values remain
        eligible. Pending dependencies raise UnloadedMetadataProjectionError before values
        are combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.languages = ['Alpha']
            >>> LiuXinWEMIValuesView(metadata).languages
            ('Alpha',)


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("languages")

    @property
    def ratings(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI ratings as distinct strings.

        WEMI values suppress only legacy mapping entries whose key casefolds to calibre;
        values are not rescaled. Pending dependencies raise UnloadedMetadataProjectionError
        before values are combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.ratings = {'overall': 4}
            >>> LiuXinWEMIValuesView(metadata).ratings
            ('4',)


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("ratings")

    @property
    def agents(self) -> tuple[str, ...]:
        """
        Project combined legacy and WEMI agents as distinct strings.

        Legacy author names precede WEMI agent names. Pending dependencies raise
        UnloadedMetadataProjectionError before values are combined.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.authors = ['Alpha']
            >>> LiuXinWEMIValuesView(metadata).agents
            ('Legacy Author', 'Alpha')


        :return: Tuple of trimmed strings in first occurrence order.
        """
        return self.relation_values("agents")

    @property
    def agent_names(self) -> tuple[str, ...]:
        """
        Expose the agents projection under its descriptive alias.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> LiuXinWEMIValuesView(metadata).agent_names
            ('Legacy Author',)


        :return: The tuple produced by agents.
        """
        return self.agents

    def _legacy_values(
        self,
        relation_key: str,
        *,
        suppress_calibre_rating: bool = False,
    ) -> tuple[str, ...]:
        """
        Extract configured legacy fields for one canonical relation key.

        Languages omit und case-insensitively; rating mappings use values with a key
        fallback and optional calibre suppression. Other mappings contribute keys. Per-field
        conversion deduplicates, but this method does not deduplicate across fields or check
        pending loaders.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Legacy']
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> values._legacy_values('tags')
            ('Legacy',)


        :param relation_key: Canonical relation key used to select legacy field names.
        :param suppress_calibre_rating: True to exclude calibre entries from legacy rating
            mappings.
        :return: Tuple of legacy strings in configured field order.
        """
        fields = self._LEGACY_FIELD_BY_RELATION.get(relation_key, ())
        values: list[str] = []
        for field in fields:
            value = self._legacy_field_value(field)
            if relation_key == "ratings":
                values.extend(
                    _iter_rating_values(
                        value,
                        suppress_calibre=suppress_calibre_rating,
                    )
                )
            elif relation_key == "languages":
                values.extend(
                    value
                    for value in _iter_text_values(value)
                    if value.casefold() != "und"
                )
            else:
                values.extend(_iter_text_values(value))
        return tuple(values)

    def _wemi_relation_values(self, relation_key: str) -> tuple[str, ...]:
        """
        Concatenate supported relation values from item, manifestation, expression and work bundles.

        Missing bundles and unsupported relations are skipped. This helper neither checks
        stack lazy dependencies nor deduplicates between levels; callers supply those steps.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': 'Graph'})])
            >>> metadata = LiuXinWEMIMetadata('Example', [], work_metadata=bundle)
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> values._wemi_relation_values('tags')
            ('Graph',)


        :param relation_key: Canonical relation key to request from each supporting bundle.
        :return: Tuple of projected strings in level order.
        """
        values: list[str] = []
        for level in self._LEVEL_ORDER:
            bundle = self._stack_bundle(level)
            if bundle is None or not _bundle_supports_relation(bundle, relation_key):
                continue
            values.extend(bundle.values.relation_values(relation_key))
        return tuple(values)

    def _stack_supports_relation(self, relation_key: str) -> bool:
        """
        Accept built-in legacy/title/identifier keys or a relation supported by any present bundle.

        Only bundle-validator KeyError denotes an unsupported relation; other errors
        propagate. This helper does not normalize keys or inspect loader state.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Legacy']
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> values._stack_supports_relation('tags'), values._stack_supports_relation('unknown')
            (True, False)


        :param relation_key: Canonical relation key to test.
        :return: True for a supported canonical key.
        """
        if relation_key in self._LEGACY_FIELD_BY_RELATION or relation_key in {
            "identifiers",
            "titles",
        }:
            return True
        return any(
            _bundle_supports_relation(bundle, relation_key)
            for bundle in (
                self._stack_bundle(level)
                for level in self._LEVEL_ORDER
            )
            if bundle is not None
        )

    def _stack_bundle(self, level: str) -> Any | None:
        """
        Delegate level lookup to get_wemi_metadata when the metadata object provides it.

        Missing or noncallable accessors return None; errors from a callable accessor
        propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
            >>> from LiuXin_alpha.metadata.api import WorkRelationLink
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> bundle = WorkMetadata()
            >>> bundle.set_relation_links('tags', [WorkRelationLink(target={'tag': 'Graph'})])
            >>> metadata = LiuXinWEMIMetadata('Example', [], work_metadata=bundle)
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> values._stack_bundle('work') is bundle
            True


        :param level: WEMI level forwarded unchanged to the source accessor.
        :return: Bundle returned by the source, or None when no callable accessor exists.
        """
        get_wemi_metadata = getattr(self._metadata, "get_wemi_metadata", None)
        if not callable(get_wemi_metadata):
            return None
        return get_wemi_metadata(level)

    @classmethod
    def _normalize_relation_key(cls, relation_key: str) -> str:
        """
        Stringify, strip and lowercase a relation key, then resolve the fixed alias map.

        Unknown normalized keys pass through; this method does not validate support.

        Example:
            >>> LiuXinWEMIValuesView._normalize_relation_key(' Authors ')
            'agents'


        :param relation_key: Key-like value to normalize.
        :return: Canonical alias target or unchanged normalized key.
        """
        normalized = str(relation_key).strip().lower()
        return cls._RELATION_ALIASES.get(normalized, normalized)

    def _raise_if_projection_unloaded(self, relation_key: str) -> None:
        """
        Raise when known pending legacy or relation dependencies block a projection.

        The exception records the requested relation and ordered dependency names. Loaders
        are not invoked by this guard.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LazyLiuXinWEMIMetadata
            >>> metadata = LazyLiuXinWEMIMetadata('Example', [])
            >>> metadata.install_lazy_value_to_id('tags', lambda: {'Deferred': 1})
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> try:
            ...     values._raise_if_projection_unloaded('tags')
            ... except UnloadedMetadataProjectionError as error:
            ...     print(error.relation_key, error.unloaded_dependencies)
            tags ('legacy:tags',)


        :param relation_key: Canonical relation key whose dependencies should be checked.
        :return: None.
        """
        dependencies = self._unloaded_projection_dependencies(relation_key)
        if dependencies:
            raise UnloadedMetadataProjectionError(relation_key, dependencies)

    def _unloaded_projection_dependencies(self, relation_key: str) -> tuple[str, ...]:
        """
        List known pending dependencies for a canonical relation without materializing them.

        Inspect mapped legacy fields for unloaded lazy values, the identifier-loaded flag
        for identifiers, then registered relation loaders in item-to-work order. Bundle
        validators resolve loader keys; unsupported bundles are skipped. Legacy fields
        outside the configured mapping are not inspected.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LazyLiuXinWEMIMetadata
            >>> metadata = LazyLiuXinWEMIMetadata('Example', [])
            >>> metadata.install_lazy_value_to_id('tags', lambda: {'Deferred': 1})
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> values._unloaded_projection_dependencies('tags')
            ('legacy:tags',)


        :param relation_key: Canonical relation key to inspect.
        :return: Ordered tuple of legacy:<field> and level:<relation> dependency names.
        """
        dependencies: list[str] = []
        data = _metadata_data(self._metadata)

        for field in self._LEGACY_FIELD_BY_RELATION.get(relation_key, ()):
            if _is_unloaded_lazy_value(data.get(field)):
                dependencies.append(f"legacy:{field}")

        if (
            relation_key == "identifiers"
            and getattr(self._metadata, "_lazy_identifiers_loaded", True) is False
        ):
            dependencies.append("legacy:identifiers")

        loaders = _lazy_relation_loaders(self._metadata)
        if loaders:
            for level in self._LEVEL_ORDER:
                bundle = self._stack_bundle(level)
                if bundle is None:
                    continue
                try:
                    validated_relation_key = bundle.validate_relation_name(relation_key)
                except KeyError:
                    continue
                if (level, validated_relation_key) in loaders:
                    dependencies.append(f"{level}:{validated_relation_key}")

        return tuple(dependencies)

    def _legacy_field_value(self, field: str) -> Any:
        """
        Prefer a raw _data entry, then a callable metadata get method.

        A present None or blank value still wins over the fallback. This helper does not
        materialize or validate a raw entry; a fallback getter retains its own behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Legacy']
            >>> values = LiuXinWEMIValuesView(metadata)
            >>> tuple(values._legacy_field_value('tags'))
            ('Legacy',)


        :param field: Legacy field name to read.
        :return: Stored or getter-provided value, or None when unavailable.
        """
        data = _metadata_data(self._metadata)
        if field in data:
            return data[field]
        get_value = getattr(self._metadata, "get", None)
        if callable(get_value):
            return get_value(field, None)
        return None


class LiuXinWEMITextView(MetadataTextViewAPI):
    """
    Render structured WEMI stack projections as display/export text.

    Example:
        >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
        >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
        >>> metadata.tags = ['Alpha', 'Beta']
        >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
        >>> text.tags
        'Alpha, Beta'
    """

    __slots__ = ("_values",)

    def __init__(self, values: MetadataValuesViewAPI) -> None:
        """
        Retain the structured values view without reading or copying its values.

        Later property reads observe the supplied view and propagate its errors.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Alpha', 'Beta']
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.tags
            'Alpha, Beta'


        :param values: Values-view implementation whose properties supply strings and
            tuples.
        :return: None.
        """
        self._values = values

    def relation_text(self, relation_key: str, separator: str = ", ") -> str:
        """
        Join the named structured relation values with the requested separator.

        Unknown-relation and unloaded-projection errors from the values view propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Alpha', 'Beta']
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.relation_text('tag', separator=' | ')
            'Alpha | Beta'


        :param relation_key: Relation key or alias understood by the supplied values view.
        :param separator: Text placed between projected strings.
        :return: Joined relation text, or an empty string for no values.
        """
        return separator.join(self._values.relation_values(relation_key))

    @property
    def tags(self) -> str:
        """
        Join projected tags with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.tags = ['Alpha']
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.tags
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.tags)

    @property
    def labels(self) -> str:
        """
        Join projected labels with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.labels = ['Alpha']
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.labels
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.labels)

    @property
    def genres(self) -> str:
        """
        Join projected genres with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.genre = 'Alpha'
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.genres
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.genres)

    @property
    def subjects(self) -> str:
        """
        Join projected subjects with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.subject = ['Alpha']
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.subjects
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.subjects)

    @property
    def series(self) -> str:
        """
        Join projected series with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.series = 'Alpha'
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.series
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.series)

    @property
    def title(self) -> str | None:
        """
        Return the primary title selected by the supplied values view.

        No additional fallback or separator is applied.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.title
            'Example'


        :return: Selected title string, or None.
        """
        return self._values.primary_title

    @property
    def titles(self) -> str:
        """
        Join all projected titles with a space, semicolon and space.

        Selection, ordering and deduplication belong to the supplied values view.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.titles
            'Example'


        :return: Joined title text, or an empty string.
        """
        return " ; ".join(self._values.titles)

    @property
    def languages(self) -> str:
        """
        Join projected languages with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.languages = ['Alpha']
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.languages
            'Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.languages)

    @property
    def ratings(self) -> str:
        """
        Join projected ratings with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.ratings = {'overall': 4}
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.ratings
            '4'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.ratings)

    @property
    def agents(self) -> str:
        """
        Join projected agents with a comma and space.

        The supplied values view determines extraction, ordering and deduplication; its
        errors propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> metadata.authors = ['Alpha']
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.agents
            'Legacy Author, Alpha'


        :return: Rendered strings, or an empty string for no values.
        """
        return ", ".join(self._values.agents)

    @property
    def agent_names(self) -> str:
        """
        Expose the agents text projection under its descriptive alias.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
            >>> metadata = LiuXinWEMIMetadata('Example', ['Legacy Author'])
            >>> text = LiuXinWEMITextView(LiuXinWEMIValuesView(metadata))
            >>> text.agent_names == text.agents
            True


        :return: The same string returned by agents.
        """
        return self.agents


def _dedupe_text(values: Any) -> tuple[str, ...]:
    """
    Stringify and trim iterable elements, retaining the first occurrence of each nonblank string.

    None is skipped; equality is case-sensitive. An input string is iterated character
    by character, so callers wrap scalar values when needed.

    Example:
        >>> _dedupe_text([' A ', None, '', 'A', 'a', 0])
        ('A', 'a', '0')


    :param values: Iterable of values to convert and deduplicate.
    :return: Tuple of distinct trimmed strings in encounter order.
    """
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return tuple(result)


def _target_text(target: Any, relation_key: str) -> str | None:
    """
    Extract display text using relation-specific field precedence and a controlled object fallback.

    Strings and numeric/bool scalars are cleaned directly. Other targets use configured
    candidate fields or generic text/name/title/value/label fields. If no candidate
    yields text, a nonempty mapping suppresses object stringification; empty mappings
    and nonmapping objects may fall back to str(target).

    Example:
        >>> _target_text({'label_text': ' Label ', 'name': 'Later'}, 'labels')
        'Label'
        >>> _target_text({'unknown': 'ignored'}, 'tags') is None
        True
        >>> _target_text({}, 'tags')
        '{}'


    :param target: Scalar, mapping, Row-like object or other relation target.
    :param relation_key: Canonical relation name selecting candidate fields; unknown
        names use generic fields.
    :return: Trimmed target text, or None when no usable text is available.
    """
    if target is None:
        return None
    if isinstance(target, str):
        return _clean_text(target)
    if isinstance(target, (int, float, bool)):
        return _clean_text(str(target))

    candidates = _TEXT_FIELD_CANDIDATES.get(
        str(relation_key),
        _GENERIC_TEXT_FIELD_CANDIDATES,
    )
    text = _first_target_text(target, candidates)
    if text is not None:
        return text

    if _target_mapping(target):
        return None
    return _clean_text(str(target))


def _first_target_text(target: Any, candidates: tuple[str, ...]) -> str | None:
    """
    Return the first nonblank candidate field from a target mapping or attributes.

    Candidate order determines precedence. A present mapping entry takes priority over
    the same-named attribute, even when blank; later candidate names are still tried. No
    whole-object string fallback occurs.

    Example:
        >>> _first_target_text({'title': ' ', 'name': ' Next '}, ('title', 'name'))
        'Next'


    :param target: Object or mapping containing candidate fields; None has no text.
    :param candidates: Ordered field names to probe.
    :return: First cleaned candidate value, or None.
    """
    if target is None:
        return None
    mapping = _target_mapping(target)
    for key in candidates:
        value = _target_value(target, mapping, key)
        text = _clean_text(value)
        if text is not None:
            return text
    return None


def _identifier_pair(target: Any) -> tuple[str, str] | None:
    """
    Extract a nonblank scheme/value pair from candidate fields or a colon-separated string.

    Fields follow the fixed scheme/value precedence. String fallback splits only at the
    first colon. Both parts are stripped without case folding, scheme validation or
    identifier normalization.

    Example:
        >>> _identifier_pair(' doi : 10.example:a ')
        ('doi', '10.example:a')
        >>> _identifier_pair({'scheme': '', 'value': '123'}) is None
        True


    :param target: Identifier mapping, object or scheme:value string.
    :return: Two-string tuple, or None when either part is unavailable or blank.
    """
    mapping = _target_mapping(target)
    scheme = _first_target_text(target, _IDENTIFIER_SCHEME_KEYS)
    value = _first_target_text(target, _IDENTIFIER_VALUE_KEYS)
    if scheme is None or value is None:
        if isinstance(target, str) and ":" in target:
            scheme, value = target.split(":", 1)
        elif not mapping:
            return None
        else:
            return None
    scheme = scheme.strip()
    value = value.strip()
    if not scheme or not value:
        return None
    return scheme, value


def _target_mapping(target: Any) -> Mapping[str, Any]:
    """
    Prefer a target mapping, then a mapping row_dict, then a mapping returned by to_mapping.

    Existing mappings are returned directly. to_mapping is called only when callable;
    nonmapping results yield an empty dictionary. Attribute and conversion exceptions
    propagate.

    Example:
        >>> from types import SimpleNamespace
        >>> payload = {'name': 'Example'}
        >>> _target_mapping(SimpleNamespace(row_dict=payload)) is payload
        True


    :param target: Object whose mapping representation is needed.
    :return: Selected mapping, or a new empty dictionary when no mapping surface is
        available.
    """
    if isinstance(target, Mapping):
        return target

    row_dict = getattr(target, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return row_dict

    to_mapping = getattr(target, "to_mapping", None)
    if callable(to_mapping):
        payload = to_mapping()
        if isinstance(payload, Mapping):
            return payload

    return {}


def _target_value(
    target: Any,
    mapping: Mapping[str, Any],
    key: str,
) -> Any:
    """
    Read a present mapping entry before falling back to the target attribute.

    A present None or blank value suppresses same-named attribute fallback. Missing
    attributes return None; other attribute errors propagate.

    Example:
        >>> from types import SimpleNamespace
        >>> _target_value(SimpleNamespace(name='Fallback'), {'name': None}, 'name') is None
        True


    :param target: Object used for attribute fallback.
    :param mapping: Previously selected mapping; a falsey mapping is skipped.
    :param key: Field name to read.
    :return: Mapping value, attribute value or None.
    """
    if mapping:
        value = mapping.get(key, _MISSING)
        if value is not _MISSING:
            return value
    return getattr(target, key, None)


def _clean_text(value: Any) -> str | None:
    """
    Convert a nonempty value to stripped text, returning None for blank results.

    Numeric zero and False remain displayable strings. Conversion and comparison errors
    are not suppressed.

    Example:
        >>> _clean_text('  '), _clean_text(0), _clean_text(False)
        (None, '0', 'False')


    :param value: Value to stringify and trim.
    :return: Trimmed string, or None for None, empty text or whitespace-only text.
    """
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _bundle_supports_relation(bundle: Any, relation_key: str) -> bool:
    """
    Check relation support by calling the bundle validator.

    Only KeyError is converted to False; any other validator exception propagates.

    Example:
        >>> from LiuXin_alpha.metadata.containers import WorkMetadata, WorkIdentity
        >>> from LiuXin_alpha.metadata.api import WorkRelationLink
        >>> bundle = WorkMetadata()
        >>> _bundle_supports_relation(bundle, 'tags'), _bundle_supports_relation(bundle, 'unknown')
        (True, False)


    :param bundle: Bundle exposing validate_relation_name.
    :param relation_key: Relation key to pass to the validator.
    :return: True when validation returns normally.
    """
    try:
        bundle.validate_relation_name(relation_key)
    except KeyError:
        return False
    return True


def _iter_text_values(value: Any) -> tuple[str, ...]:
    """
    Convert legacy scalar, collection or mapping-key values into distinct strings.

    None and empty text produce no values. Mappings contribute keys; lists, tuples, sets
    and frozensets contribute elements in their iteration order. Other objects,
    including arbitrary iterators, are treated as one scalar.

    Example:
        >>> _iter_text_values({' A ': 1, 'A': 2}), _iter_text_values('AB')
        (('A',), ('AB',))


    :param value: Legacy field value to project.
    :return: Tuple of trimmed nonblank strings in first occurrence order.
    """
    if value in (None, ""):
        return ()
    if isinstance(value, Mapping):
        return _dedupe_text(value.keys())
    if isinstance(value, (list, tuple, set, frozenset)):
        return _dedupe_text(value)
    return _dedupe_text((value,))


def _iter_rating_values(
    value: Any,
    *,
    suppress_calibre: bool = False,
) -> tuple[str, ...]:
    """
    Project legacy rating mappings by nonempty values, falling back to eligible keys.

    Optional calibre suppression compares stringified keys with casefold without
    stripping. Key fallback occurs only when no raw eligible value survived the
    None/empty filter; whitespace values can therefore suppress fallback. Nonmapping
    inputs use ordinary legacy text conversion.

    Example:
        >>> _iter_rating_values({'calibre': 4, 'overall': 8}, suppress_calibre=True)
        ('8',)
        >>> _iter_rating_values({'unrated': ''})
        ('unrated',)
        >>> _iter_rating_values({'unrated': ' '})
        ()


    :param value: Legacy rating scalar, collection or mapping.
    :param suppress_calibre: True to exclude mapping entries whose key casefolds to
        calibre.
    :return: Tuple of distinct trimmed rating strings.
    """
    if value in (None, ""):
        return ()
    if isinstance(value, Mapping):
        values = [
            rating
            for key, rating in value.items()
            if rating not in (None, "")
            and not (suppress_calibre and str(key).casefold() == "calibre")
        ]
        if values:
            return _dedupe_text(values)
        return _dedupe_text(
            key
            for key in value.keys()
            if not (suppress_calibre and str(key).casefold() == "calibre")
        )
    return _iter_text_values(value)


def _append_identifier(
    identifiers: dict[str, list[str]],
    scheme: str,
    value: str,
) -> None:
    """
    Append a stripped identifier value once within its stripped, case-sensitive scheme.

    Blank schemes or values leave the mapping unchanged. Existing value lists are
    mutated in place; schemes and identifier syntax are not normalized.

    Example:
        >>> identifiers = {}
        >>> _append_identifier(identifiers, ' isbn ', '123')
        >>> _append_identifier(identifiers, 'isbn', ' 123 ')
        >>> identifiers
        {'isbn': ['123']}


    :param identifiers: Mutable scheme-to-value-list accumulator.
    :param scheme: Scheme name stringified and stripped before lookup.
    :param value: Identifier value stringified and stripped before deduplication.
    :return: None.
    """
    scheme_text = str(scheme).strip()
    value_text = str(value).strip()
    if not scheme_text or not value_text:
        return
    values = identifiers.setdefault(scheme_text, [])
    if value_text not in values:
        values.append(value_text)


def _metadata_data(metadata: Any) -> Mapping[str, Any]:
    """
    Read _data through base attribute lookup and accept only mapping values.

    AttributeError produces an empty dictionary. The lookup bypasses custom
    __getattribute__/__getattr__ dispatch but can still evaluate a descriptor; other
    exceptions propagate. A valid mapping is returned by reference.

    Example:
        >>> from types import SimpleNamespace
        >>> payload = {'tags': ['A']}
        >>> _metadata_data(SimpleNamespace(_data=payload)) is payload
        True


    :param metadata: Object potentially carrying a legacy _data mapping.
    :return: Source mapping, or a new empty dictionary.
    """
    try:
        data = object.__getattribute__(metadata, "_data")
    except AttributeError:
        return {}
    if isinstance(data, Mapping):
        return data
    return {}


def _lazy_relation_loaders(metadata: Any) -> Mapping[tuple[str, str], Any]:
    """
    Read the pending relation-loader mapping through base attribute lookup.

    AttributeError and nonmapping values yield an empty dictionary. Valid mappings are
    retained by reference without invoking their loaders; other lookup errors propagate.

    Example:
        >>> from types import SimpleNamespace
        >>> pending = {('work', 'tags'): lambda: []}
        >>> _lazy_relation_loaders(SimpleNamespace(_lazy_relation_loaders=pending)) is pending
        True


    :param metadata: Object potentially carrying _lazy_relation_loaders.
    :return: Mapping of level/relation keys to pending loaders, or an empty dictionary.
    """
    try:
        loaders = object.__getattribute__(metadata, "_lazy_relation_loaders")
    except AttributeError:
        return {}
    if isinstance(loaders, Mapping):
        return loaders
    return {}


def _is_unloaded_lazy_value(value: Any) -> bool:
    """
    Recognize an unloaded value by loaded being exactly False and materialize being callable.

    Missing loaded defaults to True. The materialize attribute is inspected but not
    called.

    Example:
        >>> from types import SimpleNamespace
        >>> _is_unloaded_lazy_value(SimpleNamespace(loaded=False, materialize=lambda: []))
        True
        >>> _is_unloaded_lazy_value(SimpleNamespace(loaded=0, materialize=lambda: []))
        False


    :param value: Object to inspect for the lazy-value protocol.
    :return: True only for an explicitly unloaded value with a callable materializer.
    """
    if getattr(value, "loaded", True) is not False:
        return False
    return callable(getattr(value, "materialize", None))


__all__ = [
    "LiuXinWEMITextView",
    "LiuXinWEMIValuesView",
    "MetadataTextView",
    "MetadataValuesView",
]
