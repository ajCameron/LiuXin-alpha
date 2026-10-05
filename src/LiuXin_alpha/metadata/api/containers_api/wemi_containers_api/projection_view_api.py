"""
Define structured and text projection protocols plus the unloaded-data error.

Projections expose relation metadata without editing it. Field extraction, ordering
and lazy-loading policy belong to implementations; reading a property can reject
unloaded dependencies.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
    >>> metadata = WorkMetadata()
    >>> metadata.values.tags, metadata.text.tags
    ((), '')
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, TypeAlias, runtime_checkable


ProjectionIdentifierMap: TypeAlias = Mapping[str, tuple[str, ...]]


class UnloadedMetadataProjectionError(RuntimeError):
    """
    Report a projection that cannot safely omit unloaded lazy metadata.

    The error retains its relation key and dependency names and advises an explicit
    load. Constructing or raising it does not load any metadata.

    Example:
        >>> error = UnloadedMetadataProjectionError('tags', ('works',))
        >>> error.relation_key, error.unloaded_dependencies
        ('tags', ('works',))
    """

    def __init__(
        self,
        relation_key: str,
        unloaded_dependencies: tuple[str, ...] = (),
    ) -> None:
        """
        Retain projection context and build the RuntimeError message.

        Dependency names are joined in supplied order when nonempty. Arguments are stored as
        supplied, without copying or coercion.

        Example:
            >>> error = UnloadedMetadataProjectionError('tags', ('works',))

            >>> str(error)
            "Metadata projection 'tags' has unloaded lazy data. Call load('tags') before reading this projection. Unloaded dependencies: works."


        :param relation_key: Projection relation name included in the error and load advice.
        :param unloaded_dependencies: Tuple of dependency names, empty when none are
            specified.
        :return: None.
        """
        self.relation_key = relation_key
        self.unloaded_dependencies = unloaded_dependencies
        detail = ""
        if unloaded_dependencies:
            detail = " Unloaded dependencies: {}.".format(
                ", ".join(unloaded_dependencies)
            )
        super().__init__(
            "Metadata projection {!r} has unloaded lazy data. "
            "Call load({!r}) before reading this projection.{}".format(
                relation_key,
                relation_key,
                detail,
            )
        )


@runtime_checkable
class MetadataValuesViewAPI(Protocol):
    """
    Describe read-only structured projections of relation targets.

    The runtime-checkable protocol requires the named members; runtime checks do not
    validate their types or extraction policy. Lazy implementations may raise
    UnloadedMetadataProjectionError on access.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
        >>> metadata = WorkMetadata()
        >>> metadata.values.relation_values('tags')
        ()
    """

    def relation_values(self, relation_key: str) -> tuple[str, ...]:
        """
        Require a tuple of projected strings for a supported relation bucket.

        Key normalization, value extraction and ordering follow implementation policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.set_related('tags', [{'tag_name': 'History'}])
            >>> metadata.values.relation_values('tags')
            ('History',)


        :param relation_key: Relation name or implementation-supported alias to project.
        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def tags(self) -> tuple[str, ...]:
        """
        Require the structured projection of tag names.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.tags
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def labels(self) -> tuple[str, ...]:
        """
        Require the structured projection of label text.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.labels
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def genres(self) -> tuple[str, ...]:
        """
        Require the structured projection of genre text.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.genres
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def subjects(self) -> tuple[str, ...]:
        """
        Require the structured projection of subject text.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.subjects
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def series(self) -> tuple[str, ...]:
        """
        Require the structured projection of series text.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.series
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def titles(self) -> tuple[str, ...]:
        """
        Require the structured projection of title strings.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.titles
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def primary_title(self) -> str | None:
        """
        Require the preferred title chosen by the implementation's title policy.

        Fallbacks and unloaded dependencies are determined by the concrete view.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.primary_title is None
            True


        :return: Preferred title string, or None.
        """

    @property
    def identifiers(self) -> ProjectionIdentifierMap:
        """
        Require identifier strings grouped by scheme.

        The protocol specifies a Mapping with tuple values; it does not prescribe how
        schemes are normalized or whether the returned mapping is a copy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> dict(metadata.values.identifiers)
            {}


        :return: Mapping of scheme strings to tuples of identifier strings.
        """

    @property
    def languages(self) -> tuple[str, ...]:
        """
        Require the structured projection of language names or codes.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.languages
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def ratings(self) -> tuple[str, ...]:
        """
        Require the structured projection of rating values.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.ratings
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def agents(self) -> tuple[str, ...]:
        """
        Require the structured projection of agent display names.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.agents
            ()


        :return: Tuple of projected strings, possibly empty.
        """

    @property
    def agent_names(self) -> tuple[str, ...]:
        """
        Require the structured projection of agent display names through the convenience name.

        Extraction, ordering and unloaded-data handling belong to the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.values.agent_names
            ()


        :return: Tuple of projected strings, possibly empty.
        """


@runtime_checkable
class MetadataTextViewAPI(Protocol):
    """
    Describe read-only display and export text for relation projections.

    Concrete views determine joining, title fallbacks and lazy-data handling. Runtime
    protocol checks establish member presence without validating returned values.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
        >>> metadata = WorkMetadata()
        >>> metadata.text.relation_text('tags')
        ''
    """

    def relation_text(self, relation_key: str, separator: str = ", ") -> str:
        """
        Require display text for a relation using the requested separator.

        Supported keys and projected values follow the concrete view's policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.set_related('tags', [{'tag_name': 'History'}, {'tag_name': 'Science'}])
            >>> metadata.text.relation_text('tags', separator=' / ')
            'History / Science'


        :param relation_key: Relation name or implementation-supported alias to render.
        :param separator: String between projected values; defaults to comma and space.
        :return: Joined projection text; empty projections normally yield empty text.
        """

    @property
    def tags(self) -> str:
        """
        Require display text for tag names.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.tags
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def labels(self) -> str:
        """
        Require display text for label text.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.labels
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def genres(self) -> str:
        """
        Require display text for genre text.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.genres
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def subjects(self) -> str:
        """
        Require display text for subject text.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.subjects
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def series(self) -> str:
        """
        Require display text for series text.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.series
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def title(self) -> str | None:
        """
        Require the preferred display title under the implementation's title policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.title is None
            True


        :return: Preferred title string, or None.
        """

    @property
    def titles(self) -> str:
        """
        Require display text for title strings.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.titles
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def languages(self) -> str:
        """
        Require display text for language names or codes.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.languages
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def ratings(self) -> str:
        """
        Require display text for rating values.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.ratings
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def agents(self) -> str:
        """
        Require display text for agent display names.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.agents
            ''


        :return: Rendered string, possibly empty.
        """

    @property
    def agent_names(self) -> str:
        """
        Require display text for agent display names through the convenience name.

        The concrete view chooses joining and unloaded-data behavior.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.text.agent_names
            ''


        :return: Rendered string, possibly empty.
        """


__all__ = [
    "MetadataTextViewAPI",
    "MetadataValuesViewAPI",
    "ProjectionIdentifierMap",
    "UnloadedMetadataProjectionError",
]
