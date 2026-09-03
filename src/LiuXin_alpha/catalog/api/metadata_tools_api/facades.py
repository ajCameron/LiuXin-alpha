"""Mutually-referential metadata-tool facade contracts.

The three protocols live together so their peer attributes remain precisely
 typed without introducing module import cycles.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from queue import Queue
from typing import Any, Literal, Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.metadata_tools_api.common import (
    DateLike,
    IsoDateLike,
    LinkPriority,
)
from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI


@runtime_checkable
class AddAPI(Protocol):
    """Create concrete metadata rows and return database ``RowAPI`` objects.

    ``AddAPI`` is intentionally creation-oriented. For exact reuse, prefer
    ``catalog.ensure`` or a repository's ``match_or_create``. WEMI methods use
    storage-prefixed keyword names because they map directly onto the current
    schema.

    Example::

        work = catalog.add.work(
            work_title="Frankenstein",
            work_original_year=1818,
            work_is_fiction=1,
        )
        expression = catalog.add.expression(
            expression_label="1818 English text",
            expression_language=english_language_id,
        )

    These calls create rows; they do not automatically link an independently
    created Work, Expression, and Manifestation. Use repository
    ``match_or_create`` methods or coordinated mutation helpers when creating a
    connected graph.
    """

    db: DatabaseAPI
    ensure: EnsureAPI | None
    apply: ApplyAPI | None

    def title(
        self,
        title: str,
        title_sort: str | None = None,
        title_phash: str | None = None,
        title_creator_sort: str | None = None,
        title_pub_date: DateLike | None = None,
        title_copyright_date: IsoDateLike | None = None,
        title_wikipedia: str | None = None,
        title_fiction_length_category: int | None = None,
        title_type: str | None = None,
        title_wordcount: int | None = None,
        title_source: str | None = None,
        title_source_path: str | Sequence[str] | None = None,
        title_source_name: str | Sequence[str] | None = None,
        title_created_datestamp: DateLike | None = None,
        title_datestamp: DateLike | None = None,
        override_title_row: RowAPI | None = None,
    ) -> RowAPI:
        """Create a legacy title projection or equivalent WEMI title graph.

        ``override_title_row`` reuses a caller-supplied title row instead of
        inserting another. Source path/name may be one value or a sequence.

        :return: Created or overridden title ``RowAPI``.
        """
        ...

    def book(
        self,
        title_row: RowAPI,
        book_sort: str | None = None,
        book_flags: str | None = None,
        book_pubdate: DateLike | None = None,
        book_copyright_date: IsoDateLike | None = None,
        book_uuid: str | None = None,
        book_has_cover: bool = False,
        book_has_local_cover: bool | None = None,
        book_last_modified: DateLike | None = None,
        book_fingerprint: set[str] | str | None = None,
        book_paths: str | Sequence[str] | None = None,
        book_size: int | None = None,
        book_rating: int | float | None = None,
        book_created_datestamp: DateLike | None = None,
        book_datestamp: DateLike | None = None,
    ) -> RowAPI:
        """Create or resolve the legacy book projection for ``title_row``.

        This compatibility helper translates book-shaped input into current
        storage. It is not the preferred route for explicit WEMI construction.

        :param title_row: Existing title row returned by :meth:`title`.
        :return: Resolved book-compatible row.
        """
        ...

    def work(
        self,
        *,
        work_title: str,
        work_canonical_title: str | None = None,
        work_sort_title: str | None = None,
        work_creator_sort: str | None = None,
        work_type: str | None = None,
        work_medium: str | None = None,
        work_flags: str | None = None,
        work_original_language: str | int | None = None,
        work_original_date: DateLike | None = None,
        work_original_year: int | None = None,
        work_original_copyright_date: IsoDateLike | None = None,
        work_wikipedia_link: str | None = None,
        work_is_fiction: int | None = None,
        work_audience: str | None = None,
        work_completion_status: str | None = None,
        work_discovery_note: str | None = None,
        work_created_timestamp: DateLike | None = None,
    ) -> RowAPI:
        """Create a Work row from schema-prefixed values.

        ``work_title`` is required and is the human-readable preferred title.
        Language may be an existing ID or a value understood by the helper.

        :return: Newly inserted Work ``RowAPI``.
        """
        ...

    def expression(
        self,
        *,
        expression_subtitle: str | None = None,
        expression_title_override: str | None = None,
        expression_type: str | None = None,
        expression_label: str | None = None,
        expression_year: int | None = None,
        expression_is_preferred: int | None = None,
        expression_original_date: DateLike | None = None,
        expression_original_copyright_date: IsoDateLike | None = None,
        expression_flags: str | Iterable[str] | None = None,
        expression_language: str | int | None = None,
        expression_mode: str | None = None,
        expression_wordcount: int | None = None,
        expression_fiction_length_category: int | None = None,
        expression_cut_type: str | None = None,
        expression_nominal_duration_seconds: int | None = None,
        expression_status: str | None = None,
        expression_origin_note: str | None = None,
    ) -> RowAPI:
        """Create an unlinked Expression row.

        Use Expression for language/revision/performance-specific metadata.
        This method does not choose or link a Work.

        :return: Newly inserted Expression ``RowAPI``.
        """
        ...

    def manifestation(
        self,
        *,
        manifestation_subtitle: str | None = None,
        manifestation_carrier_type: str | None = None,
        manifestation_format_detail: str | None = None,
        manifestation_edition_statement: str | None = None,
        manifestation_pub_year: int | None = None,
        manifestation_pub_date: IsoDateLike | None = None,
        manifestation_flags: str | None = None,
        manifestation_page_count: int | None = None,
        manifestation_runtime_minutes: int | None = None,
        manifestation_region_code: str | None = None,
        manifestation_status: str | None = None,
        manifestation_note: str | None = None,
    ) -> RowAPI:
        """Create an unlinked Manifestation row.

        Use Manifestation for edition/carrier/publication-specific metadata.
        This method does not choose or link an Expression.

        :return: Newly inserted Manifestation ``RowAPI``.
        """
        ...

    def item(
        self,
        item_manifestation_id: str | int | None = None,
        item_flags: str | None = None,
        item_type: str | None = None,
        item_location: str | None = None,
        item_inventory_code: str | None = None,
        item_original_date: DateLike | None = None,
        item_original_copyright_date: IsoDateLike | None = None,
        item_source: str | None = None,
        item_source_detail: str | None = None,
        item_source_path: str | None = None,
        item_source_name: str | None = None,
        item_acquired_date: IsoDateLike | None = None,
        item_acquired_price_minor: float | None = None,
        item_lifecycle_status: str | None = None,
        item_condition: str | None = None,
    ) -> RowAPI:
        """Create an Item row, optionally assigned to a Manifestation ID.

        Item is the copy/observation level for source, inventory, acquisition,
        location, lifecycle, and condition data.

        :return: Newly inserted Item ``RowAPI``.
        """
        ...

    def agent(
        self,
        agent_canonical_name: str,
        *,
        agent_type: str = "person",
        agent_sort_name: str | None = None,
        agent_aliases: str | Sequence[str] | None = None,
        agent_note: str | None = None,
        agent_created_timestamp_ep_k: DateLike | None = None,
        human_sidecar: dict[str, Any] | None = None,
        org_sidecar: dict[str, Any] | None = None,
        linked_languages: Iterable[RowAPI | str | int] | None = None,
        linked_notes: Iterable[RowAPI | str] | None = None,
        linked_synopses: Iterable[RowAPI | str] | None = None,
        linked_images: Iterable[RowAPI] | None = None,
    ) -> RowAPI:
        """Create a generic Agent plus optional person/org sidecar and links.

        ``agent_type`` selects the aggregate shape. Sidecar mappings are stored
        with the Agent; supplied language/note/synopsis/image values are linked
        during the same high-level operation.

        :return: Newly created Agent ``RowAPI``.
        """
        ...

    def creator(
        self,
        creator: str,
        creator_sort: str | None = None,
        creator_short_name: str | None = None,
        creator_last_name: str | None = None,
        creator_phash: str | None = None,
        creator_legal_name: str | None = None,
        creator_birth_date: IsoDateLike | None = None,
        creator_death_date: IsoDateLike | None = None,
        creator_type: str = "authors",
        creator_seminal_work: str | None = None,
        creator_one_person: bool = True,
        creator_wikipedia: str | None = None,
        creator_imdb: str | None = None,
        creator_link: str | None = None,
        creator_created_datestamp: DateLike | None = None,
        creator_datestamp: DateLike | None = None,
        creator_language: RowAPI | str | int | None = None,
        creator_bio: RowAPI | str | None = None,
        creator_image: RowAPI | None = None,
    ) -> RowAPI:
        """Create a person Agent using legacy creator-shaped arguments.

        :return: Newly created person Agent ``RowAPI``.
        """
        ...

    def organisation(
        self,
        organisation: str,
        organisation_sort: str | None = None,
        organisation_aliases: str | Sequence[str] | None = None,
        organisation_note: str | None = None,
        organisation_legal_name: str | None = None,
        organisation_trading_name: str | None = None,
        organisation_registration_id: str | None = None,
        organisation_jurisdiction: str | None = None,
        organisation_founded_date: IsoDateLike | None = None,
        organisation_dissolved_date: IsoDateLike | None = None,
        organisation_website: str | None = None,
        organisation_contact_email: str | None = None,
        organisation_description: str | None = None,
        organisation_parent: RowAPI | int | None = None,
        organisation_relation_type: str = "imprint_of",
        organisation_relation_note: str | None = None,
        organisation_language: RowAPI | str | int | None = None,
        organisation_synopsis: RowAPI | str | None = None,
    ) -> RowAPI:
        """Create an organisation Agent and optional parent relationship.

        :return: Newly created organisation Agent ``RowAPI``.
        """
        ...

    def organization(
        self,
        organization: str,
        organization_sort: str | None = None,
        organization_aliases: str | Sequence[str] | None = None,
        organization_note: str | None = None,
        organization_legal_name: str | None = None,
        organization_trading_name: str | None = None,
        organization_registration_id: str | None = None,
        organization_jurisdiction: str | None = None,
        organization_founded_date: IsoDateLike | None = None,
        organization_dissolved_date: IsoDateLike | None = None,
        organization_website: str | None = None,
        organization_contact_email: str | None = None,
        organization_description: str | None = None,
        organization_parent: RowAPI | int | None = None,
        organization_relation_type: str = "imprint_of",
        organization_relation_note: str | None = None,
        organization_language: RowAPI | str | int | None = None,
        organization_synopsis: RowAPI | str | None = None,
    ) -> RowAPI:
        """US spelling alias for :meth:`organisation`."""
        ...

    def publisher(
        self,
        publisher: str,
        publisher_sort: str | None = None,
        publisher_phash: str | None = None,
        publisher_description: RowAPI | str | None = None,
        publisher_wikipedia: str | None = None,
        publisher_website: str | None = None,
        publisher_parent: RowAPI | int | None = None,
        publishr_position: int | str | None = None,
        publisher_full: str | None = None,
    ) -> RowAPI:
        """Create a publisher as an organisation Agent.

        :return: Newly created publisher/Agent ``RowAPI``.
        """
        ...

    def comment(self, comment: str) -> RowAPI:
        """Create a Comment row from non-empty text and return it."""
        ...

    def genre(
        self,
        genre: str,
        genre_sort: str | None = None,
        genre_phash: str | None = None,
        genre_parent: RowAPI | None = None,
        genre_position: int | float | str | None = None,
        genre_full: str | None = None,
        genre_datestamp: DateLike | None = None,
    ) -> RowAPI:
        """Create a Genre row and optional parent relationship."""
        ...

    def identifier(self, identifier: str, identifier_type: str) -> RowAPI:
        """Create or ensure a logical Identifier for a scheme/value pair."""
        ...

    def language(self, language_name: str, language_code: str) -> RowAPI:
        """Create a Language row with its human name and canonical code."""
        ...

    def note(self, note: str) -> RowAPI:
        """Create a Note row from non-empty text and return it."""
        ...

    def series(
        self,
        series: str,
        series_sort: str | None = None,
        series_phash: str | None = None,
        series_parent: RowAPI | None = None,
        series_parent_position: int | float | str | None = None,
        series_full: str | None = None,
        series_creator: RowAPI | None = None,
        series_note: RowAPI | str | None = None,
    ) -> RowAPI:
        """Create a Series row with optional parent, creator, and note links."""
        ...

    def subject(
        self,
        subject: str,
        subject_sort: str | None = None,
        subject_parent: RowAPI | None = None,
    ) -> RowAPI:
        """Create a Subject row with an optional parent Subject."""
        ...

    def synopsis(self, synopsis: str) -> RowAPI:
        """Create a Synopsis row from non-empty text and return it."""
        ...

    def tag(self, tag: str, tag_phash: str | None = None) -> RowAPI:
        """Create a Tag row, optionally retaining a supplied phonetic hash."""
        ...


@runtime_checkable
class ApplyAPI(Protocol):
    """Link metadata rows or convenient scalar values to resource rows.

    String/scalar inputs are resolved or created through the Catalog-owned
    ``ensure``/``add`` helpers before linking. Row inputs are linked directly.
    Methods return the relevant row where useful; callers should not assume the
    input row itself was mutated.

    Example::

        catalog.apply.identifier(
            resource_row=work_row,
            identifier="Q150827",
            identifier_type="wikidata",
            identifier_priority=0,
        )
        catalog.apply.tag(["gothic", "science fiction"], work_row)
    """

    db: DatabaseAPI
    add: AddAPI | None
    ensure: EnsureAPI | None

    def comments(self, comment: RowAPI | str, resource_row: RowAPI) -> RowAPI:
        """Resolve/create and link a Comment to ``resource_row``."""
        ...

    def cover(self, cover: RowAPI, resource_row: RowAPI) -> RowAPI:
        """Link an existing Cover row to ``resource_row``."""
        ...

    def creator(
        self,
        resource_row: RowAPI,
        creator_row: RowAPI,
        creator_role: str = "authors",
        creator_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """Credit ``creator_row`` on a resource with role and ordering."""
        ...

    def genre(
        self,
        resource_row: RowAPI,
        genre: RowAPI | str,
        genre_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """Resolve/create and link a Genre to a resource."""
        ...

    def identifier(
        self,
        resource_row: RowAPI,
        identifier: RowAPI | str,
        identifier_type: str,
        identifier_priority: LinkPriority = "highest",
        validate_id: bool = True,
    ) -> RowAPI:
        """Resolve/create and link a typed Identifier to a resource.

        ``validate_id`` enables scheme-specific validation where available.
        """
        ...

    def language(
        self,
        language: RowAPI | str,
        resource_row: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """Resolve/create and link a Language with an optional link type."""
        ...

    def contained_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """Mark a language as contained in a title."""
        ...

    def available_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """Mark a language as available for a title."""
        ...

    def primary_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """Set a title's primary language."""
        ...

    def note(self, note: RowAPI | str, resource: RowAPI) -> RowAPI:
        """Resolve/create and link a Note to a resource."""
        ...

    def publisher(self, publisher: RowAPI | str, title_row: RowAPI) -> RowAPI:
        """Resolve/create and link a publisher Agent to a title row."""
        ...

    def rating(self, rating: RowAPI | int | float, rating_type: str, resource_row: RowAPI) -> RowAPI:
        """Resolve and link a Rating value of ``rating_type``."""
        ...

    def series(
        self,
        series: RowAPI | str,
        series_index: int | float | str,
        resource_row: RowAPI,
        stand: bool = True,
    ) -> tuple[RowAPI, RowAPI]:
        """Resolve/create a Series and link it with its index row/value."""
        ...

    def subject(self, subject: RowAPI | str, resource_row: RowAPI, stand: bool = True) -> None:
        """Resolve/create and link a Subject to a resource."""
        ...

    def synopsis(self, synopsis: RowAPI | str, resource: RowAPI) -> RowAPI:
        """Resolve/create and link a Synopsis to a resource."""
        ...

    def tag(self, tag: RowAPI | str | Iterable[str], resource: RowAPI) -> None:
        """Resolve/create and link one Tag or an iterable of Tag text values."""
        ...


@runtime_checkable
class EnsureAPI(Protocol):
    """Resolve conventional metadata values, creating only when absent.

    These are compatibility conveniences returning ``RowAPI`` objects. They
    use entity-specific standardization and should not be confused with the
    richer evidence-bearing ``catalog.matching`` API.

    Example::

        language = catalog.ensure.language("eng", lang_code=True)
        tag = catalog.ensure.tag("gothic")
        author = catalog.ensure.creator_blind("Mary Shelley")
    """

    db: DatabaseAPI
    add: AddAPI

    def creator(self, creator_name: str, match_queue: Queue[RowAPI]) -> None:
        """Put matching Creator rows on ``match_queue``, creating when absent.

        This queue-based form preserves multiple legacy candidates. Prefer
        :meth:`creator_blind` only when selecting the first candidate is safe.
        """
        ...

    def creator_blind(
        self,
        creator_name: str,
        seminal_work: str | None = None,
        standardize: bool = True,
    ) -> RowAPI:
        """Return the first standardized Creator match, creating when absent."""
        ...

    def genre(self, genre_string: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a Genre, optionally standardizing input text."""
        ...

    def identifier(self, identifier: str, identifier_type: str, error: bool = True) -> RowAPI:
        """Resolve or create a typed Identifier, validating when ``error``."""
        ...

    def language(
        self,
        language_string: str,
        lang_code: bool | Literal["either"] = False,
    ) -> RowAPI:
        """Resolve/create a Language by name, code, or either interpretation."""
        ...

    def publisher(self, publisher: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a publisher Agent from input text."""
        ...

    def rating(self, rating: int | float) -> RowAPI:
        """Resolve the canonical Rating row for a numeric value."""
        ...

    def series(
        self,
        creator_rows: Sequence[RowAPI] | None,
        series_name: str,
        series_queue: Queue[RowAPI] | None = None,
        confidence: bool = False,
        stand: bool = True,
        use_phash: bool = True,
    ) -> RowAPI:
        """Resolve/create a Series, optionally returning candidates by queue."""
        ...

    def series_blind(
        self,
        creator_rows: Sequence[RowAPI] | None,
        series_name: str,
        stand: bool = True,
        use_phash: bool = True,
    ) -> RowAPI:
        """Resolve/create a Series and return the first selected row directly."""
        ...

    def subject(self, subject: str, standardize: bool = True) -> RowAPI:
        """Resolve or create a Subject, optionally standardizing input text."""
        ...

    def tag(self, tag_text: str) -> RowAPI:
        """Resolve or create an exact Tag row."""
        ...


__all__ = ["AddAPI", "ApplyAPI", "EnsureAPI"]
