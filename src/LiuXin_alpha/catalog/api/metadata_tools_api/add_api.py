"""Adder API contracts for catalog metadata tools."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

from LiuXin_alpha.catalog.api.metadata_tools_api.common import DateLike, IsoDateLike
from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_tools_api.apply_api import ApplyAPI
    from LiuXin_alpha.catalog.api.metadata_tools_api.ensure_api import EnsureAPI


@runtime_checkable
class AddAPI(Protocol):
    """Composition root for metadata row creation helpers."""

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
        """Create a legacy title projection or the equivalent WEMI graph."""
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
        """Create or resolve the legacy book projection for a title row."""
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
        """Create a WEMI work row."""
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
        """Create a WEMI expression row."""
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
        """Create a WEMI manifestation row."""
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
        """Create a WEMI item row."""
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
        """Create a generic agent row plus optional sidecar/link rows."""
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
        """Create a person agent for a creator."""
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
        """Create an organisation agent."""
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
        """Create a publisher as an organisation agent."""
        ...

    def comment(self, comment: str) -> RowAPI:
        """Create a comment row."""
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
        """Create a genre row."""
        ...

    def identifier(self, identifier: str, identifier_type: str) -> RowAPI:
        """Create or ensure an identifier row."""
        ...

    def language(self, language_name: str, language_code: str) -> RowAPI:
        """Create a language row."""
        ...

    def note(self, note: str) -> RowAPI:
        """Create a note row."""
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
        """Create a series row."""
        ...

    def subject(
        self,
        subject: str,
        subject_sort: str | None = None,
        subject_parent: RowAPI | None = None,
    ) -> RowAPI:
        """Create a subject row."""
        ...

    def synopsis(self, synopsis: str) -> RowAPI:
        """Create a synopsis row."""
        ...

    def tag(self, tag: str, tag_phash: str | None = None) -> RowAPI:
        """Create a tag row."""
        ...


__all__ = ["AddAPI"]
