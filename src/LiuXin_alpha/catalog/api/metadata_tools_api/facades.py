"""
Keep mutually referring Add, Apply and Ensure protocols together to avoid import cycles.

These describe legacy row-oriented helpers. Their methods have distinct
validation, mutation and recovery policies; no shared atomicity guarantee
is implied by facade composition.
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
    """
    Group row creation with explicit peer wiring and shared helper precedence.

    Construction retains a database, leaving ensure/apply as None until the
    Catalog composition root wires them. WEMIAdderMixin precedes
    AgentCreatorOrgMixin, so its date coercion methods also service Agent calls.
    LabelMixin is retained separately and is not part of this facade.

    Example:
        After composition, ``catalog.add.identifier`` uses catalog.ensure; a
        standalone Add needs that peer supplied before identifier creation.
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
        """
        Create or refresh a legacy title or its WEMI compatibility graph.

        Presence of works selects WEMI behavior. A resolved override ID can create a
        missing Work with that ID. Populate Work, Expression and Manifestation, then
        create/update source Items; old surplus Items remain. A single recognized
        extension determines format/carrier hints. Link-lookup failures are treated
        as absent links; projection lookup failures fall back to the Work. Store the
        latest WEMI rows in _last_title_wemi_bundle. No enclosing transaction prevents
        partial updates or creation on later failures.

        Example:
            An override follows the first existing Expression and Manifestation, then updates Items by position.


        :param title: Title value; None is rejected by title(), but blank text is not checked.
        :param title_sort: Explicit sort text; None uses best-effort generation.
        :param title_phash: Legacy hash; None generates it on the legacy path, ignored on WEMI.
        :param title_creator_sort: Creator-sort text mapped to Work or legacy title storage.
        :param title_pub_date: Publication date mapped to Work/Expression epoch values and publication hints.
        :param title_copyright_date: Copyright value; None falls back to publication date.
        :param title_wikipedia: Wikipedia link mapped to Work or legacy title storage.
        :param title_fiction_length_category: Fiction-length category mapped to Expression or legacy title storage.
        :param title_type: Type mapped to Work or legacy title storage.
        :param title_wordcount: Word count mapped to Expression or legacy title storage.
        :param title_source: Discovery/source value; can trigger creation of one source-only Item.
        :param title_source_path: Path value(s), recursively split on (#BREAK#) on WEMI.
        :param title_source_name: Name value(s), recursively split on (#BREAK#) on WEMI.
        :param title_created_datestamp: Creation time; legacy None uses utcnow(), WEMI uses active epoch conversion.
        :param title_datestamp: Legacy datestamp only; ignored on WEMI.
        :param override_title_row: Existing title/work-like Row to update; work_id/title_id/book_work_id are tried.
        :return: Title projection when available, otherwise the Work Row; legacy path returns its title Row.
        :raises InputIntegrityError: Title is None.
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
        """
        Resolve a projected book or insert a legacy book row for a title.

        A writable/missing books entry chooses the legacy path, which rejects an
        existing title ID and writes in stages. View mode searches book_work_id;
        on a miss, a truthy title column may trigger title() graph creation, then
        a retry and an ID lookup. Resolving a projection can therefore mutate the
        WEMI graph. No enclosing transaction is opened.

        Example:
            When books is a view, optional book metadata is ignored and the existing Work projection is returned.


        :param title_row: Title Row used to resolve legacy title_id or a WEMI Work ID.
        :param book_sort: Legacy book sort; ignored when books is a view.
        :param book_flags: Legacy flags; ignored when books is a view.
        :param book_pubdate: Legacy publication date; None falls back to the title date.
        :param book_copyright_date: Legacy copyright date; falls back to supplied publication date, then title date.
        :param book_uuid: Legacy UUID; None generates a unique group ID.
        :param book_has_cover: Legacy cover flag; no asset validation.
        :param book_has_local_cover: Legacy local-cover flag.
        :param book_last_modified: Legacy modification time; None uses the creation timestamp.
        :param book_fingerprint: Legacy fingerprint; None derives relationship tokens from the title.
        :param book_paths: Legacy paths value stored unchanged.
        :param book_size: Legacy size value stored unchanged.
        :param book_rating: Legacy rating value stored unchanged.
        :param book_created_datestamp: Legacy creation value assigned at final sync, even when None.
        :param book_datestamp: Legacy datestamp stored unchanged.
        :return: First matching compatibility book Row, or newly populated legacy book Row.
        :raises InputIntegrityError: View mode cannot extract a Work ID.
        :raises DatabaseIntegrityError: Legacy book already exists or no projected book can be found.
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
        """
        Insert an unlinked Work from schema-prefixed metadata.

        No relationship graph or transaction is created here. Database validation
        remains authoritative; optional None values are retained in the payload.

        Example:
            ``catalog.add.work(work_title="Frankenstein")`` creates only a Work.


        :param work_title: Preferred title, passed to storage without local blank validation.
        :param work_canonical_title: Canonical title; None uses work_title.
        :param work_sort_title: Sort title; None calls title_sort(work_title).
        :param work_creator_sort: Creator-sort text stored unchanged.
        :param work_type: Work type stored unchanged.
        :param work_medium: Medium stored unchanged.
        :param work_flags: Flags stored unchanged.
        :param work_original_language: Language name/code/ID resolved with best_effort_language_id, or None.
        :param work_original_date: Original date passed through the active epoch-ms coercion helper.
        :param work_original_year: Original year stored unchanged, not inferred from the date.
        :param work_original_copyright_date: Copyright value passed through the active ISO-date coercion helper.
        :param work_wikipedia_link: Wikipedia link stored unchanged.
        :param work_is_fiction: Fiction flag stored without bool coercion.
        :param work_audience: Audience stored unchanged.
        :param work_completion_status: Completion status stored unchanged.
        :param work_discovery_note: Discovery note stored unchanged.
        :param work_created_timestamp: Creation time; a successful conversion also sets modification time.
        :return: Created database Row; synchronization and schema errors propagate.
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
        """
        Insert an unlinked Expression with language and normalized flags.

        Example:
            Flags ``"draft, draft, revised"`` become ``"draft,revised"``; no Work link is added.


        :param expression_subtitle: Subtitle stored unchanged.
        :param expression_title_override: Title override stored unchanged.
        :param expression_type: Expression type stored unchanged.
        :param expression_label: Expression label stored unchanged.
        :param expression_year: Year stored unchanged.
        :param expression_is_preferred: Preferred flag stored without coercion.
        :param expression_original_date: Original date passed through active epoch-ms coercion.
        :param expression_original_copyright_date: Copyright value passed through active ISO-date coercion.
        :param expression_flags: Comma-separated string or iterable; strip, omit blanks and deduplicate in order.
        :param expression_language: Language name/code/ID resolved with best_effort_language_id, or None.
        :param expression_mode: Mode stored unchanged.
        :param expression_wordcount: Word count stored unchanged.
        :param expression_fiction_length_category: Fiction-length category stored unchanged.
        :param expression_cut_type: Cut type stored unchanged.
        :param expression_nominal_duration_seconds: Duration in seconds stored unchanged.
        :param expression_status: Status stored unchanged.
        :param expression_origin_note: Origin note stored unchanged.
        :return: Created database Row; synchronization and schema errors propagate.
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
        """
        Insert an unlinked Manifestation describing an edition or carrier.

        Example:
            Creating a Manifestation does not select or link an Expression.


        :param manifestation_subtitle: Subtitle stored unchanged.
        :param manifestation_carrier_type: Carrier type stored unchanged.
        :param manifestation_format_detail: Format detail stored unchanged.
        :param manifestation_edition_statement: Edition statement stored unchanged.
        :param manifestation_pub_year: Publication year stored unchanged.
        :param manifestation_pub_date: Publication date passed through active ISO-date coercion.
        :param manifestation_flags: Flags stored unchanged.
        :param manifestation_page_count: Page count stored unchanged.
        :param manifestation_runtime_minutes: Runtime in minutes stored unchanged.
        :param manifestation_region_code: Region code stored unchanged.
        :param manifestation_status: Status stored unchanged.
        :param manifestation_note: Note stored unchanged.
        :return: Created database Row; synchronization and schema errors propagate.
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
        """
        Insert an Item with optional Manifestation assignment and source metadata.

        Example:
            A source path records provenance; this helper does not copy or inspect a file.


        :param item_manifestation_id: Manifestation ID passed unchanged; no local lookup or conversion.
        :param item_flags: Flags stored unchanged.
        :param item_type: Item type stored unchanged.
        :param item_location: Location stored unchanged.
        :param item_inventory_code: Inventory code stored unchanged.
        :param item_original_date: Original date passed through active epoch-ms coercion.
        :param item_original_copyright_date: Copyright value passed through active ISO-date coercion.
        :param item_source: Source label stored unchanged.
        :param item_source_detail: Source detail stored unchanged.
        :param item_source_path: Source path stored unchanged; no filesystem access.
        :param item_source_name: Source name stored unchanged.
        :param item_acquired_date: Acquisition date passed through active ISO-date coercion.
        :param item_acquired_price_minor: Price in minor units stored unchanged; no currency conversion.
        :param item_lifecycle_status: Lifecycle status stored unchanged.
        :param item_condition: Condition stored unchanged.
        :return: Created database Row; synchronization and schema errors propagate.
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
        """
        Insert an Agent, then optional sidecars and related metadata.

        The agents table is checked before type validation. Sidecar capability checks
        and attachment resolution occur after Agent insertion; failures can leave a
        partial aggregate. Add resolves shared date helpers through WEMIAdderMixin
        before AgentCreatorOrgMixin. No encompassing transaction is opened.

        Example:
            Adding both sidecar payloads attempts both independently of the selected Agent type.


        :param agent_canonical_name: Canonical name stored unchanged; no local nonempty check.
        :param agent_type: Case/whitespace-normalized type; common person/organisation aliases are accepted.
        :param agent_sort_name: Sort name stored unchanged.
        :param agent_aliases: String or sequence; strip and deduplicate case-insensitively, joining with (#BREAK#).
        :param agent_note: Note stored unchanged.
        :param agent_created_timestamp_ep_k: Creation timestamp via the active coercion helper; also sets modification time.
        :param human_sidecar: Truthy person-sidecar payload to upsert after Agent insertion.
        :param org_sidecar: Truthy organisation-sidecar payload to upsert after Agent insertion.
        :param linked_languages: Language Rows, strings or IDs resolved and linked with native type/priority zero.
        :param linked_notes: Concrete Rows or strings inserted as Notes and linked at priority zero.
        :param linked_synopses: Concrete Rows or strings inserted as Synopses and linked at priority zero.
        :param linked_images: Only concrete Rows are linked as agent_photo; other values are ignored.
        :return: Created database Row; synchronization and schema errors propagate.
        :raises InputIntegrityError: Required tables, type or attachment inputs are unsupported.
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
        """
        Translate legacy Creator fields into a person Agent and attachments.

        Requires insertable agents and human_agents before role validation. Missing
        identifier-table capability silently omits identifiers. All aggregate writes
        follow agent() without an encompassing transaction.

        Example:
            A multiword name is split heuristically into first, middle and last names.


        :param creator: Name converted to stripped text.
        :param creator_sort: Sort name; None derives it with author_to_author_sort.
        :param creator_short_name: Preferred name and optional alias.
        :param creator_last_name: Explicit family name; None uses the final split name part.
        :param creator_phash: Truthy phonetic value retained as an alias and optional identifier.
        :param creator_legal_name: Truthy legal name retained as an alias when different from canonical text.
        :param creator_birth_date: Birth date through active ISO-date coercion.
        :param creator_death_date: Death date through active ISO-date coercion.
        :param creator_type: Normalized role checked against CREATOR_TYPES, then retained in Agent notes.
        :param creator_seminal_work: Truthy seminal-work description retained in Agent notes.
        :param creator_one_person: Flag encoded as 0/1 in notes; Agent type remains person.
        :param creator_wikipedia: Optional wikipedia_url identifier.
        :param creator_imdb: Optional imdb_id identifier.
        :param creator_link: Optional url identifier.
        :param creator_created_datestamp: Truthy creation date preferred over creator_datestamp.
        :param creator_datestamp: Fallback timestamp when creation date is false-valued.
        :param creator_language: Optional Language Row/string/ID to link.
        :param creator_bio: Text stored in biography and also linked as a Note; a Row is only linked.
        :param creator_image: Concrete image Row to link; other values are ignored.
        :return: Created database Row; synchronization and schema errors propagate.
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
        """
        Insert an organisation Agent with a sidecar and optional parent relation.

        Required Agent/sidecar tables are checked before insertion. Parent ID
        validation occurs after creation when the relation table is available.
        DatabaseIntegrityError while inserting the parent relation is suppressed;
        other failures propagate after earlier writes. No enclosing transaction
        is opened.

        Example:
            If org_agent_relations is unavailable, a supplied parent is silently ignored.


        :param organisation: Name converted to stripped text.
        :param organisation_sort: Sort name; None uses the stripped name.
        :param organisation_aliases: Aliases normalized by agent().
        :param organisation_note: Agent note text.
        :param organisation_legal_name: Legal name stored in the organisation sidecar.
        :param organisation_trading_name: Trading name stored in the sidecar.
        :param organisation_registration_id: Registration ID stored unchanged.
        :param organisation_jurisdiction: Jurisdiction stored unchanged.
        :param organisation_founded_date: Foundation date through active ISO-date coercion.
        :param organisation_dissolved_date: Dissolution date through active ISO-date coercion.
        :param organisation_website: Website stored in the sidecar and optionally as a primary url identifier.
        :param organisation_contact_email: Contact email stored unchanged.
        :param organisation_description: Description text stored unchanged.
        :param organisation_parent: Concrete parent Row or integer Agent ID; evaluated only if the relation table is insertable.
        :param organisation_relation_type: Relation type, default imprint_of, stored unchanged.
        :param organisation_relation_note: Relation note stored on the edge and appended to Agent notes.
        :param organisation_language: Optional Language Row/string/ID to link.
        :param organisation_synopsis: Optional Synopsis Row/string to link.
        :return: Created database Row; synchronization and schema errors propagate.
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
        """
        Describe the US-spelled compatibility alias and its concrete keyword limitation.

        The implementation translates only organization and organization_sort.
        Remaining options work positionally or with UK-spelled implementation
        keywords; supplying their US-spelled protocol keywords raises TypeError.
        The advertised signature is retained for compatibility, with this mismatch
        recorded rather than silently changing executable behavior.

        Example:
            ``catalog.add.organization("Publisher", organization_sort="Publisher")``
            uses the two supported US keyword spellings.


        :param organization: Name converted to stripped text.
        :param organization_sort: Sort name; None uses the stripped name.
        :param organization_aliases: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_note: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_legal_name: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_trading_name: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_registration_id: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_jurisdiction: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_founded_date: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_dissolved_date: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_website: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_contact_email: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_description: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_parent: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_relation_type: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_relation_note: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_language: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :param organization_synopsis: US-spelled keyword not translated by the current concrete alias; use its organisation_* counterpart.
        :return: Organisation Agent Row when arguments are accepted.
        """
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
        """
        Translate legacy Publisher values into an organisation Agent.

        Unsupported non-text/non-Row descriptions are ignored. Aggregate and later
        identifier writes are sequential, with no encompassing transaction.

        Example:
            A description Row from synopses is linked as a Synopsis; other Row tables use the Note-link path.


        :param publisher: Publisher name forwarded to organisation.
        :param publisher_sort: Optional sort name.
        :param publisher_phash: Truthy hash encoded as an alias and optional identifier.
        :param publisher_description: Text for sidecar description, or Row linked as Synopsis/Note by its table.
        :param publisher_wikipedia: Optional primary wikipedia_url identifier.
        :param publisher_website: Website forwarded to organisation for sidecar and url identifier.
        :param publisher_parent: Parent organisation Row/ID forwarded with imprint_of relation type.
        :param publishr_position: Legacy misspelled position parameter; non-None is encoded as an alias.
        :param publisher_full: Truthy full name encoded as an alias.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        ...

    def comment(self, comment: str) -> RowAPI:
        """
        Insert a legacy comment row from the supplied value.

        Assign the identifying column on a new Row and sync once. No resource link
        or reuse lookup is performed.

        Example:
            An empty value reaches Row validation unchanged; this helper does not reject it first.


        :param comment: Text assigned unchanged; no local nonempty or type validation.
        :return: Created database Row; synchronization and schema errors propagate.
        """
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
        """
        Insert legacy Genre columns, including an optional parent ID.

        Example:
            A supplied parent is stored as a textual ID; this method does not create an intralink.


        :param genre: Genre text stored unchanged.
        :param genre_sort: Sort value; None is retained.
        :param genre_phash: Phonetic value; None is retained.
        :param genre_parent: Parent object exposing row_id, converted to text; None clears it.
        :param genre_position: Parent position stored unchanged.
        :param genre_full: Full hierarchical name stored unchanged.
        :param genre_datestamp: Timestamp; None uses utcnow().
        :return: Created database Row; synchronization and schema errors propagate.
        """
        ...

    def identifier(self, identifier: str, identifier_type: str) -> RowAPI:
        """
        Delegate identifier creation to the wired Ensure helper.

        Example:
            The default Ensure error policy propagates duplicate insertion errors; this is not unconditional reuse.


        :param identifier: Identifier text; scheme validation belongs to Ensure.
        :param identifier_type: Identifier scheme passed unchanged.
        :return: Row returned by ensure.identifier; missing peer wiring raises AttributeError.
        """
        ...

    def language(self, language_name: str, language_code: str) -> RowAPI:
        """
        Insert a language name/code pair without normalization.

        Example:
            The helper stores exactly the supplied code; use Ensure for compatibility lookup.


        :param language_name: Human-readable name assigned to language.
        :param language_code: Code assigned to language_code without validation.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        ...

    def note(self, note: str) -> RowAPI:
        """
        Insert a legacy note row from the supplied value.

        Assign the identifying column on a new Row and sync once. No resource link
        or reuse lookup is performed.

        Example:
            An empty value reaches Row validation unchanged; this helper does not reject it first.


        :param note: Text assigned unchanged; no local nonempty or type validation.
        :return: Created database Row; synchronization and schema errors propagate.
        """
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
        """
        Insert a Series, then optionally attach a Creator and Note.

        Parent type is checked before sync. Creator hashing may fail before its later
        type check; attachment validation occurs after insertion. A wired Apply peer
        is required for attachments. No enclosing transaction is opened.

        Example:
            A failure linking the Note can leave the already-created Series and Creator link.


        :param series: Series name stored unchanged.
        :param series_sort: Sort text; None uses title_sort.
        :param series_phash: Hash; None generates it from the Creator name, if supplied, and Series name.
        :param series_parent: Concrete parent Series Row, or None.
        :param series_parent_position: Parent position; ignored when there is no parent.
        :param series_full: Full hierarchical name stored unchanged.
        :param series_creator: Concrete Creator Row; linked after syncing the Series.
        :param series_note: Concrete Note Row or text to insert/link after syncing the Series.
        :return: Created database Row; synchronization and schema errors propagate.
        :raises InputIntegrityError: A parent, Creator or Note argument has an unsupported type.
        """
        ...

    def subject(
        self,
        subject: str,
        subject_sort: str | None = None,
        subject_parent: RowAPI | None = None,
    ) -> RowAPI:
        """
        Insert a Subject with optional sort text and parent Row.

        Example:
            A parent Row contributes row_id; no separate intralink is created.


        :param subject: Subject text stored unchanged.
        :param subject_sort: Sort text; None uses make_title_search_term(subject).
        :param subject_parent: Concrete parent Row, or None.
        :return: Created database Row; synchronization and schema errors propagate.
        :raises InputIntegrityError: A non-None parent is not a concrete Row.
        """
        ...

    def synopsis(self, synopsis: str) -> RowAPI:
        """
        Insert a legacy synopsis row from the supplied value.

        Assign the identifying column on a new Row and sync once. No resource link
        or reuse lookup is performed.

        Example:
            An empty value reaches Row validation unchanged; this helper does not reject it first.


        :param synopsis: Text assigned unchanged; no local nonempty or type validation.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        ...

    def tag(self, tag: str, tag_phash: str | None = None) -> RowAPI:
        """
        Insert a Tag with a supplied or generated search hash.

        Example:
            Supplying an empty hash preserves it because only None requests generation.


        :param tag: Tag text preserved unchanged.
        :param tag_phash: Search hash; None calls make_tag_search_term(tag).
        :return: Created database Row; synchronization and schema errors propagate.
        """
        ...


@runtime_checkable
class ApplyAPI(Protocol):
    """
    Group row-link helpers with caller-wired Add and Ensure peers.

    Several methods insert/resolve metadata before validating a resource, and
    some recover integrity errors by deleting/replacing links. No common
    transaction wraps these operations. Return values are usually link Rows.

    Example:
        After Catalog composition, resolve metadata through catalog.apply and
        retain returned link Rows when relationship attributes are needed.
    """

    db: DatabaseAPI
    add: AddAPI | None
    ensure: EnsureAPI | None

    def comments(self, comment: RowAPI | str, resource_row: RowAPI) -> RowAPI:
        """
        Create or reuse a Comment Row and link it to a resource.

        Require a discoverable link table, then interlink with the resource as primary.
        No enclosing transaction or duplicate-error recovery is provided.

        Example:
            Text creation happens before link-table validation; a later failure can leave the new Row.


        :param comment: Concrete Row or text passed to the wired Add helper.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Input type or resource relationship is unsupported.
        """
        ...

    def cover(self, cover: RowAPI, resource_row: RowAPI) -> RowAPI:
        """
        Link an existing concrete Cover Row to a resource.

        Example:
            No cover file is read or created; this operation only links metadata Rows.


        :param cover: Concrete Row accepted without checking its table locally.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: The value is not a Row or no cover link table is found.
        """
        ...

    def creator(
        self,
        resource_row: RowAPI,
        creator_row: RowAPI,
        creator_role: str = "authors",
        creator_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """
        Credit a supplied Creator Row on a compatible resource.

        No Creator type or role validation is added by this wrapper.

        Example:
            The legacy creators-to-resource route must exist before linking.


        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param creator_row: Creator Row passed unchanged.
        :param creator_role: Role forwarded as the link type.
        :param creator_priority: Priority forwarded unchanged, default highest.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: The resource has no discoverable Creator link table.
        """
        ...

    def genre(
        self,
        resource_row: RowAPI,
        genre: RowAPI | str,
        genre_priority: LinkPriority = "highest",
    ) -> RowAPI:
        """
        Resolve a Genre after checking its resource link route.

        Example:
            An unsupported resource fails before text is ensured.


        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param genre: Concrete Row, or text resolved by ensure.genre.
        :param genre_priority: Priority forwarded unchanged.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Link route or Genre input type is unsupported.
        """
        ...

    def identifier(
        self,
        resource_row: RowAPI,
        identifier: RowAPI | str,
        identifier_type: str,
        identifier_priority: LinkPriority = "highest",
        validate_id: bool = True,
    ) -> RowAPI:
        """
        Validate selected identifier schemes and link a resolved Identifier.

        Local validation lowercases the scheme without stripping and checks only
        isbn/issn. Link-table discovery precedes resolution; a Row bypasses text
        validation. No enclosing transaction is opened.

        Example:
            Setting validate_id=False does not disable Ensure.identifier's scheme validation.


        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param identifier: Concrete Row or text delegated to add.identifier.
        :param identifier_type: Scheme forwarded as link type and to identifier creation.
        :param identifier_priority: Priority forwarded unchanged.
        :param validate_id: Enable the local ISBN/ISSN precheck for text; downstream validation still applies.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Local validation, input type or link route is unsupported.
        """
        ...

    def language(
        self,
        language: RowAPI | str,
        resource_row: RowAPI,
        link_type: str | None = None,
    ) -> RowAPI:
        """
        Resolve a Language and replace its link after an integrity error.

        The unlink/retry sequence has no enclosing transaction; retry failure may
        leave the original relation removed.

        Example:
            A DatabaseIntegrityError causes unlink_interlink followed by one retry.


        :param language: Concrete Row or name text delegated to ensure.language.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param link_type: Relationship type forwarded unchanged.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Language input is neither a Row nor text.
        """
        ...

    def contained_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """
        Link a Language with the contained_in relationship type.

        Example:
            Duplicate/integrity errors are suppressed without checking their cause.


        :param language: Concrete Language Row; enforced with an assertion.
        :param title_row: Title Row used as primary endpoint.
        :return: None; ignores DatabaseIntegrityError from linking.
        :raises AssertionError: Language is not a concrete Row, when assertions are enabled.
        """
        ...

    def available_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """
        Link a Language with the available_language relationship type.

        Example:
            Duplicate/integrity errors are suppressed without checking their cause.


        :param language: Concrete Language Row; enforced with an assertion.
        :param title_row: Title Row used as primary endpoint.
        :return: None; ignores DatabaseIntegrityError from linking.
        :raises AssertionError: Language is not a concrete Row, when assertions are enabled.
        """
        ...

    def primary_language(self, language: RowAPI, title_row: RowAPI) -> None:
        """
        Set a legacy title's primary Language through metadata_sql.

        Example:
            This method does not return an interlink Row or suppress SQL failures.


        :param language: Concrete Language Row, asserted before use.
        :param title_row: Title Row whose row_id identifies the title.
        :return: None; forwards title and Language row IDs.
        :raises AssertionError: Language is not a concrete Row, when assertions are enabled.
        """
        ...

    def note(self, note: RowAPI | str, resource: RowAPI) -> RowAPI:
        """
        Create or reuse a Note, then link it as the primary endpoint.

        Text insertion can precede a resource type/link-table failure. No enclosing
        transaction protects the aggregate.

        Example:
            The Note is the primary link endpoint; highest priority is requested.


        :param note: Concrete Row or text inserted by add.note.
        :param resource: Concrete resource Row, validated after Note resolution.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Note/resource type or link route is unsupported.
        """
        ...

    def publisher(self, publisher: RowAPI | str, title_row: RowAPI) -> RowAPI:
        """
        Resolve a Publisher and link it, promoting an existing legacy link on error.

        Recovery assumes publisher_title_link_priority and raises it above the global
        maximum. A failed lookup or conversion during recovery propagates after
        earlier resolution; no enclosing transaction is opened.

        Example:
            Any DatabaseIntegrityError triggers an existing-link lookup and priority update.


        :param publisher: Concrete Row or text ensured without standardization.
        :param title_row: Title Row used as secondary endpoint.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Publisher input has an unsupported type.
        """
        ...

    def rating(
        self, rating: RowAPI | int | float, rating_type: str, resource_row: RowAPI
    ) -> RowAPI:
        """
        Resolve a Rating and replace same-type links after an integrity error.

        Requests priority=not_set. Recovery is not wrapped in a transaction, so a
        retry failure can leave existing same-type links removed.

        Example:
            On DatabaseIntegrityError, unlink all Ratings of rating_type and retry once.


        :param rating: Concrete Row or numeric value; floats truncate to int and bool is accepted.
        :param rating_type: Link type used in creation and recovery deletion.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Rating input or link route is unsupported.
        """
        ...

    def series(
        self,
        series: RowAPI | str,
        series_index: int | float | str,
        resource_row: RowAPI,
        stand: bool = True,
    ) -> tuple[RowAPI, RowAPI]:
        """
        Resolve a Series and attach its index to the resource link.

        Example:
            Unpack ``link_row, series_row``; the first result carries the relationship index.


        :param series: Concrete Row or text resolved by ensure.series_blind.
        :param series_index: Index value forwarded unchanged to interlink_rows.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param stand: Standardization preference passed only when resolving text.
        :return: Tuple (link Row, Series Row), in that order.
        :raises InputIntegrityError: Series input is neither Row nor text.
        """
        ...

    def subject(
        self, subject: RowAPI | str, resource_row: RowAPI, stand: bool = True
    ) -> None:
        """
        Resolve a Subject and link it to the resource.

        Example:
            The current Ensure implementation searches exact string text regardless of stand.


        :param subject: Concrete Row or text delegated to ensure.subject.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param stand: Forwarded to Ensure.subject, whose current implementation ignores it.
        :return: None; the interlink result is discarded.
        :raises InputIntegrityError: Subject input is neither Row nor text.
        """
        ...

    def synopsis(self, synopsis: RowAPI | str, resource: RowAPI) -> RowAPI:
        """
        Create or reuse a Synopsis Row and link it to a resource.

        Require a discoverable link table, then interlink with the resource as primary.
        No enclosing transaction or duplicate-error recovery is provided.

        Example:
            Text creation happens before link-table validation; a later failure can leave the new Row.


        :param synopsis: Concrete Row or text passed to the wired Add helper.
        :param resource: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Input type or resource relationship is unsupported.
        """
        ...

    def tag(self, tag: RowAPI | str | Iterable[str], resource: RowAPI) -> None:
        """
        Link Tag Rows or recursively process iterables using the legacy branch order.

        An empty iterable, including empty text, returns without validating the
        resource. The iterable branch makes later list/set and string resolution
        branches unreachable for normal values of those types. Row inputs check
        a tags-to-resource link table, then link Tag as primary. No transaction
        wraps processing of several elements.

        Example:
            Pass an existing Tag Row for the direct linking path. A nonempty string
            recurses into its own characters and can raise RecursionError.


        :param tag: RowAPI object or iterable; string iteration currently precedes the text branch.
        :param resource: RowAPI resource, validated only once a Tag Row has been resolved.
        :return: None; duplicate/integrity errors during linking are suppressed.
        :raises InputIntegrityError: A noniterable value, resource or link route is unsupported.
        :raises RecursionError: A nonempty string is recursively iterated instead of resolved.
        """
        ...


@runtime_checkable
class EnsureAPI(Protocol):
    """
    Group compatibility lookup/create helpers with a borrowed database.

    Construction creates an Add helper, which Catalog composition later replaces
    with its shared, peer-wired Add. Methods differ in duplicate handling,
    normalization and queue behavior; these operations have no common transaction
    or uniform guarantee of reuse without insertion.

    Example:
        Use creator_blind for a first-match Row, or creator with a queue to inspect
        its legacy candidate sequence and possible creation.
    """

    db: DatabaseAPI
    add: AddAPI

    def creator(self, creator_name: str, match_queue: Queue[RowAPI]) -> None:
        """
        Queue exact and phonetic Creator candidates, creating if no exact IDs were recorded.

        Exact candidates are deduplicated by row_id. Phonetic candidates are checked
        against exact IDs but never added to that set, so duplicates can recur.
        Creation is based on the exact-ID set, not whether the queue has matches.

        Example:
            Phonetic-only matches can be queued and still followed by a newly created Creator.


        :param creator_name: Single name, standardized after rejecting any ampersand.
        :param match_queue: Queue receiving Rows; put may block according to queue capacity.
        :return: None; results are delivered through the queue.
        :raises InputIntegrityError: The input contains an ampersand.
        """
        ...

    def creator_blind(
        self,
        creator_name: str,
        seminal_work: str | None = None,
        standardize: bool = True,
    ) -> RowAPI:
        """
        Select the first exact or phonetic Creator match, otherwise create one.

        No ambiguity resolution or transaction protects lookup/create.

        Example:
            An existing phonetic match wins without comparing the seminal-work hint.


        :param creator_name: Creator name to search.
        :param seminal_work: Seminal-work hint used only when creating.
        :param standardize: Standardize the name before both lookup forms.
        :return: First candidate in database order or newly created person Agent.
        """
        ...

    def genre(self, genre_string: str, standardize: bool = True) -> RowAPI:
        """
        Resolve a Genre, attempting duplicate maintenance before creating.

        After maintenance, search again and fail if multiple rows remain. If no
        match remains, create a Row with only the genre column populated.

        Example:
            Multiple matches invoke fix_duplicates even when the caller disabled standardization.


        :param genre_string: Genre input; None is rejected, other values are converted to text.
        :param standardize: Apply standardize_genre before lookup.
        :return: Unique matching Genre Row or newly inserted Genre Row.
        :raises InputIntegrityError: Genre input is None.
        :raises DatabaseIntegrityError: Duplicate maintenance leaves multiple matches.
        """
        ...

    def identifier(
        self, identifier: str, identifier_type: str, error: bool = True
    ) -> RowAPI:
        """
        Normalize a scheme, validate its value and attempt Identifier insertion.

        There is no pre-insertion reuse lookup. Recovery searches identifier text
        without a scheme filter; zero/multiple matches raise DatabaseIntegrityError.
        No encompassing transaction is opened.

        Example:
            error=False still validates the scheme and value before attempting insertion.


        :param identifier: Identifier text; ISBN/ISSN/DOI checkers may normalize it.
        :param identifier_type: Scheme standardized with standardize_id_name.
        :param error: True propagates sync errors; false attempts recovery after DatabaseIntegrityError.
        :return: Inserted Row, or the sole value-only recovery match.
        :raises InputIntegrityError: Scheme or validated identifier value is invalid.
        :raises DatabaseIntegrityError: Insertion fails under strict policy or recovery is not unique.
        """
        ...

    def language(
        self,
        language_string: str,
        lang_code: bool | Literal["either"] = False,
    ) -> RowAPI:
        """
        Resolve a language by normalized then original name/code, or insert it.

        Insertion writes the standardized input to both language and language_code;
        it does not independently derive a human name and canonical code.

        Example:
            With either mode, name lookups precede code lookups; multiple matches in
            any attempted lookup fail immediately.


        :param language_string: Language input; None rejected and other values converted to text.
        :param lang_code: Exactly True for code, False for name, or "either" for name then code.
        :return: First unique match or inserted language Row.
        :raises InputIntegrityError: Input is None or the mode is unsupported.
        :raises DatabaseIntegrityError: Any attempted lookup finds multiple rows.
        """
        ...

    def publisher(self, publisher: str, standardize: bool = True) -> RowAPI:
        """
        Reuse an exact Publisher value or create an organisation Agent.

        The current multiple-match error-reporting branch references an undefined
        publisher_string name and can raise NameError before its intended integrity
        error. This compatibility defect is retained.

        Example:
            No exact match delegates to add.publisher with the selected spelling.


        :param publisher: Publisher input; None is rejected.
        :param standardize: Apply standardize_publisher before lookup.
        :return: Sole matching Publisher Row or newly created organisation Agent.
        :raises InputIntegrityError: Publisher is None.
        :raises NameError: Multiple matches reach the malformed error-reporting branch.
        """
        ...

    def rating(self, rating: int | float) -> RowAPI:
        """
        Look up the prepopulated Rating Row at int(value)+1.

        No range validation or repair of missing Rating rows occurs.

        Example:
            A rating of 3.9 looks up row ID 4.


        :param rating: Numeric/coercible value; int() truncation and conversion errors apply.
        :return: Database lookup result, possibly None; no row is inserted.
        """
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
        """
        Find Series candidates, optionally queueing them before creation.

        Queued candidates are not deduplicated. Without a queue, the first exact or
        phonetic match wins. Creation is not transactionally coupled to lookup;
        queue insertion may block and failures can follow earlier queued results.

        Example:
            With a queue, existing candidates do not prevent a subsequent creation attempt.


        :param creator_rows: Optional Creator Rows whose creator columns seed phonetic hashes.
        :param series_name: Name converted to text; exact lookup uses this pre-standardized spelling.
        :param series_queue: Optional queue; when supplied, found candidates are queued without early return.
        :param confidence: When creating with Creator Rows, attach only the first if this flag is true.
        :param stand: Standardize the name for hashes and creation, not the initial exact lookup.
        :param use_phash: Search Creator-specific hashes and then the name-only hash.
        :return: First matching Row without a queue, otherwise a newly created Row also queued.
        """
        ...

    def series_blind(
        self,
        creator_rows: Sequence[RowAPI] | None,
        series_name: str,
        stand: bool = True,
        use_phash: bool = True,
    ) -> RowAPI:
        """
        Run queued Series resolution and return its first queued candidate.

        On DatabaseIntegrityError, search the original exact name and take index zero.
        An empty recovery result raises IndexError; an unexpectedly empty queue raises
        queue.Empty. Other failures propagate.

        Example:
            An existing queued match may be returned after series() has also inserted a new row.


        :param creator_rows: Optional Creator Rows passed to series.
        :param series_name: Series name passed unchanged.
        :param stand: Standardization preference passed to series.
        :param use_phash: Phonetic lookup preference passed to series.
        :return: First queued Row, or first exact-name recovery Row after DatabaseIntegrityError.
        """
        ...

    def subject(self, subject: str, standardize: bool = True) -> RowAPI:
        """
        Reuse the first exact Subject string or create one without a parent.

        Example:
            standardize=False and standardize=True currently follow the same lookup path.


        :param subject: Value converted with six_unicode.
        :param standardize: Retained compatibility argument; currently ignored.
        :return: First matching Row or newly inserted Subject Row.
        """
        ...

    def tag(self, tag_text: str) -> RowAPI:
        """
        Resolve by search hash, then exact text, otherwise insert a Tag.

        TypeError from hash generation becomes InputIntegrityError. Multiple
        matches at either lookup raise; no lookup/create transaction is opened.

        Example:
            A unique phonetic/search-hash match wins before exact text is checked.


        :param tag_text: Text preserved for insertion; make_tag_search_term derives the matching hash.
        :return: Unique hash/text match or newly created Tag Row.
        :raises InputIntegrityError: Hash generation rejects the input type.
        :raises DatabaseIntegrityError: A hash or exact-text lookup has multiple matches.
        """
        ...


__all__ = ["AddAPI", "ApplyAPI", "EnsureAPI"]
