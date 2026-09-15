"""
Insert individual WEMI Rows for the legacy Add composition.
"""


import datetime

from collections.abc import Iterable
from typing import Optional, Union

from LiuXin_alpha.databases.api import RowAPI
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort, to_epoch_ms
from LiuXin_alpha.utils.language_tools import best_effort_language_id


class WEMIAdderMixin:
    """
    Provide unlinked entity insertion and shared date/flag coercion.

    The host supplies db. Add also uses these coercion methods for Agent and
    legacy-title workflows because this mixin precedes their helper definitions.

    Example:
        Create an Expression and link it to a Work separately through repository
        or coordinated mutation operations.
    """
    @staticmethod
    def _coerce_epoch_ms(value: Optional[Union[int, float, datetime.date, datetime.datetime, str]]) -> Optional[int]:
        """
        Preserve integer epoch values and best-effort convert other date inputs.

        Conversion Exceptions are suppressed; this helper does not impose a timezone
        policy beyond to_epoch_ms.

        Example:
            An integer, including bool, is returned unchanged; other supported inputs call to_epoch_ms.


        :param value: Integer, float, date/datetime, string, or None.
        :return: Integer milliseconds, or None for unsupported/failed conversions.
        """

        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, (float, datetime.datetime, datetime.date, str)):
            try:
                return int(to_epoch_ms(value))
            except Exception:
                return None
        return None

    @staticmethod
    def _coerce_iso_date(value: Optional[Union[datetime.date, datetime.datetime, str]]) -> Optional[str]:
        """
        Convert date objects to ISO days while preserving other string forms.

        No whitespace stripping or validation is applied to strings.

        Example:
            >>> WEMIAdderMixin._coerce_iso_date(" 2026-01-02 ")
            ' 2026-01-02 '


        :param value: Date/datetime, stringifiable value, or None.
        :return: ISO day, str(value), or None.
        """

        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value.date().isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _serialize_expression_flags(value: Optional[Iterable[str]]) -> Optional[str]:
        """
        Strip, deduplicate and comma-join flag tokens in first-occurrence order.

        Nonstring tokens are stringified; case variants remain distinct.

        Example:
            >>> WEMIAdderMixin._serialize_expression_flags("draft, draft, revised")
            'draft,revised'


        :param value: Comma-separated string, token iterable, or None.
        :return: Joined flags, or None when no nonblank tokens remain.
        """

        if value is None:
            return None
        if isinstance(value, str):
            tokens = value.split(",")
        else:
            tokens = value
        flags = [str(token).strip() for token in tokens if str(token).strip()]
        return ",".join(dict.fromkeys(flags)) or None

    def work(
        self,
        *,
        # - Titles and sorting
        work_title: str,
        work_canonical_title: Optional[str] = None,
        work_sort_title: Optional[str] = None,
        work_creator_sort: Optional[str] = None,
        # - Core Identity
        work_type: Optional[str] = None,
        work_medium: Optional[str] = None,
        # - Flags for sorting e.t.c
        work_flags: Optional[str] = None,
        # - Original context
        work_original_language: Optional[Union[str, int]] = None,
        work_original_date: Optional[Union[int, float, datetime.date, datetime.datetime, str]] = None,
        work_original_year: Optional[int] = None,
        work_original_copyright_date: Optional[Union[datetime.date, datetime.datetime, str]] = None,
        # - Work metadata and references
        work_wikipedia_link: Optional[str] = None,
        # - High-level classification
        work_is_fiction: Optional[int] = None,
        work_audience: Optional[str] = None,
        work_completion_status: Optional[str] = None,
        # - Concept-level provenance / notes
        work_discovery_note: Optional[str] = None,
        work_created_timestamp: Optional[Union[int, float, datetime.datetime, str]] = None,
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
        # Normalize/create

        # - Titles and sorting
        # We need at lest the work title
        new_row_dict = {"work_title": work_title}
        if work_canonical_title is not None:
            new_row_dict["work_canonical_title"] = work_canonical_title
        else:
            new_row_dict["work_canonical_title"] = work_title
        if work_sort_title is not None:
            new_row_dict["work_sort_title"] = work_sort_title
        else:
            new_row_dict["work_sort_title"] = title_sort(work_title)
        new_row_dict["work_creator_sort"] = work_creator_sort

        new_row_dict["work_type"] = work_type
        new_row_dict["work_medium"] = work_medium

        # - Flags for sorting e.t.c
        new_row_dict["work_flags"] = work_flags

        # - Original context
        if work_original_language is not None:
            new_row_dict["work_original_language_id"] = best_effort_language_id(self.db, work_original_language)
        else:
            new_row_dict["work_original_language_id"] = None
        new_row_dict["work_original_date"] = self._coerce_epoch_ms(work_original_date)
        new_row_dict["work_original_year"] = work_original_year
        new_row_dict["work_original_copyright_date"] = self._coerce_iso_date(work_original_copyright_date)

        # - Work metadata and references
        new_row_dict["work_wikipedia_link"] = work_wikipedia_link

        # - High-level classification
        new_row_dict["work_is_fiction"] = work_is_fiction
        new_row_dict["work_audience"] = work_audience
        new_row_dict["work_completion_status"] = work_completion_status

        # - Concept-level provenance / notes
        new_row_dict["work_discovery_note"] = work_discovery_note
        created_epk = self._coerce_epoch_ms(work_created_timestamp)
        if created_epk is not None:
            new_row_dict["work_created_timestamp_ep_k"] = created_epk
            new_row_dict["work_modified_timestamp_ep_k"] = created_epk

        return Row.from_idless_row_dict(self.db, new_row_dict, table="works")

    def expression(
        self,
        *,
        # -Titles (generally formed from Work title; override only when truly different)
        expression_subtitle: Optional[str] = None,
        expression_title_override: Optional[str] = None,
        # - Core identity
        expression_type: Optional[str] = None,
        expression_label: Optional[str] = None,
        expression_year: Optional[int] = None,
        expression_is_preferred: Optional[int] = None,
        # - Expression dates
        expression_original_date: Optional[Union[int, float, datetime.date, datetime.datetime, str]] = None,
        expression_original_copyright_date: Optional[Union[datetime.date, datetime.datetime, str]] = None,
        # - Expression flags
        expression_flags: Optional[Iterable[str]] = None,
        # - Language & mode
        expression_language: Optional[Union[str, int]] = None,
        expression_mode: Optional[str] = None,
        # - Text-centric details
        expression_wordcount: Optional[int] = None,
        expression_fiction_length_category: Optional[int] = None,
        # - AV centric details
        expression_cut_type: Optional[str] = None,
        expression_nominal_duration_seconds: Optional[int] = None,
        expression_status: Optional[str] = None,
        expression_origin_note: Optional[str] = None,
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
        # -Titles (generally formed from Work title; override only when truly different)
        new_row_dict = {"expression_subtitle": expression_subtitle}
        # Need to pull the display and entry trick for safer unicode
        new_row_dict["expression_title_override"] = expression_title_override

        # - Core identity
        new_row_dict["expression_type"] = expression_type
        new_row_dict["expression_label"] = expression_label
        new_row_dict["expression_year"] = expression_year
        new_row_dict["expression_is_preferred"] = expression_is_preferred

        # - Expression dates
        new_row_dict["expression_original_date"] = self._coerce_epoch_ms(expression_original_date)
        new_row_dict["expression_original_copyright_date"] = self._coerce_iso_date(
            expression_original_copyright_date
        )

        # - Expression flags
        new_row_dict["expression_flags"] = self._serialize_expression_flags(expression_flags)

        # - Language & mode
        if expression_language is not None:
            new_row_dict["expression_language_id"] = best_effort_language_id(self.db, expression_language)
        else:
            new_row_dict["expression_language_id"] = None
        new_row_dict["expression_mode"] = expression_mode

        # - Text-centric details
        new_row_dict["expression_wordcount"] = expression_wordcount
        new_row_dict["expression_fiction_length_category"] = expression_fiction_length_category

        # - AV centric details
        new_row_dict["expression_cut_type"] = expression_cut_type
        new_row_dict["expression_nominal_duration_seconds"] = expression_nominal_duration_seconds
        new_row_dict["expression_status"] = expression_status
        new_row_dict["expression_origin_note"] = expression_origin_note

        return Row.from_idless_row_dict(database=self.db, row_dict=new_row_dict, table="expressions")

    def manifestation(
        self,
        *,
        # - Title details
        manifestation_subtitle: Optional[str] = None,
        # - Carrier / format
        manifestation_carrier_type: Optional[str] = None,
        manifestation_format_detail: Optional[str] = None,
        # - Edition / publication info
        manifestation_edition_statement: Optional[str] = None,
        manifestation_pub_year: Optional[int] = None,
        manifestation_pub_date: Optional[Union[datetime.date, datetime.datetime, str]] = None,
        # - Flags
        manifestation_flags: Optional[str] = None,
        # - Physical / technical characteristics (stable for the product)
        manifestation_page_count: Optional[int] = None,
        manifestation_runtime_minutes: Optional[int] = None,
        manifestation_region_code: Optional[str] = None,
        # - Status / notes
        manifestation_status: Optional[str] = None,
        manifestation_note: Optional[str] = None,
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
        # - Title details
        new_manifestation_row = {"manifestation_subtitle": manifestation_subtitle}

        # - Carrier / format
        new_manifestation_row["manifestation_carrier_type"] = manifestation_carrier_type
        new_manifestation_row["manifestation_format_detail"] = manifestation_format_detail

        # - Edition / publication info
        new_manifestation_row["manifestation_edition_statement"] = manifestation_edition_statement
        new_manifestation_row["manifestation_pub_year"] = manifestation_pub_year
        new_manifestation_row["manifestation_pub_date"] = self._coerce_iso_date(manifestation_pub_date)

        # - Flags
        new_manifestation_row["manifestation_flags"] = manifestation_flags

        # - Physical / technical characteristics (stable for the product)
        new_manifestation_row["manifestation_page_count"] = manifestation_page_count
        new_manifestation_row["manifestation_runtime_minutes"] = manifestation_runtime_minutes
        new_manifestation_row["manifestation_region_code"] = manifestation_region_code

        # - Status / notes
        new_manifestation_row["manifestation_status"] = manifestation_status
        new_manifestation_row["manifestation_note"] = manifestation_note

        return Row.from_idless_row_dict(database=self.db, row_dict=new_manifestation_row, table="manifestations")

    def item(
        self,
        # - Relation to manifestation
        item_manifestation_id: Optional[Union[str, int]] = None,
        # - Flags to control operations of the system
        item_flags: Optional[str] = None,
        # - Type of item
        item_type: Optional[str] = None,
        # - Location / inventory
        item_location: Optional[str] = None,
        item_inventory_code: Optional[str] = None,
        # - Item dates
        item_original_date: Optional[Union[int, float, datetime.date, datetime.datetime, str]] = None,
        item_original_copyright_date: Optional[Union[datetime.date, datetime.datetime, str]] = None,
        # - Source / provenance (per-copy)
        item_source: Optional[str] = None,
        item_source_detail: Optional[str] = None,
        item_source_path: Optional[str] = None,
        item_source_name: Optional[str] = None,
        # - Acquisition / lifecycle
        item_acquired_date: Optional[Union[datetime.date, datetime.datetime, str]] = None,
        item_acquired_price_minor: Optional[float] = None,
        item_lifecycle_status: Optional[str] = None,
        item_condition: Optional[str] = None,
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
        item_new_row_dict = {"item_manifestation_id": item_manifestation_id}

        # - Flags to control operations of the system
        item_new_row_dict["item_flags"] = item_flags

        # - Type of item
        item_new_row_dict["item_type"] = item_type

        # - Location / inventory
        item_new_row_dict["item_location"] = item_location
        item_new_row_dict["item_inventory_code"] = item_inventory_code

        # - Item dates
        item_new_row_dict["item_original_date"] = self._coerce_epoch_ms(item_original_date)
        item_new_row_dict["item_original_copyright_date"] = self._coerce_iso_date(item_original_copyright_date)

        # - Source / provenance (per-copy)
        item_new_row_dict["item_source"] = item_source
        item_new_row_dict["item_source_detail"] = item_source_detail
        item_new_row_dict["item_source_path"] = item_source_path
        item_new_row_dict["item_source_name"] = item_source_name

        # - Acquisition / lifecycle
        item_new_row_dict["item_acquired_date"] = self._coerce_iso_date(item_acquired_date)
        item_new_row_dict["item_acquired_price_minor"] = item_acquired_price_minor
        item_new_row_dict["item_lifecycle_status"] = item_lifecycle_status
        item_new_row_dict["item_condition"] = item_condition

        return Row.from_idless_row_dict(self.db, row_dict=item_new_row_dict, table="items")
