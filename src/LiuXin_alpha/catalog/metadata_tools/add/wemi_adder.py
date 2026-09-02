"""WEMI entity creation workflows for legacy metadata tools."""


import datetime

from collections.abc import Iterable
from typing import Optional, Union

from LiuXin_alpha.databases.api import RowAPI
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort, to_epoch_ms
from LiuXin_alpha.utils.language_tools import best_effort_language_id


class WEMIAdderMixin:
    """
    Add methods for the basic WEMI classes.
    """
    @staticmethod
    def _coerce_epoch_ms(value: Optional[Union[int, float, datetime.date, datetime.datetime, str]]) -> Optional[int]:
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
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value.date().isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _serialize_expression_flags(value: Optional[Iterable[str]]) -> Optional[str]:
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
        Add methods for the Work entry of the WEMI tables.

        :param work_title:
        :param work_canonical_title:
        :param work_sort_title:
        :param work_creator_sort:
        :param work_type:
        :param work_medium:
        :param work_flags:
        :param work_original_language:
        :param work_original_date:
        :param work_original_copyright_date:
        :param work_wikipedia_link:
        :param work_is_fiction:
        :param work_audience:
        :param work_completion_status:
        :param work_discovery_note:
        :param work_created_timestamp:
        :return:
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
        Add methods for the Expression table.

        :param expression_type:
        :param expression_label:
        :param expression_year:
        :param expression_is_preferred:
        :param expression_language:
        :param expression_mode:
        :param expression_title_override:
        :param expression_subtitle:
        :param expression_wordcount:
        :param expression_fiction_length_category:
        :param expression_cut_type:
        :param expression_nominal_duration_seconds:
        :param expression_status:
        :param expression_origin_note:
        :return:
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
        Add methods for the Manifestation table.

        :param manifestation_subtitle:
        :param manifestation_carrier_type:
        :param manifestation_format_detail: Specific format or product label,
            such as ``EPUB``, ``PDF``, ``A-format paperback``, or ``4K UHD BD``.
            Use ``manifestation_carrier_type`` for the broader carrier family.
        :param manifestation_edition_statement:
        :param manifestation_pub_year:
        :param manifestation_pub_date:
        :param manifestation_flags:
        :param manifestation_page_count:
        :param manifestation_runtime_minutes:
        :param manifestation_region_code:
        :param manifestation_status:
        :param manifestation_note:
        :return:
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
        Add methods for the Item table.

        :param item_id:
        :param item_manifestation_id:
        :param item_type:
        :param item_location:
        :param item_inventory_code:
        :param item_source:
        :param item_source_detail:
        :param item_acquired_date:
        :param item_acquired_price_minor:
        :param item_lifecycle_status:
        :param item_condition:
        :return:
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
