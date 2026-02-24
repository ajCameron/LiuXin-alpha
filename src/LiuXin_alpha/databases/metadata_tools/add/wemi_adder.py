
import datetime

from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.databases.api import RowAPI

from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort, to_epoch_ms

from LiuXin_alpha.utils.language_tools import best_effort_language_id

from typing import Optional, Union


class WEMIAdderMixin:
    """
    Add methods for the basic WEMI classes.
    """
    def work(self,
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
             work_original_date: Optional[str] = None,
             work_original_copyright_date: Optional[str] = None,

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
            new_row_dict["work_sort_title"] = title_sort(work_sort_title)
        new_row_dict["work_creator_sort"] = work_creator_sort

        new_row_dict["work_type"] = work_type
        new_row_dict["work_medium"] = work_medium

        # - Flags for sorting e.t.c
        new_row_dict["work_flags"] = work_flags

        # - Original context
        if work_original_language is not None:
            new_row_dict["work_original_language"] = best_effort_language_id(self.db, work_original_language)
        else:
            new_row_dict["work_original_language"] = None
        new_row_dict["work_original_date"] = work_original_date
        new_row_dict["work_original_copyright_date"] = work_original_copyright_date

        # - Work metadata and references
        new_row_dict["work_wikipedia_link"] = work_wikipedia_link

        # - High-level classification
        new_row_dict["work_is_fiction"] = work_is_fiction
        new_row_dict["work_audience"] = work_audience
        new_row_dict["work_completion_status"] = work_completion_status

        # - Concept-level provenance / notes
        new_row_dict["work_discovery_note"] = work_discovery_note
        new_row_dict["work_created_timestamp"] = work_created_timestamp

        return Row.from_idless_row_dict(self.db, new_row_dict)

    def expression(self,
                   *,
                   # -Titles (generally formed from Work title; override only when truly different)
                   expression_subtitle: Optional[str],
                   expression_title_override: Optional[str] = None,

                   # - Core identity
                   expression_type: Optional[str] = None,
                   expression_label: Optional[str] = None,
                   expression_year: Optional[int] = None,
                   expression_is_preferred: Optional[int] = None,

                    # - Expression dates
                   expression_original_date: Optional[datetime.datetime] = None,
                   expression_original_copyright_date: Optional[datetime.datetime] = None,

                    # - Expression flags
                   expression_flags: Optional[str] = None,

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
        new_row_dict["expression_original_date"] = expression_original_date
        new_row_dict["expression_original_copyright_date"] = expression_original_copyright_date

        # - Expression flags
        new_row_dict["expression_flags"] = expression_flags

        # - Language & mode
        new_row_dict["expression_language"] = expression_language
        new_row_dict["expression_mode"] = expression_mode

        # - Text-centric details
        new_row_dict["expression_wordcount"] = expression_wordcount
        new_row_dict["expression_fiction_length_category"] = expression_fiction_length_category

        # - AV centric details
        new_row_dict["expression_cut_type"] = expression_cut_type
        new_row_dict["expression_nominal_duration_seconds"] = expression_nominal_duration_seconds
        new_row_dict["expression_status"] = expression_status
        new_row_dict["expression_origin_note"] = expression_origin_note

        return Row.from_idless_row_dict(database=self.db, row_dict=new_row_dict)

    def manifestation(self,
                      *,
                      # - Title details
                      manifestation_subtitle: Optional[str],

                      # - Carrier / format
                      manifestation_carrier_type: Optional[str] = None,
                      manifestation_format_detail: Optional[str] = None,

                      # - Edition / publication info
                      manifestation_edition_statement: Optional[str] = None,
                      manifestation_pub_year: Optional[int] = None,
                      manifestation_pub_date: Optional[Union[datetime.datetime, str, int, float]] = None,

                      # - Flags
                      manifestation_flags: Optional[str] = None,

                      # - Physical / technical characteristics (stable for the product)
                      manifestation_page_count: Optional[int] = None,
                      manifestation_runtime_minutes: Optional[int] = None,
                      manifestation_region_code: Optional[str],

                      # - Status / notes
                      manifestation_status: Optional[str],
                      manifestation_note: Optional[str]) -> RowAPI:
        """
        Add methods for the Manifestation table.

        :param manifestation_subtitle:
        :param manifestation_carrier_type:
        :param manifestation_format_detail:
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
        new_manifestation_row["manifestation_pub_date"] = manifestation_pub_date

        # - Flags
        new_manifestation_row["manifestation_flags"] = manifestation_flags

        # - Physical / technical characteristics (stable for the product)
        new_manifestation_row["manifestation_page_count"] = manifestation_page_count
        new_manifestation_row["manifestation_runtime_minutes"] = manifestation_runtime_minutes
        new_manifestation_row["manifestation_region_code"] = manifestation_region_code

        # - Status / notes
        new_manifestation_row["manifestation_status"] = manifestation_status
        new_manifestation_row["manifestation_note"] = manifestation_note

        return Row.from_idless_row_dict(database=self.db, row_dict=new_manifestation_row)

    def item(self,
             # - Relation to manifestation
             item_manifestation_id: Optional[str] = None,

             # - Flags to control operations of the system
             item_flags: Optional[str] = None,

             # - Type of item
             item_type: Optional[str],

             # - Location / inventory
             item_location: Optional[str],
             item_inventory_code: Optional[str],
             item_source: Optional[str],
             item_source_detail: Optional[str],
             item_acquired_date: Optional[Union[datetime.datetime, str, int, float]],
             item_acquired_price_minor: Optional[float],
             item_lifecycle_status: Optional[str],
             item_condition: Optional[str]
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




