
import datetime

from LiuXin_alpha.databases.api import RowAPI

from typing import Optional, Union


class WEMIAdderMixin:
    """
    Add methods for the basic WEMI classes.
    """
    def work(self,
             # - Core Identity
             work_type: str,
             work_medium: str,
             # - Titles and sorting
             work_title: str,
             work_canonical_title: str,
             work_sort_title: str,
             work_creator_sort: str,
             # - Flags for sorting e.t.c
             work_flags: str,
             # - Original context
             work_original_language: Optional[Union[str, int]],
             work_original_date: Optional[str],
             work_original_copyright_date: Optional[str],
             # - Work metadata and references
             work_wikipedia_link: Optional[str],
             # - High-level classification
             work_is_fiction: Optional[int],
             work_audience: Optional[str],
             work_completion_status: Optional[str],
             # - Concept-level provenance / notes
             work_discovery_note: Optional[str],
             work_created_timestamp: Optional[Union[int, float, datetime.datetime, str]],
             ) -> RowAPI:
        """
        Add methods for the Work entry of the WEMI tables.

        :param work_type:
        :param work_medium:
        :param work_title:
        :param work_canonical_title:
        :param work_sort_title:
        :param work_original_language:
        :param work_original_year:
        :param work_wikipedia_link:
        :param work_is_fiction:
        :param work_audience:
        :param work_completion_status:
        :param work_discovery_note:
        :param work_created_timestamp:
        :return:
        """
        new_row_dict = {}



    def expression(self,
                   expression_type: Optional[str],
                   expression_label: Optional[str],
                   expression_year: Optional[int],
                   expression_is_preferred: Optional[int],
                   expression_language: Optional[Union[str, int]],
                   expression_mode: Optional[str],
                   expression_title_override: Optional[str],
                   expression_subtitle: Optional[str],
                   expression_wordcount: Optional[int],
                   expression_fiction_length_category: Optional[int],
                   expression_cut_type: Optional[str],
                   expression_nominal_duration_seconds: Optional[int],
                   expression_status: Optional[str],
                   expression_origin_note: Optional[str],
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

    def manifestation(self,
                      manifestation_carrier_type: Optional[str],
                      manifestation_format_detail: Optional[str],
                      manifestation_edition_statement: Optional[str],
                      manifestation_pub_year: Optional[int],
                      manifestation_pub_date: Optional[Union[datetime.datetime, str, int, float]],
                      manifestation_page_count: Optional[int],
                      manifestation_runtime_minutes: Optional[int],
                      manifestation_region_code: Optional[str],
                      manifestation_status: Optional[str],
                      manifestation_note: Optional[str]) -> RowAPI:
        """
        Add methods for the Manifestation table.

        :param manifestation_carrier_type:
        :param manifestation_format_detail:
        :param manifestation_edition_statement:
        :param manifestation_pub_year:
        :param manifestation_pub_date:
        :param manifestation_page_count:
        :param manifestation_runtime_minutes:
        :param manifestation_region_code:
        :param manifestation_status:
        :param manifestation_note:
        :return:
        """

    def item(self,
             item_id: Optional[int],
             item_manifestation_id: Optional[str],
             item_type: Optional[str],
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



