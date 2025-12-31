
from __future__ import division, absolute_import, print_function, annotations

from typing import Optional, Union

import os
import re
import pprint
from collections import OrderedDict
from copy import deepcopy
from numbers import Number

from LiuXin_alpha.constants import ALLOWED_DOC_TYPES
from LiuXin_alpha.constants import check_image_tuple

# from LiuXin.databases.row_collection import RowCollection
#
# from LiuXin.exceptions import InputIntegrityError
# from LiuXin.exceptions import LogicalError
# from LiuXin.exceptions import DatabaseIntegrityError
#
# from LiuXin.file_formats.chardet import force_encoding
#
# from LiuXin.metadata import check_isbn
# from LiuXin.metadata import string_to_authors
# from LiuXin.metadata import authors_to_sort_string
# from LiuXin.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.constants import CREATOR_DROP_REGEX_SET, CREATOR_CATEGORIES, CREATOR_TYPES, CREATOR_TYPE_CAT_DIR, EXTERNAL_EBOOK_ID_SCHEMA, EXTERNAL_EBOOK_REKEY_SCHEME
from LiuXin_alpha.metadata.constants import INTERNAL_EBOOK_ID_SCHEMA
from LiuXin_alpha.metadata.constants import INTERNAL_EBOOK_REKEY_SCHEME
from LiuXin_alpha.metadata.constants import METADATA_NULL_VALUES
from LiuXin_alpha.metadata.standardize import standardize_id_name, standardize_creator_category, string_to_authors, standardize_lang, standardize_internal_id_name, standardize_rating_type, standardize_tag

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iterkeys as iterkeys
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata.help_methods import BookMetadataHelpMixin


from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError, LogicalError


class FactoryMethodsMixin:
    """
    Mixin for the factory methods.
    """
    def from_title_row(self, title_row):
        """
        Takes a title row from the databases. Uses it to populate all the metadata associated with that row.

        All data is deleted before the new data is copied in.
        Some fields are not populated.
        All comments are just stored in notes.
        All imprints are just stored as publishers (though an attempt is made after to work out which is which).
        Some fields are flat out ignored.
        :param title_row: A Row
        :return:
        """
        _data = object.__getattribute__(self, "_data")

        db = title_row.db

        title_row_collection = RowCollection(title_row)

        # Loading the title sections
        title_row_dict = title_row_collection["titles"][0]
        _data["title"] = title_row_dict["title"]
        _data["title_sort"] = title_row_dict["title"]
        _data["wordcount"] = title_row_dict["title_wordcount"]
        _data["pubdate"] = title_row_dict["title_pubdate"]

        # Transfer tables which have a standard interface (one column needed from each - associate the name of that
        # column with the id of the entry in that table)
        main_tables = deepcopy(db.get_categorized_tables()["main"])
        standard_tables = [
            "genres",
            "notes",
            "publishers",
            "series",
            "synopses",
            "subjects",
            "tags",
        ]

        for table in standard_tables:

            standard_rows = title_row_collection[table]
            standard_display_column = db.get_display_column(table)
            for standard_link in standard_rows:
                dis_value = standard_link[standard_display_column]

                if table not in _data:
                    _data[table] = OrderedDict()

                try:
                    if dis_value not in _data[table]:
                        _data[table][dis_value] = standard_link
                except KeyError as e:
                    if table == "genres":
                        if dis_value not in _data["genres"]:
                            _data["genres"][dis_value] = standard_link
                    else:
                        raise KeyError(f"Error on table {table}") from e

        creator_rows = title_row_collection["creators"]
        for creator_link in creator_rows:
            creator_type = creator_link["creator_title_link_type"]
            if creator_type is None:
                wrn_str = "Unable to match creator type.\nDefaulting to author.\n"
                default_log.log_variables(
                    wrn_str,
                    "WARN",
                    ("creator_link['creator_id']", creator_link["creator_id"]),
                )
            creator_name = creator_link["creator"]
            creator_type = standardize_creator_category(creator_type)
            if creator_type in _data:
                _data[creator_type][creator_name] = creator_link
            elif creator_type not in _data:
                _data[creator_type] = OrderedDict()
                _data[creator_type][creator_name] = creator_link
            else:
                raise LogicalError
        try:
            main_tables.remove("creators")
        except ValueError:
            pass

        identifier_rows = title_row_collection["identifiers"]
        for id_link in identifier_rows:
            id_type = id_link["identifier_type"]
            id_value = id_link["identifier"]
            external_id_type = standardize_id_name(id_type)
            internal_id_type = standardize_internal_id_name(id_type)

            # Preforming sanity checks on the claimed values
            if external_id_type is None and internal_id_type is None:
                err_str = "Unable to normalize id name.\n"
                err_str += "Consider sources of error.\n"
                err_str += "id_type: " + six_unicode(id_type) + "\n"
                err_str += "id_value: " + six_unicode(id_value) + "\n"
                err_str += "id_row: " + six_unicode(id_link) + "\n"
                raise DatabaseIntegrityError(err_str)
            elif external_id_type is not None and internal_id_type is None:
                final_id_type = external_id_type
            elif external_id_type is None and internal_id_type is not None:
                final_id_type = internal_id_type
            elif external_id_type is not None and internal_id_type is not None:
                err_str = "Identifier matched of being both internal and external type.\n"
                err_str += "Consider sources of error.\n"
                err_str += "external_id_type: " + six_unicode(external_id_type) + "\n"
                err_str += "internal_id_type: " + six_unicode(internal_id_type) + "\n"
                err_str += "id_type: " + six_unicode(id_type) + "\n"
                err_str += "id_value: " + six_unicode(id_value) + "\n"
                err_str += "id_row: " + six_unicode(id_link) + "\n"
                raise InputIntegrityError(err_str)

            if final_id_type in _data:
                _data[final_id_type].add(id_value)
            elif final_id_type not in _data:
                _data[final_id_type] = set()
                _data[final_id_type].add(id_value)

        lang_rows = title_row_collection["languages"]
        if len(lang_rows) > 0:
            lang_row = lang_rows[0]
            _data["language"] = lang_row["language"]
        for lang_link in lang_rows:
            lang_name = lang_link["language"]
            _data["languages_available"][lang_name] = lang_link

        # If an entry is below the top level of the publishers table, it's considered an imprint.
        pub_rows = title_row_collection["publishers"]
        for pub_link in pub_rows:
            pub_name = pub_link["publisher"]
            pub_parent = pub_link["publisher_parent"]

            if "imprints" not in _data:
                _data["imprints"] = OrderedDict()

            if "publishers" not in _data:
                _data["publishers"] = OrderedDict()

            if pub_parent is None or pub_parent.lower() == "none":
                _data["imprints"][pub_name] = pub_link
            else:
                _data["publishers"][pub_name] = pub_link

        # The series rows are loaded as normal. The series index is given for the position of the title in the first
        # series it's linked to.
        series_rows = title_row_collection["series"]
        if len(series_rows) > 0:
            main_series_row = series_rows[0]
            _data["series_index"] = main_series_row["series_title_link_priority"]


