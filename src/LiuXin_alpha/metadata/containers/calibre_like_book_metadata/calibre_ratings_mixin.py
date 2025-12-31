

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

class RatingsMethodsMixin:
    """
    Mixin for the ratings method.
    """

    def _set_ratings_from_value(self, value):

        _data = object.__getattribute__(self, "_data")

        if isinstance(value, (tuple, list)):
            if len(value) == 2:
                o_value = value[0]
                st_value = standardize_rating_type(o_value)
                if st_value is not None:
                    _data["ratings"][st_value] = value[1]
                elif st_value is None:
                    err_str = "Unable to match rating type to the known rating types.\n"
                    default_log.log_variables(
                        err_str,
                        "WARNING",
                        ("rating_type", o_value),
                        ("value", value),
                    )
                    _data["ratings"][o_value] = value[1]
                else:
                    raise NotImplementedError("This position should never be reached")
            else:
                err_str = "Rating not properly formed"
                default_log.log_variables(err_str, "WARNING", ("value", value))

        elif isinstance(value, dict):
            for rating_type in value:
                s_rating_type = standardize_rating_type(rating_type)
                rating = value[rating_type]
                if s_rating_type is not None:
                    _data["ratings"][s_rating_type] = rating
                elif s_rating_type is None:
                    err_str = "Unable to match rating type to the known rating types.\n"
                    default_log.log_variables(err_str, "WARNING", ("rating_type", rating_type))
                    _data["ratings"][rating_type] = rating
                else:
                    raise NotImplementedError("This position should never be reached")
        else:
            raise NotImplementedError("This position should never be reached.")
        return

