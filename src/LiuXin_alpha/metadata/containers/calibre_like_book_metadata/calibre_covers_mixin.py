


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


from LiuXin_alpha.errors import InputIntegrityError



class CoverMethodsMixin:
    """
    Methods to manipulate covers and cover data.
    """
    # Todo: Check that the tuple is the right way round for calibre
    def add_cover(self, data, typ="path", cover_id=None):
        """
        Takes image data, followed by the type of data it is. Preforms some basic checks and adds it to the object,
        Cover tuples are of the form typ (generally the extension of the file) followed by data - the data the file
        is composed of.
        Note - open file handles are fragile and can be closed by return statements - if an open file handle is passed
        into this method it will be read into memory and the data included here. Dump it to disk and pass in a path
        instead?
        If you have open file handlers produced by a read process please include them in register_file_for_cleanup so
        that they can be properly closed after being added to the databases.
        :param data:
        :param typ:
        """
        if hasattr(data, "read"):
            data = data.read()

        image_tuple = (typ, data)
        status, message = check_image_tuple(image_tuple)

        # Write the data directly into _data - to avoid a call to __setattr__
        _data = object.__getattribute__(self, "_data")

        if status:
            _data["cover_data"][image_tuple] = cover_id
        else:
            raise InputIntegrityError(message)

        self.register_file_for_cleanup(data)
