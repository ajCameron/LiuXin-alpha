


"""
Add validated cover payloads to legacy metadata value-to-row storage.

Cover tuples contain a type marker and payload. File-like inputs are consumed to
bytes before validation; the original stream is not retained or closed by add_cover.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py
"""
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
# from LiuXin_alpha.metadata import check_isbn
# from LiuXin_alpha.metadata import string_to_authors
# from LiuXin_alpha.metadata import authors_to_sort_string
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
    Supply cover insertion for an owner with _data and register_file_for_cleanup.

    The owner must provide a cover_data mapping; the mixin does not initialize storage.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py
    """
    # Todo: Check that the tuple is the right way round for calibre
    def add_cover(self, data, typ="path", cover_id=None):
        """
        Read a stream if supplied, validate the cover tuple, and store its optional row id.

        check_image_tuple decides validity and provides the InputIntegrityError message on
        failure. A successful call registers the resulting payload for cleanup, which is
        bytes after a stream read. Register the original stream separately if metadata
        should later close it.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


        :param data: Cover path, bytes, or readable payload; a stream is read from its
            current position.
        :param typ: Cover type marker, defaulting to path.
        :param cover_id: Optional database row id associated with this cover tuple.
        :return: None.
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
