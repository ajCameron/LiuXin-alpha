


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

from LiuXin_alpha.metadata.standardize import standardize_identifier_value

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata.help_methods import BookMetadataHelpMixin

from LiuXin_alpha.metadata.identifiers import clean_id_key, clean_id_value



from LiuXin_alpha.errors import InputIntegrityError



class IdentifiersMethodsMixin:
    """
    Methods to enable the class to handle identifiers.
    """

    @property
    def identifiers(self) -> dict[str, list[str]]:
        """
        Returns the identifiers dict for this book.

        :return:
        """
        _data = object.__getattribute__(self, "_data")

        ids_dict = dict()
        for id_type in EXTERNAL_EBOOK_ID_SCHEMA:
            if id_type in _data:
                ids_dict[id_type] = deepcopy([id_val for id_val in _data[id_type].keys()])
        return deepcopy(ids_dict)

    @property
    def internal_identifiers(self) -> dict[str, list[str]]:
        """
        Returns all the internal identifiers for this book.

        :return:
        """
        _data = object.__getattribute__(self, "_data")

        ids_dict = dict()
        for id_type in INTERNAL_EBOOK_ID_SCHEMA:
            if id_type in _data:
                ids_dict[id_type] = deepcopy([id_val for id_val in _data[id_type].keys()])
        return deepcopy(ids_dict)

    def get_identifiers(self):
        """
        Returns the identifiers as a dictionary of sets.

        The dict is small, and the penalty for using a reference where a copy is needed is large. Also, we don't want
        any manipulations of the returned dict to show up in the book.
        You can pass it back in using the set_identifiers method.
        For calibre emulation.
        :return identifiers_dict:
        """
        _data = object.__getattribute__(self, "_data")
        ids_dict = dict()

        for id_type in EXTERNAL_EBOOK_ID_SCHEMA:
            if id_type in _data:
                ids_dict[id_type] = set(_data[id_type].keys())
        return deepcopy(ids_dict)

    def get_internal_identifiers(self):
        """
        Return a copy of the internal identifiers dictionary of sets.
        The dict is small, and the penalty for using a reference where a copy is needed is large. Also, we don't want
        any manipulations of the returned dict to show up in the book.
        You can pass it back in using the set_internal_identifiers method.
        For calibre emulation.
        :return identifiers_dict:
        """
        _data = object.__getattribute__(self, "_data")
        ids_dict = dict()
        for id_type in INTERNAL_EBOOK_ID_SCHEMA:
            if id_type in _data:
                ids_dict[id_type] = set(_data[id_type].keys())
        return deepcopy(ids_dict)

    # copied from calibre
    @staticmethod
    def _clean_identifier(typ, val):
        """
        Attempts to tidy a type, value pair of identifiers

        :param typ: The type of the identifier
        :param val: The value of the identifier
        :return typ, val: Cleaned pair
        """
        if typ:
            typ = clean_id_key(typ)
        if val:
            val = clean_id_value(val)
        return typ, val

    def read_identifiers(self, identifiers):
        """
        Front end for the set_identifiers method.

        :param identifiers:
        :return:
        """
        return self.set_identifiers(identifiers)

    # copied from calibre
    def set_identifiers(
            self,
            identifiers: dict[str, Optional[Union[list[str], tuple[str, ...], set[str], str, dict[str, Optional[int]]]]],
            update: bool = True) -> None:
        """
        Set all identifiers. Note that, if any of the identifiers mentioned in the :param identifiers: dict have already
        been set, this method will delete them.

        calibre compliant.
        :param identifiers: A dictionary of identifiers keyed by the types, valued with the identifiers - either as a
                            string, a set or an OrderedDict.
        :param update: Whether to update the identifiers dict or not.:
        :return:
        """
        _data = object.__getattribute__(self, "_data")

        cleaned = {clean_id_key(k): v for k, v in iteritems(identifiers) if k and v}
        for typ in cleaned:

            typ_stand = standardize_id_name(typ)

            # If standardization has nullified the type, then it will have been logged and we can continue
            if typ_stand is None:
                continue

            # If the type is not, already, in data then we can add it.
            if typ_stand not in _data:
                _data[typ_stand] = dict()

            ids = cleaned[typ]

            if isinstance(ids, six_string_types):
                ids = clean_id_value(ids)
                _data[typ_stand][ids] = None

            # In this case, we have a dict keyed with the id, and valued with its database id.
            # Which is just an int.
            elif isinstance(ids, OrderedDict):

                # Clean up the vals before writing out
                cleaned_vals_dict = OrderedDict()
                for key, val in ids.items():
                    cleaned_vals_dict[clean_id_value(key)] = val

                # If not update, wipe it
                if not update:
                    _data[typ_stand] = deepcopy(ids)
                else:
                    _data[typ_stand].update(deepcopy(ids))

            elif isinstance(ids, (list, tuple, set, frozenset)):
                for id_val in ids:
                    _data[typ_stand][clean_id_value(id_val)] = None

            else:
                err_str = "Unable to add identifiers - format not recognized"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("typ", typ),
                    ("typ_stand", typ_stand),
                    ("ids", ids),
                    ("ids_type", type(ids)),
                )
                raise NotImplementedError(err_str)

    # copied from calibre
    def set_identifier(self, typ: str, val: Optional[str, list[str], set[str]]) -> None:
        """
        Add an identifier - if the value is None, deletes all the identifiers of that type.

        calibre compliant
        :param typ:
        :param val:
        :return:
        """
        _data = object.__getattribute__(self, "_data")

        typ, val = self._clean_identifier(typ, val)
        typ = standardize_id_name(typ)

        if typ is not None and typ in _data:

            if val is None:
                _data[typ] = OrderedDict()
            else:
                _data[typ][val] = None

        elif typ is not None and typ not in _data:
            _data[typ] = OrderedDict()
            _data[typ][val] = None

        elif typ is None:
            return

        else:
            raise NotImplementedError("This position should never be reached.")

    # copied from calibre
    def has_identifier(self, typ):
        """
        Tests to see if a type of identifier has been added.

        calibre compliant.
        :param typ:
        :return:
        """
        typ_stand = standardize_id_name(typ)
        if typ_stand is None:
            return False

        _data = object.__getattribute__(self, "_data")
        if typ_stand not in _data:
            return False
        return True if _data[typ_stand] else False


    def add_identifiers(self, identifiers: dict[str, Union[str, list[str], set[str]]]) -> None:
        """
        Takes a dictionary of identifiers keyed by their type. Adds them in.

        If the value is a string then adds it in directly. If the value is a set or other iterable iterates over it
        and adds them all in.
        :param identifiers:
        :return:
        """
        _data = object.__getattribute__(self, "_data")
        identifiers = deepcopy(identifiers)

        for id_type in identifiers:

            value = identifiers[id_type]
            s_id_type = standardize_id_name(id_type)

            # If the id cannot be standardized, then we can't proceed.
            if s_id_type is None:
                err_str = "Unable to standardize identifier type"
                default_log.log_variables(err_str, "ERROR", ("id_type", id_type), ("identifiers", identifiers))
                continue

            # Checks to see if the identifier is a simple string - if it is then just store it
            if isinstance(value, six_string_types):
                value = standardize_identifier_value(value)

                # Creating the set to store the value if required
                if s_id_type not in _data:
                    _data[s_id_type] = dict()
                    _data[s_id_type][value] = None
                    return

                # If the id type already exists store the value in it and proceed
                _data[s_id_type][value] = None
                return

            # If the identifiers object isn't a base string, itterate over it and add every value to the set
            if s_id_type not in _data:
                _data[s_id_type] = dict()

            for id_val in value:
                id_val = standardize_identifier_value(id_val)
                _data[s_id_type][id_val] = None

    # Todo: These two classes are basic identical. DRY.
    def add_internal_identifiers(self, identifiers):
        """
        Takes a dictionary of identifiers keyed by their type - adds them.

        If the value is a string
        :param identifiers:
        :return:
        """
        _data = object.__getattribute__(self, "_data")

        identifiers = deepcopy(identifiers)

        for id_type in identifiers:

            value = identifiers[id_type]
            s_id_type = standardize_internal_id_name(id_type)

            # If the id cannot be standardized, then we can't proceed.
            if s_id_type is None:
                raise InputIntegrityError(None)

            # Checks to see if the identifier is a simple string - if it is, just store it
            if isinstance(value, six_string_types):
                value = standardize_identifier_value(value)

                # Creating the dict to store the value if required
                if s_id_type not in _data:
                    _data[s_id_type] = dict()
                    _data[s_id_type][value] = None
                    continue

                # If the id type already exists store the value in it and proceed
                _data[s_id_type][value] = None
                continue

            # If the identifiers object isn't a base string, itterate over it and add every value to the set
            if s_id_type not in _data:
                _data[s_id_type] = dict()

            for id_val in value:
                id_val = standardize_identifier_value(id_val)
                _data[s_id_type][id_val] = None

    def _set_identifier_from_normed_key(self, identifiers_key: str, value: str) -> None:
        """
        Set an identifier from a normalized key

        :param normed_identifier_key:
        :param value:
        :return:
        """
        _data = object.__getattribute__(self, "_data")
        if identifiers_key in _data:
            _data[identifiers_key][value] = None
            return
        elif identifiers_key not in _data:
            _data[identifiers_key] = OrderedDict()
            _data[identifiers_key][value] = None
            return
        else:
            raise NotImplementedError

    def _set_internal_identifier_from_normed_key(self, internal_ids_key: str, value: str) -> None:
        """
        Set an internal identifier from a normalized key

        :param internal_ids_key:
        :param value:
        :return:
        """
        _data = object.__getattribute__(self, "_data")
        if internal_ids_key in _data:
            _data[internal_ids_key][value] = None
            return
        elif internal_ids_key not in _data:
            _data[internal_ids_key] = OrderedDict()
            _data[internal_ids_key][value] = None
            return
        else:
            raise NotImplementedError


