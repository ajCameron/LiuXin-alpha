


"""
Manage external and internal identifier stores with legacy Calibre-compatible helpers.

Each scheme stores identifier values as keys with optional row ids. Snapshot
properties expose lists; get methods expose sets. Cleaning, merge behavior, and
supported input forms differ between insertion methods.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py
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

from LiuXin_alpha.metadata.standardize import standardize_identifier_value

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata.help_methods import BookMetadataHelpMixin

from LiuXin_alpha.metadata.identifiers import clean_id_key, clean_id_value



from LiuXin_alpha.errors import InputIntegrityError



class IdentifiersMethodsMixin:
    """
    Provide identifier snapshots and updates for an owner exposing the _data stores.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py
    """

    @property
    def identifiers(self) -> dict[str, list[str]]:
        """
        Copy recognized external identifier values into per-scheme lists.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :return: New scheme-to-list mapping, including empty recognized stores.
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
        Copy recognized internal identifier values into per-scheme lists.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :return: New internal-scheme-to-list mapping, including empty stores.
        """
        _data = object.__getattribute__(self, "_data")

        ids_dict = dict()
        for id_type in INTERNAL_EBOOK_ID_SCHEMA:
            if id_type in _data:
                ids_dict[id_type] = deepcopy([id_val for id_val in _data[id_type].keys()])
        return deepcopy(ids_dict)

    def get_identifiers(self):
        """
        Copy external identifier values into sets, omitting database row values.

        Example:
            >>> from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
            >>> book = CalibreLikeLiuXinBookMetaData()
            >>> book.set_identifier('isbn', '9780306406157')
            >>> book.get_identifiers()['isbn'] == {'9780306406157'}
            True


        :return: Independent scheme-to-set dictionary.
        """
        _data = object.__getattribute__(self, "_data")
        ids_dict = dict()

        for id_type in EXTERNAL_EBOOK_ID_SCHEMA:
            if id_type in _data:
                ids_dict[id_type] = set(_data[id_type].keys())
        return deepcopy(ids_dict)

    def get_internal_identifiers(self):
        """
        Copy internal identifier values into sets, omitting database row values.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :return: Independent internal-scheme-to-set dictionary.
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
        Clean truthy scheme/value inputs with shared identifier normalizers; retain false inputs.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param typ: Identifier scheme string or false value.
        :param val: Identifier value accepted by clean_id_value, or false input.
        :return: Pair of cleaned scheme and value.
        """
        if typ:
            typ = clean_id_key(typ)
        if val:
            val = clean_id_value(val)
        return typ, val

    def read_identifiers(self, identifiers):
        """
        Forward a scheme mapping to set_identifiers with its default merge policy.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param identifiers: Scheme-to-identifier input accepted by set_identifiers.
        :return: Return from set_identifiers, normally None.
        """
        return self.set_identifiers(identifiers)

    # copied from calibre
    def set_identifiers(
            self,
            identifiers: dict[str, Optional[Union[list[str], tuple[str, ...], set[str], str, dict[str, Optional[int]]]]],
            update: bool = True) -> None:
        """
        Add external identifiers from supported scheme-value shapes.

        False keys/values are omitted and unrecognized normalized schemes are skipped.
        Strings and sequences/sets add cleaned values with None row ids. OrderedDict input
        is copied and either merged or replaced according to update; its original keys are
        retained despite constructing a cleaned temporary mapping. Plain dict values are
        unsupported. Unmentioned schemes are never cleared.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param identifiers: Scheme mapping with string, list, tuple, set/frozenset, or
            OrderedDict values.
        :param update: Whether OrderedDict values merge; false replaces only that scheme for
            OrderedDict input.
        :return: None.
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
        Clean and normalize an external scheme, then insert its value or clear its existing store.

        None clears an existing scheme; false non-None values are stored as values. Unknown
        schemes are ignored. If a recognized store is absent it is created and the supplied
        value is inserted, including None.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param typ: External scheme or recognized alias.
        :param val: Identifier value accepted by the cleaner, or None to clear an existing
            scheme.
        :return: None.
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
        Normalize a scheme and test whether its stored mapping is nonempty.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param typ: External identifier scheme or alias.
        :return: True for a recognized nonempty external identifier store.
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
        Normalize external scheme names and add copied scalar or iterable values.

        Unknown schemes are logged and skipped. A scalar string is normalized, inserted, and
        returns from the entire method; use iterable values to process later schemes in the
        same call.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param identifiers: External scheme-to-string-or-iterable mapping.
        :return: None.
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
        Normalize internal schemes and add each copied string or iterable value.

        An unknown scheme produces InputIntegrityError. Unlike add_identifiers, a scalar
        string continues to later schemes.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param identifiers: Internal scheme-to-string-or-iterable mapping.
        :return: None.
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
        Insert an unmodified external identifier value with a None row id, creating the store if absent.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param identifiers_key: Already normalized external scheme key.
        :param value: Hashable identifier value; no cleaning is performed.
        :return: None.
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
        Insert an unmodified internal identifier value with a None row id, creating the store if absent.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_identifiers.py


        :param internal_ids_key: Already normalized internal scheme key.
        :param value: Hashable identifier value; no cleaning is performed.
        :return: None.
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


