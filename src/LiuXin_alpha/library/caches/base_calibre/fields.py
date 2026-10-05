"""
Model cached library fields and their value mappings.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise fields through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from __future__ import unicode_literals, division, absolute_import, print_function

import os
from datetime import datetime
from locale import atof
from functools import partial

from typing import Optional, Callable, Any, TypeVar, Union, Iterable, Generic, Iterator, Mapping

from LiuXin_alpha.customize.cache.base_field import BaseField

from LiuXin_alpha.library.tag_classes import BaseTagClass
from LiuXin_alpha.databases.utils import force_to_bool
from LiuXin_alpha.caches.write import get_writer, DummyWriter
from LiuXin_alpha.databases.db_types import (
    LangMap,
    SrcTableID,
    DstTableID,
    CreatorDataDict,
    SpecificFormat,
    GenericFormat,
    CoverID,
)

from LiuXin_alpha.errors import NotInCache

from LiuXin_alpha.metadata.ebook_metadata_tools import author_to_author_sort

from LiuXin_alpha.preferences import preferences as tweaks

from LiuXin_alpha.utils.date import UNDEFINED_DATE, clean_date_for_sort, parse_date
from LiuXin_alpha.utils.text.icu import sort_key
from LiuXin_alpha.utils.localization import calibre_langcode_to_name, trans as _

T = TypeVar("T")
D = TypeVar("D")


def bool_sort_key(
    bools_are_tristate: bool,
) -> Callable[[Any,], Optional[bool]]:
    """
    Returns a sort key suitable for use with tristate bools.

    Example:
        Exercise bool sort key through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param bools_are_tristate: Value supplied for bools are tristate under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (
        (lambda x: {True: 1, False: 2, None: 3}.get(x, 3))
        if bools_are_tristate
        else lambda x: {True: 1, False: 2, None: 2}.get(x, 2)
    )


def identity(x: D) -> D:
    """
    Just returns itself.

    Example:
        Exercise identity through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return x


IDENTITY = identity


class InvalidLinkTable(Exception):
    """
    Raised when trying to link two tables which are not linkable.

    Example:
        Exercise InvalidLinkTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name) -> None:
        """
        Initialize and validate the invalidlinktable state.

        Example:
            Exercise InvalidLinkTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        Exception.__init__(self, name)
        self.field_name = name


class CalibreBaseField(BaseField[T]):
    """
    Basis for a representation of a field on the database. Usually organized via the book.

    Example:
        Exercise CalibreBaseField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(
        self,
        name: str,
        table: str,
        bools_are_tristate: bool,
        # generic_val: D = "",  # Todo: This seems to be a good way to get typing info into the system
        link_attributes = None,
        main_table: Optional[str] = None,
        auxiliary_table: Optional[str] = None,
    ) -> None:
        """
        Startup the field.

        Example:
            Exercise CalibreBaseField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :param link_attributes: Value supplied for link attributes under the utility
            contract.
        :param main_table: Value supplied for main table under the utility contract.
        :param auxiliary_table: Value supplied for auxiliary table under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(
            name=name,
            table=table,
            bools_are_tristate=bools_are_tristate,
            link_attributes=link_attributes,
            main_table=main_table,
            auxiliary_table=auxiliary_table,
        )
        dt: str = self.metadata["datatype"]

        # Characterize the additional properties of the link
        if link_attributes is not None:
            self.link_attributes = link_attributes
        else:
            try:
                self.link_attributes = self.metadata["link_attrs"]
            except KeyError:
                self.link_attributes = None

        if main_table is not None:
            self.main_table = main_table
        else:
            try:
                self.main_table = self.metadata["main_table"]
            except KeyError:
                self.main_table = None
        if auxiliary_table is not None:
            self.auxiliary_table = auxiliary_table
        else:
            try:
                self.auxiliary_table = self.metadata["auxiliary_table"]
            except KeyError:
                self.auxiliary_table = None

        # This will be compared to the output of sort_key() which is a bytestring, therefore it is safer to have it be a
        # bytestring.
        # Coercing an empty bytestring to unicode will never fail, but the output of sort_key cannot be coerced to
        # unicode.
        self._default_sort_key: Optional[Union[bytes, int]] = b""

        if dt in {"int", "float", "rating"}:
            self._default_sort_key = 0

        elif dt == "bool":
            self._default_sort_key = None

            self._sort_key = bool_sort_key(bools_are_tristate)

        elif dt == "datetime":

            self._default_sort_key = UNDEFINED_DATE

            if tweaks["sort_dates_using_visible_fields"]:
                fmt = None
                if name in {"timestamp", "pubdate", "last_modified"}:
                    fmt = tweaks["gui_%s_display_format" % name]
                elif self.metadata["is_custom"]:
                    fmt = self.metadata.get("display", {}).get("date_format", None)
                self._sort_key = partial(clean_date_for_sort, fmt=fmt)

        if self.name == "languages":

            self._sort_key = lambda x: sort_key(calibre_langcode_to_name(x))

        self.is_multiple = bool(self.metadata["is_multiple"]) or self.name == "formats"

        self.sort_sort_key = True

        if self.is_multiple and "&" in self.metadata["is_multiple"]["list_to_ui"]:
            self._sort_key = lambda x: sort_key(author_to_author_sort(x))
            self.sort_sort_key = False

        if name == "identifier":
            self._default_value = {}
        elif name == "tags":
            self._default_value = set()
        elif name == "languages":
            self._default_value = None
        else:
            self._default_value = () if self.is_multiple else None
        self.category_formatter = type("")

        if dt == "rating":
            self.category_formatter = lambda x: "\u2605" * int(x / 2)

        elif name == "languages":
            self.category_formatter = calibre_langcode_to_name

        # Used to preform writes out to the actual database
        self._writer = get_writer(self)
        self.series_field = None

        try:
            self.table.writer = self._writer
        # Table probably doesn't exist
        except AttributeError:
            pass

        # We need to start additional link attribute fields to characterize additional attributes of the link
        self.link_attr_fields = dict()
        self.startup_link_attr_fields()

    # --------------
    #
    # - READ METHODS
    # The read logic is confined to the individual tables - however the separate attribute fields contain tables
    # which must also be individually read
    def read_attribute_tables(self, db) -> None:
        """
        Preform a read of data from the database into the attribute fields contained within this field.

        Example:
            Exercise CalibreBaseField.read attribute tables through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for attr_field_name in self.link_attr_fields:
            self.link_attr_fields[attr_field_name].table.read(db)

    #
    # --------------


class CalibreBaseOneToOneField(CalibreBaseField[T]):
    """
    A 1-1 mapping must exist between a table and the one represented by this field.

    Example:
        Exercise CalibreBaseOneToOneField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def ids_for_book(self, book_id: SrcTableID) -> tuple[DstTableID, ...]:
        """
        In the case of a 1-1 table the item id is the same as the book - as it's stored in the same row of the db.

        Example:
            Exercise CalibreBaseOneToOneField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.book_in_cache(book_id):
            return tuple(
                [
                    book_id,
                ]
            )
        else:
            raise NotInCache

    def books_for(self, item_id: DstTableID) -> set[SrcTableID]:
        """
        In the case of a 1-1 table the item id is the same as the book - as it's stored in the same row of the db.

        Example:
            Exercise CalibreBaseOneToOneField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.item_in_cache(item_id):
            return {
                item_id,
            }
        else:
            raise NotInCache

    def book_in_cache(self, book_id: SrcTableID) -> bool:
        """
        Checks that the given book is in the cache - returns True iff the book exists in the cache and False otherwise.

        Example:
            Exercise CalibreBaseOneToOneField.book in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def item_in_cache(self, item_id: DstTableID) -> bool:
        """
        Checks that the given item is in the cache - returns True iff the item is in the cache and False otherwise.

        Example:
            Exercise CalibreBaseOneToOneField.item in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseOneToManyField(CalibreBaseOneToOneField):
    """
    For a Many-to-Many or One-to-Many table that has to pretend to be a 1-1 table.

    Example:
        Exercise BaseOneToManyField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def ids_for_book(self, book_id: SrcTableID) -> set[DstTableID]:
        """
        The table is pretending to be 1-1 - so this method does not make sense.

        Example:
            Exercise BaseOneToManyField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> set[SrcTableID]:
        """
        The table is pretending to be 1-1 - so this method does not make sense.

        Example:
            Exercise BaseOneToManyField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseCompositeField(CalibreBaseOneToOneField):
    """
    A composite field is composed of a composite of metadata from other fields.

    Example:
        Exercise BaseCompositeField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    is_composite: bool = True
    SIZE_SUFFIX_MAP: dict[str, int] = {suffix: i for i, suffix in enumerate(("", "K", "M", "G", "T", "P", "E"))}

    def __init__(self, name: str, table, bools_are_tristate: bool) -> None:
        """
        Construct a composite field - a field composed of multiple other pieces of data.

        Example:
            Exercise BaseCompositeField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreBaseOneToOneField.__init__(self, name, table, bools_are_tristate)

        m = self.metadata
        self._composite_name = "#" + m["label"]

        try:
            self.splitter = m["is_multiple"].get("cache_to_list", None)
        except AttributeError:
            self.splitter = None

        composite_sort = m.get("display", {}).get("composite_sort", None)
        if composite_sort == "number":
            self._default_sort_key = 0
            self._sort_key = self.number_sort_key

        elif composite_sort == "date":
            self._default_sort_key = UNDEFINED_DATE
            self._filter_date = lambda x: x
            if tweaks["sort_dates_using_visible_fields"]:
                fmt = m.get("display", {}).get("date_format", None)
                self._filter_date = partial(clean_date_for_sort, fmt=fmt)
            self._sort_key = self.date_sort_key

        elif composite_sort == "bool":
            self._default_sort_key = None
            self._bool_sort_key = bool_sort_key(bools_are_tristate)
            self._sort_key = self.bool_sort_key

        elif self.splitter is not None:
            self._default_sort_key = ()
            self._sort_key = self.multiple_sort_key

        else:
            self._sort_key = sort_key

    def multiple_sort_key(self, val: str) -> tuple[str, ...]:
        """
        Split the multiple entries into a tuple and sort them using `sort_key`.

        Example:
            Exercise BaseCompositeField.multiple sort key through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        val = (sort_key(x.strip()) for x in (val or "").split(self.splitter))
        return tuple(sorted(val))

    def number_sort_key(self, val: str) -> Union[int, float, str]:
        """
        Produces a sort key from a numerical value.

        Example:
            Exercise BaseCompositeField.number sort key through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            p = 1
            if val and val.endswith("B"):
                p = 1 << (10 * self.SIZE_SUFFIX_MAP.get(val[-2:-1], 0))
                val = val[: (-2 if p > 1 else -1)].strip()
            val = atof(val) * p
        except (TypeError, AttributeError, ValueError, KeyError):
            val = 0.0
        return val

    def date_sort_key(self, val: Union[str, datetime]) -> datetime:
        """
        Produce a sort key from a date value

        Example:
            Exercise BaseCompositeField.date sort key through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            val = self._filter_date(parse_date(val))
        except (TypeError, ValueError, AttributeError, KeyError):
            val = UNDEFINED_DATE
        return val

    def bool_sort_key(self, val: Any) -> Optional[bool]:
        """
        Produce a sort key from any value.

        Example:
            Exercise BaseCompositeField.bool sort key through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._bool_sort_key(force_to_bool(val))

    def clear_caches(self, book_ids: Optional[Iterator[str]] = None) -> bool:
        """
        Clear the internal caches stored in the field.

        Example:
            Exercise BaseCompositeField.clear caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_value_with_cache(self, book_id: SrcTableID, get_metadata: Callable[[...], ...]) -> Any:
        """
        Return a value using the composite cache.

        Example:
            Exercise BaseCompositeField.get value with cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param get_metadata: Value supplied for get metadata under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def sort_keys_for_books(self, get_metadata: Callable[[...], ...], lang_map: LangMap) -> Any:
        """
        Return sort keys for all books.

        Example:
            Exercise BaseCompositeField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterator[SrcTableID], default_value: Optional[D] = None
    ) -> Iterator[D]:
        """
        Iter all searchable values.

        Example:
            Exercise BaseCompositeField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_composite_categories(
        self,
        tag_class,
        book_rating_map: dict[SrcTableID, float],
        book_ids: Iterable[SrcTableID],
        is_multiple: bool,
        get_metadata: Callable[[...], ...],
    ):
        """
        Return the categories for the current composite field.

        Example:
            Exercise BaseCompositeField.get composite categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param book_ids: Book identities included in the batched read operation.
        :param is_multiple: Value supplied for is multiple under the utility contract.
        :param get_metadata: Value supplied for get metadata under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_books_for_val(
        self, value: T, get_metadata: Callable[[...], ...], book_ids: Iterable[SrcTableID]
    ) -> Iterator[SrcTableID]:
        """
        Iterate through all values - generating the custom values and checking to see if books match those.

        Example:
            Exercise BaseCompositeField.get books for val through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param value: Value normalized, stored, formatted or returned.
        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def update_db(self, book_id_to_val_map: Mapping[SrcTableID, T], db, allow_case_change: bool = False) -> bool:
        """
        Preform an update of the database - should return the data needed to preform an update of the cache.

        Example:
            Exercise BaseCompositeField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Composite field fields cannot be directly updated")


# Todo: Actually store what's on devices
class BaseOnDeviceField(CalibreBaseOneToOneField[bool]):
    """
    Base for the OnDevice field.

    Example:
        Exercise BaseOnDeviceField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name: str, table=None, bools_are_tristate: bool = False) -> None:
        """
        Generate the OnDeviceField - will be mostly empty.

        Example:
            Exercise BaseOnDeviceField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.book_on_device_func = None
        self.is_multiple = False
        self._metadata = {
            "table": None,
            "column": None,
            "datatype": "text",
            "is_multiple": {},
            "kind": "field",
            "name": _("On Device"),
            "search_terms": ["ondevice"],
            "is_custom": False,
            "is_category": False,
            "is_csp": False,
            "display": {},
        }

        self.writer = DummyWriter(None)

    @property
    def metadata(self) -> dict[str, Any]:
        """
        Return the "metadata" which defines this table.

        Example:
            Exercise BaseOnDeviceField.metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._metadata

    @metadata.setter
    def metadata(self, value: Any) -> None:
        """
        Refuse to set the metadata for this field.

        Example:
            Exercise BaseOnDeviceField.metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError(f"Cannot set metadata as {value=}")

    def clear_caches(self, book_ids: Optional[Iterable[SrcTableID]] = None) -> bool:
        """
        Clear the internal field cache.

        Example:
            Exercise BaseOnDeviceField.clear caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def book_on_device(self, book_id: SrcTableID) -> bool:
        """
        Has the book currently been loaded to the currently connected device?

        Example:
            Exercise BaseOnDeviceField.book on device through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def set_book_on_device_func(self, func: Callable[[...], ...]) -> None:
        """
        Sets the function used to check to see if the given book is on the device.

        Example:
            Exercise BaseOnDeviceField.set book on device func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param func: Value supplied for func under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.book_on_device_func = func

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> bool:
        """
        Where is the book currently stored?

        Example:
            Exercise BaseOnDeviceField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __iter__(self):
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise BaseOnDeviceField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(())

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID, Optional[T]], bool]:
        """
        Returns a sort key for the book - used to order the entries on the table.

        Example:
            Exercise BaseOnDeviceField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.for_book

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates, default_value=None
    ) -> Iterator[tuple[T, set[SrcTableID]]]:
        """
        Iterate over the values which _can_ be searched for.

        Example:
            Exercise BaseOnDeviceField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class CalibreBaseManyToOneField(CalibreBaseField[T]):

    """
    Provide the calibrebasemanytoonefield contract for validated ebook processing.

    Example:
        Exercise CalibreBaseManyToOneField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    is_many: bool = True

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> T:
        """
        Get the field value for a given book_id.

        Example:
            Exercise CalibreBaseManyToOneField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Iterable[DstTableID]:
        """
        Return the target ids which the book is linked to.

        Example:
            Exercise CalibreBaseManyToOneField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Takes the id of the item linked to the book and returns all the books linked to it.

        Example:
            Exercise CalibreBaseManyToOneField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __iter__(self) -> SrcTableID:
        """
        Returns an iterable of all the ids available in the target table.

        Example:
            Exercise CalibreBaseManyToOneField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Produces a sort key function - a function which takes a book_id and produces a sort key.

        Example:
            Exercise CalibreBaseManyToOneField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterator[SrcTableID], default_value: Optional[T] = None
    ) -> Iterator[tuple[[T], set[SrcTableID]]]:
        """
        Iterate over values from the target table that can be searched.

        Example:
            Exercise CalibreBaseManyToOneField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def book_value_map(self) -> Mapping[SrcTableID, T]:
        """
        Keyed with the book id and valued with the value for that book.

        Example:
            Exercise CalibreBaseManyToOneField.book value map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class CalibreBaseManyToManyField(CalibreBaseField[T]):
    """
    Basis for the Many-to-many fields - fields where many books can be assigned to many items (e.g. tags).

    Example:
        Exercise CalibreBaseManyToManyField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    # Todo: Should not be able to change these
    # This should probably be OneToMany
    is_many: bool = True
    # This means that Many books can be linked to Many items - probably
    is_many_many: bool = True

    def __init__(self, name: str, table, bools_are_tristate: bool) -> None:
        """
        Starts up the many-to-many table.

        Example:
            Exercise CalibreBaseManyToManyField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreBaseField.__init__(self, name=name, table=table, bools_are_tristate=bools_are_tristate)

    def for_book(self, book_id: int, default_value: Optional[T] = None) -> Iterable[int]:
        """
        Return the values for given book. Will return values as a tuple by default.

        Example:
            Exercise CalibreBaseManyToManyField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Iterator[DstTableID]:
        """
        Return the ids linked to a given book.

        Example:
            Exercise CalibreBaseManyToManyField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def books_for(self, item_id: SrcTableID) -> Iterator[DstTableID]:
        """
        Return the book ids linked to the given item_id

        Example:
            Exercise CalibreBaseManyToManyField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[T]:
        """
        Iterate through all the ids on the field for the book.

        Example:
            Exercise CalibreBaseManyToManyField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def sort_keys_for_books(self, get_metadata: Callable[[...], ...], lang_map) -> tuple[T, ...]:
        """
        Returns a tuple of the sort keys used to order the books

        Example:
            Exercise CalibreBaseManyToManyField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates, default_value: Optional[T] = None
    ) -> Iterator[T]:
        """
        Iterate through values which are valid search targets for the table.

        Example:
            Exercise CalibreBaseManyToManyField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_counts(self, candidates: Iterator[SrcTableID]) -> tuple[tuple[int, set[SrcTableID]]]:
        """
        Generator which yields the counts - the number of tags a book has and a set of book ids all of which have that number of tags.

        Example:
            Exercise CalibreBaseManyToManyField.iter counts through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param candidates: Value supplied for candidates under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_usage_counts(self, item_ids: Iterator[DstTableID]) -> Iterator[tuple[DstTableID, int]]:
        """
        Generator which yields all the dst table ids and the number of books they're linked to.

        Example:
            Exercise CalibreBaseManyToManyField.iter usage counts through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_ids: Value supplied for item ids under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def book_value_map(self) -> Mapping[SrcTableID, T]:
        """
        Keyed with the id of the book and valued with the values connected to that book.

        Example:
            Exercise CalibreBaseManyToManyField.book value map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseIdentifiersField(CalibreBaseManyToManyField[T]):
    """
    Basis for the identifiers table.

    Example:
        Exercise BaseIdentifiersField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[T]:
        """
        Return the identifiers for a given book id.

        Example:
            Exercise BaseIdentifiersField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Sort by identifier keys - not sure if this is a particularly useful thing to do - in this case.

        Example:
            Exercise BaseIdentifiersField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterable[SrcTableID], default_value=()
    ) -> Iterable[tuple[T, set[int]]]:
        """
        Iter through searchable identifiers.

        Example:
            Exercise BaseIdentifiersField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_categories(
        self,
        tag_class,
        book_rating_map: Mapping[SrcTableID, float],
        lang_map: LangMap,
        book_ids: Optional[Iterable[SrcTableID]] = None,
    ):
        """
        Return the category classes for the field.

        Example:
            Exercise BaseIdentifiersField.get categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseAuthorsField(CalibreBaseManyToManyField[str]):
    """
    Basis for the authors field - in fact, for arbitrary creators.

    Example:
        Exercise BaseAuthorsField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def author_data(self, author_id: DstTableID) -> CreatorDataDict:
        """
        Provides all available author data for a given author id.

        Example:
            Exercise BaseAuthorsField.author data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param author_id: Value supplied for author id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def category_sort_value(self, item_id: DstTableID, book_ids: Iterator[SrcTableID], lang_map: LangMap) -> str:
        """
        Return the author sort field for the given item_id.

        Example:
            Exercise BaseAuthorsField.category sort value through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def db_author_sort_for_book(self, book_id: SrcTableID) -> str:
        """
        Returns the author sort value for the specific book from the database.

        Example:
            Exercise BaseAuthorsField.db author sort for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def author_sort_for_book(self, book_id: SrcTableID) -> str:
        """
        Build and return the author sort of the book - the joined author sort for all the authrors.

        Example:
            Exercise BaseAuthorsField.author sort for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


# Todo: This doesn't make awesome amounts of sense anywhere except for books
class BaseFormatsField(CalibreBaseManyToManyField[T]):
    """
    Basis for the formats field - provides a convenient front end for information stored in the formats table.

    Example:
        Exercise BaseFormatsField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[T]:
        """
        Returns all the formats for the given book id.

        Example:
            Exercise BaseFormatsField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def format_fname(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> str:
        """
        Returns the file name for the given format.

        Example:
            Exercise BaseFormatsField.format fname through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Probably going to need to add some more backing and options to this
    def format_floc(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> Union[str, os.PathLike]:
        """
        Return the Location of a given format (stands for format file loc).

        Example:
            Exercise BaseFormatsField.format floc through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def has_format(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> bool:
        """
        Does the given format exist for the given book id?

        Example:
            Exercise BaseFormatsField.has format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: True when the documented condition holds; otherwise False.
        """
        raise NotImplementedError

    def has_priority_fmt(self, book_id: SrcTableID, priority_fmt: SpecificFormat) -> bool:
        """
        Check to see if the given book has the given format.

        Example:
            Exercise BaseFormatsField.has priority fmt through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param priority_fmt: Value supplied for priority fmt under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        raise NotImplementedError

    def add_format(self, book_id: SrcTableID, fmt: SpecificFormat, fmt_loc: Union[str, os.PathLike]) -> bool:
        """
        Add a format to a book.

        Example:
            Exercise BaseFormatsField.add format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param fmt_loc: Value supplied for fmt loc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def remove_fmt(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> bool:
        """
        Remove a fmt from the cache.

        Example:
            Exercise BaseFormatsField.remove fmt through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def reload_book_from_db(self, db, book_id: SrcTableID) -> bool:
        """
        Reload all the information from a book from the database - the ultimate source of truth of the system.

        Example:
            Exercise BaseFormatsField.reload book from db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(
        self,
        get_metadata: Callable[
            [
                SrcTableID,
            ],
            Optional[set[SpecificFormat]],
        ],
        candidates: Iterable[SrcTableID],
        default_value: None = None,
    ) -> Iterator[tuple[GenericFormat, set[SrcTableID]]]:
        """
        Searchable values should be the available formats for each of the given books.

        Example:
            Exercise BaseFormatsField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_categories(self, tag_class, book_rating_map, lang_map: LangMap, book_ids: Iterable[SrcTableID] = None):
        """
        Does not make sense in this context - so not implemented.

        Example:
            Exercise BaseFormatsField.get categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        raise NotImplementedError


class BaseCoverField(CalibreBaseManyToManyField):
    """
    Provides a front end to the information stored in the Covers table.

    Example:
        Exercise BaseCoverField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def cover_id(self, book_id: SrcTableID, default_value: None = None) -> CoverID:
        """
        Returns the id of the cover which is primary for the book.

        Example:
            Exercise BaseCoverField.cover id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def cover_loc(self, book_id: SrcTableID, default_value: None = None) -> Union[str, os.PathLike]:
        """
        Returns the loc of the cover that is primary for that book.

        Example:
            Exercise BaseCoverField.cover loc through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseSeriesField(CalibreBaseManyToOneField[T]):
    """
    Used for storing series field information.

    Example:
        Exercise BaseSeriesField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Produces a function which takes the id of the given book and produces a string sort key for that book.

        Example:
            Exercise BaseSeriesField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def category_sort_value(self, item_id: DstTableID, book_ids: Iterable[SrcTableID], lang_map: LangMap) -> str:
        """
        Returns the sort value for the given target value in the other table.

        Example:
            Exercise BaseSeriesField.category sort value through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseTagsField(CalibreBaseManyToManyField):
    """
    Provide the basetagsfield contract for validated ebook processing.

    Example:
        Exercise BaseTagsField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def get_news_category(
        self, tag_class: BaseTagClass, book_ids: Optional[Iterable[SrcTableID]] = None
    ) -> Iterable[BaseTagClass]:
        """
        Categories are used in the display - specify a tag and it'll generate a list of tag classes.

        Example:
            Exercise BaseTagsField.get news category through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseLinkAttributeField(Generic[T]):
    """
    Base field for a link attribute - stores additional data to further characterize the link between the two tables.

    Example:
        Exercise BaseLinkAttributeField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(
        self,
        name: str,
        link_table_name: str,
        link_field: BaseField,
        link_attribute_table,
        main_table_name: str,
        auxiliary_table_name: str,
    ) -> None:
        """
        Set the basic properties of the field and the link.

        Example:
            Exercise BaseLinkAttributeField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param link_table_name: Value supplied for link table name under the utility
            contract.
        :param link_field: Value supplied for link field under the utility contract.
        :param link_attribute_table: Value supplied for link attribute table under the
            utility contract.
        :param main_table_name: Value supplied for main table name under the utility
            contract.
        :param auxiliary_table_name: Value supplied for auxiliary table name under the
            utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.link_table_name = link_table_name
        self.link_field = link_field
        self.link_attribute_table = link_attribute_table
        self.main_table = main_table_name
        self.auxiliary_table = auxiliary_table_name

        self.table = self.link_attribute_table


class BaseOneToOneField(BaseField):
    """
    A 1-1 mapping must exist between books and these fields. (E.g. Books to languages in calibre).

    Example:
        Exercise BaseOneToOneField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def ids_for_book(self, book_id: SrcTableID) -> tuple[DstTableID]:
        """
        In the case of a 1-1 table the id of the item can be same as the book - it's stored in the same row of the db.

        Example:
            Exercise BaseOneToOneField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.book_in_cache(book_id):
            return tuple(
                [
                    book_id,
                ]
            )
        else:
            raise NotInCache

    def books_for(self, item_id: DstTableID) -> set[SrcTableID]:
        """
        For a 1-1 table the id of the item can be same as the book - if it's stored in the same row of the db.

        Example:
            Exercise BaseOneToOneField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.item_in_cache(item_id):
            return {
                item_id,
            }
        else:
            raise NotInCache

    def book_in_cache(self, book_id: SrcTableID) -> bool:
        """
        Checks that the given book is in the cache - returns True if the book exists in the cache, False otherwise.

        Example:
            Exercise BaseOneToOneField.book in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def item_in_cache(self, item_id: DstTableID) -> bool:
        """
        Checks that the given item is in the cache - returns True if the item exists in the cache and False otherwise.

        Example:
            Exercise BaseOneToOneField.item in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseManyToOneField(BaseField[T]):

    # Todo: Protect this from change
    """
    Provide the basemanytoonefield contract for validated ebook processing.

    Example:
        Exercise BaseManyToOneField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    is_many: bool = True

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[T]:
        """
        Get the field value for a given book_id.

        Example:
            Exercise BaseManyToOneField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Optional[Iterable[DstTableID]]:
        """
        Return the ids which the book is linked to.

        Example:
            Exercise BaseManyToOneField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Takes the id of the item linked to the book and returns all the books linked to it.

        Example:
            Exercise BaseManyToOneField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[SrcTableID]:
        """
        Returns an iterable of all the ids available in the target table.

        Example:
            Exercise BaseManyToOneField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Produces the sort key function - takes the id of a book and produces a sort key for that book for this table.

        Example:
            Exercise BaseManyToOneField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterable[SrcTableID], default_value: Optional[T] = None
    ) -> Iterator[tuple[Optional[T], set[SrcTableID]]]:
        """
        Iterate over values from the target table that can be searched.

        Example:
            Exercise BaseManyToOneField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def book_value_map(self) -> Mapping[SrcTableID, Optional[T]]:
        """
        Keyed with the book id and valued with the value for that book.

        Example:
            Exercise BaseManyToOneField.book value map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseManyToManyField(BaseField[T]):
    """
    Basis for the Many-to-many fields - fields where many books can be assigned to many items.

    Example:
        Exercise BaseManyToManyField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    # Todo: Should not be able to change these
    is_many: bool = True
    is_many_many: bool = True

    def __init__(self, name: str, table, bools_are_tristate: bool) -> None:
        """
        Initialize and validate the basemanytomanyfield state.

        Example:
            Exercise BaseManyToManyField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseField.__init__(self, name=name, table=table, bools_are_tristate=bools_are_tristate)

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[tuple[T, ...]]:
        """
        Return the values for given book. Will return values as a tuple by default.

        Example:
            Exercise BaseManyToManyField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Iterable[DstTableID]:
        """
        Return the ids linked to a given book.

        Example:
            Exercise BaseManyToManyField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Return the book ids linked to the given item_id

        Example:
            Exercise BaseManyToManyField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[SrcTableID]:
        """
        Iterate through the ids of all the books which have values for this table.

        Example:
            Exercise BaseManyToManyField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Returns a function used to generate a sort key for the given book.

        Example:
            Exercise BaseManyToManyField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterable[SrcTableID], default_value: Optional[T] = None
    ) -> Iterator[tuple[Optional[T], set[SrcTableID]]]:
        """
        Iterate through values which are valid search targets for the table.

        Example:
            Exercise BaseManyToManyField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_counts(self, candidates: Iterable[SrcTableID]) -> Iterator[tuple[int, set[SrcTableID]]]:
        """
        Iter through usage counts for all the tags.

        Example:
            Exercise BaseManyToManyField.iter counts through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param candidates: Value supplied for candidates under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def book_value_map(self):
        """
        Keyed with the id of the book and valued with the values connected to that book

        Example:
            Exercise BaseManyToManyField.book value map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError
