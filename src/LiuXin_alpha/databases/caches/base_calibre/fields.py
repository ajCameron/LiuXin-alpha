"""
Fields are one step of abstraction up from tables - collections of data in a form that people might actually want.

Originally, in calibre, intended to be fields viewed in the GUI, they have been generalised.
LiuXin allows you - with the right interface - to browse any of the base tables.
E.g. the "tags" field - which contains information from the "tags" table.
E.g. the "titles" field of the "tags" table - which might contain each title the tag is linked to.

Thus, there are a few pieces of information important to any field.
 - which table the field is "of" or "in"
 - which other table the field is "viewing"

Depending on how you implement the cache, "fields" might be an abstraction.
However, to keep the cache interface the same, you will probably have to impelment something which _looks_ vaguely like
a field.
"""


from __future__ import unicode_literals, division, absolute_import, print_function

import os
from datetime import datetime
from locale import atof
from functools import partial

from typing import Optional, Callable, Any, TypeVar, Union, Iterable, Generic, Iterator, Mapping

from LiuXin.customize.cache.base_field import BaseField

from LiuXin.databases.tag_classes import BaseTagClass
from LiuXin.databases.utils import force_to_bool
from LiuXin.databases.write import get_writer
from LiuXin.databases.write import DummyWriter
from LiuXin.databases.db_types import (
    LangMap,
    SrcTableID,
    DstTableID,
    CreatorDataDict,
    SpecificFormat,
    GenericFormat,
    CoverID,
)

from LiuXin.exceptions import NotInCache

from LiuXin.metadata import author_to_author_sort

from LiuXin.preferences import preferences as tweaks

from LiuXin.utils.date import UNDEFINED_DATE, clean_date_for_sort, parse_date
from LiuXin.utils.icu import sort_key
from LiuXin.utils.localization import calibre_langcode_to_name
from LiuXin.utils.localization import _

T = TypeVar("T")
D = TypeVar("D")


def bool_sort_key(
    bools_are_tristate: bool,
) -> Callable[[Any,], Optional[bool]]:
    """
    Returns a sort key suitable for use with tristate bools.

    calibre allows "bools" to be True, False or None.
    This can confuse sorting - so use this function to generate a sort key for them.
    :param bools_are_tristate:
    :return:
    """
    return (
        (lambda x: {True: 1, False: 2, None: 3}.get(x, 3))
        if bools_are_tristate
        else lambda x: {True: 1, False: 2, None: 2}.get(x, 2)
    )


def identity(x: D) -> D:
    """
    Just returns itself.

    :param x:
    :return x:
    """
    return x


IDENTITY = identity


class InvalidLinkTable(Exception):
    """
    Raised when trying to link two tables which are not linkable.
    """

    def __init__(self, name) -> None:
        Exception.__init__(self, name)
        self.field_name = name


class CalibreBaseField(BaseField[T]):
    """
    Basis for a representation of a field on the database.

    Cached information from the database is stored in the table object.
    The field provides convenient access methods to it.
    """

    def __init__(
        self,
        name: str,
        table,
        bools_are_tristate: bool,
        # generic_val: D = "",  # Todo: This seems to be a good way to get typing info into the system
        link_attributes=None,
        main_table: Optional[str] = None,
        auxiliary_table: Optional[str] = None,
    ) -> None:
        """

        :param name: Name of the field
        :param table: The table the field is in
        :param bools_are_tristate: If True then bools are permitted to take three values - True, False and None
        :param link_attributes: The names of the additional attributes that the link has (e.g. "index")
        :param main_table: It is helpful to be able to generically refer to the tables being linked.
                           While "main" and "auxiliary" are not hard and fast they should be taken as a guide (and
                           if one of the two is a title or book, that should probably always been main.
        :param auxiliary_table:
        :return:
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

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ METHODS
    # The read logic is confined to the individual tables - however the separate attribute fields contain tables
    # which must also be individually read
    def read_attribute_tables(self, db) -> None:
        """
        Preform a read of data from the database into the attribute fields contained within this field.

        :param db:
        :return:
        """
        for attr_field_name in self.link_attr_fields:
            self.link_attr_fields[attr_field_name].table.read(db)

    #
    # ------------------------------------------------------------------------------------------------------------------


class CalibreBaseOneToOneField(CalibreBaseField[T]):
    """
    A 1-1 mapping must exist between a table and the one represented by this field.

    E.g. "books" to "book_uuid".
    E.g. "titles" to "title_sort".
    E.g. "tags" to "tag" (the tags table value).
    """

    def ids_for_book(self, book_id: SrcTableID) -> tuple[DstTableID]:
        """
        In the case of a 1-1 table the item id is the same as the book - as it's stored in the same row of the db.

        :param book_id: Tuple of
        :return:
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

        :param item_id:
        :return:
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

        Depends strongly on the storage backend - so not implemented here.
        :param book_id:
        :return in_cache:
        """
        raise NotImplementedError

    def item_in_cache(self, item_id: DstTableID) -> bool:
        """
        Checks that the given item is in the cache - returns True iff the item is in the cache and False otherwise.

        Depends strongly on the storage backend - so not implemented here.
        :param item_id:
        :return in_cache:
        """
        raise NotImplementedError


class BaseOneToManyField(CalibreBaseOneToOneField):
    """
    For a Many-to-Many or One-to-Many table that has to pretend to be a 1-1 table.
    """

    def ids_for_book(self, book_id: SrcTableID) -> set[DstTableID]:
        """
        The table is pretending to be 1-1 - so this method does not make sense.

        :param book_id:
        :return:
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> set[SrcTableID]:
        """
        The table is pretending to be 1-1 - so this method does not make sense.

        :param item_id:
        :return:
        """
        raise NotImplementedError


class BaseCompositeField(CalibreBaseOneToOneField):
    """
    A composite field is composed of a composite of metadata from other fields.
    """

    is_composite: bool = True
    SIZE_SUFFIX_MAP: dict[str, int] = {suffix: i for i, suffix in enumerate(("", "K", "M", "G", "T", "P", "E"))}

    def __init__(self, name: str, table, bools_are_tristate: bool) -> None:
        """
        Construct a composite field - a field composed of multiple other pieces of data.

        The formatter which does the work of following the template is stored over in metadata as mi.formatter
        :param name: Name of the composite field
        :param table: Table
        :param bools_are_tristate:
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

        :param val:
        :return sort_key:
        """
        val = (sort_key(x.strip()) for x in (val or "").split(self.splitter))
        return tuple(sorted(val))

    def number_sort_key(self, val: str) -> Union[int, float, str]:
        """
        Produces a sort key from a numerical value.

        :param val:
        :return:
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

        :param val:
        :return:
        """
        try:
            val = self._filter_date(parse_date(val))
        except (TypeError, ValueError, AttributeError, KeyError):
            val = UNDEFINED_DATE
        return val

    def bool_sort_key(self, val: Any) -> Optional[bool]:
        """
        Produce a sort key from any value.

        :param val:
        :return:
        """
        return self._bool_sort_key(force_to_bool(val))

    def clear_caches(self, book_ids: Optional[Iterator[str]] = None) -> bool:
        """
        Clear the internal caches stored in the field.

        :param book_ids: Clear caches for only these book ids
        :return status: Did the cache clear successfully?
        """
        raise NotImplementedError

    def get_value_with_cache(self, book_id: SrcTableID, get_metadata: Callable[[...], ...]) -> Any:
        """
        Return a value using the composite cache.

        :param book_id: Id for the book to get the value for
        :param get_metadata: Function to produce the composite metadata result
        :return:
        """
        raise NotImplementedError

    def sort_keys_for_books(self, get_metadata: Callable[[...], ...], lang_map: LangMap) -> Any:
        """
        Return sort keys for all books.

        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterator[SrcTableID], default_value: Optional[D] = None
    ) -> Iterator[D]:
        """
        Iter all searchable values.

        :param get_metadata:
        :param candidates:
        :param default_value:
        :return:
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

        :param tag_class:
        :param book_rating_map:
        :param book_ids:
        :param is_multiple:
        :param get_metadata:
        :return:
        """
        raise NotImplementedError

    def get_books_for_val(
        self, value: T, get_metadata: Callable[[...], ...], book_ids: Iterable[SrcTableID]
    ) -> Iterator[SrcTableID]:
        """
        Iterate through all values - generating the custom values and checking to see if books match those.

        This can be an extremely expensive operation.
        :param value: Value to search for
        :param get_metadata: Function to get the metadata
        :param book_ids: Restrict to searching in these books (used for virtual libraries)
        :return:
        """
        raise NotImplementedError

    def update_db(self, book_id_to_val_map: Mapping[SrcTableID, T], db, allow_case_change: bool = False) -> bool:
        """
        Preform an update of the database - should return the data needed to preform an update of the cache.

        :param book_id_to_val_map: Keyed with the item id and valued with the new item value
                                   Composite fields depend on other fields - and so direct update is blocked.
        :param db:
        :param allow_case_change:
        :return status: Did the update go through?
        """
        raise NotImplementedError("Composite field fields cannot be directly updated")


# Todo: Actually store what's on devices
class BaseOnDeviceField(CalibreBaseOneToOneField[bool]):
    """
    Base for the OnDevice field.
    """

    def __init__(self, name: str, table=None, bools_are_tristate: bool = False) -> None:
        """
        Generate the OnDeviceField - will be mostly empty.

        :param name:
        :param table:
        :param bools_are_tristate:
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

        :return:
        """
        return self._metadata

    @metadata.setter
    def metadata(self, value: Any) -> None:
        """
        Refuse to set the metadata for this field.

        It doesn't make sense to - as this is an internal field.
        :param value:
        :return:
        """
        raise NotImplementedError(f"Cannot set metadata as {value=}")

    def clear_caches(self, book_ids: Optional[Iterable[SrcTableID]] = None) -> bool:
        """
        Clear the internal field cache.

        :param book_ids: The ids to remove cached
        :return status: Did the cache clear for the given book ids?
        """
        raise NotImplementedError

    def book_on_device(self, book_id: SrcTableID) -> bool:
        """
        Has the book currently been loaded to the currently connected device?

        :param book_id: Has the given book been loaded onto the connected device?
        :return status: Is the book on the connected device?
        """
        raise NotImplementedError

    def set_book_on_device_func(self, func: Callable[[...], ...]) -> None:
        """
        Sets the function used to check to see if the given book is on the device.

        :param func:
        :return:
        """
        self.book_on_device_func = func

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> bool:
        """
        Where is the book currently stored?

        :param book_id: The id of the book to check for load
        :param default_value: Doesn't really make sense in this context
        :return book_on_device: Has the book been loaded onto the device?
        """
        raise NotImplementedError

    def __iter__(self):
        return iter(())

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID, Optional[T]], bool]:
        """
        Returns a sort key for the book - used to order the entries on the table.

        :param get_metadata:
        :param lang_map:
        :return:
        """
        return self.for_book

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates, default_value=None
    ) -> Iterator[tuple[T, set[SrcTableID]]]:
        """
        Iterate over the values which _can_ be searched for.

        :param get_metadata:
        :param candidates: Restrict to the given book ids - useful for virtual libraries.
        :param default_value:
        :return searchable_values: An iterator of tuples
                                   - first element being the values
                                   - second element being a set of book_ids which would be returned by searching for
                                     that value
        """
        raise NotImplementedError


class CalibreBaseManyToOneField(CalibreBaseField[T]):

    is_many: bool = True

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> T:
        """
        Get the field value for a given book_id.

        :param book_id:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Iterable[DstTableID]:
        """
        Return the target ids which the book is linked to.

        If there is no value set the return will be None
        If there is a value set for the book then it'll be returned as a tuple of length one.
        :param book_id:
        :return:
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Takes the id of the item linked to the book and returns all the books linked to it.

        :param item_id:
        :return:
        """
        raise NotImplementedError

    def __iter__(self) -> SrcTableID:
        """
        Returns an iterable of all the ids available in the target table.

        :return:
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Produces a sort key function - a function which takes a book_id and produces a sort key.

        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterator[SrcTableID], default_value: Optional[T] = None
    ) -> Iterator[tuple[[T], set[SrcTableID]]]:
        """
        Iterate over values from the target table that can be searched.

        The iterable is a tuple - the first entry being the searchable value and the second entry being all the book ids
        associated with that value.
        :param get_metadata:
        :param candidates: A list of book ids to restrict the search to - used for virtual tables
        :param default_value:
        :return:
        """
        raise NotImplementedError

    @property
    def book_value_map(self) -> Mapping[SrcTableID, T]:
        """
        Keyed with the book id and valued with the value for that book.

        Contains all book values - so will be computationally expensive.
        :return:
        """
        raise NotImplementedError


class CalibreBaseManyToManyField(CalibreBaseField[T]):
    """
    Basis for the Many-to-many fields - fields where many books can be assigned to many items (e.g. tags).
    """

    # Todo: Should not be able to change these
    # This should probably be OneToMany
    is_many: bool = True
    # This means that Many books can be linked to Many items - probably
    is_many_many: bool = True

    def __init__(self, name: str, table, bools_are_tristate: bool) -> None:
        """
        Starts up the many-to-many table.

        :param name:
        :param table:
        :param bools_are_tristate:
        """
        CalibreBaseField.__init__(self, name=name, table=table, bools_are_tristate=bools_are_tristate)

    def for_book(self, book_id: int, default_value: Optional[T] = None) -> Iterable[int]:
        """
        Return the values for given book. Will return values as a tuple by default.

        :param book_id: The id of the book to return values for
        :param default_value: If the book is linked to no entries in the other table, return thris defaudlt valsue.
        :return:
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Iterator[DstTableID]:
        """
        Return the ids linked to a given book.

        :param book_id:
        :return:
        """
        raise NotImplementedError

    def books_for(self, item_id: SrcTableID) -> Iterator[DstTableID]:
        """
        Return the book ids linked to the given item_id

        :param item_id:
        :return book_ids:
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[T]:
        """
        Iterate through all the ids on the field for the book.

        :return:
        """
        raise NotImplementedError

    def sort_keys_for_books(self, get_metadata: Callable[[...], ...], lang_map) -> tuple[T, ...]:
        """
        Returns a tuple of the sort keys used to order the books

        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates, default_value: Optional[T] = None
    ) -> Iterator[T]:
        """
        Iterate through values which are valid search targets for the table.

        :param get_metadata:
        :param candidates:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    def iter_counts(self, candidates: Iterator[SrcTableID]) -> tuple[tuple[int, set[SrcTableID]]]:
        """
        Generator which yields the counts - the number of tags a book has and a set of book ids all of which have that
        number of tags.

        Why does this exist?
        Not _entirely_ sure.
        :param candidates:
        :return:
        """
        raise NotImplementedError

    def iter_usage_counts(self, item_ids: Iterator[DstTableID]) -> Iterator[tuple[DstTableID, int]]:
        """
        Generator which yields all the dst table ids and the number of books they're linked to.

        :param item_ids:
        :return:
        """
        raise NotImplementedError

    @property
    def book_value_map(self) -> Mapping[SrcTableID, T]:
        """
        Keyed with the id of the book and valued with the values connected to that book.

        :return:
        """
        raise NotImplementedError


class BaseIdentifiersField(CalibreBaseManyToManyField[T]):
    """
    Basis for the identifiers table.
    """

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[T]:
        """
        Return the identifiers for a given book id.

        If we're a calibre compatible table, this will be just the highest priority identifiers.
        If we're not, then it'll be an identifier dict.
        :param book_id:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Sort by identifier keys - not sure if this is a particularly useful thing to do - in this case.

        However, it's often needed for interfaces.
        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterable[SrcTableID], default_value=()
    ) -> Iterable[tuple[T, set[int]]]:
        """
        Iter through searchable identifiers.

        :param get_metadata:
        :param candidates: Iterable of book ids
        :param default_value:
        :return:
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

        :param tag_class:
        :param book_rating_map: Keyed with the book ids and valued with, presumably, a float
        :param lang_map:
        :param book_ids: Restrict the categories search to the following book ids.
        :return:
        """
        raise NotImplementedError


class BaseAuthorsField(CalibreBaseManyToManyField[str]):
    """
    Basis for the authors field - in fact, for arbitrary creators.
    """

    def author_data(self, author_id: DstTableID) -> CreatorDataDict:
        """
        Provides all available author data for a given author id.

        :param author_id: Id from the creator table.
        :return: A dictionary keyed with the type of data and valued with the value for that.
                 Keys - "name", "sort" and "link".
        """
        raise NotImplementedError

    def category_sort_value(self, item_id: DstTableID, book_ids: Iterator[SrcTableID], lang_map: LangMap) -> str:
        """
        Return the author sort field for the given item_id.

        :param item_id: The id of the creator to retrieve the sort value for
        :param book_ids: Not used in this case
        :param lang_map: Not used in this case
        :return:
        """
        raise NotImplementedError

    def db_author_sort_for_book(self, book_id: SrcTableID) -> str:
        """
        Returns the author sort value for the specific book from the database.

        This is a value set on a book by book basis.
        Default value is automatically generated from the sort strigs of all the creators (authors) of the book.
        :param book_id:
        :return:
        """
        raise NotImplementedError

    def author_sort_for_book(self, book_id: SrcTableID) -> str:
        """
        Build and return the author sort of the book - the joined author sort for all the authrors.

        :param book_id:
        :return:
        """
        raise NotImplementedError


# Todo: This doesn't make awesome amounts of sense anywhere except for books
class BaseFormatsField(CalibreBaseManyToManyField[T]):
    """
    Basis for the formats field - provides a convenient front end for information stored in the formats table.

    Two pieces of information need to be conveyed with the format identifier - the format itself and it's priority in
    the title.
    Thus formats appear as something like EPUB_1.
    """

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[T]:
        """
        Returns all the formats for the given book id.
        :param book_id:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    def format_fname(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> str:
        """
        Returns the file name for the given format.

        :param book_id:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    # Todo: Probably going to need to add some more backing and options to this
    def format_floc(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> Union[str, os.PathLike]:
        """
        Return the Location of a given format (stands for format file loc).

        If you pass in a SpecificFormat it will return the location of that format.
        If you pass in a GenericFormat it will return the location of the highest priority format.
        :param book_id:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    def has_format(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> bool:
        """
        Does the given format exist for the given book id?

        If you pass in a SpecificFormat it will check for that format.
        If you pass in a GenericFormat it will return True if there are any files of that format in the book.
        :param book_id:
        :param fmt:
        :return:
        """
        raise NotImplementedError

    def has_priority_fmt(self, book_id: SrcTableID, priority_fmt: SpecificFormat) -> bool:
        """
        Check to see if the given book has the given format.

        :param book_id:
        :param priority_fmt:
        :return:
        """
        raise NotImplementedError

    def add_format(self, book_id: SrcTableID, fmt: SpecificFormat, fmt_loc: Union[str, os.PathLike]) -> bool:
        """
        Add a format to a book.

        FMT should include the priority of that FMT in the book (e.g. not EPUB, but EPUB_1).
        Note - If you want to add an ORIGINAL_FMT - call this function with the full FMT string, including priority.
        This function will reject any FMT which is not a priority fmt - that is to say something of the form EPUB_1
        :return status: Was the format successfully added?
        """
        raise NotImplementedError

    def remove_fmt(self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat]) -> bool:
        """
        Remove a fmt from the cache.

        If you pass in a SpecificFormat then that, and only that, format will be removed.
        If you pass in a GenericFormat, then
        :param book_id:
        :param fmt:
        :return status: Was the format successfully removed?
        """
        raise NotImplementedError

    def reload_book_from_db(self, db, book_id: SrcTableID) -> bool:
        """
        Reload all the information from a book from the database - the ultimate source of truth of the system.

        :param db:
        :param book_id:
        :return status: Did the reload from the database go through?
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
    ) -> Iterator[GenericFormat, set[SrcTableID]]:
        """
        Searchable values should be the available formats for each of the given books.

        Yields an iterator which produces tuples.
        Element 1 - The GenericFormat (e.g. "EPUB")
        Element 2 - The set of book ids which have that GenericFormat
        :param get_metadata: Function to produce metadata for each book
        :param candidates: The book ids to search in (used in virtual libraries)
        :param default_value: The value to return if there's no value for the book
        :return:
        """
        raise NotImplementedError

    def get_categories(self, tag_class, book_rating_map, lang_map: LangMap, book_ids: Iterable[SrcTableID] = None):
        """
        Does not make sense in this context - so not implemented.

        :param tag_class:
        :param book_rating_map:
        :param lang_map:
        :param book_ids:
        :return:
        """

        raise NotImplementedError


class BaseCoverField(CalibreBaseManyToManyField):
    """
    Provides a front end to the information stored in the Covers table.

    The cover field deals solely with whether the book HAS a cover.
    This field stores more detailed information about the multiple covers available to the book - including the location
    of the cover and which cover is primary for each book.
    """

    def cover_id(self, book_id: SrcTableID, default_value: None = None) -> CoverID:
        """
        Returns the id of the cover which is primary for the book.

        Returns None if there are no covers linked to the book.
        :param book_id: The id of the book to get the primary cover for.
        :param default_value:
        :return cover_id:
        """
        raise NotImplementedError

    def cover_loc(self, book_id: SrcTableID, default_value: None = None) -> Union[str, os.PathLike]:
        """
        Returns the loc of the cover that is primary for that book.

        :param book_id:
        :param default_value:
        :return:
        """
        raise NotImplementedError


class BaseSeriesField(CalibreBaseManyToOneField[T]):
    """
    Used for storing series field information.
    """

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Produces a function which takes the id of the given book and produces a string sort key for that book.

        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def category_sort_value(self, item_id: DstTableID, book_ids: Iterable[SrcTableID], lang_map: LangMap) -> str:
        """
        Returns the sort value for the given target value in the other table.

        :param item_id:
        :param book_ids:
        :param lang_map:
        :return:
        """
        raise NotImplementedError


class BaseTagsField(CalibreBaseManyToManyField):
    def get_news_category(
        self, tag_class: BaseTagClass, book_ids: Optional[Iterable[SrcTableID]] = None
    ) -> Iterable[BaseTagClass]:
        """
        Categories are used in the display - specify a tag and it'll generate a list of tag classes.

        This contains all the books with the news tag AND the new tag provided.
        :param tag_class:
        :param book_ids:
        :return:
        """
        raise NotImplementedError


class BaseLinkAttributeField(Generic[T]):
    """
    Base field for a link attribute - stores additional data to further characterize the link between the two tables.

    This field is a generic field - with the generic being the datatype of the extra value.

    E.g. the "series" field also has "series_number" or "series_position" attributes.
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
        :param name:
        :param link_table_name:
        :param link_field: The field object representing the link between the main and auxiliary fields.
        :param link_attribute_table: Table which does the actual work of storing the link data.
        :param main_table_name: The name of the main table
        :param auxiliary_table_name: The name of the auxiliary table
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
    """

    def ids_for_book(self, book_id: SrcTableID) -> tuple[DstTableID]:
        """
        In the case of a 1-1 table the id of the item can be same as the book - it's stored in the same row of the db.

        :param book_id: Tuple of
        :return:
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

        :param item_id:
        :return:
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

        Depends strongly on the storage backend - so not implemented here.
        :param book_id:
        :return book_in_cache:
        """
        raise NotImplementedError

    def item_in_cache(self, item_id: DstTableID) -> bool:
        """
        Checks that the given item is in the cache - returns True if the item exists in the cache and False otherwise.

        Depends strongly on the storage backend - so not implemented here.
        :param item_id:
        :return:
        """
        raise NotImplementedError


class BaseManyToOneField(BaseField[T]):

    # Todo: Protect this from change
    is_many: bool = True

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[T]:
        """
        Get the field value for a given book_id.

        The field is ManyToOne - so many books are assigned to a single value.
        :param book_id:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Optional[Iterable[DstTableID]]:
        """
        Return the ids which the book is linked to.

        This is a ManyToOne field so there should be, at most, one.
        If there is no value set the return will be None.
        If there is a value set for the book then it'll be returned as a tuple of length one.
        :param book_id:
        :return:
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Takes the id of the item linked to the book and returns all the books linked to it.

        This is a ManyToOne field - so there could be any number of ids linked to it.
        This will be in the form of an iterable of IDs for the other table.
        :param item_id:
        :return:
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[SrcTableID]:
        """
        Returns an iterable of all the ids available in the target table.

        :return:
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Produces the sort key function - takes the id of a book and produces a sort key for that book for this table.

        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterable[SrcTableID], default_value: Optional[T] = None
    ) -> Iterator[tuple[Optional[T], set[SrcTableID]]]:
        """
        Iterate over values from the target table that can be searched.

        The iterable is a tuple - the first entry being the searchable value and the second entry being all the book ids
        associated with that value.
        :param get_metadata: Function to get the metadata for the book
        :param candidates: Collection of book ids - values will be generated for these
        :param default_value: Override default value for when no value is set.
        :return:
        """
        raise NotImplementedError

    @property
    def book_value_map(self) -> Mapping[SrcTableID, Optional[T]]:
        """
        Keyed with the book id and valued with the value for that book.

        Contains all book values - so will be computationally expensive.
        :return:
        """
        raise NotImplementedError


class BaseManyToManyField(BaseField[T]):
    """
    Basis for the Many-to-many fields - fields where many books can be assigned to many items.

    E.g. "tags" - many tags are linked to many books and visa-versa
    """

    # Todo: Should not be able to change these
    is_many: bool = True
    is_many_many: bool = True

    def __init__(self, name: str, table, bools_are_tristate: bool) -> None:
        """


        :param name:
        :param table:
        :param bools_are_tristate:
        """
        BaseField.__init__(self, name=name, table=table, bools_are_tristate=bools_are_tristate)

    def for_book(self, book_id: SrcTableID, default_value: Optional[T] = None) -> Optional[tuple[T, ...]]:
        """
        Return the values for given book. Will return values as a tuple by default.

        :param book_id: The id of the book to return values for
        :param default_value: If the book is linked to no entries in the other table, return thris defaudlt valsue.
        :return:
        """
        raise NotImplementedError

    def ids_for_book(self, book_id: SrcTableID) -> Iterable[DstTableID]:
        """
        Return the ids linked to a given book.

        :param book_id:
        :return:
        """
        raise NotImplementedError

    def books_for(self, item_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Return the book ids linked to the given item_id

        :param item_id:
        :return:
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[SrcTableID]:
        """
        Iterate through the ids of all the books which have values for this table.

        Note - in the case where you can set values as None, this might requie some fiddling.
        :return:
        """
        raise NotImplementedError

    def sort_keys_for_books(
        self, get_metadata: Callable[[...], ...], lang_map: LangMap
    ) -> Callable[[SrcTableID,], str]:
        """
        Returns a function used to generate a sort key for the given book.

        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def iter_searchable_values(
        self, get_metadata: Callable[[...], ...], candidates: Iterable[SrcTableID], default_value: Optional[T] = None
    ) -> Iterator[tuple[Optional[T], set[SrcTableID]]]:
        """
        Iterate through values which are valid search targets for the table.

        :param get_metadata:
        :param candidates:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    def iter_counts(self, candidates: Iterable[SrcTableID]) -> Iterator[int, set[SrcTableID]]:
        """
        Iter through usage counts for all the tags.

        Returns a series of tuples - the first element being the value of the table and the second element being the
        usage count for that element.
        :param candidates:
        :return:
        """
        raise NotImplementedError

    @property
    def book_value_map(self):
        """
        Keyed with the id of the book and valued with the values connected to that book

        :return:
        """
        raise NotImplementedError
