"""
Declares an API for the field module.

Fields are one step of abstraction up from tables - collections of data in a form that people might actually want.

They may, or may not, be externally accessible outside the cache.
The only API which _really_ matters is the cache one - on paper.
Some plugins might go rogue and access the fields directly.
However, if you have field like objects within your cache, they should inherit from the objects here if possible.
"""

from __future__ import unicode_literals, division, absolute_import, print_function

import datetime
from copy import deepcopy

from typing import Optional, Callable, TypeVar, Union, Generic, Iterable, Iterator, Any

from LiuXin_alpha.utils.text.icu import sort_key
from LiuXin_alpha.caches.write import get_writer, DummyWriter

from LiuXin_alpha.databases.db_types import (
    SrcTableID,
    DstTableID,
)


T = TypeVar("T")
D = TypeVar("D")


def identity(x: D) -> D:
    """
    Just returns itself.

    :param x:
    :return x:
    """
    return x


IDENTITY = identity


class BaseField(Generic[T]):
    """
    Basis for a representation of a field on the database.

    Cached information from the database is stored in the table object.
    The field provides convenient access methods to it.
    """

    _default_sort_key: Optional[Union[bytes, int, datetime.datetime, tuple]]
    _sort_key: Callable[
        [
            D,
        ],
        Union[D, tuple[str]],
    ]
    # Union[T, tuple[str]] - because the composite field returns a tuple of strings - for some reason

    is_many: bool = False
    is_many_many: bool = False
    is_composite: bool = False

    generic_val: T

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
        # Todo: datatype, table_type should be enums
        self.name: str = name
        self.table = table

        # Store common field configuration so mixins (e.g. calibre-emulation
        # fields) can rely on these attributes existing.
        self.bools_are_tristate: bool = bools_are_tristate
        self.link_attributes = link_attributes
        self.main_table: Optional[str] = main_table
        self.auxiliary_table: Optional[str] = auxiliary_table

        # Link-attribute fields (e.g. series_index) are stored here when present.
        # Most fields have none.
        self.link_attr_fields: dict[str, Any] = {}

        dt: str = self.metadata["datatype"]
        self.has_text_data: bool = dt in {"text", "comments", "series", "enumeration"}

        # Some codepaths expect this to exist for writer selection.
        self.table_type = self.table.table_type

        self._sort_key = sort_key if dt in ("text", "series", "enumeration") else IDENTITY

        # Ensure *all* fields have a writer early, so calibre-style field init
        # can safely do `self.table.writer = self.writer` without exploding.
        try:
            self._writer = get_writer(self)
        except Exception:
            # Ultra-safe fallback: supports cache init even when writer selection
            # can't be resolved yet (or a field is intentionally non-writable).
            self._writer = DummyWriter(self)

        try:
            self.table.writer = self._writer
        except AttributeError:
            # Some ephemeral / test tables may not expose writer slots.
            pass

    def get_link_attrs(self) -> Iterable[str]:
        """
        Return valid link_attr names.

        :return:
        """
        return self.link_attr_fields.keys()

    def __getitem__(self, item: str) -> Any:
        """
        Allows a [] interface to the stored link_attrs.

        This will return the link attribute with the given name.
        :param item:
        :return value: The result of getting this item from the link_attr_fields dict.
        """
        return self.link_attr_fields[item]

    def startup_link_attr_fields(self):
        """
        Startup the link attribute fields - which additionally characterizes the link between main and auxiliary tables.

        E.g. "index" on a "series" field in "titles".
        :return:
        """
        pass

    def read_attribute_tables(self, db) -> None:
        """
        Read any *link-attribute* tables associated with this field.

        In calibre-style schemas, some many-to-many links have extra columns
        (for example, a series link might have a series index). Those extras
        are represented as additional "attribute fields" stored in
        ``self.link_attr_fields``.

        Most fields have no attribute tables, so the default implementation is
        a no-op.

        :param db: Database backend / driver wrapper used by tables to read.
        """
        # Be defensive: link_attr_fields may be empty (normal) or contain
        # objects that are either Field-like (with .table) or Table-like.
        for _name, attr in getattr(self, "link_attr_fields", {}).items():
            table = getattr(attr, "table", attr)
            read = getattr(table, "read", None)
            if callable(read):
                read(db)

    # Allows for updating the writer stored in the table at the same time as the writer here is updated
    # Should be simplified
    @property
    def writer(self):
        """
        Write is a tool to writing data out to the table in the database when it's changed in the field.

        :return:
        """
        return self._writer

    @writer.setter
    def writer(self, new_writer) -> None:
        """
        Changing the writer should also change the writer in the table.

        :param new_writer:
        :return:
        """
        self._writer = new_writer
        try:
            self.table.writer = self._writer
        except AttributeError:
            pass

    @property
    def default_value(self) -> D:
        """
        Return the default value for this field.

        :return:
        """
        return deepcopy(self._default_value)

    @property
    def metadata(self):
        """
        Return the metadata of the underlying table.

        :return:
        """
        return self.table.metadata

    def book_in_cache(self, book_id: int) -> bool:
        """
        Check to see if the given book is in the folder store - returns True if it is and False if it isn't.

        :param book_id:
        :return True/False:
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def item_in_cache(self, item_id: int) -> bool:
        """
        Return True if the given item is in the cache and False otherwise.

        If the table and the auxiliary table are OneToOne then the id of the item is assumed to be the id of that table.
        E.g. if this field is for "titles" then the id will just be the id of the title.
        E.g. if this field is for "title tags" then the id will be the id of a given tag in the auxiliary table.
        :param item_id:
        :return True/False:
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def for_book(self, book_id: int, default_value: Optional[D] = None):
        """
        Return the value of this field for the book identified by book_id.

        When no value is found, returns ``default_value``.
        :param book_id:
        :param default_value:
        :return:
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def ids_for_book(self, book_id: int) -> tuple[int, ...]:
        """
        Return a tuple of items ids for items associated with the book identified by book_ids.

        Returns an empty tuple if no such items are found.
        :param book_id:
        :return:
        """
        raise NotImplementedError("Method not implemented in base Table method")

    # Todo: In a system where order has meaning, shouldn't this be a tuple?
    def books_for(self, item_id: int) -> set[int, ...]:
        """
        Return the ids of all books associated with the item identified by item_id as a set. An empty set is returned if
        no books are found.
        :param item_id:
        :return:
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def __iter__(self) -> Iterator[D]:
        """
        Iterate over the ids for all values in this field.

        WARNING: Some fields such as composite fields and virtual fields like ondevice do not have ids for their values,
        in such cases this is an empty iterator.
        """
        return iter(())

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Return a function that maps book_id to sort_key.

        The sort key is suitable for use in sorting the list of all books by this field, via the python cmp method.
        :param get_metadata:
        :param lang_map:
        :return:
        """
        raise NotImplementedError

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Return a generator that yields items of the form (value, set of books ids that have this value).

        Here, value is a searchable value.
        Returned books_ids are restricted to the set of ids in candidates.
        :param get_metadata:
        :param candidates:
        :param default_value:
        :return:
        """
        raise NotImplementedError

    def get_categories(self, tag_class, book_rating_map, lang_map, book_ids: Iterable[int] = None):
        """
        Still not 100% sure what this is supposed to do.

        It's probably broken though.
        :param tag_class:
        :param book_rating_map:
        :param lang_map:
        :param book_ids:
        :return:
        """
        raise NotImplementedError

    def update_cache(self, book_id_val_map: dict[int, D], id_map: Optional[dict[int, D]] = None) -> bool:
        """
        Preform an update of the book_col_map (also the col_book_map, if required).
        :param book_id_val_map: Keyed with the value of the book and valued with the new value for that book (in cases
                                where the book_item map is one to one or the item can be uniquely identified in some
                                other way, then this value might just be the new value for the book)
        :param id_map: Keyed with the id of the update and valued with the new value.
        :return status: Did the field update successfully
        """
        raise NotImplementedError

    def update_db(self, book_id_to_val_map: dict[int, D], db, allow_case_change: bool = False) -> bool:
        """
        Preform an update of the database - should return the data needed to preform an update of the cache.

        :param book_id_to_val_map: Keyed with the item id and valued with the new item value
                                   If the map is 1-1 this might just be the new value - if it's a different type of map
                                   it might be more complicated.
        :param db:
        :param allow_case_change:
        :return status: Did the database update successfully?
        """
        raise NotImplementedError


class BaseOneToManyField(BaseField[T]):
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
