"""
Model cached metadata fields and their lookup behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base field through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
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

    Example:
        Exercise identity through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return x


IDENTITY = identity


class BaseField(Generic[T]):
    """
    Basis for a representation of a field on the database.

    Example:
        Exercise BaseField through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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
        Initialize and validate the basefield state.

        Example:
            Exercise BaseField.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


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

        Example:
            Exercise BaseField.get link attrs through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.link_attr_fields.keys()

    def __getitem__(self, item: str) -> Any:
        """
        Allows a [] interface to the stored link_attrs.

        Example:
            Exercise BaseField.  getitem   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.link_attr_fields[item]

    def startup_link_attr_fields(self):
        """
        Startup the link attribute fields - which additionally characterizes the link between main and auxiliary tables.

        Example:
            Exercise BaseField.startup link attr fields through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def read_attribute_tables(self, db) -> None:
        """
        Read any *link-attribute* tables associated with this field.

        Example:
            Exercise BaseField.read attribute tables through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise BaseField.writer through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._writer

    @writer.setter
    def writer(self, new_writer) -> None:
        """
        Changing the writer should also change the writer in the table.

        Example:
            Exercise BaseField.writer through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param new_writer: Value supplied for new writer under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise BaseField.default value through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self._default_value)

    @property
    def metadata(self):
        """
        Return the metadata of the underlying table.

        Example:
            Exercise BaseField.metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.metadata

    def book_in_cache(self, book_id: int) -> bool:
        """
        Check to see if the given book is in the folder store - returns True if it is and False if it isn't.

        Example:
            Exercise BaseField.book in cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def item_in_cache(self, item_id: int) -> bool:
        """
        Return True if the given item is in the cache and False otherwise.

        Example:
            Exercise BaseField.item in cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def for_book(self, book_id: int, default_value: Optional[D] = None):
        """
        Return the value of this field for the book identified by book_id.

        Example:
            Exercise BaseField.for book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def ids_for_book(self, book_id: int) -> tuple[int, ...]:
        """
        Return a tuple of items ids for items associated with the book identified by book_ids.

        Example:
            Exercise BaseField.ids for book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Method not implemented in base Table method")

    # Todo: In a system where order has meaning, shouldn't this be a tuple?
    def books_for(self, item_id: int) -> set[int, ...]:
        """
        Return the ids of all books associated with the item identified by item_id as a set. An empty set is returned if no books are found.

        Example:
            Exercise BaseField.books for through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Method not implemented in base Table method")

    def __iter__(self) -> Iterator[D]:
        """
        Iterate over the ids for all values in this field.

        Example:
            Exercise BaseField.  iter   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(())

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Return a function that maps book_id to sort_key.

        Example:
            Exercise BaseField.sort keys for books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Return a generator that yields items of the form (value, set of books ids that have this value).

        Example:
            Exercise BaseField.iter searchable values through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_categories(self, tag_class, book_rating_map, lang_map, book_ids: Iterable[int] = None):
        """
        Still not 100% sure what this is supposed to do.

        Example:
            Exercise BaseField.get categories through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def update_cache(self, book_id_val_map: dict[int, D], id_map: Optional[dict[int, D]] = None) -> bool:
        """
        Preform an update of the book_col_map (also the col_book_map, if required).

        Example:
            Exercise BaseField.update cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def update_db(self, book_id_to_val_map: dict[int, D], db, allow_case_change: bool = False) -> bool:
        """
        Preform an update of the database - should return the data needed to preform an update of the cache.

        Example:
            Exercise BaseField.update db through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseOneToManyField(BaseField[T]):
    """
    For a Many-to-Many or One-to-Many table that has to pretend to be a 1-1 table.

    Example:
        Exercise BaseOneToManyField through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def ids_for_book(self, book_id: SrcTableID) -> set[DstTableID]:
        """
        The table is pretending to be 1-1 - so this method does not make sense.

        Example:
            Exercise BaseOneToManyField.ids for book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


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

                python -m pytest -q tests/customize/test_customize_base.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError
