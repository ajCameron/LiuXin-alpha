#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

# Fields are in memory things which must be stored in addition to the other stuff in the library - for example, what is
# on a specific device - represents a column on the database
# Fields hold a reference to the table object the field is in - so multiple fields can be assigned to a table.

"""
Model cached library fields and their value mappings.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise fields through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function

from collections import defaultdict, Counter
from copy import deepcopy
from threading import Lock

from LiuXin_alpha.utils.libraries.liuxin_six import iterkeys
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.databases.macro_types import LinkValue

# Todo: These should be stored in base.tables
from LiuXin_alpha.customize.cache.base_tables import (
    ONE_ONE,
    MANY_ONE,
    MANY_MANY,
    ONE_MANY,
    null,
)

from LiuXin_alpha.library.caches.base_calibre.fields import (
    InvalidLinkTable,
    IDENTITY,
)
from LiuXin_alpha.library.caches.base_calibre.fields import BaseField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseOneToOneField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseOneToManyField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseCompositeField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseOnDeviceField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseManyToOneField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseManyToManyField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseIdentifiersField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseAuthorsField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseFormatsField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseCoverField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseSeriesField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseTagsField
from LiuXin_alpha.library.caches.base_calibre.fields import BaseLinkAttributeField
from LiuXin_alpha.library.caches.calibre.tables.base import CalibreVirtualTable

from LiuXin_alpha.library.caches.calibre.tables.one_many_tables import (
    CalibrePriorityTypedOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables import (
    CalibrePriorityOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables import (
    CalibreTypedOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables import CalibreOneToManyTable

from LiuXin_alpha.library.caches.calibre.tables.many_one_tables import (
    CalibrePriorityManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables import (
    CalibrePriorityTypedManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables import (
    CalibreTypedManyToOneTable,
)

from LiuXin_alpha.library.caches.calibre.tables.many_many_tables import (
    CalibrePriorityManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables import (
    CalibrePriorityTypedManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables import (
    CalibreTypedManyToManyTable,
)

from LiuXin_alpha.library.caches.calibre.tables.link_attribute_tables import (
    create_link_attribute_table,
)

from LiuXin_alpha.library.caches.utils import LazySortMap

from LiuXin_alpha.errors import NoSuchBook, NoSuchFormatInCache, NotInCache, InvalidDBUpdate, InvalidCacheUpdate, InvalidUpdate

from LiuXin_alpha.storage.api.models import Location

from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort

from LiuXin_alpha.preferences import preferences as tweaks

from LiuXin_alpha.utils.text.icu import sort_key
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems, basestring



__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class CalibreField(BaseField):
    """
    Represents a field on the books table - here for calibre emulation.

    Example:
        Exercise CalibreField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    _complex_update = False

    @property
    def complex_update(self) -> bool:
        """
        Get the complex_update status of the field.

        Example:
            Exercise CalibreField.complex update through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._complex_update

    @complex_update.setter
    def complex_update(self, val: bool) -> None:
        """
        Refuse to set the complex_update of the field.

        Example:
            Exercise CalibreField.complex update through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise AttributeError("complex_update cannot be dynamically set.")

    # Used to update the write in the table when changes are made to the writer here
    @property
    def writer(self):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._writer

    @writer.setter
    def writer(self, new_writer):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_writer: Value supplied for new writer under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._writer = new_writer
        self.table.writer = self._writer

    def startup_link_attr_fields(self):
        """
        Startup the link attribute fields - which additionally characterizes the link between the main and auxiliary table.

        Example:
            Exercise CalibreField.startup link attr fields through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # In the null case where no attributes need to be loaded.
        if self.link_attributes is None:
            return

        for link_attr_name in self.link_attributes:
            # Need to startup the table
            link_attr_field = calibre_create_link_attribute_field(name=link_attr_name, field=self)
            self.link_attr_fields[link_attr_name] = link_attr_field

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - METHOD UPDATE METHODS

    def _change_update_cache_method(self, new_method):
        """
        Change the currently in use update_cache method - used to bind a new method with new behavior over the top of the old.

        Example:
            Exercise CalibreField. change update cache method through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_method: Value supplied for new method under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        object.__setattr__(self, "update_cache", new_method)

    def _change_ids_for_book_method(self, new_method):
        """
        Change the currently in use ids_for_book method - which binds a new method with new behavior (suited to the particular backend in use) over the top of the currently in use method.

        Example:
            Exercise CalibreField. change ids for book method through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_method: Value supplied for new method under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        object.__setattr__(self, "ids_for_book", new_method)

    def _change_for_book_method(self, new_method):
        """
        Change the currently in use for_book method - which binds a new method with new behavior (suited to the particular backend in use) over the top of the currently in use method.

        Example:
            Exercise CalibreField. change for book method through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_method: Value supplied for new method under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        object.__setattr__(self, "for_book", new_method)

    def _change_books_for_method(self, new_method):
        """
        Perform the change books for method operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreField. change books for method through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_method: Value supplied for new method under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        object.__setattr__(self, "books_for", new_method)

    def _change_iter_searchable_values(self, new_method):
        """
        Perform the change iter searchable values operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreField. change iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_method: Value supplied for new method under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        object.__setattr__(self, "iter_searchable_values", new_method)

    #
    # ------------------------------------------------------------------------------------------------------------------

    def get_categories(self, tag_class, book_rating_map, lang_map, book_ids=None):
        """
        Still not 100% sure what this is supposed to do. It's probably broken though.

        Example:
            Exercise CalibreField.get categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []
        if not self.is_many:
            return ans

        id_map = self.table.id_map
        special_sort = hasattr(self, "category_sort_value")
        for item_id, item_book_ids in iteritems(self.table.col_book_map):

            if book_ids is not None:
                item_book_ids = item_book_ids.intersection(book_ids)

            if item_book_ids:
                ratings = tuple(r for r in (book_rating_map.get(book_id, 0) for book_id in item_book_ids) if r > 0)
                avg = sum(ratings) / len(ratings) if ratings else 0
                try:
                    name = self.category_formatter(id_map[item_id])
                except KeyError:
                    # db has entries in the link table without entries in the
                    # id table, for example, see
                    # https://bugs.launchpad.net/bugs/1218783
                    raise InvalidLinkTable(self.name)
                sval = self.category_sort_value(item_id, item_book_ids, lang_map) if special_sort else name
                c = tag_class(
                    name,
                    id=item_id,
                    sort=sval,
                    avg=avg,
                    id_set=item_book_ids,
                    count=len(item_book_ids),
                )
                ans.append(c)

        return ans

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE METHODS
    def update(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreField.update through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.table.update_precheck(book_id_item_id_map=book_id_to_val_map, id_map_update=dict())

        # Update the database (do this first and keep this separate - if this fails then we don't want to update the
        # cache)
        # Todo: Need to make sure everything returns an appropriate dirtied for later use by the update_cache method
        try:
            dirtied = self.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)
        except NotImplementedError as e:
            err_str = "NotImplementedError while trying to update_db "
            err_str = default_log.log_exception(err_str, e, "ERROR", ("f", self))
            raise InvalidUpdate(err_str)
        except (InvalidCacheUpdate, InvalidDBUpdate) as e:
            err_str = "Error while trying to update_db "
            err_str = default_log.log_exception(err_str, e, "ERROR", ("f", self))
            raise InvalidUpdate(err_str)
        except Exception as e:
            err_str = "Error while calling to f.writer.set_books"
            default_log.log_exception(err_str, e, "ERROR", ("f", self))
            raise

        id_map = None
        book_col_map = None
        if isinstance(dirtied, dict):
            id_map = dirtied["id_map"]
            book_col_map = dirtied["book_col_map"]
            dirtied = dirtied["dirtied"]

        book_col_map = book_col_map if book_col_map is not None else book_id_to_val_map

        if hasattr(self, "internal_update_used") and self.internal_update_used:
            pass
        elif self.name in [
            "publisher",
        ]:
            pass
        else:
            self.update_cache(book_col_map, id_map=id_map)

        if dirtied is None:
            err_str = "f.update_db returned None - which should not happen"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("f", self),
                ("f.table", self.table),
                ("f.table.name", self.table.name),
                ("type(f.writer.set_books", type(self.writer.set_books)),
                ("type(f.writer)", type(self.writer)),
            )
            raise AssertionError(err_str)

        return dirtied

    def update_preflight(self, book_id_item_id_map, id_map_update, dirtied=None):
        """
        Gives the table a chance to bring the :param book_id_item_id_map:

        Example:
            Exercise CalibreField.update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param dirtied: Value supplied for dirtied under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_preflight(book_id_item_id_map, id_map_update, dirtied=dirtied)

    def update_precheck(self, book_id_item_id_map, id_map_update):
        """
        Preform validation that the update is correctly formatted (refers to ids which exist). As the details of what constitutes a valid update depends on how the data is ordered, this check should be preformed at the table level.

        Example:
            Exercise CalibreField.update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.table.update_precheck(book_id_item_id_map, id_map_update)

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.table.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)
        except AttributeError:
            raise NotImplementedError

    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preforms an update of the cache.

        Example:
            Exercise CalibreField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.table.update_cache(book_id_val_map, id_map)

    def internal_update_cache(self, book_id_item_id_map, id_map_update):
        """
        Update cache with some additional information provided

        Example:
            Exercise CalibreField.internal update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.internal_update_cache(book_id_item_id_map, id_map_update)

    #
    # ------------------------------------------------------------------------------------------------------------------


# Todo: Check, here and in tables, that we're okay with the class variables - method resolution order is fine?
# Todo: This should really inherit from Field - fix inheritance for the others
class CalibreOneToOneField(BaseOneToOneField, CalibreField):
    """
    A 1-1 mapping must exist between books and these fields. (E.g. The uuid of a book).

    Example:
        Exercise CalibreOneToOneField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    table_type: int = ONE_ONE

    def __init__(self, name, table, bools_are_tristate):
        """
        Preforms custom startup - depending on the type of table different behavior is required to emulate a OneToOne table.

        Example:
            Exercise CalibreOneToOneField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreOneToOneField, self).__init__(name, table, bools_are_tristate)

        self.table_is_pseudo = False

        # Examine the table to check that we CAN emulate a OneToOne field with the given table
        # Preform the needed changes to the access methods to accomodate the backend table
        if isinstance(table, (CalibrePriorityOneToManyTable,)):
            self._change_update_cache_method(self._use_table_otm_update_cache)
            self._change_ids_for_book_method(self._to_many_ids_for_book)
            self._change_for_book_method(self._to_many_for_book)
            self._change_books_for_method(self._one_to_many_books_for)

            # Register that the field is a pseudo field - that the underlying table is not of the same type as the field
            self.table_is_pseudo = True
        elif isinstance(table, (CalibrePriorityManyToManyTable,)):
            self._change_update_cache_method(self._use_table_update_cache)
            self._change_ids_for_book_method(self._to_many_ids_for_book)
            # Todo: These should probably be different methods
            self._change_for_book_method(self._to_many_for_book)
            self._change_books_for_method(self._many_to_many_books_for)
            self._change_iter_searchable_values(self._many_to_many_iter_searchable_values)

            self.table_is_pseudo = True

        elif isinstance(table, (CalibrePriorityManyToOneTable,)):

            self._change_update_cache_method(self._use_table_mto_update_cache)

            self.table_is_pseudo = True

    # Used to update the write in the table when changes are made to the writer here
    @property
    def writer(self):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToOneField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._writer

    @writer.setter
    def writer(self, new_writer):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToOneField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_writer: Value supplied for new writer under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._writer = new_writer
        self.table.writer = self._writer

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BACKEND ACCESS METHODS
    def _to_many_ids_for_book(self, book_id):
        """
        ids_for_book method suitable for the case where many items are linked to each book with only a priority for sorting and not a type for sub-typing.

        Example:
            Exercise CalibreOneToOneField. to many ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.book_col_map:
            return None
        book_vals = self.table.book_col_map[book_id]
        return (book_vals[0],) if len(book_vals) > 0 else None

    def _to_many_for_book(self, book_id, default_value=None):
        """
        Return the primary value string for the book.

        Example:
            Exercise CalibreOneToOneField. to many for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.book_col_map:
            return default_value
        book_val_ids = self.table.book_col_map[book_id]
        book_val_id = book_val_ids[0] if len(book_val_ids) > 0 else None
        if book_val_id is None:
            return default_value
        return self.table.id_map[book_val_id]

    def _one_to_many_books_for(self, item_id):
        """
        Perform the one to many books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToOneField. one to many books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.col_book_map:
            raise NotInCache
        return self.table.col_book_map[item_id]

    def _many_to_many_books_for(self, item_id):
        """
        Perform the many to many books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToOneField. many to many books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.col_book_map:
            raise NotInCache
        item_ids = self.table.col_book_map[item_id]
        return item_ids

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE CACHE METHODS
    def _use_table_update_cache(self, book_id_val_map, id_map=None):
        """
        Use the update_cache method from the table to preform updates to it's internal data stores.

        Example:
            Exercise CalibreOneToOneField. use table update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_cache(book_id_val_map=book_id_val_map, id_map=id_map)

    def _use_table_otm_update_cache(self, book_id_val_map, id_map=None):
        """
        Preform an update - with a preflight.

        Example:
            Exercise CalibreOneToOneField. use table otm update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id_val_map, id_map = self.table.update_preflight(book_id_val_map, id_map)
        return self.table.update_cache(book_id_val_map=book_id_val_map, id_map=id_map)

    def _use_table_mto_update_cache(self, book_id_val_map, id_map=None):
        """
        Preform an update - with a preflight.

        Example:
            Exercise CalibreOneToOneField. use table mto update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id_val_map, id_map = self.table.update_preflight(book_id_val_map, id_map)
        return self.table.update_cache(book_id_val_map=book_id_val_map, id_map=id_map)

    #
    # ------------------------------------------------------------------------------------------------------------------

    def book_in_cache(self, book_id):
        """
        Return True if the book is in the cache and False otherwise.

        Example:
            Exercise CalibreOneToOneField.book in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return book_id in self.table.book_col_map

    def item_in_cache(self, item_id):
        """
        Return True if the book is in the cache and False otherwise. If the table and the backend are OneToOne then the id of the item is assumed to be the id of the title.

        Example:
            Exercise CalibreOneToOneField.item in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return item_id in self.table.book_col_map

    def for_book(self, book_id, default_value=None):
        """
        Returns the cached value for the book, or the default value if that evaluates to None. Raises NotInCache if there is no value in the cache for the given title.

        Example:
            Exercise CalibreOneToOneField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.book_col_map:
            raise NotInCache
        cache_val = self.table.book_col_map.get(book_id, default_value)
        if cache_val:
            return cache_val
        else:
            return default_value

    def books_for(self, item_id):
        """
        Returns the books for the given item id. By default this is assumed to be a one to one field - so the id of the item is the same as the id of the book. If this is not the case (as for when the table is pseudo) other logic needs to be substituted and used.

        Example:
            Exercise CalibreOneToOneField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Checks that the id of the item - which is the same as the id of the title - is known to the system
        if item_id not in self.table.book_col_map:
            raise NotInCache
        return {
            item_id,
        }

    # Todo: THis may behave ... oddly ... with LiuXin emulation.
    def __iter__(self):
        """
        Iterates through the book_col_map - returning all the book ids in it.

        Example:
            Exercise CalibreOneToOneField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iterkeys(self.table.book_col_map)

    def update_db(self, book_id_to_val_map, db, dirtied=None, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreOneToOneField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param dirtied: Value supplied for dirtied under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)

    # Todo: This needs to be true for every other field type as well
    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preform an update of the book_col_map (also the col_book_map, if required).

        Example:
            Exercise CalibreOneToOneField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.table.book_col_map.update(book_id_val_map)

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Perform the sort keys for books operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToOneField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bcmg = self.table.book_col_map.get
        dk = self._default_sort_key
        sk = self._sort_key
        if sk is IDENTITY:
            return lambda book_id: bcmg(book_id, dk)
        return lambda book_id: sk(bcmg(book_id, dk))

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise CalibreOneToOneField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        cbm = self.table.book_col_map
        for book_id in candidates:
            yield cbm.get(book_id, default_value), {book_id}

    def _many_to_many_iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Perform the many to many iter searchable values operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToOneField. many to many iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        cbm = self.table.book_col_map
        idm = self.table.id_map
        for book_id in candidates:
            book_ids = cbm.get(book_id, default_value)
            book_val = idm[book_ids[0]] if len(book_ids) > 0 else None
            yield book_val, {book_id}


class CalibreCompositeField(BaseCompositeField, CalibreField):
    """
    Composite fields are meta-fields produced using data from a number of other fields. Rendering composite fields can be an expensive operaiton - this field caches the results of those renders for later, easy retrieval. Most of the cached data is stored over in the table object - this is one of the few exceptions.

    Example:
        Exercise CalibreCompositeField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name, table, bools_are_tristate):
        """
        Construct the field to represent the cache. The formatter which does the work of following the template is stored over in metadata as mi.formatter

        Example:
            Exercise CalibreCompositeField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseCompositeField.__init__(self, name=name, table=table, bools_are_tristate=bools_are_tristate)

        self._lock = Lock()
        self._render_cache = {}

    def __render_composite(self, book_id, mi, formatter, template_cache):
        """
        INTERNAL USE ONLY. DO NOT USE THIS OUTSIDE THIS CLASS!

        Example:
            Exercise CalibreCompositeField.  render composite through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param formatter: Template formatter supplying evaluation services and context.
        :param template_cache: Value supplied for template cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = formatter.safe_format(
            self.metadata.get("display", {}).get("composite_template", ""),
            mi,
            _("TEMPLATE ERROR"),
            mi,
            column_name=self._composite_name,
            template_cache=template_cache,
        ).strip()
        with self._lock:
            self._render_cache[book_id] = ans
        return ans

    def _render_composite_with_cache(self, book_id, mi, formatter, template_cache):
        """
        INTERNAL USE ONLY. DO NOT USE METHOD DIRECTLY. INSTEAD USE db.composite_for() OR mi.get(). Those methods make sure there is no risk of infinite recursion when evaluating templates that refer to themselves.

        Example:
            Exercise CalibreCompositeField. render composite with cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param formatter: Template formatter supplying evaluation services and context.
        :param template_cache: Value supplied for template cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            ans = self._render_cache.get(book_id, None)
        if ans is None:
            return self.__render_composite(book_id, mi, formatter, template_cache)
        return ans

    def clear_caches(self, book_ids=None):
        """
        Perform the clear caches operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreCompositeField.clear caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with self._lock:
            if book_ids is None:
                self._render_cache.clear()
            else:
                for book_id in book_ids:
                    self._render_cache.pop(book_id, None)

    def get_value_with_cache(self, book_id, get_metadata):
        """
        Return value with cache under the format's safety and compatibility rules.

        Example:
            Exercise CalibreCompositeField.get value with cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param get_metadata: Value supplied for get metadata under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            ans = self._render_cache.get(book_id, None)
        if ans is None:
            mi = get_metadata(book_id)
            return self.__render_composite(book_id, mi, mi.formatter, mi.template_cache)
        return ans

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Perform the sort keys for books operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreCompositeField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        gv = self.get_value_with_cache
        sk = self._sort_key
        if sk is IDENTITY:
            return lambda book_id: gv(book_id, get_metadata)
        return lambda book_id: sk(gv(book_id, get_metadata))

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise CalibreCompositeField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        val_map = defaultdict(set)
        splitter = self.splitter
        for book_id in candidates:
            vals = self.get_value_with_cache(book_id, get_metadata)
            vals = (vv.strip() for vv in vals.split(splitter)) if splitter else (vals,)
            for v in vals:
                if v:
                    val_map[v].add(book_id)
        for val, book_ids in iteritems(val_map):
            yield val, book_ids

    def get_composite_categories(self, tag_class, book_rating_map, book_ids, is_multiple, get_metadata):
        """
        Return composite categories under the format's safety and compatibility rules.

        Example:
            Exercise CalibreCompositeField.get composite categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param book_ids: Book identities included in the batched read operation.
        :param is_multiple: Value supplied for is multiple under the utility contract.
        :param get_metadata: Value supplied for get metadata under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []
        id_map = defaultdict(set)
        for book_id in book_ids:
            val = self.get_value_with_cache(book_id, get_metadata)
            vals = [x.strip() for x in val.split(is_multiple)] if is_multiple else [val]
            for val in vals:
                if val:
                    id_map[val].add(book_id)
        for item_id, item_book_ids in iteritems(id_map):
            ratings = tuple(r for r in (book_rating_map.get(book_id, 0) for book_id in item_book_ids) if r > 0)
            avg = sum(ratings) / len(ratings) if ratings else 0
            c = tag_class(
                item_id,
                id=item_id,
                sort=item_id,
                avg=avg,
                id_set=item_book_ids,
                count=len(item_book_ids),
            )
            ans.append(c)
        return ans

    def get_books_for_val(self, value, get_metadata, book_ids):
        """
        Return books for val under the format's safety and compatibility rules.

        Example:
            Exercise CalibreCompositeField.get books for val through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param value: Value normalized, stored, formatted or returned.
        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        is_multiple = self.table.metadata["is_multiple"].get("cache_to_list", None)
        ans = set()
        for book_id in book_ids:
            val = self.get_value_with_cache(book_id, get_metadata)
            vals = {x.strip() for x in val.split(is_multiple)} if is_multiple else [val]
            if value in vals:
                ans.add(book_id)
        return ans


class CalibreOnDeviceField(BaseOnDeviceField, CalibreField):
    """
    Provide the calibreondevicefield contract for validated ebook processing.

    Example:
        Exercise CalibreOnDeviceField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, name, table, bools_are_tristate):

        """
        Initialize and validate the calibreondevicefield state.

        Example:
            Exercise CalibreOnDeviceField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreOnDeviceField, self).__init__(name, table, bools_are_tristate)

        self.table = CalibreVirtualTable(name="ondevice", datatype="bool")

        self.cache = {}
        self._lock = Lock()

    @property
    def writer(self):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOnDeviceField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._writer

    @writer.setter
    def writer(self, new_writer):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOnDeviceField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_writer: Value supplied for new writer under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._writer = new_writer

    def clear_caches(self, book_ids=None):
        """
        Perform the clear caches operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOnDeviceField.clear caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with self._lock:
            if book_ids is None:
                self.cache.clear()
            else:
                for book_id in book_ids:
                    self.cache.pop(book_id, None)

    def book_on_device(self, book_id):
        """
        Perform the book on device operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOnDeviceField.book on device through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            ans = self.cache.get(book_id, null)
        if ans is null and callable(self.book_on_device_func):
            ans = self.book_on_device_func(book_id)
            with self._lock:
                self.cache[book_id] = ans
        return None if ans is null else ans

    def set_book_on_device_func(self, func):
        """
        Set book on device func under the format's safety and compatibility rules.

        Example:
            Exercise CalibreOnDeviceField.set book on device func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param func: Value supplied for func under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.book_on_device_func = func

    def for_book(self, book_id, default_value=None):
        """
        Perform the for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOnDeviceField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        loc = []
        count = 0
        on = self.book_on_device(book_id)
        if on is not None:
            m, a, b, count = on[:4]
            if m is not None:
                loc.append(_("Main"))
            if a is not None:
                loc.append(_("Card A"))
            if b is not None:
                loc.append(_("Card B"))
        return ", ".join(loc) + ((" (%s books)" % count) if count > 1 else "")

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise CalibreOnDeviceField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        val_map = defaultdict(set)
        for book_id in candidates:
            val_map[self.for_book(book_id, default_value=default_value)].add(book_id)
        for val, book_ids in iteritems(val_map):
            yield val, book_ids


class CalibreOneToManyField(BaseOneToManyField, CalibreField):
    """
    A 1-Many field is for the case where one book is linked to many targets - but no other books are linked to any of the items linked to (e.g. comments - one book is linked to many targets).

    Example:
        Exercise CalibreOneToManyField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    table_type: int = ONE_MANY

    def __init__(self, name, table, bools_are_tristate=False):

        # Set the global default for val_unique
        # Todo: This should not be done here
        """
        Initialize and validate the calibreonetomanyfield state.

        Example:
            Exercise CalibreOneToManyField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        if not hasattr(table, "val_unique"):
            table.val_unique = False

        super(CalibreOneToManyField, self).__init__(name=name, table=table, bools_are_tristate=bools_are_tristate)

        # Needs to be in this order due to the inheritance chain
        if isinstance(table, CalibrePriorityTypedOneToManyTable):
            self._change_for_book_method(self._ptotm_for_book)
            self._change_ids_for_book_method(self._ptotm_ids_for_book)

        elif isinstance(table, CalibrePriorityOneToManyTable):

            self._change_for_book_method(self._potm_for_book)
            self._change_ids_for_book_method(self._potm_ids_for_book)

        elif isinstance(table, CalibreTypedOneToManyTable):
            self._change_for_book_method(self._totm_for_book)
            self._change_ids_for_book_method(self._totm_ids_for_book)

        # Todo: Probably should just ... be the default?
        elif isinstance(table, CalibreOneToManyTable):
            self._change_for_book_method(self._otm_for_book)
            self._change_ids_for_book_method(self._otm_ids_for_book)

        if self.table.val_unique:
            if isinstance(table, CalibreOneToManyTable):
                self._change_books_for_method(self._totm_unique_books_for)
            elif isinstance(table, CalibrePriorityOneToManyTable):
                self._change_books_for_method(self._potm_unique_books_for)
            elif isinstance(table, CalibreTypedOneToManyTable):
                self._change_books_for_method(self._totm_unique_books_for)
            elif isinstance(table, CalibrePriorityTypedOneToManyTable):
                self._change_books_for_method(self._totm_unique_books_for)
            else:
                self._change_books_for_method(self.unique_books_for)

    # Used to update the write in the table when changes are made to the writer here
    @property
    def writer(self):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._writer

    @writer.setter
    def writer(self, new_writer):
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField.writer through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param new_writer: Value supplied for new writer under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._writer = new_writer
        self.table.writer = self._writer

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FOR_BOOK METHODS

    def for_book(self, book_id, default_value=None):
        """
        Return either the single value for the given book_id or all the comments linked to the book

        Example:
            Exercise CalibreOneToManyField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ids = self.table.book_col_map.get(book_id, None)
        id_ = ids[0] if ids is not None and len(ids) != 0 else None
        if id_ is not None:
            ans = self.table.id_map[id_]
        else:
            ans = default_value
        return ans

    def _otm_for_book(self, book_id, default_value=None):
        """
        Replacement for_book method for when the backend table is of default CalibreOneToMany type.

        Example:
            Exercise CalibreOneToManyField. otm for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache("book_id not found in the cache")
        ids = self.table.book_col_map.get(book_id, None)
        if not ids:
            return default_value
        return set(self.table.id_map[id_] for id_ in ids)

    def _totm_for_book(self, book_id, default_value=None):
        """
        Replacement for_book method for when the backend table is of CalibrePriorityTypedOneToMany type.

        Example:
            Exercise CalibreOneToManyField. totm for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        book_data = self.table.vals_book_data(book_id=book_id)
        if self._is_data_null(book_data):
            return default_value
        else:
            return book_data

    def _potm_for_book(self, book_id, default_value=None):
        """
        Replacement for_book method for when the backend table is of CalibrePriorityOneToMany type.

        Example:
            Exercise CalibreOneToManyField. potm for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache("book_id not found in cache")
        ids = self.table.book_col_map.get(book_id, ())
        if ids:
            return list(self.table.id_map[id_] for id_ in ids)
        else:
            return default_value

    def _ptotm_for_book(self, book_id, default_value=None):
        """
        Replacement for_book method for when the backend table is of CalibrePriorityTypedOneToMany type.

        Example:
            Exercise CalibreOneToManyField. ptotm for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        vals_book_data = self.table.vals_book_data(book_id=book_id)
        if self._is_data_null(vals_book_data):
            return default_value
        else:
            return vals_book_data

    def _is_data_null(self, data_map):
        """
        Returns True if the given data map has no content and False otherwise.

        Example:
            Exercise CalibreOneToManyField. is data null through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param data_map: Value supplied for data map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for link_type, link_vals in iteritems(data_map):
            if link_vals:
                return False
        return True

    #
    # ------------------------------------------------------------------------------------------------------------------

    # Todo: Actually in the pseudo one to one type field
    def ids_for_book(self, book_id):
        """
        Returns the ids for the given book

        Example:
            Exercise CalibreOneToManyField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self.table.book_col_map.get(book_id, None))

    def _otm_ids_for_book(self, book_id, default_value=None):
        """
        Replacement for_book method for when the backend table is of default CalibreOneToMany type.

        Example:
            Exercise CalibreOneToManyField. otm ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache("book_id not found in the cache")
        ids = self.table.book_col_map.get(book_id, None)
        if not ids:
            return default_value
        return set(ids)

    def _potm_ids_for_book(self, book_id, default_value=None):
        """
        Replacement for_book method for when the backend table is of CalibrePriorityOneToMany type.

        Example:
            Exercise CalibreOneToManyField. potm ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache("book_id not found in cache")
        ids = self.table.book_col_map.get(book_id, ())
        if ids:
            return list(ids)
        else:
            return default_value

    def _totm_ids_for_book(self, book_id, default_value=None):
        """
        Returns the ids for the given book when the map is of TypedOneToMany form

        Example:
            Exercise CalibreOneToManyField. totm ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        book_data = self.table.book_data(book_id=book_id)
        if self._is_data_null(book_data):
            return default_value
        else:
            return book_data

    def _ptotm_ids_for_book(self, book_id, default_value=None):
        """
        Returns the ids for the given book when the map is of PriorityTypedOneToMany form

        Example:
            Exercise CalibreOneToManyField. ptotm ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        book_data = self.table.book_data(book_id=book_id)
        if self._is_data_null(book_data):
            return default_value
        else:
            return book_data

    # Todo: Should optionally only work with primary entities
    def books_for(self, item_id, default_value=None):
        # This is the correct version for not_unique one_to_many relations - if something is not linked it's not
        # considered
        # Also allows certain tables - like notes - to be reused to provide notes for multiple different entities -
        # which makes searching easier
        """
        Perform the books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.col_book_map:
            raise NotInCache
        return self.table.col_book_map.get(item_id, default_value)

    def _ptotm_books_for(self, item_id, default_value=None):
        # This is the correct version for not_unique one_to_many relations - if something is not linked it's not
        # considered
        # Also allows certain tables - like notes - to be reused to provide notes for multiple different entities -
        # which makes searching easier
        """
        Perform the ptotm books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField. ptotm books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.col_book_map:
            raise NotInCache
        book_data = self.table.col_book_map.get(item_id, default_value)
        if book_data is None:
            return default_value

        if self._is_data_null(book_data):
            return default_value
        else:
            return book_data

    def unique_books_for(self, item_id, default_value=None):
        """
        Perform the unique books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField.unique books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        return self.table.col_book_map.get(item_id, default_value)

    def _potm_unique_books_for(self, item_id, default_value=None):
        # This is the correct version for not_unique one_to_many relations - if something is not linked it's not
        # considered
        # Also allows certain tables - like notes - to be reused to provide notes for multiple different entities -
        # which makes searching easier
        """
        Perform the potm unique books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField. potm unique books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        book_data = self.table.col_book_map.get(item_id, default_value)
        if book_data is None:
            return default_value

        if self._is_data_null(book_data):
            return default_value
        else:
            return book_data

    def _totm_unique_books_for(self, item_id, default_value=None):
        # This is the correct version for not_unique one_to_many relations - if something is not linked it's not
        # considered
        # Also allows certain tables - like notes - to be reused to provide notes for multiple different entities -
        # which makes searching easier
        """
        Perform the totm unique books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField. totm unique books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        book_data = self.table.col_book_map.get(item_id, default_value)
        if book_data is None:
            return default_value

        if isinstance(book_data, int):
            return book_data

        if self._is_data_null(book_data):
            return default_value
        else:
            return book_data

    def __iter__(self):
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iterkeys(self.table.id_map)

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Perform the sort keys for books operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sk_map = LazySortMap(self._default_sort_key, self._sort_key, self.table.id_map)
        bcmg = self.table.book_col_map.get
        return lambda book_id: sk_map(bcmg(book_id, None))

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise CalibreOneToManyField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        cbm = self.table.col_book_map
        empty = set()
        for item_id, val in iteritems(self.table.id_map):
            book_ids = cbm.get(item_id, empty).intersection(candidates)
            if book_ids:
                yield val, book_ids

    @property
    def book_value_map(self):
        """
        Perform the book value map operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToManyField.book value map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return {book_id: self.table.id_map[item_id] for book_id, item_id in iteritems(self.table.book_col_map)}
        except KeyError:
            raise InvalidLinkTable(self.name)

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preforms an update to the database.

        Example:
            Exercise CalibreOneToManyField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE_CACHE METHODS

    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preforms an update of the internal cache.

        Example:
            Exercise CalibreOneToManyField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.table.update_cache(book_id_val_map, id_map)

    #
    # ------------------------------------------------------------------------------------------------------------------


# Todo: We need an index field which does something clever to keep the index data in line
class CalibreManyToOneField(BaseManyToOneField, CalibreField):

    """
    Provide the calibremanytoonefield contract for validated ebook processing.

    Example:
        Exercise CalibreManyToOneField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    table_type: int = MANY_ONE

    def __init__(self, name, table, bools_are_tristate=False):
        """
        Initialize and validate the calibremanytoonefield state.

        Example:
            Exercise CalibreManyToOneField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreManyToOneField, self).__init__(name=name, table=table, bools_are_tristate=bools_are_tristate)

        if isinstance(table, CalibreTypedManyToOneTable):
            self._change_books_for_method(self._ptmto_books_for)

        elif isinstance(table, CalibrePriorityTypedManyToOneTable):
            self._change_books_for_method(self._ptmto_books_for)

        self.internal_update_used = True

    # The update_cache methods are stored here to allow emulation of different field types
    def update_cache(self, book_id_val_map, id_map=None):
        """
        Update the book_col_map - indicating if we're using a cover or not.

        Example:
            Exercise CalibreManyToOneField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_cache(book_id_val_map, id_map)

    # Todo: Need a compatible parameter - for getting at the full data when we're a pseudo-table
    def for_book(self, book_id, default_value=None):
        """
        Returns the value of the target table for the given "book" (actually the primary table of the pair - which is books by default but could be something else if you've - for example - made a field representing the links between series and tags - for example) Note that there is an asymmetry here between this and books_for - books_for returns the ids of the books in question - this returns the values for the given book.

        Example:
            Exercise CalibreManyToOneField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        ids = self.table.book_col_map.get(book_id, None)
        if ids is not None:
            ans = self.table.id_map[ids]
        else:
            ans = default_value
        return ans

    # Todo: Make sure every instance of this function has a default value
    def ids_for_book(self, book_id, default_value=None):
        """
        Return the id associated with a given book (there should only ever be one - as this is a ManyToOne table)

        Example:
            Exercise CalibreManyToOneField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        ids = self.table.book_col_map.get(book_id, None)
        if ids is not None:
            return ids
        else:
            return default_value

    # Todo: Should optionally only work with primary items - but not in this method
    # Todo: Souldn't throw NotInCache when the item is on the database - NotPrimary or NotInUse?
    def books_for(self, item_id, default_value=None):
        """
        Perform the books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToOneField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        item_data = self.table.col_book_map.get(item_id, set())
        return item_data if item_data else default_value

    def _ptmto_books_for(self, item_id, default_value=None):
        """
        Perform the ptmto books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToOneField. ptmto books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        item_data = self.table.item_data(item_id)
        return item_data if not self._is_data_null(item_data) else default_value

    def _is_data_null(self, data_map):
        """
        Returns True if the given data map has no content and False otherwise.

        Example:
            Exercise CalibreManyToOneField. is data null through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param data_map: Value supplied for data map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for link_type, link_vals in iteritems(data_map):
            if link_vals:
                return False
        return True

    def __iter__(self):
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToOneField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iterkeys(self.table.id_map)

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Perform the sort keys for books operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToOneField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sk_map = LazySortMap(self._default_sort_key, self._sort_key, self.table.id_map)
        bcmg = self.table.book_col_map.get
        return lambda book_id: sk_map(bcmg(book_id, None))

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise CalibreManyToOneField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        cbm = self.table.col_book_map
        empty = set()
        for item_id, val in iteritems(self.table.id_map):
            book_ids = cbm.get(item_id, empty).intersection(candidates)
            if book_ids:
                yield val, book_ids

    @property
    def book_value_map(self):
        """
        Perform the book value map operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToOneField.book value map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return {book_id: self.table.id_map[item_id] for book_id, item_id in iteritems(self.table.book_col_map)}
        except KeyError:
            raise InvalidLinkTable(self.name)

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreManyToOneField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)


class CalibreManyToOneFieldCustom(CalibreManyToOneField):
    """
    Provide the calibremanytoonefieldcustom contract for validated ebook processing.

    Example:
        Exercise CalibreManyToOneFieldCustom through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def update(self, book_id_to_val_map, db, allow_case_change=False):

        """
        Perform the update operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToOneFieldCustom.update through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return super(CalibreManyToOneFieldCustom, self).update(
            book_id_to_val_map=book_id_to_val_map,
            db=db,
            allow_case_change=allow_case_change,
        )


class CalibreRatingField(CalibreManyToOneField):
    """
    Provide the calibreratingfield contract for validated ebook processing.

    Example:
        Exercise CalibreRatingField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, name, table, bools_are_tristate=False):
        """
        Initialize and validate the calibreratingfield state.

        Example:
            Exercise CalibreRatingField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreRatingField, self).__init__(name=name, table=table, bools_are_tristate=bools_are_tristate)

        self._change_books_for_method(self._ratings_books_for)
        self._change_for_book_method(self._ratings_for_book)

    def _ratings_for_book(self, book_id, default_value=None):
        """
        Custom method - which ignores all but the calibre ratings and returns them

        Example:
            Exercise CalibreRatingField. ratings for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rating_val = self.table.book_col_map.get(book_id, None)
        if rating_val is None:
            return default_value
        else:
            return self.table.id_map[rating_val]

    def _ratings_books_for(self, item_id, default_value=None):
        """
        Custom method - which ignores all but the calibre ratings and returns that as a set.

        Example:
            Exercise CalibreRatingField. ratings books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        ratings_dict = self.table.item_data(item_id)

        if default_value == ratings_dict:
            return default_value
        else:
            return ratings_dict["calibre"]


class CalibreManyToManyField(BaseManyToManyField, CalibreField):

    """
    Provide the calibremanytomanyfield contract for validated ebook processing.

    Example:
        Exercise CalibreManyToManyField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    table_type: int = MANY_MANY

    def __init__(self, name, table, bools_are_tristate=False):

        """
        Initialize and validate the calibremanytomanyfield state.

        Example:
            Exercise CalibreManyToManyField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreManyToManyField, self).__init__(name=name, table=table, bools_are_tristate=bools_are_tristate)

        # In some cases it makes sense to let the Writer preform an update to the cache using the internal_update_cache
        # method while it preforms the Write - if this is True then assume this has been done while updating in the
        # set_field method
        self.internal_update_used = True

        self.is_sub_table = False

        self.table.writer = self.writer

        # Change out some of the methods - if needed due to the structure of the table
        if isinstance(table, CalibrePriorityTypedManyToManyTable):
            self._change_for_book_method(self._ptmtm_for_book)
            self._change_ids_for_book_method(self._ptmtm_ids_for_book)

            self._change_books_for_method(self._ptmtm_books_for)

        elif isinstance(table, CalibreTypedManyToManyTable):
            self._change_for_book_method(self._ptmtm_for_book)
            self._change_ids_for_book_method(self._ptmtm_ids_for_book)

            self._change_books_for_method(self._tmtm_books_for)

        elif isinstance(table, CalibrePriorityManyToManyTable):
            self._change_for_book_method(self._pmtm_for_book)
            self._change_ids_for_book_method(self._pmtm_ids_for_book)

            self._change_books_for_method(self._pmtm_books_for)

    # Todo: This should fail unless it can be actually accomplished
    def get_subfield(self, type_filter=None):
        """
        Returns a subfield of items of the given type.

        Example:
            Exercise CalibreManyToManyField.get subfield through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param type_filter: Value supplied for type filter under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sub_table = self.table.get_subtable(type_filter=type_filter)
        return CalibreManyToManyField(name=self.name, table=sub_table)

    def internal_update_cache(self, book_id_item_id_map, id_map_update):
        """
        Update cache with some additional information provided - used in write when it needs to know some info about the cache before writing out to the database.

        Example:
            Exercise CalibreManyToManyField.internal update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.internal_update_cache(book_id_item_id_map, id_map_update)

    def _pmtm_for_book(self, book_id, default_value=None, as_tuple=False):

        """
        Perform the pmtm for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToManyField. pmtm for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :param as_tuple: Value supplied for as tuple under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache

        book_data = self.table.book_col_map.get(book_id, default_value)

        if not book_data:
            return default_value
        if book_data != default_value:
            return [self.table.id_map[iid] for iid in book_data]
        else:
            return default_value

    def _pmtm_ids_for_book(self, book_id, default_value=None, as_tuple=False):

        """
        Perform the pmtm ids for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToManyField. pmtm ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :param as_tuple: Value supplied for as tuple under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache

        book_data = self.table.book_col_map.get(book_id, default_value)

        if not book_data:
            return default_value
        if book_data != default_value:
            return self.table.book_col_map[book_id]
        else:
            return default_value

    def _is_data_null(self, data_map):
        """
        Returns True if the given data map has no content and False otherwise.

        Example:
            Exercise CalibreManyToManyField. is data null through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param data_map: Value supplied for data map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for link_type, link_vals in iteritems(data_map):
            if link_vals:
                return False
        return True

    # Todo: Need to handle the default_value and as_tuple parameters properly
    def _ptmtm_ids_for_book(self, book_id, default_value=None, as_tuple=False):
        """
        PriorityTyped ids_for_book method

        Example:
            Exercise CalibreManyToManyField. ptmtm ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :param as_tuple: Value supplied for as tuple under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        book_data = self.table.book_data(book_id=book_id)
        if self._is_data_null(book_data):
            return default_value
        else:
            return book_data

    def for_book(self, book_id, default_value=None, as_tuple=False):
        """
        Return the cache value for the given book_id

        Example:
            Exercise CalibreManyToManyField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :param as_tuple: Value supplied for as tuple under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # If the book_id is not known, then raise NotInCache - as the book is unknown to the system
        if book_id not in self.table.seen_book_ids:
            raise NotInCache

        # Book is known but there is not set value for it
        if book_id not in self.table.book_col_map.keys():
            return default_value

        # Book is in the cache and has values - process and return
        ids = self.table.book_col_map.get(
            book_id,
        )
        if ids:
            ans = (self.table.id_map[i] for i in ids)
            if self.table.sort_alpha:
                ans = tuple(sorted(ans, key=sort_key)) if as_tuple else set(ans)
            else:
                ans = tuple(ans) if as_tuple else set(ans)
        else:
            ans = default_value
        return ans

    # Todo: Need to handle the default_value and as_tuple parameters properly
    def _ptmtm_for_book(self, book_id, default_value=None, as_tuple=False):
        """
        PriorityTyped for_book method.

        Example:
            Exercise CalibreManyToManyField. ptmtm for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :param as_tuple: Value supplied for as tuple under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        table_data = self.table.vals_book_data(book_id=book_id)
        if self._is_data_null(table_data):
            return default_value
        else:
            return table_data

    def ids_for_book(self, book_id, default_value=None):
        """
        Perform the ids for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToManyField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        table_data = self.table.book_col_map.get(book_id, default_value)
        if table_data == default_value:
            return default_value
        else:
            return set(table_data)

    def books_for(self, item_id, default_value=()):
        """
        Perform the books for operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToManyField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        return self.table.col_book_map.get(item_id, default_value)

    def _pmtm_books_for(self, item_id, default_value=None):
        """
        Priority books_for method.

        Example:
            Exercise CalibreManyToManyField. pmtm books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        item_data = self.table.col_book_map.get(item_id, default_value)

        # Todo: Upgrade _is_data_null and re-merge this mess
        if not item_data:
            return default_value
        return item_data

    def _tmtm_books_for(self, item_id, default_value=None):
        """
        Priority books_for method.

        Example:
            Exercise CalibreManyToManyField. tmtm books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        item_data = self.table.item_data(item_id)

        # Todo: Upgrade _is_data_null and re-merge this mess
        if self._is_data_null(item_data):
            return default_value
        return item_data

    def _ptmtm_books_for(self, item_id, default_value=None):
        """
        Priority books_for method.

        Example:
            Exercise CalibreManyToManyField. ptmtm books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id not in self.table.id_map:
            raise NotInCache
        item_data = self.table.item_data(item_id)

        # Todo: Upgrade _is_data_null and re-merge this mess
        if self._is_data_null(item_data):
            return default_value
        return item_data

    def __iter__(self):
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToManyField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iterkeys(self.table.id_map)

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Perform the sort keys for books operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToManyField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sk_map = LazySortMap(self._default_sort_key, self._sort_key, self.table.id_map)
        bcmg = self.table.book_col_map.get
        dsk = (self._default_sort_key,)
        if self.sort_sort_key:

            def sk(book_id):
                """
                Perform the sk operation under explicit file-format and conversion rules.

                Example:
                    Exercise CalibreManyToManyField.sort keys for books.sk through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param book_id: Value supplied for book id under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return tuple(sorted(sk_map(x) for x in bcmg(book_id, ()))) or dsk

        else:

            def sk(book_id):
                """
                Perform the sk operation under explicit file-format and conversion rules.

                Example:
                    Exercise CalibreManyToManyField.sort keys for books.sk through a consuming regression::

                        python -m pytest -q tests/library/test_unified_library.py


                :param book_id: Value supplied for book id under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                return tuple(sk_map(x) for x in bcmg(book_id, ())) or dsk

        return sk

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Used to preform a search on this field - iterates through and yields matches with the ids of the search items.

        Example:
            Exercise CalibreManyToManyField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        try:
            cbm = self.table.col_book_map
        except AttributeError:
            # Todo: Need to deal with TypedManyToMany tables and others without this structure
            raise StopIteration
        else:
            empty = set()
            for item_id, val in iteritems(self.table.id_map):
                book_ids = set([iid for iid in cbm.get(item_id, empty)]).intersection(candidates)
                if book_ids:
                    yield val, book_ids

    def iter_counts(self, candidates: int):
        """
        Generator which yields the count - the number of tags a book has and a set of book ids all of which have that number of tags.

        Example:
            Exercise CalibreManyToManyField.iter counts through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param candidates: Value supplied for candidates under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        val_map = defaultdict(set)
        cbm = self.table.book_col_map
        for book_id in candidates:
            val_map[len(cbm.get(book_id, ()))].add(book_id)
        for count, book_ids in iteritems(val_map):
            yield count, book_ids

    @property
    def book_value_map(self):
        """
        Perform the book value map operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreManyToManyField.book value map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return {
                book_id: tuple(self.table.id_map[item_id] for item_id in item_ids)
                for book_id, item_ids in iteritems(self.table.book_col_map)
            }
        except KeyError:
            raise InvalidLinkTable(self.name)

    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preforms an update of the cache - via the update_cache method of the table.

        Example:
            Exercise CalibreManyToManyField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_cache(book_id_val_map=book_id_val_map, id_map=id_map)

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreManyToManyField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)

    # Todo: Is this an update for the database, update for the cache, or both
    # Todo: Should be applied before both - with a variable indicating the type of update so the right error can be
    # raised if it fails
    def update_precheck(self, book_id_item_id_map, id_map_update):
        """
        Checks that an update is valid before it's preformed. Calls out to the table method - as the exact form of the needed update depends on the table.

        Example:
            Exercise CalibreManyToManyField.update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.table.update_precheck(book_id_item_id_map, id_map_update)
        except InvalidCacheUpdate as e:
            err_str = "Update precheck failed"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("book_id_item_id_map", book_id_item_id_map),
                ("id_map_update", id_map_update),
            )
            raise InvalidDBUpdate(err_str)

    def update_preflight(self, book_id_item_id_map, id_map_update, dirtied=None):
        """
        Gives the table a chance to bring the :param book_id_item_id_map: into a standard form.

        Example:
            Exercise CalibreManyToManyField.update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param dirtied: Value supplied for dirtied under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_preflight(book_id_item_id_map, id_map_update, dirtied=dirtied)


class CalibreIdentifiersField(CalibreManyToManyField, BaseIdentifiersField, CalibreField):
    """
    Provide the calibreidentifiersfield contract for validated ebook processing.

    Example:
        Exercise CalibreIdentifiersField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def for_book(self, book_id, default_value=None, compatible=True):
        """
        Perform the for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreIdentifiersField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :param compatible: Value supplied for compatible under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ids = self.table.book_col_map[book_id]["isbn"]
        if not ids:
            try:
                ids = default_value.copy()  # in case default_value is a mutable dict
            except AttributeError:
                ids = default_value
        return ids

    # Todo: Need to unify the interface of talking to the database - database and cache calls should be always distinct?
    # Except is that confusing? When you call a method called set_identifier - you do sort of expect the identifier
    # to be set everywhere
    def set_identifier(self, book_id, typ, val):
        """
        Set an identifier for a given book

        Example:
            Exercise CalibreIdentifiersField.set identifier through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        val = (
            [
                val,
            ]
            if isinstance(val, basestring)
            else val
        )
        self.table.book_col_map[book_id][typ] = set([v for v in val])

        # Todo: Also need to update the database
        # Todo: Need to update the col_book_map as well

    def set_identifiers_from_set_dict(self, book_id, set_dict, db):
        """
        Set all the identifiers for a given book. All identifiers will be replaced with the ids specified in the new set. Currently expects a dictionary keyed with the type of the id and valued with a set of the new identifiers.

        Example:
            Exercise CalibreIdentifiersField.set identifiers from set dict through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param set_dict: Value supplied for set dict under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for typ, val_set in iteritems(set_dict):
            self.table.book_col_map[book_id][typ] = val_set

        self.table.write_to_db(book_id, db)
        # Todo: Also need to update the database
        # Todo: Need to update the col_book_map as well

    # Todo: Need to re-engineer so this actually works (introduce the concept of a primary isbn)?
    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Sort by identifier keys

        Example:
            Exercise CalibreIdentifiersField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bcmg = self.table.book_col_map.get
        dv = {self._default_sort_key: None}
        return lambda book_id: tuple(sorted(iterkeys(bcmg(book_id, dv))))

    def iter_searchable_values(self, get_metadata, candidates, default_value=()):
        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise CalibreIdentifiersField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        bcm = self.table.book_col_map
        for book_id in candidates:
            val = bcm.get(book_id, default_value)
            if val:
                yield val, {book_id}

    def get_categories(self, tag_class, book_rating_map, lang_map, book_ids=None):
        """
        Return categories under the format's safety and compatibility rules.

        Example:
            Exercise CalibreIdentifiersField.get categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []

        for id_key, item_book_ids in iteritems(self.table.col_book_map):
            if book_ids is not None:
                item_book_ids = item_book_ids.intersection(book_ids)
            if item_book_ids:
                c = tag_class(id_key, id_set=item_book_ids, count=len(item_book_ids))
                ans.append(c)
        return ans

    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preforms an update of the cache.

        Example:
            Exercise CalibreIdentifiersField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_cache(book_id_val_map=book_id_val_map, id_map=id_map)

    def update_db(self, book_id_val_map, db, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreIdentifiersField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_db(book_id_val_map, db, allow_case_change=allow_case_change)

    def write_to_db(self, book_id, db):
        """
        Preform a write out to the database from the contents of the cache.

        Example:
            Exercise CalibreIdentifiersField.write to db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.write_to_db(book_id=book_id, db=db)


class CalibreAuthorsField(CalibreManyToManyField, BaseAuthorsField, CalibreField):
    """
    Provide the calibreauthorsfield contract for validated ebook processing.

    Example:
        Exercise CalibreAuthorsField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def for_book(self, book_id, default_value=None, as_tuple=False):
        """
        Perform the for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreAuthorsField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :param as_tuple: Value supplied for as tuple under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.book_in_cache(book_id):
            raise NotInCache
        author_ids = self.table.book_col_map[book_id]
        return tuple([self.table.id_map[author_id] for author_id in author_ids])

    def book_in_cache(self, book_id):
        """
        Perform the book in cache operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreAuthorsField.book in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return book_id in self.table.book_col_map

    def ids_for_book(self, book_id, default_value=None):
        """
        Perform the ids for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreAuthorsField.ids for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.seen_book_ids:
            raise NotInCache
        table_data = self.table.book_col_map.get(book_id, default_value)
        if table_data == default_value:
            return default_value
        else:
            return table_data

    def books_for(self, item_id):
        """
        Returns the books linked to the given item.

        Example:
            Exercise CalibreAuthorsField.books for through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if item_id in self.table.book_col_map:
            return self.table.col_book_map[item_id]
        else:
            raise NotInCache("item_id not found in cache")

    def author_data(self, author_id):
        """
        Returns all the author information for a specific author id.

        Example:
            Exercise CalibreAuthorsField.author data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param author_id: Value supplied for author id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "name": self.table.id_map[author_id],
            "sort": self.table.asort_map[author_id],
            "link": self.table.alink_map[author_id],
        }

    def category_sort_value(self, item_id, book_ids, lang_map):
        """
        Perform the category sort value operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreAuthorsField.category sort value through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.asort_map[item_id]

    def db_author_sort_for_book(self, book_id):
        """
        Returns the author sort value for the specific book from the database.

        Example:
            Exercise CalibreAuthorsField.db author sort for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.author_sort_field.for_book(book_id)

    def author_sort_for_book(self, book_id):
        """
        Perform the author sort for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreAuthorsField.author sort for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return " & ".join(self.table.asort_map[k] for k in self.table.book_col_map[book_id])

    def update(self, book_id_to_val_map, db, allow_case_change=False):

        # Apply a special update - to deal with calibre compatibility
        """
        Perform the update operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreAuthorsField.update through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        new_book_id_to_val_map = dict()
        for book_id, book_val in iteritems(book_id_to_val_map):

            if isinstance(book_val, basestring):
                # Passing in as a list will ensure replacement of the full authors list
                new_book_id_to_val_map[book_id] = list(book_val.split(","))
            else:
                new_book_id_to_val_map[book_id] = book_val

        return super(CalibreAuthorsField, self).update(
            book_id_to_val_map=new_book_id_to_val_map,
            db=db,
            allow_case_change=allow_case_change,
        )

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreAuthorsField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)


# Todo: Add update_loc function - then make sure that it's actually used everywhere it should be in cache
class CalibreFormatsField(CalibreManyToManyField, BaseFormatsField):
    """
    Provides a front end to the information stored in the Formats table.

    Example:
        Exercise CalibreFormatsField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize and validate the calibreformatsfield state.

        Example:
            Exercise CalibreFormatsField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreManyToManyField.__init__(self, *args, **kwargs)

        self.check_fmt_is_priority_fmt = self.table.check_fmt_is_priority_fmt
        self.stand_fmt = self.table.stand_fmt
        self.prep_base_fmt = self.table.prep_base_fmt

    def for_book(self, book_id, default_value=None):
        """
        Perform the for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreFormatsField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.book_col_map.get(book_id, default_value)

    def format_fname(self, book_id, fmt):
        """
        Perform the format fname operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreFormatsField.format fname through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.table.fname_map[book_id][fmt.upper()]
        except KeyError:
            raise NoSuchFormatInCache

    def format_floc(self, book_id, fmt):
        """
        Returns the format location for the given book.

        Example:
            Exercise CalibreFormatsField.format floc through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.table.book_file_loc_map[book_id][fmt.upper()]
        except KeyError:
            raise NoSuchFormatInCache

    def has_format(self, book_id, fmt):
        """
        Does the book have any instances of the given format

        Example:
            Exercise CalibreFormatsField.has format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: True when the documented condition holds; otherwise False.
        """
        if book_id not in self.table.book_fmts_map:
            raise NoSuchBook
        return fmt in self.table.book_fmts_map[book_id]

    def has_priority_fmt(self, book_id, priority_fmt):
        """
        Check to see if the given book has the given format.

        Example:
            Exercise CalibreFormatsField.has priority fmt through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param priority_fmt: Value supplied for priority fmt under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        if book_id not in self.table.book_fmts_map:
            raise NoSuchBook
        return priority_fmt in self.table.book_col_map[book_id]

    def add_format(self, book_id, fmt, fmt_loc):
        """
        Note that a format has been added to a book. FMT must include the priority of that FMT in the book (e.g. not EPUB, but EPUB_1). Note - If you want to add an ORIGINAL_FMT - call this function with the full FMT string, including priority. This function will reject any FMT which is not a priority fmt - that is to say something of the form EPUB_1

        Example:
            Exercise CalibreFormatsField.add format through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param fmt_loc: Value supplied for fmt loc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        book_id = int(book_id)
        fmt = self.stand_fmt(fmt)
        base_fmt = self.prep_base_fmt(fmt)

        assert fmt not in self.for_book(book_id=1, default_value=None), "Cannot use this method to replace a fmt"
        assert isinstance(fmt_loc, Location), "fmt_loc must be a Location object"

        # Validate the given fmt - check that it's a priority fmt
        if not self.check_fmt_is_priority_fmt(fmt):
            raise NotImplementedError(
                "Can only accept a priority fmt - {} is not a priority_fmt (e.g. EPUB_1)" "".format(fmt)
            )

        # Update the maps stored in the table
        # fname_map
        fmt_name = self.table.fsm.path.getname(fmt_loc)
        self.table.fname_map[book_id][fmt] = fmt_name

        # book_file_map
        fmt_file_id = int(fmt_loc["file_row"]["file_id"])
        self.table.book_file_map[book_id][fmt] = fmt_file_id

        # size_map
        fmt_size = self.table.fsm.path.getsize(fmt_loc)
        self.table.size_map[book_id][fmt] = fmt_size

        # col_book_map
        # Adding ORIGINAL_FMT will do nothing - as we're using the base_fmt, not the fmt
        self.table.col_book_map[base_fmt].add(book_id)

        # book_col_map
        # Update and sort the old entry - then make it into a tuple and set it
        old_entry_tuple = self.table.book_col_map[book_id]
        old_entry_list = set([fe for fe in old_entry_tuple])
        old_entry_list.add(fmt)
        new_entry_tupe = tuple(sorted(old_entry_list))
        self.table.book_col_map[book_id] = new_entry_tupe

        # book_col_count_map
        if not fmt.startswith("ORIGINAL"):
            # Ignore backup books as they're just duplicated
            try:
                self.table.book_col_count_map[book_id][base_fmt] += 1
            except KeyError:
                self.table.book_col_count_map[book_id][base_fmt] = 1

        # book_file_loc_map
        self.table.book_file_loc_map[book_id][fmt] = fmt_loc

        # book_fmts_map
        self.table.book_fmts_map[book_id].add(base_fmt)

    # Todo: Removing a fmt should also take out the backups
    def remove_fmt(self, book_id, fmt):
        """
        Remove a fmt from the cache.

        Example:
            Exercise CalibreFormatsField.remove fmt through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        book_id = int(book_id)
        fmt = self.stand_fmt(fmt)
        base_fmt = self.prep_base_fmt(fmt)

        # Validate the given fmt - check that it's a priority fmt
        if not self.check_fmt_is_priority_fmt(fmt):
            raise NotImplementedError(
                "Can only accept a priority fmt - {} is not a priority_fmt (e.g. EPUB_1)" "".format(fmt)
            )

        available_fmtds = self.for_book(book_id=book_id)
        rekey_dict = self.calculate_rekey_map(old_fmts=available_fmtds, remove_fmt=fmt)

        # Update the maps stored in the table
        # fname_map
        self.table.fname_map[book_id] = self.rekey_dict(self.table.fname_map[book_id], rekey_dict)

        # book_file_map
        self.table.book_file_map[book_id] = self.rekey_dict(self.table.book_file_map[book_id], rekey_dict)

        # size_map
        self.table.size_map[book_id] = self.rekey_dict(self.table.size_map[book_id], rekey_dict)

        # col_book_map
        after_fmts = set([self.prep_base_fmt(bk_fmt) for bk_fmt in self.table.fname_map[book_id]])
        # Need to remove the book_id from the col_book_map if there is not more book formats of this type in the book
        if not fmt.startswith("ORIGINAL"):
            # Check the fname_map - if there is another entry with this fmt, then skip - if not, remove
            if self.prep_base_fmt(fmt) not in after_fmts:
                self.table.col_book_map[base_fmt].pop(base_fmt, None)

        # book_col_map
        # Update and sort the old entry - remove the value - then make a tuple and set it
        new_entry_tuple = tuple(sorted(rekey_dict.values()))
        self.table.book_col_map[book_id] = new_entry_tuple

        # book_col_count_map
        if not fmt.startswith("ORIGINAL"):
            # Ignore backup books as they're just duplicates
            self.table.book_col_count_map[book_id][base_fmt] -= 1
            if not self.table.book_col_count_map[book_id][base_fmt]:
                del self.table.book_col_count_map[book_id][base_fmt]

        # book_file_loc_map
        self.table.book_file_loc_map[book_id] = self.rekey_dict(self.table.book_file_loc_map[book_id], rekey_dict)

        # book_fmts_map
        self.table.book_fmts_map[book_id] = after_fmts

    def calculate_rekey_map(self, old_fmts, remove_fmt):
        """
        Calculates a rekey map - keyed with the old name of the fmt and valued with the new. Used when a middling priority fmt is remove to calculate how the designations of the fmts have to change.

        Example:
            Exercise CalibreFormatsField.calculate rekey map through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param old_fmts: Value supplied for old fmts under the utility contract.
        :param remove_fmt: Value supplied for remove fmt under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # We're simply removing a backup - remove it and return
        if remove_fmt.startswith("ORIGINAL"):
            rekey_dict = dict((of, of) for of in old_fmts if of != remove_fmt)
            return rekey_dict

        # # Detected if the remove_fmt has a original fmt - if it does, remove it
        theo_original_fmt = "ORIGINAL_{}".format(remove_fmt)
        old_fmts = tuple([x for x in old_fmts if x != theo_original_fmt])

        assert remove_fmt in old_fmts
        old_fmts = sorted(old_fmts)
        remove_fmt_type = self.prep_base_fmt(remove_fmt)

        rekey_dict = dict()
        displace = False
        for i in range(len(old_fmts)):
            current_fmt = old_fmts[i]

            if not current_fmt.startswith(remove_fmt_type):
                rekey_dict[old_fmts[i]] = old_fmts[i]
            else:
                if current_fmt == remove_fmt:
                    displace = True
                    continue
                if displace:
                    rekey_dict[old_fmts[i]] = old_fmts[i - 1]
                else:
                    rekey_dict[old_fmts[i]] = old_fmts[i]

        return rekey_dict

    # Todo: This is a method from the python utils - use it instead of this mess
    @staticmethod
    def rekey_dict(old_dict, rekey_map, new_dict=None):
        """
        Preforms the actual rekey for a dictionary.

        Example:
            Exercise CalibreFormatsField.rekey dict through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param old_dict: Value supplied for old dict under the utility contract.
        :param rekey_map: Value supplied for rekey map under the utility contract.
        :param new_dict: Value supplied for new dict under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if new_dict is None:
            new_dict = dict()

        for old_key in old_dict:
            if old_key in rekey_map:
                new_dict[rekey_map[old_key]] = old_dict[old_key]
        return new_dict

    # Todo: This should be part of the base class - as it needs to work for everything
    def reload_book_from_db(self, db, book_id):
        """
        Reload all the information from a book from the database.

        Example:
            Exercise CalibreFormatsField.reload book from db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.table.reload_book_from_db(db=db, book_id=book_id)

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):

        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise CalibreFormatsField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        val_map = defaultdict(set)
        cbm = self.table.book_col_map
        for book_id in candidates:
            vals = cbm.get(book_id, ())
            for val in vals:
                val_map[self.prep_base_fmt(val)].add(book_id)

        for val, book_ids in iteritems(val_map):
            yield val, book_ids

    def get_categories(self, tag_class, book_rating_map, lang_map, book_ids=None):
        """
        Return categories under the format's safety and compatibility rules.

        Example:
            Exercise CalibreFormatsField.get categories through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_rating_map: Value supplied for book rating map under the utility
            contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []

        for fmt, item_book_ids in iteritems(self.table.col_book_map):
            if book_ids is not None:
                item_book_ids = item_book_ids.intersection(book_ids)
            if item_book_ids:
                c = tag_class(fmt, id_set=item_book_ids, count=len(item_book_ids))
                ans.append(c)
        return ans

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preforms a update to the database.

        Example:
            Exercise CalibreFormatsField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_db(book_id_to_val_map, db, allow_case_change=allow_case_change)


# Todo: Make this actually true - does not talk to the covers cache at the moment
class CalibreCoversField(CalibreManyToManyField, BaseCoverField):
    """
    Provides a front end to the information stored in the Covers table. Provides information as to if the book has a cover. Also provides options to access (through the cover cache) more detailed information about the multiple covers available to the book - including their locations and which of them is primary for the given title.

    Example:
        Exercise CalibreCoversField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, *args, **kwargs):
        """
        Startup for the cache.

        Example:
            Exercise CalibreCoversField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreCoversField, self).__init__(*args, **kwargs)

        self.internal_update_used = False

        self.table.writer = self.writer

    # Todo: Need to change the signature of update_cache to be book_col_map, id_map
    # Todo: Check there are update cache methods for all the defined fields
    def update_cache(self, book_col_map, id_map=None):
        """
        Update the book_col_map - indicating if we're using a cover or not.

        Example:
            Exercise CalibreCoversField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_col_map: Value supplied for book col map under the utility contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.table.book_col_map.update(book_col_map)

    def for_book(self, book_id, default_value=None):
        """
        Returns True if the book has a cover - false otherwise

        Example:
            Exercise CalibreCoversField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if book_id not in self.table.book_col_map:
            return default_value
        return self.table.book_col_map.get(book_id, default_value)

    def path_for_book(self, book_id, default_value=None):
        """
        Return a path to a cover in the cover cache.

        Example:
            Exercise CalibreCoversField.path for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.fsm.cover_cache.get_book_cover(book_id=book_id)

    def cover_id(self, book_id, default_value=None):
        """
        Returns the id of the cover which is primary for the book.

        Example:
            Exercise CalibreCoversField.cover id through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            book_cover_id = self.table.book_cover_map.book_cover_map[book_id]
        except KeyError:
            return default_value
        return book_cover_id if book_cover_id is not None else default_value

    def cover_loc(self, book_id, default_value=None):
        """
        Returns the loc of the cover that is primary for that book.

        Example:
            Exercise CalibreCoversField.cover loc through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Retrieve the id_loc map for the individual book - return the loc of the first entry in that map
        try:
            book_id_loc_map = self.table.book_cover_loc_map[book_id]
        except KeyError:
            return default_value

        try:
            book_primary_cover_id = book_id_loc_map.keys()[0]
        except IndexError:
            return default_value

        return book_id_loc_map[book_primary_cover_id]


class LazySeriesSortMap(object):

    """
    Provide the lazyseriessortmap contract for validated ebook processing.

    Example:
        Exercise LazySeriesSortMap through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    __slots__ = ("default_sort_key", "sort_key_func", "id_map", "cache")

    def __init__(self, default_sort_key, sort_key_func, id_map):
        """
        Initialize and validate the lazyseriessortmap state.

        Example:
            Exercise LazySeriesSortMap.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param default_sort_key: Value supplied for default sort key under the utility
            contract.
        :param sort_key_func: Value supplied for sort key func under the utility contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.default_sort_key = default_sort_key
        self.sort_key_func = sort_key_func
        self.id_map = id_map
        self.cache = {}

    def __call__(self, item_id, lang):
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise LazySeriesSortMap.  call   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param lang: Value supplied for lang under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.cache[(item_id, lang)]
        except KeyError:
            try:
                val = self.cache[(item_id, lang)] = self.sort_key_func(self.id_map[item_id], lang)
            except KeyError:
                val = self.cache[(item_id, lang)] = self.default_sort_key
            return val


# Todo: Need to write a CalibrePriorityManyToManyField object
class CalibreSeriesField(CalibreManyToManyField, BaseSeriesField):
    """
    Provide the calibreseriesfield contract for validated ebook processing.

    Example:
        Exercise CalibreSeriesField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, name, table, bools_are_tristate=False):
        """
        Initialize and validate the calibreseriesfield state.

        Example:
            Exercise CalibreSeriesField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreSeriesField, self).__init__(name=name, table=table, bools_are_tristate=bools_are_tristate)

        # Put the for_book method back - as it was overridden in the base method
        self._change_for_book_method(self._series_for_book)

    def _series_for_book(self, book_id, default_value=None):
        """
        In compatible mode returns the primary series for the work - otherwise returns a list of all the ids linked to the book.

        Example:
            Exercise CalibreSeriesField. series for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_series_ids = self.table.book_col_map[book_id]
        if not book_series_ids:
            return default_value
        series_id = book_series_ids[0]
        return self.table.id_map[series_id]

    def sort_keys_for_books(self, get_metadata, lang_map):
        """
        Perform the sort keys for books operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreSeriesField.sort keys for books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sso = tweaks["title_series_sorting"]
        ssk = self._sort_key
        ts = title_sort

        def sk(val, lang):
            """
            Perform the sk operation under explicit file-format and conversion rules.

            Example:
                Exercise CalibreSeriesField.sort keys for books.sk through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param val: Template or metadata value evaluated by the operation.
            :param lang: Value supplied for lang under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return ssk(ts(val, order=sso, lang=lang))

        sk_map = LazySeriesSortMap(self._default_sort_key, sk, self.table.id_map)
        bcmg = self.table.book_col_map.get
        lang_map = {k: v[0] if v else None for k, v in iteritems(lang_map)}

        def key(book_id):
            """
            Perform the key operation under explicit file-format and conversion rules.

            Example:
                Exercise CalibreSeriesField.sort keys for books.key through a consuming regression::

                    python -m pytest -q tests/library/test_unified_library.py


            :param book_id: Value supplied for book id under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            lang = lang_map.get(book_id, None)
            return sk_map(bcmg(book_id, None), lang)

        return key

    def category_sort_value(self, item_id, book_ids, lang_map):
        """
        Perform the category sort value operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreSeriesField.category sort value through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :param lang_map: Value supplied for lang map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lang = None
        tss = tweaks["title_series_sorting"]
        if tss != "strictly_alphabetic":
            c = Counter()

            for book_id in book_ids:
                l = lang_map.get(book_id, None)
                if l:
                    c[l[0]] += 1

            if c:
                lang = c.most_common(1)[0][0]
        val = self.table.id_map[item_id]
        return title_sort(val, order=tss, lang=lang)

    # Todo: Need to standardize the signatures of these
    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preform an update of the cache.

        Example:
            Exercise CalibreSeriesField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_cache(book_id_val_map, id_map)

    def update_preflight(self, book_id_item_id_map, id_map_update, dirted=None):
        """
        Perform the update preflight operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreSeriesField.update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param dirted: Value supplied for dirted under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return super(CalibreSeriesField, self).update_preflight(book_id_item_id_map, id_map_update, dirtied=dirted)

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preform updates of the series index of the table.

        Example:
            Exercise CalibreSeriesField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return super(CalibreSeriesField, self).update_db(book_id_to_val_map, db, allow_case_change)


class CalibreTagsField(CalibreManyToManyField, BaseTagsField):
    """
    Provide the calibretagsfield contract for validated ebook processing.

    Example:
        Exercise CalibreTagsField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def for_book(self, book_id, default_value=None):
        """
        Perform the for book operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreTagsField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ids = self.table.book_col_map.get(
            book_id,
        )
        if ids:
            ans = (self.table.id_map[i] for i in ids)
            return set(ans)
        else:
            ans = default_value
        return ans

    def update_preflight(self, book_id_item_id_map, id_map_update, dirtied=None):
        """
        Gives the table a chance to bring the :param book_id_item_id_map:

        Example:
            Exercise CalibreTagsField.update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param dirtied: Value supplied for dirtied under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.update_preflight(book_id_item_id_map, id_map_update, dirtied=dirtied)

    def update_cache(self, book_col_map, id_map):
        """
        Preform an update on the tags field.

        Example:
            Exercise CalibreTagsField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_col_map: Value supplied for book col map under the utility contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.table.book_col_map.update(book_col_map)
        if id_map is not None:
            self.table.id_map.update(id_map)

    def internal_update_cache(self, book_id_item_id_map, id_map_update):
        """
        Preform an internal update of the cache.

        Example:
            Exercise CalibreTagsField.internal update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.internal_update_cache(book_id_item_id_map, id_map_update)

    def get_news_category(self, tag_class, book_ids=None):
        """
        Return news category under the format's safety and compatibility rules.

        Example:
            Exercise CalibreTagsField.get news category through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param tag_class: Value supplied for tag class under the utility contract.
        :param book_ids: Book identities included in the batched read operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []

        # Seek the new tags
        news_id = None
        for item_id, val in iteritems(self.table.id_map):
            if val == _("News"):
                news_id = item_id
                break
        if news_id is None:
            return ans

        # Process books which possess the news tag
        news_books = self.table.col_book_map[news_id]
        if book_ids is not None:
            news_books = news_books.intersection(book_ids)
        if not news_books:
            return ans
        for item_id, item_book_ids in iteritems(self.table.col_book_map):
            item_book_ids = item_book_ids.intersection(news_books)
            if item_book_ids:
                name = self.category_formatter(self.table.id_map[item_id])
                if name == _("News"):
                    continue
                c = tag_class(
                    name,
                    id=item_id,
                    sort=name,
                    id_set=item_book_ids,
                    count=len(item_book_ids),
                )
                ans.append(c)
        return ans


class CalibreLanguagesField(CalibreManyToManyField):
    """
    Represents the language field.

    Example:
        Exercise CalibreLanguagesField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name, table, bools_are_tristate=False):
        """
        Initialize and validate the calibrelanguagesfield state.

        Example:
            Exercise CalibreLanguagesField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreLanguagesField, self).__init__(name=name, table=table, bools_are_tristate=bools_are_tristate)

        self._change_for_book_method(self._langs_for_book)

    def _langs_for_book(self, book_id, default_value=None):
        """
        Value for the language field of the book - should be the string corresponding to the primary language.

        Example:
            Exercise CalibreLanguagesField. langs for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            lang_id_set = self.table.book_col_map["primary"][book_id]
        except KeyError:
            return default_value

        if len(lang_id_set) == 0:
            return default_value
        elif len(lang_id_set) == 1:
            lang_id = list(lang_id_set)[0]
        else:
            err_str = "title had more than one primary language associated with it"
            err_str = default_log.log_variables(err_str, "ERROR", ("book_id", book_id))
            raise NotImplementedError(err_str)

        return self.table.id_map[lang_id]

    def update_preflight(self, book_id_item_id_map, id_map_update, dirtied=None):
        """
        Gives the table a chance to bring the :param book_id_item_id_map:

        Example:
            Exercise CalibreLanguagesField.update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param dirtied: Value supplied for dirtied under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        new_book_id_item_id_dict = dict()
        for book_id, book_val in iteritems(book_id_item_id_map):

            if isinstance(book_val, int):
                new_book_id_item_id_dict[book_id] = {
                    "primary": {
                        book_val,
                    }
                }
            elif isinstance(book_val, dict):
                # Todo: Need to check that the dictionary is properly formed
                new_book_id_item_id_dict[book_id] = book_val
            elif isinstance(book_val, basestring):
                raise NotImplementedError("book_id_item_id_map is malformed - valued with string")
            else:
                raise NotImplementedError("Cannot preform update_preflight on specific form")

        return new_book_id_item_id_dict

    # Todo: Probably want something with this general form for all the fields
    def update_db_preflight(self, book_id_val_map):
        """
        Does preflight work to bring the update dict into a standard form before it's applied to the database.

        Example:
            Exercise CalibreLanguagesField.update db preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        new_update_dict = dict()
        for book_id, type_dict in iteritems(book_id_val_map):
            if isinstance(type_dict, dict):
                new_update_dict[book_id] = type_dict
            elif isinstance(type_dict, basestring):
                new_update_dict[book_id] = {
                    "primary": [
                        type_dict,
                    ]
                }
            else:
                raise NotImplementedError("Cannot update - type_dict not recognized")

        return new_update_dict

    def update_db_precheck(self, book_id_val_map):
        """
        Preform a precheck of the update book_id_val_map before trying to write it out to the database. This allows for calling an abort on a malformed update before trying to update the database at all. No changes are made to the book_id_val_map by this method - just errors out of the update is not valid.

        Example:
            Exercise CalibreLanguagesField.update db precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        assert isinstance(book_id_val_map, dict)

        # Checks that all the book ids tagged for update are known to the database and that the set of things to apply
        # to them is of a recognizable form
        for book_id, type_dict in iteritems(book_id_val_map):

            if book_id not in self.table.seen_books:
                raise InvalidDBUpdate("book_id not found in cache")

            if not isinstance(type_dict, dict):
                err_str = "book_id_val_map was not well formed"
                err_str = default_log.log_variables(err_str, "ERROR", ("book_id_val_map", book_id_val_map))
                raise InvalidDBUpdate(err_str)

            # Todo: Check that the keys of the type_dict is one of the know and permissible language types
            for update_type, update_vals in iteritems(type_dict):
                assert isinstance(update_vals, list)
                for lang_id in update_vals:
                    if isinstance(lang_id, int):
                        if lang_id not in self.table.id_map:
                            raise InvalidDBUpdate("lang_id was not found in cache")
                    elif isinstance(lang_id, basestring):
                        pass
                    else:
                        raise InvalidDBUpdate("Unexpected object pretending to be a language_id")

    def update_db(self, book_id_to_val_map, db, allow_case_change=False):
        """
        Preform an update of the languages data stored on the database. Prechecks are run to ensure that the update is valid.

        Example:
            Exercise CalibreLanguagesField.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Deal with the cases where the map is just valued with a string - these are indicators to set the string as
        # the primary language code for the title - process these and then remove them
        new_book_id_to_val_map = dict()
        for book_id, book_val in iteritems(book_id_to_val_map):
            # id, val is processed and dropped
            if isinstance(book_val, basestring):
                # Todo: Need a search method for the language code and name
                catalog = Catalog(db)
                language_match = catalog.languages.exact(book_val)
                if not language_match.is_match or language_match.entity_id is None:
                    raise InvalidDBUpdate("Language could not be resolved: {!r}".format(book_val))
                catalog.create_writer("works", "language").write(
                    {book_id: LinkValue(language_match.entity_id)},
                    link_type="primary",
                )
            # id and val are retained for conventional update
            else:
                new_book_id_to_val_map[book_id] = book_val
        book_id_to_val_map = new_book_id_to_val_map

        book_id_to_val_map = self.update_db_preflight(book_id_to_val_map)
        self.update_db_precheck(book_id_to_val_map)

        return super(CalibreLanguagesField, self).update_db(
            book_id_to_val_map=new_book_id_to_val_map,
            db=db,
            allow_case_change=allow_case_change,
        )

    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preform an update of the cache using the provided book_id_val_map

        Example:
            Exercise CalibreLanguagesField.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Todo: This needs to be cache_update_preflight
        book_id_val_map = self.update_preflight(book_id_item_id_map=book_id_val_map, id_map_update=None)

        self.table.update_cache(book_id_val_map=book_id_val_map, id_map=id_map)


class CalibreLinkAttributeField(BaseLinkAttributeField, CalibreField):
    """
    Provides a consistent interface to the underlying data stored in the link table filed

    Example:
        Exercise CalibreLinkAttributeField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(
        self,
        name,
        link_table_name,
        link_field,
        link_attribute_table,
        main_table_name,
        auxiliary_table_name,
    ):
        """
        Set the basic properties of the field and the link.

        Example:
            Exercise CalibreLinkAttributeField.  init   through a consuming regression::

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
        # Stores the
        super(CalibreLinkAttributeField, self).__init__(
            name=name,
            link_table_name=link_table_name,
            link_field=link_field,
            link_attribute_table=link_attribute_table,
            main_table_name=main_table_name,
            auxiliary_table_name=auxiliary_table_name,
        )

        # The form of the return from the link field depends on what sort of field has been loaded
        if isinstance(link_field, (CalibreOneToOneField, CalibreSeriesField)):
            self._change_for_book_method(self._one_to_one_for_book)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TABLE PASS THROUGH METHODS
    # For reasons of presenting a consistent interface with the table, passing through the methods - so that the
    # interface matches
    def book_in_cache(self, book_id):
        """
        Perform the book in cache operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreLinkAttributeField.book in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.link_field.book_in_cache(book_id)

    def item_in_cache(self, item_id):
        """
        Perform the item in cache operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreLinkAttributeField.item in cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.link_field.item_in_cache(item_id)

    def _one_to_one_for_book(self, book_id, default_value=None):
        """
        Return the

        Example:
            Exercise CalibreLinkAttributeField. one to one for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        aux_id = self.link_field.ids_for_book(book_id=book_id)[0]
        return self.link_attribute_table.get_property(main_id=book_id, auxiliary_id=aux_id)

    #
    # ------------------------------------------------------------------------------------------------------------------


def calibre_create_field(name, table, bools_are_tristate):
    """
    Takes a table field and the other properties needed to instantiate it - constructs the Table object and returns it.

    Example:
        Exercise calibre create field through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param table: Value supplied for table under the utility contract.
    :param bools_are_tristate: Value supplied for bools are tristate under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cls = {
        ONE_ONE: CalibreOneToOneField,
        ONE_MANY: CalibreOneToManyField,
        MANY_ONE: CalibreManyToOneField,
        MANY_MANY: CalibreManyToManyField,
    }[table.table_type]

    if name == "authors":
        cls = CalibreAuthorsField
    elif name in ["comments", "publisher"]:
        cls = CalibreOneToOneField
    elif name == "ondevice":
        cls = CalibreOnDeviceField
    elif name == "formats":
        cls = CalibreFormatsField
    elif name == "identifiers":
        cls = CalibreIdentifiersField
    elif name == "tags":
        cls = CalibreTagsField
    elif name in ("cover", "covers"):
        cls = CalibreCoversField
    elif name == "languages":
        cls = CalibreLanguagesField
    elif name == "rating":
        cls = CalibreRatingField
    elif table.metadata["datatype"] == "composite":
        cls = CalibreCompositeField
    elif table.metadata["datatype"] == "series":
        cls = CalibreSeriesField
    return cls(name, table, bools_are_tristate)


def calibre_create_custom_column_field(name, table, bools_are_tristate):
    """
    Takes a custom column table and returns a field wrapped around it.

    Example:
        Exercise calibre create custom column field through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param table: Value supplied for table under the utility contract.
    :param bools_are_tristate: Value supplied for bools are tristate under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cls = {
        ONE_ONE: CalibreOneToOneField,
        ONE_MANY: CalibreOneToManyField,
        MANY_ONE: CalibreManyToOneFieldCustom,
        MANY_MANY: CalibreManyToManyField,
    }[table.table_type]

    return cls(name, table, bools_are_tristate)


def calibre_create_link_attribute_field(name, field, bools_are_tristate=False):
    """
    A link attribute field is a sub-field of a main field which contains additional information about the link joining the main and auxiliary tables. These are created associated with a field.

    Example:
        Exercise calibre create link attribute field through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param field: Metadata or template field addressed by the operation.
    :param bools_are_tristate: Value supplied for bools are tristate under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    link_table = create_link_attribute_table(link_field=field, attribute_name=name)
    return CalibreLinkAttributeField(
        name=name,
        link_table_name=field.table.name,
        link_field=field,
        link_attribute_table=link_table,
        main_table_name=field.table.main_table_name,
        auxiliary_table_name=field.table.auxiliary_table_name,
    )
