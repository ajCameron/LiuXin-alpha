
"""
Model cached one-to-one relations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise one one tables through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from collections import defaultdict, OrderedDict

from LiuXin_alpha.customize.cache.base_tables import (
    BaseOneToOneTable,
    BasePathTable,
    BaseSizeTable,
    BaseUUIDTable,
    BaseCompositeTable,
)

from LiuXin_alpha.errors import InvalidCacheUpdate

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.customize.cache.base_tables import ONE_ONE, null
from LiuXin_alpha.customize.cache.base_tables import BaseCoversTable

from LiuXin_alpha.utils.language_tools import plural_singular_mapper


class CalibreOneToOneTable(BaseOneToOneTable):
    """
    Represents data that is unique per book - i.e. unique 1-1 mapping - e.g. uuid, title, e.t.c

    Example:
        Exercise CalibreOneToOneTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    # Todo: You should not be able to change these - raise ConstantPropertyError if you try
    table_type = ONE_ONE

    # Mildly annoying that these need to have to be set
    priority = False
    typed = False

    def __init__(self, name, metadata, link_table=None, custom=False):
        """
        Setup for a OneToOne table - store metadata and initialize the database.

        Example:
            Exercise CalibreOneToOneTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreOneToOneTable, self).__init__(name, metadata, link_table, custom)

        self.book_col_map = self._book_col_map_factory()
        self.col_book_map = self._col_book_map_factory()

        self.seen_book_ids = set()

    @staticmethod
    def _book_col_map_factory():
        """
        Returns an empty book col map - ready for data load.

        Example:
            Exercise CalibreOneToOneTable. book col map factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return dict()

    @staticmethod
    def _col_book_map_factory():
        """
        Returns an empty col book map - ready for data load.

        Example:
            Exercise CalibreOneToOneTable. col book map factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return dict()

    def read(self, db):
        """
        Read the book relevant bits of the given table into memory. Currently stored in self.book_col_map, a dictionary keyed with the book_id and valued with the column value

        Example:
            Exercise CalibreOneToOneTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            in_table = self.metadata["in_table"]
        except KeyError:
            in_table = None

        if in_table != "titles":
            self.set_link_tables(db)

        if self.custom:

            self.read_one_to_one_from_custom(db)

        else:

            # If we're in the books table, then query the meta view - the canonical location for most of the quantities
            # to do with a book

            # Construct a query to deal with columns in the "books" table - actually references the meta view - where
            # information about each book is aggregated
            if self.metadata["table"] in ("books", "titles") or in_table in (
                "books",
                "titles",
            ):

                self.read_one_to_one_from_meta(db)

            else:

                self.read_one_to_one_from_other_table(db)

        # Todo: This needs to be taken account of in the update method
        # Invert the book col map to produce the col book map
        self.col_book_map = {(v, k) for k, v in iteritems(self.book_col_map)}

    def read_one_to_one_from_meta(self, db):
        """
        Preform a read from the meta table.

        Example:
            Exercise CalibreOneToOneTable.read one to one from meta through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        idcol = "id"
        col_name = self.metadata["column"]

        # Read the aggregate data off the meta view
        stmt = "SELECT {0}, {1} FROM meta;".format(idcol, col_name)
        fallback_stmt = "SELECT {0}, cast({1} as blob) FROM meta;".format(idcol, col_name)

        # Query the database and serialize the data
        query = db.driver_wrapper.execute(stmt)
        if self.unserialize is None:
            try:
                self.book_col_map = dict(query)
            except UnicodeDecodeError:
                # The database might be damaged. Try and work around it be ignoring failures to decode utf-8
                query = db.driver_wrapper.execute(fallback_stmt)
                self.book_col_map = {k: bytes(val).decode("utf-8", "replace") for k, val in query}
        else:
            us = self.unserialize
            self.book_col_map = {book_id: us(val) for book_id, val in query}

        self.seen_book_ids = set([book_id for book_id, val in query])

    def read_one_to_one_from_custom(self, db):
        """
        Read from a custom one to one table.

        Example:
            Exercise CalibreOneToOneTable.read one to one from custom through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Todo: Currently this assumes a titles or books custom column - which needs to be not always true
        try:
            can_link_table = "books_{}_link".format(self.metadata["table"])
        except KeyError:
            # We're probably in a Virtual or Composite table
            return
        assert can_link_table not in db.custom_tables, "Link table found for a OneToOneTable"

        cc_table = self.metadata["table"]
        cc_col = plural_singular_mapper(cc_table)

        # Todo: As with the rest of the sql, needs to be moved into macros
        stmt = "SELECT {col}_book, {col}_value FROM {table};".format(table=cc_table, col=cc_col)
        fallback_stmt = "SELECT {col}_book, cast({col}_value as blob) FROM {table};".format(table=cc_table, col=cc_col)

        # Query the database and serialize the data
        query = db.driver_wrapper.execute(stmt)
        if self.unserialize is None:
            try:
                self.book_col_map = dict(query)
            except UnicodeDecodeError:
                # The database might be damaged. Try and work around it be ignoring failures to decode utf-8
                query = db.driver_wrapper.execute(fallback_stmt)
                self.book_col_map = {k: bytes(val).decode("utf-8", "replace") for k, val in query}
        else:
            us = self.unserialize
            self.book_col_map = {book_id: us(val) for book_id, val in query}

        good_ids_stmt = "SELECT book_id FROM books;"
        self.seen_book_ids = set(b_id[0] for b_id in db.driver_wrapper.execute(good_ids_stmt))

    def read_one_to_one_from_other_table(self, db):
        """
        Read a one to one relation from another table. One to one relations with custom tables can be enforced by creating the link table with the right properties.

        Example:
            Exercise CalibreOneToOneTable.read one to one from other table through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        table = self.metadata["table"] if self.metadata["table"] is not None else self.metadata["in_table"]
        idcol = "id" if self.metadata["table"] == "books" else db.driver_wrapper.get_id_column(table)

        link_table_name = db.driver_wrapper.get_link_table_name("titles", self.metadata["table"])
        link_table_book_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="title_id"
        )
        link_table_other_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type=idcol
        )

        stmt = "SELECT {0}.{1}, {2}.{3} FROM {0} INNER JOIN {2} ON {0}.{5} = {2}.{4};".format(
            link_table_name,
            link_table_book_col,
            self.metadata["table"],
            self.metadata["column"],
            idcol,
            link_table_other_col,
        )
        fallback_stmt = "SELECT {0}.{1}, cast({2}.{3} as blob) FROM {0} INNER JOIN {2} ON {0}.{5} = {2}.{4};".format(
            link_table_name,
            link_table_book_col,
            self.metadata["table"],
            self.metadata["column"],
            idcol,
            link_table_other_col,
        )

        # Query the database and serialize the data
        query = db.driver_wrapper.execute(stmt)
        if self.unserialize is None:
            try:
                self.book_col_map = dict(query)
            except UnicodeDecodeError:
                # The database might be damaged. Try and work around it be ignoring failures to decode utf-8
                query = db.driver_wrapper.execute(fallback_stmt)
                self.book_col_map = {k: bytes(val).decode("utf-8", "replace") for k, val in query}
        else:
            us = self.unserialize
            self.book_col_map = {book_id: us(val) for book_id, val in query}

    def remove_books(self, book_ids, db):
        """
        Remove books from the cache.

        Example:
            Exercise CalibreOneToOneTable.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clean = set()
        for book_id in book_ids:
            val = self.book_col_map.pop(book_id, null)
            if val is not null:
                if hasattr(val, "__iter__"):
                    for val_id in val:
                        clean.add(val_id)
                else:
                    clean.add(val)
        return clean

    def update_precheck(self, book_id_item_id_map, id_map_update):
        """
        Perform the update precheck operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreOneToOneTable.update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


# Todo: Generalize to Locations
class CalibrePathTable(CalibreOneToOneTable, BasePathTable):
    """
    Contains a Location object for every book folder on the database.

    Example:
        Exercise CalibrePathTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name, metadata, link_table=None):
        """
        Initialize and validate the calibrepathtable state.

        Example:
            Exercise CalibrePathTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreOneToOneTable.__init__(self, name, metadata, link_table=None)

        self.fsm = None

    def set_path(self, book_id, path, db):
        """
        Update the cache with the path - a specilized write which just does this.

        Example:
            Exercise CalibrePathTable.set path through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.book_col_map[book_id] = path
        self.set_db_path(book_id, path, db)

    def read(self, db):
        """
        Read data from the database through the fsm - add it to the local storage structure. Data is stored in the form of Location objects for the books.

        Example:
            Exercise CalibrePathTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.book_col_map = dict()

        for book_row in db.get_all_rows("books"):

            # Acquire all the folders associated with each of the books
            book_folder_rows = db.get_interlinked_rows(primary_row=book_row, secondary_table="folders")

            # Try to make location objects for each of the rows - if that fails then continue with the rows
            book_folder_locs = []
            for book_folder_row in book_folder_rows:
                try:
                    book_folder_loc = self.fsm.get_loc(book_folder_row)
                except Exception as e:
                    err_str = "self.fsm.get_loc has failed. This is a problem."
                    default_log.log_variables(err_str, e, "ERROR", ("book_folder_row", book_folder_row))
                    raise
                else:
                    book_folder_locs.append(book_folder_loc)

            self.book_col_map[int(book_row["book_id"])] = book_folder_locs


class CalibreSizeTable(CalibreOneToOneTable, BaseSizeTable):
    """
    Books are linked to folders. Files are, in turn, contained in those folders. The size of a book is considered to be the sum of the sizes of all the files linked to a folder linked to the book

    Example:
        Exercise CalibreSizeTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name, metadata, link_table=None):

        """
        Initialize and validate the calibresizetable state.

        Example:
            Exercise CalibreSizeTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreOneToOneTable.__init__(self, name, metadata, link_table)

        self.size_mode = None
        self._parse_size_mode()

    def read(self, db):
        """
        The recorded size in the books table is the combined size of all the files linked to all the folders linked to an individual book. This is settable in preferences - you can display the minimum, maximum or total size of all the files.

        Example:
            Exercise CalibreSizeTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.size_mode == "sum":
            query = db.metadata_sql.read_book_sizes_sum_mode()
        elif self.size_mode == "max":
            query = db.metadata_sql.read_book_sizes_max_mode()
        elif self.size_mode == "min":
            query = db.metadata_sql.read_book_sizes_min_mode()
        else:
            raise NotImplementedError("Given size mode is not supported")

        self.book_col_map = dict(query)

    def update_sizes(self, size_map):
        """
        Update the cache when changes occur to the overall size of the files stored in the folder store manager.

        Example:
            Exercise CalibreSizeTable.update sizes through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param size_map: Value supplied for size map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.book_col_map.update(size_map)


class CalibreUUIDTable(CalibreOneToOneTable, BaseUUIDTable):
    """
    Stores the 1-1 correspondence between books and UUIDs

    Example:
        Exercise CalibreUUIDTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name, metadata, link_table=None):

        """
        Initialize and validate the calibreuuidtable state.

        Example:
            Exercise CalibreUUIDTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreOneToOneTable.__init__(self, name, metadata, link_table)

        self.uuid_to_id_map = dict()

    def read(self, db):
        """
        Read the data from the database into memory - creates a mapping between book_ids->uuids and uuids->book_ids

        Example:
            Exercise CalibreUUIDTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        CalibreOneToOneTable.read(self, db)

        self.uuid_to_id_map = {v: k for k, v in iteritems(self.book_col_map)}

    def update_uuid_cache(self, book_id_val_map):
        """
        Updates the uuid cache

        Example:
            Exercise CalibreUUIDTable.update uuid cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for book_id, uuid in iteritems(book_id_val_map):
            self.uuid_to_id_map.pop(self.book_col_map.get(book_id, None), None)  # discard old uuid
            self.uuid_to_id_map[uuid] = book_id

    def remove_books(self, book_ids, db):
        """
        Remove books from the cache - doesn't clear them from the database.

        Example:
            Exercise CalibreUUIDTable.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clean = set()
        for book_id in book_ids:
            val = self.book_col_map.pop(book_id, null)
            if val is not null:
                self.uuid_to_id_map.pop(val, None)
                clean.add(val)
        return clean

    def lookup_by_uuid(self, uuid):
        """
        Perform the lookup by uuid operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreUUIDTable.lookup by uuid through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param uuid: Value supplied for uuid under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.uuid_to_id_map.get(uuid, None)


# Todo: Not tested, as I don't have a composite column right now to test it on
class CalibreCompositeTable(CalibreOneToOneTable, BaseCompositeTable):
    """
    Provide the calibrecompositetable contract for validated ebook processing.

    Example:
        Exercise CalibreCompositeTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, name, metadata, link_table=None, custom=False):

        """
        Initialize and validate the calibrecompositetable state.

        Example:
            Exercise CalibreCompositeTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreOneToOneTable.__init__(self, name, metadata, link_table, custom=custom)

        self.composite_template = None
        self.contains_html = False
        self.make_category = False
        self.composite_sort = False
        self.use_decorations = False

    def read(self, db):
        """
        Values for composite caches are generated on the fly - thus no need to try and generate and store them all at once. This method sets some import metadata properties

        Example:
            Exercise CalibreCompositeTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        d = self.metadata["display"]
        self.composite_template = ["composite_template"]
        self.contains_html = d.get("contains_html", False)
        self.make_category = d.get("make_category", False)
        self.composite_sort = d.get("composite_sort", False)
        self.use_decorations = d.get("use_decorations", False)

        self.book_col_map = {}

    def remove_books(self, book_ids, db):
        """
        Perform the remove books operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreCompositeTable.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return set()


# Todo: If a cover is linked to two titles then what happens when it's updated for one.
# Todo: Just copy the cover if it's linked to two books at the same time
# Todo: Add cover_size column to the covers table and make sure it's actually being populated
# Todo: Be able to reload data for a single book from the database
class CalibreCoversTable(CalibreOneToOneTable, BaseCoversTable):
    """
    The covers table is analogous to the Formats table, but for covers. Different information matters, so the maps are somewhat different. Be aware of the differences when accessing the tables directly. The Covers table contains the following maps. cname_map - A dictionary of lists keyed with the book_id, then valued with an OrderedDict of the cover_ids, which is valued with the actual names of their files. book_cover_map - A dictionary of ints - keyed with the id of the book and then valued with the id of the cover which is primary for that book size_map - A dictionary of dictionaries - keyed with the id of a book and valued with an OrderedDict - keyed with the id of the cover associated with it and valued with the size of that cover. book_cover_loc_map - A dictionary of OrderedDicts - keyed with the id of the book, then valued with an OrderedDict keyed with the id of the cover and valued with the location of the cover.

    Example:
        Exercise CalibreCoversTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name, metadata, link_table=None):

        """
        Initialize and validate the calibrecoverstable state.

        Example:
            Exercise CalibreCoversTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        CalibreOneToOneTable.__init__(self, name, metadata, link_table)

        self.metadata = metadata

        self.cname_map = defaultdict(OrderedDict)
        self.book_cover_map = dict()
        self.size_map = defaultdict(OrderedDict)
        self.book_cover_loc_map = defaultdict(OrderedDict)

        # Needed to calculate the location of each of the files
        self.fsm = None

    def read(self, db):
        """
        Perform the read operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreCoversTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.read_maps(db)

    def read_id_maps(self, db):
        """
        Read id maps under the format's safety and compatibility rules.

        Example:
            Exercise CalibreCoversTable.read id maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def fix_case_duplicates(self, db):
        """
        Perform the fix case duplicates operation under explicit file-format and conversion rules.

        Example:
            Exercise CalibreCoversTable.fix case duplicates through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def read_maps(self, db):
        """
        Create the cover maps - these are described in detail in the class doc string.

        Example:
            Exercise CalibreCoversTable.read maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cname_map = defaultdict(OrderedDict)
        book_cover_map = dict()
        book_cover_loc_map = defaultdict(OrderedDict)

        for (
            book_id,
            cover_id,
            cover_name,
        ) in db.metadata_sql.read_book_id_with_cover_id_and_cover_nmame():
            # Record the name for the file type
            cname_map[book_id][cover_id] = cover_name

            if book_id not in book_cover_map:
                book_cover_map[book_id] = cover_id

            # Record the cover location - if it can be found
            cover_row = db.get_row_from_id("covers", cover_id)
            try:
                cover_loc = self.fsm.get_loc(asset_row=cover_row)
            except:
                cover_loc = None
            book_cover_loc_map[book_id][cover_id] = cover_loc

        self.cname_map = cname_map
        self.book_cover_map = book_cover_map
        self.book_cover_loc_map = book_cover_loc_map

        # Read into the book_col_map - keyed with the book id and valued with the
        self._read_books_has_cover_value(db)

    def _read_books_has_cover_value(self, db):
        """
        Read from the books has_cover column - which indicates when the book has been used.

        Example:
            Exercise CalibreCoversTable. read books has cover value through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        stmt = "SELECT book_id, book_has_cover FROM books;".format(self.metadata["table"])

        # Query the database and serialize the data
        query = db.driver_wrapper.execute(stmt)
        us = bool
        self.book_col_map = {book_id: us(val) for book_id, val in query}

    def remove_books(self, book_ids, db):
        """
        Remove the specified books from the cache.

        Example:
            Exercise CalibreCoversTable.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clean = CalibreOneToOneTable.remove_books(self, book_ids, db)

        for book_id in book_ids:
            self.cname_map.pop(book_id, None)
            self.book_cover_map.pop(book_id, None)
            self.book_cover_loc_map.pop(book_id, None)

        return clean


class CalibreCustomColumnsOneToOneTable(CalibreOneToOneTable):
    """
    Custom Column table for all the Custom Columns which are effectively one to one - like integers

    Example:
        Exercise CalibreCustomColumnsOneToOneTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, name, metadata, link_table=None, custom=False):
        """
        Setup for a OneToOne table - store metadata and initialize the database.

        Example:
            Exercise CalibreCustomColumnsOneToOneTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibreCustomColumnsOneToOneTable, self).__init__(name, metadata, link_table, custom)

    def update_precheck(self, book_id_item_id_map, id_map_update, acceptance_functions=None):
        """
        Check that an update is of a valid form before writing it out to the cache and the database. Called when you know the ids you want to assign to the book after the update. Checks those ids are valid. No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists as expected - this will not be corrected. Raised InvalidCacheUpdate if the cache update is invalid in some way.

        Example:
            Exercise CalibreCustomColumnsOneToOneTable.update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param acceptance_functions: Value supplied for acceptance functions under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if hasattr(book_id_item_id_map, "checked") and book_id_item_id_map.checked:
            return

        for book_id, book_vals in iteritems(book_id_item_id_map):

            if book_vals is None:
                continue

            if book_id not in self.seen_book_ids:
                raise InvalidCacheUpdate("Cannot match book_id - cannot preform update as cannot link")

            # Check that the new values are not ordered - explicitly this table does not store ordered information
            # and should reject any attempt to feed it such
            if isinstance(book_vals, (set, frozenset, list, dict, tuple)):
                raise InvalidCacheUpdate("Map needs to be valued with a set or frozenset")

            if acceptance_functions is not None:
                for acceptance_function in acceptance_functions:
                    try:
                        acceptance_function(book_vals)
                    except Exception as e:
                        raise InvalidCacheUpdate(e)


class CalibreCustomColumnsOneToOneTableFloatInt(CalibreCustomColumnsOneToOneTable):

    """
    Provide the calibrecustomcolumnsonetoonetablefloatint contract for validated ebook processing.

    Example:
        Exercise CalibreCustomColumnsOneToOneTableFloatInt through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    pass


class CalibreCustomColumnsOneToOneTableDatetime(CalibreCustomColumnsOneToOneTable):

    """
    Provide the calibrecustomcolumnsonetoonetabledatetime contract for validated ebook processing.

    Example:
        Exercise CalibreCustomColumnsOneToOneTableDatetime through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    pass


class CalibreCustomColumnsOneToOneTableBool(CalibreCustomColumnsOneToOneTableFloatInt):

    """
    Provide the calibrecustomcolumnsonetoonetablebool contract for validated ebook processing.

    Example:
        Exercise CalibreCustomColumnsOneToOneTableBool through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    pass
