"""
Provide specialized many-to-many relation tables.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise specific many to many tables through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

import re
from collections import defaultdict

from typing import TypeVar, Optional, Union, Iterable, Literal

from LiuXin_alpha.plugins.cache.base_tables import (
    BaseCreatorsTable,
    BaseFormatsTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.priority_many_to_many_table import (
    CalibrePriorityManyToManyTable,
)
from LiuXin_alpha.databases.db_types.database_types import InterLinkTableName, SrcTableID, DstTableID, SpecificFormat, \
    GenericFormat, MetadataDict

from LiuXin_alpha.metadata.author_names import author_to_author_sort

from LiuXin_alpha.preferences import preferences

from LiuXin_alpha.utils.libraries.liuxin_six import (
    dict_iteritems as iteritems,
    dict_itervalues as itervalues, basestring
)
from LiuXin_alpha.utils.logging import default_log


T = TypeVar("T")


class CalibreAuthorsTable(CalibrePriorityManyToManyTable[T], BaseCreatorsTable):
    """
    Represents the authors' subset of the creators table, and the link between that and books.

    Example:
        Exercise CalibreAuthorsTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    _priority: bool = True
    _typed: bool = False

    def __init__(self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None) -> None:
        """
        Initialize the authors table.

        Example:
            Exercise CalibreAuthorsTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        CalibrePriorityManyToManyTable.__init__(self, name, metadata, link_table)

        self.name = "creators"
        # Todo: Check that this is set when subtables are created
        self.table_type_filter = "authors"

        # Set the link table for the authors - which is a subset for the creators table
        self.link_table = "creator_title_links"
        self.link_table_bt_id_column = "creator_title_link_title_id"
        self.link_table_table_id_column = "creator_title_link_creator_id"
        self.link_table_type = "creator_title_link_type"

        self.alink_map: dict[DstTableID, str] = {}
        self.asort_map: dict[DstTableID, str] = {}

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, Union[DstTableID, list[DstTableID, ...], tuple[DstTableID, ...], str]],
        id_map_update: dict[DstTableID, str],
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, Iterable[DstTableID]], dict[DstTableID, str]]:
        """
        Transforms :param book_id_item_id_map: into a form which can be written out to the database.

        Example:
            Exercise CalibreAuthorsTable.update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param dirtied: Value supplied for dirtied under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        dirtied = set() if dirtied is None else dirtied

        # Standardize the book_id_item_id_map
        clean_book_id_item_id_map = dict()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if isinstance(book_vals, int):
                clean_book_id_item_id_map[book_id] = [
                    book_vals,
                ] + list(self.book_col_map[book_id])

            elif isinstance(book_vals, (list, tuple)):
                clean_book_id_item_id_map[book_id] = book_vals

            elif isinstance(book_vals, basestring):

                # If the value for the book_id_item_id_map is a string, then check the id_map_update for it
                # Then look up the id and treat it as if we've been given an id
                if book_vals in id_map_update.values():
                    book_val_id = None
                    for item_id, str_val in iteritems(id_map_update):
                        if str_val == book_vals:
                            book_val_id = item_id
                            break
                    assert book_val_id is not None
                    clean_book_id_item_id_map[book_id] = [
                        book_val_id,
                    ] + list(self.book_col_map[book_id])
                else:
                    clean_book_id_item_id_map[book_id] = [
                        book_vals,
                    ] + list(self.book_col_map[book_id])

            else:
                err_msg = [
                    "Unexpected form of the value of the update preflight",
                    "book_id_item_id_map: {}".format(book_id_item_id_map),
                    "id_map_update: {}".format(id_map_update),
                    "book_id: {}".format(book_id),
                    "book_vals: {}".format(book_vals),
                ]
                raise NotImplementedError("\n".join(err_msg))

        return clean_book_id_item_id_map, id_map_update

    def read_id_maps(self, db) -> None:
        """Reject legacy creator operations before changing database or cache."""
        raise NotImplementedError(
            "Legacy creator tables are retired. Use Catalog.agents with explicit "
            "Agent identities and credit roles; creator IDs cannot be reused as Agent "
            "IDs."
        )

    def read_maps(self, db, type_filter: str = "authors") -> None:
        """
        Read the maps, filtering to remove any creator who doesn't have the role of author in a given work.

        Example:
            Exercise CalibreAuthorsTable.read maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        book_col_map = defaultdict(list)
        col_book_map = self.col_book_map

        # Get the id column for the target table
        idcol = db.driver_wrapper.get_id_column("creators")

        # Get the name of the link table and the relevant columns
        link_table_name = db.driver_wrapper.get_link_table_name(table1="titles", table2="creators")
        link_table_book = db.driver_wrapper.get_link_column(table1="titles", table2="creators", column_type="title_id")
        link_table_other = db.driver_wrapper.get_link_column(table1="titles", table2="creators", column_type=idcol)
        link_table_priority = db.driver_wrapper.get_link_column(
            table1="titles", table2="creators", column_type="priority"
        )

        if type_filter is None:
            stmt = self.selectq_desc.format(link_table_book, link_table_other, link_table_name, link_table_priority)
        else:
            link_table_type = db.driver_wrapper.get_link_column(table1="titles", table2="creators", column_type="type")
            stmt = self.selectq_filter_desc.format(
                link_table_book,
                link_table_other,
                link_table_name,
                link_table_priority,
                link_table_type,
                type_filter,
            )

        # Preform the search and cache the results
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].append(book_id)
            book_col_map[book_id].append(item_id)

        # Convert everything into a tuple
        self.book_col_map = {k: tuple(v) for k, v in iteritems(book_col_map)}

        # Read the ids from the books table - so that we know what ids are valid for the table
        title_query = db.driver_wrapper.execute("SELECT title_id FROM titles")
        self.seen_book_ids = set([tr[0] for tr in title_query])

    def set_sort_names(self, aus_map: dict[DstTableID, str], db) -> dict[DstTableID, str]:
        """Reject legacy creator operations before changing database or cache."""
        raise NotImplementedError(
            "Legacy creator tables are retired. Use Catalog.agents with explicit "
            "Agent identities and credit roles; creator IDs cannot be reused as Agent "
            "IDs."
        )

    def set_links(self, link_map: dict[DstTableID, str], db) -> dict[DstTableID, str]:
        """Reject legacy creator operations before changing database or cache."""
        raise NotImplementedError(
            "Legacy creator tables are retired. Use Catalog.agents with explicit "
            "Agent identities and credit roles; creator IDs cannot be reused as Agent "
            "IDs."
        )

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove books from this cache.

        Example:
            Exercise CalibreAuthorsTable.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clean = CalibreManyToManyTable.remove_books(self, book_ids, db)

        for item_id in clean:
            self.alink_map.pop(item_id, None)
            self.asort_map.pop(item_id, None)

        return clean

    def rename_item(self, item_id: DstTableID, new_name: str, db) -> tuple[set[SrcTableID], DstTableID]:
        """
        Rename items in the authors table.

        Example:
            Exercise CalibreAuthorsTable.rename item through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ret = CalibreManyToManyTable.rename_item(self, item_id, new_name, db)

        # If something has gone wrong - renaming a thing which no longer exists? Scrub the entry and continue
        if item_id not in self.id_map:
            self.alink_map.pop(item_id, None)
            self.asort_map.pop(item_id, None)
        else:
            # Was a simple rename, update the author sort value
            self.set_sort_names(
                {item_id: author_to_author_sort(new_name, method=None)}, db
            )

        return ret

    def remove_items(
        self, item_ids: Iterable[DstTableID], db, restrict_to_book_ids: Iterable[SrcTableID] = None
    ) -> None:
        """
        Remove items by ids from the cache and the database.

        Example:
            Exercise CalibreAuthorsTable.remove items through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_ids: Value supplied for item ids under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :param restrict_to_book_ids: Value supplied for restrict to book ids under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise ValueError("Direct removal of authors is not allowed")


class CalibreFormatsTable(CalibreManyToManyTable, BaseFormatsTable):
    """
    Contains informaiton about the formats contained in the database table.

    Example:
        Exercise CalibreFormatsTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    _priority: bool = True
    _typed: bool = False

    def __init__(self, name: str, metadata: MetadataDict, link_table: InterLinkTableName = None) -> None:
        """
        Initialize the formats table.

        Example:
            Exercise CalibreFormatsTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        CalibreManyToManyTable.__init__(self, name, metadata, link_table)

        self.fname_map = defaultdict(dict)
        self.size_map = defaultdict(dict)
        self.col_book_map = None
        self.book_col_count_map = None
        self.book_file_map = None
        self.book_file_loc_map = None
        self.book_fmts_map = None

        self.size_mode: Literal["sum", "max", "min"] = "max"
        self.__parse_size_mode()

        # Needed in order to calculate the location of each of the files
        self.fsm = None

    def __parse_size_mode(self) -> None:
        """
        Parse the preferences to determine how the size of the book should be calculated

        Example:
            Exercise CalibreFormatsTable.  parse size mode through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pref_size_mode = preferences["book_size_display_mode"]
        if pref_size_mode.lower() not in ["sum", "max", "min"]:
            wrn_str = "Unable to parse preferences:book_size while creating the size table.\n"
            wrn_str += "preferences:book_size - {}".format(pref_size_mode)
            wrn_str += "defaulting to max.\n"
            default_log.warn(wrn_str)
            self.size_mode = "sum"
        else:
            self.size_mode = pref_size_mode.lower()

    def read_id_maps(self, db) -> None:
        """
        Id maps are not needed in this case.

        Example:
            Exercise CalibreFormatsTable.read id maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def fix_case_duplicates(self, db) -> None:
        """
        A nonsensical thing to do in this context.

        Example:
            Exercise CalibreFormatsTable.fix case duplicates through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    # Todo: Deal with unattatched folder stores
    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """Reject legacy book/file reads before clearing any cached inventory."""
        raise NotImplementedError(
            "Legacy book/file and backup links are retired. Resolve explicit WEMI "
            "attachments, assets and replicas before populating format inventory."
        )

    # Todo: Should also mark the associated books for removal from the folder stores
    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove the specified books from the cache.

        Example:
            Exercise CalibreFormatsTable.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clean = CalibreManyToManyTable.remove_books(self, book_ids, db)

        for book_id in book_ids:
            self.fname_map.pop(book_id, None)
            self.book_file_map.pop(book_id, None)
            self.size_map.pop(book_id, None)

            for fmt_set in itervalues(self.col_book_map):
                fmt_set.discard(book_id)

            self.book_col_map.pop(book_id, None)
            self.book_col_count_map.pop(book_id, None)
            self.book_file_loc_map.pop(book_id, None)
            self.book_fmts_map.pop(book_id, None)

        return clean

    def set_fname(self, book_id: SrcTableID, fmt: SpecificFormat, fname: str, db) -> None:
        """Update a known file row's recorded name, then its cached value.

        The existing map must already identify the file. This does not rename
        source bytes or infer WEMI attachments. Database failures preserve the
        cache; callers rolling back an enclosing transaction must refresh it.
        """
        file_id = self.book_file_map[book_id][fmt]
        names = self.fname_map[book_id]
        db.macros.update_row("files", file_id, {"file_name": fname})
        names[fmt] = fname

        # Todo: Notify the maintainer that the file name has changed - so it can change the file on disk

    def remove_formats(
        self, formats_map: dict[DstTableID, Iterable[Union[SpecificFormat, GenericFormat]]], db
    ) -> Union[int, dict[DstTableID, int]]:
        """Reject legacy format removal before deleting rows or clearing maps.

        A file row is not a WEMI attachment or replica. Removal needs an asset
        retention policy and a supported inventory reload before it is safe.
        """
        raise NotImplementedError(
            "Legacy book/file links are retired. Resolve WEMI attachments and "
            "asset/replica retention before removing formats."
        )

    def remove_items(
        self, item_ids: Iterable[DstTableID], db, restrict_to_book_ids: Iterable[SrcTableID] = None
    ) -> bool:
        """
        Attempts to remove items from the cache by id.

        Example:
            Exercise CalibreFormatsTable.remove items through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_ids: Value supplied for item ids under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :param restrict_to_book_ids: Value supplied for restrict to book ids under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Cannot delete a format directly - must remove it from the book")

    def rename_item(self, item_id: DstTableID, new_name: str, db) -> bool:
        """
        Directly rename an item in the formats table.

        Example:
            Exercise CalibreFormatsTable.rename item through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Meaningless to rename formats in the format table")

    def reload_book_from_db(self, db, book_id: DstTableID) -> None:
        """Reject legacy book/file reads before clearing any cached inventory."""
        raise NotImplementedError(
            "Legacy book/file and backup links are retired. Resolve explicit WEMI "
            "attachments, assets and replicas before populating format inventory."
        )

    def update_fmt(
        self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat], fname: str | None, size: int, db
    ) -> None:
        """Update a mapped file row's recorded size/name before its cache maps.

        Generic formats select the existing first-priority entry. This updates
        database metadata only; it neither writes bytes nor creates attachments.
        Database failures preserve the cache. An enclosing transaction rollback
        still requires the caller to refresh its cache.
        """
        base_format = fmt.split("_")[0]
        if not self.check_fmt_is_priority_fmt(fmt):
            fmt += "_1"

        files = self.book_file_map[book_id]
        file_id = files.get(fmt)
        if file_id is None:
            # Retain the legacy fallback for maps with an extra priority suffix.
            file_id = files[fmt + "_1"]
        names = self.fname_map[book_id]
        sizes = self.size_map[book_id]
        values: dict[str, int | str] = {"file_size_bytes": size}
        if fname is not None:
            values["file_name"] = fname
        db.macros.update_row("files", file_id, values)

        fmts = set(self.book_col_map.get(book_id, ()))
        fmts.add(fmt)
        self.book_col_map[book_id] = tuple(sorted(fmts))
        self.col_book_map.setdefault(base_format, set()).add(book_id)
        if fname is not None:
            names[fmt] = fname
        sizes[fmt] = size

    def get_last_priority_fmt(self, book_id: SrcTableID, fmt: GenericFormat) -> SpecificFormat:
        """
        Return the highest priority fmt for the title - needed when adding a fmt to the end of the priority stack.

        Example:
            Exercise CalibreFormatsTable.get last priority fmt through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        base_fmt = self.prep_base_fmt(fmt)
        in_use_fmts = set([fn for fn in self.fname_map[book_id].keys()])
        for i in range(1, 10000):
            test_fmt = "{}_{}".format(base_fmt, i + 1)
            if test_fmt not in in_use_fmts:
                return "{}_{}".format(base_fmt, i)
        raise KeyError("Cannot get_last_priority_fmt - out of range")

    def get_all_priority_fmts(self, book_id: SrcTableID, fmt: GenericFormat) -> Iterable[SpecificFormat]:
        """
        Return all the priority fmts corresponding to a given base fmt.

        Example:
            Exercise CalibreFormatsTable.get all priority fmts through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        base_fmt = self.prep_base_fmt(fmt)
        in_use_fmts = set([fn for fn in self.fname_map[book_id].keys()])

        fmt_re = r"^{}_[0-9]+$".format(base_fmt)
        fmt_pat = re.compile(fmt_re, re.I)

        found_fmts = set()
        for priority_fmt in in_use_fmts:

            if fmt_pat.match(priority_fmt):
                found_fmts.add(priority_fmt)

        return found_fmts
