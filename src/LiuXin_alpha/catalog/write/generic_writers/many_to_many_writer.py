from __future__ import division, absolute_import, print_function, unicode_literals

from copy import deepcopy

from LiuXin_alpha.catalog.write.base_writer import BaseWriter
from LiuXin_alpha.catalog.write.library_macros import library_set_publisher, library_unset_series, \
    library_set_series
from LiuXin_alpha.catalog.write.utils import UpdateDict
from LiuXin_alpha.errors import InvalidUpdate, NotInCache
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, dict_itervalues as itervalues, \
    basestring
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import uniq
from LiuXin_alpha.utils.text.icu import safe_lower, strcmp


class ManyToManyWriter(BaseWriter):
    def __init__(self, field):
        super(ManyToManyWriter, self).__init__(field)
        self.set_books_func = self.generic_many_many
        self.set_books = self.no_adapter_set_books

        # Set the individual methods that'll do the work
        self.db_clean_links = {"languages": self.language_many_many_db_clean_links}.get(
            self.name, self.generic_many_many_db_clean_links
        )

        self.db_update_links = {
            "publisher": self.do_publisher_many_many_db_update,
            "authors": self.authors_many_many_db_update,
            "languages": self.language_many_many_db_update,
            "series": self.do_series_many_many_db_update,
        }.get(self.name, self.do_generic_many_to_many_db_update)

        # Todo: Seems to be being used to do some of the lifting on the db_clean_unused method
        self.db_remove_links = {"series": self.series_many_many_db_remove_links}.get(
            self.name, self.generic_many_many_db_remove_links
        )

        self.db_id_matcher = {
            "languages": self.get_language_id,
            "series": self.get_series_id,
        }.get(self.name, self.get_db_id)

        self.db_clean_unused_items = {
            "publisher": self.do_publisher_many_one_clear_unused,
            "series": self.dummy_many_one_clear_unused,
        }.get(self.name, None)

        if field.table.priority is False and field.table.typed is False:
            self.set_books_func = self.generic_many_many

        elif field.table.priority is True and field.table.typed is False:
            self.set_books_func = self.generic_many_many

        elif field.table.priority is False and field.table.typed is True:
            self.set_books_func = self.generic_many_many

        elif field.table.priority is True and field.table.typed is True:
            self.set_books_func = self.generic_many_many

        else:
            raise NotImplementedError

    def generic_many_many(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Update entries for a table which has a priority many to many link to books. E.G. publishers.
        :param book_id_val_map:
        :param db:
        :param field:
        :param allow_case_change:
        :param args:
        :return:
        """
        if args:
            info_str = "Unexpected arguments passed to many_many"
            default_log.log_variables(info_str, "INFO", ("args", args))

        # Todo: Need to actually plumb this in - and also write it
        db_clean_unused_items = self.db_clean_unused_items

        dirtied = set()
        m = field.metadata
        table = field.table
        dt = m["datatype"]
        is_authors = field.name == "authors"

        # Todo: This is HEINOUSLY stupidly inefficient. FIX THIS MESS!
        # Map values to db ids, including any new values - this will be used to match any new values to existing ones on the
        # database
        # 1) Build a val_id map for every element
        kmap = safe_lower if dt == "text" else lambda x: x
        rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # 2) Check to see if the table has some entries that differ only in case, fix it
        if len(rid_map) != len(table.id_map):
            table.fix_case_duplicates(db)
            rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # 3) kmap is used to eliminate
        id_map_update = dict()
        try:
            book_id_val_map, id_map_update = field.update_preflight(book_id_val_map, dict(), dirtied)
        except AttributeError:
            pass
        except NotImplementedError as e:
            # Probably an unexpected case in the update_preflight logic
            err_str = "Error when trying to run update_preflight"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("book_id_val_map", book_id_val_map))
            raise InvalidUpdate(err_str)

        # Todo: Need to rename this to something a but more revealing - db_update_precheck?
        # Todo: Ideally, this should occur AFTER the id_map_update is created - go back and change it
        field.table.update_precheck(book_id_val_map, id_map_update)
        book_id_val_map = UpdateDict(book_id_val_map)
        book_id_val_map.checked = True

        if field.name == "tags":
            for target_book_id, update_form in iteritems(book_id_val_map):
                if isinstance(update_form, set):
                    db.macros.break_generic_link(
                        link_table="tag_title_links",
                        link_col="tag_title_link_title_id",
                        remove_id=target_book_id,
                    )

        # 3) Eliminate duplicates
        if field.name not in ["series", "authors", "publisher"]:
            try:
                book_id_val_map = self._do_duplicate_elimination(book_id_val_map, kmap)
            except TypeError as e:
                err_str = "TypeError while trying to normalize the book_id_val_map"
                default_log.log_exception(err_str, e, "ERROR", ("book_id_val_map", book_id_val_map))
                raise

        # 4) Match the remaining values to their corresponding entries on the table (creating them if required)
        # Generate maps keyed with the normalized
        val_map = {}
        case_changes = {}
        self._do_db_id_match(
            book_id_val_map,
            db,
            m,
            table,
            kmap,
            rid_map,
            allow_case_change,
            case_changes,
            val_map,
            is_authors=is_authors,
        )

        # Todo: Move this into the database metadata
        if field.name in ["series", "authors", "publisher", "publishers"]:
            update_id_map = {value: key for key, value in iteritems(val_map)}
            book_id_val_map, id_map_update = field.update_preflight(book_id_val_map, update_id_map)

        id_map_update = {v: k for k, v in iteritems(val_map)}

        # If any case changes have occurred, preform them
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m, is_authors=is_authors)
            if is_authors:
                for item_id, val in iteritems(case_changes):
                    for book_id in table.col_book_map[item_id]:
                        current_sort = field.db_author_sort_for_book(book_id)
                        new_sort = field.author_sort_for_book(book_id)
                        if strcmp(current_sort, new_sort) == 0:
                            # The sort strings differ only by case, update the db sort
                            field.author_sort_field.writer.set_books({book_id: new_sort}, db)

        book_id_item_id_map = self._do_vals_to_ids(book_id_val_map, val_map)

        # Todo: This might fail - we're using tupes here and lists elsewhere - need a more complex test
        # Todo: Will also probably trip NotInCache a few times - need to fix that
        # Ignore those items whose value is the same as the current value
        try:
            book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != field.ids_for_book(k)}
        except NotInCache:
            raise InvalidUpdate

        # Update the dirtied set with the books that are actually going to be modified.
        dirtied |= set(book_id_item_id_map)

        # Remove any duplicated which might have worked their way into the maps
        # (by this point it should just be
        book_id_item_id_map = self._do_duplicate_elimination(book_id_item_id_map, kmap=lambda x: x)

        # Before actually running the update we need to check that the update is valid (refers to objects which exist)
        try:
            field.update_precheck(book_id_item_id_map, id_map_update)
        except AttributeError:
            pass

        # Use the internal_update_cache method to preform a cache update which returns useful information
        updated, deleted = field.internal_update_cache(book_id_item_id_map, id_map_update=id_map_update)

        override_link_type = getattr(table, "table_type_filter", None)
        self.db_update_links(
            db=db,
            table=table,
            field=field,
            is_custom_series=False,
            updated=updated,
            deleted=deleted,
            link_type=override_link_type,
        )

        # Remove no longer used items
        remove = {item_id for item_id in table.id_map if not table.col_book_map.get(item_id, False)}

        # Todo: Fix this and plumb it back in
        # if remove:
        #
        #     db_remove_links(db, table, field, remove, is_authors)
        #
        #     # Todo: Need to move this over into the cache - probably never actually being used at present
        #     for item_id in remove:
        #         del table.id_map[item_id]
        #         table.col_book_map.pop(item_id, None)
        #         if is_authors:
        #             table.asort_map.pop(item_id, None)
        #             table.alink_map.pop(item_id, None)

        if db_clean_unused_items is not None:
            pass

        update_data = dict()
        update_data["dirtied"] = dirtied
        update_data["cache_update_needed"] = False
        update_data["id_map"] = id_map_update
        update_data["book_col_map"] = book_id_item_id_map

        return update_data

    def _do_vals_to_ids(self, book_id_val_map, val_map):
        """
        Take a book_id_val_map turn it into a book_id_item_id map by replacing all the vals with their corresponding
        item ids
        :param book_id_val_map:
        :param val_map:
        :return:
        """

        def _val_to_id(_id, val_map):
            if isinstance(_id, int):
                return _id
            else:
                return val_map[_id]

        book_id_item_id_map = dict()
        for book_id, book_vals in iteritems(book_id_val_map):
            if book_vals is None:
                book_id_item_id_map[book_id] = None
            elif isinstance(book_vals, (tuple, list)):
                book_id_item_id_map[book_id] = [_val_to_id(_val, val_map) for _val in book_vals]
            elif isinstance(book_vals, set):
                book_id_item_id_map[book_id] = set([_val_to_id(_val, val_map) for _val in book_vals])
            elif isinstance(book_vals, dict):
                book_id_item_id_map[book_id] = self._do_vals_to_ids(book_vals, val_map)
            else:
                raise NotImplementedError
        return book_id_item_id_map

    def _do_duplicate_elimination(self, book_id_val_map, kmap):
        """
        Eliminate any duplicates using the provided hash function - recursing if the dictionary structure is nested
        :param book_id_val_map:
        :param kmap:
        :return:
        """
        dupe_free_dict = dict()
        for key, vals in iteritems(book_id_val_map):
            if vals is None:
                dupe_free_dict[key] = None
            elif isinstance(vals, set):
                dupe_free_dict[key] = vals
            elif isinstance(vals, (tuple, list)):
                dupe_free_dict[key] = uniq(vals, kmap)
            elif isinstance(vals, dict):
                dupe_free_dict[key] = self._do_duplicate_elimination(vals, kmap)
            else:
                raise NotImplementedError
        return dupe_free_dict

    def _do_db_id_match(
        self,
        book_id_val_map,
        db,
        m,
        table,
        kmap,
        rid_map,
        allow_case_change,
        case_changes,
        val_map,
        is_authors=False,
    ):

        db_id_matcher = self.db_id_matcher

        # Todo: Ideally the update dict should have been unmangled by this point
        for vals in itervalues(book_id_val_map):
            if vals is None:
                continue

            if isinstance(vals, (basestring,)):
                db_id_matcher(
                    vals,
                    db,
                    m,
                    table,
                    kmap,
                    rid_map,
                    allow_case_change,
                    case_changes,
                    val_map,
                    is_authors=is_authors,
                )
                continue

            elif isinstance(vals, (list, tuple, set)):
                for val in vals:
                    if not isinstance(val, int):
                        db_id_matcher(
                            val,
                            db,
                            m,
                            table,
                            kmap,
                            rid_map,
                            allow_case_change,
                            case_changes,
                            val_map,
                            is_authors=is_authors,
                        )
                    else:
                        pass

            elif isinstance(vals, dict):
                self._do_db_id_match(
                    vals,
                    db,
                    m,
                    table,
                    kmap,
                    rid_map,
                    allow_case_change,
                    case_changes,
                    val_map,
                    is_authors=is_authors,
                )

            else:
                raise NotImplementedError

    @staticmethod
    def do_publisher_many_many_db_update(
        db,
        table,
        field=None,
        is_custom_series=False,
        updated=None,
        deleted=None,
        is_authors=False,
        link_type=None,
    ):
        """
        Do an update to the publisher table.
        :param db: The database to preform the update on
        :param table:
        :param field: The field to do the update on
        :param is_custom_series:
        :param updated: The dictionary to preform the update with - keyed with the id of the book and valued with
        :param deleted:
        :return:
        """
        deleted = deleted if deleted is not None else {}
        updated = updated if updated is not None else {}

        id_map = dict()
        book_col_map = dict()

        # Do the publisher update
        for book_id in updated:
            pub_val = updated[book_id]
            pub_id, new_pub_val = library_set_publisher(db=db, title_id=book_id, publisher_id=pub_val)

            book_col_map[book_id] = pub_id
            id_map[pub_id] = new_pub_val

        # For every element in the deleted set, nullify each of the elements
        for book_id in deleted:
            library_set_publisher(db=db, title_id=book_id, publisher=None, publisher_id=None)

            book_col_map[book_id] = None

        return book_col_map, id_map

    # Todo: Check this is only taking out authors - might need to be renamed
    @staticmethod
    def authors_many_many_db_update(
        db,
        table,
        field=None,
        is_custom_series=False,
        updated=None,
        deleted=None,
        is_authors=False,
        link_type=None,
    ):
        """
        Do update in the authors table.
        :param db:
        :param table:
        :param updated:
        :param deleted: Not currently used
        :param is_authors:
        :return:
        """
        deleted = deleted if deleted is not None else {}
        updated = updated if updated is not None else {}

        vals = ((book_id, val) for book_id, vals in iteritems(updated) for val in vals)

        # Todo: HAVE to standardize creator and other types - triggers in the database?
        db.macros.break_creator_title_links(title_id=(k for k in updated))
        db.macros.break_creator_title_links(title_id=(k for k in deleted))

        # Todo: Fold into a library author set method
        db.macros.make_creator_title_links(id_pairs=vals)

    # Todo: What about the nullified elements?
    # Todo: What about all the OTHER languages? Are they being handled correctly?
    # Todo: This should ALL be in the languages table!?
    @staticmethod
    def language_many_many_db_update(db, table, updated, is_authors, field=None, is_custom_series=False):
        """
        Preform an update of the languages linked to a book.
        :param db:
        :param table:
        :param updated:
        :param is_authors:
        :return:
        """
        for book_id in updated:
            lang_id = updated[book_id][0]
            db.macros.set_title_primary_language(book_id, lang_id)

    @staticmethod
    def do_series_many_many_db_update(
        db,
        table=None,
        field=None,
        is_custom_series=False,
        is_authors=False,
        updated=None,
        deleted=None,
        link_type=None,
    ):
        """
        Do an update on a series table.
        :param db:
        :param table:
        :param field:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Do the series update
        for book_id in updated:
            series_id = updated[book_id]
            if isinstance(series_id, list):
                # Any entries in both the old and the new list will be reordered - but we need to eliminate entries from
                # the new list which do no appear in the old
                # Have to go for the database as the cache has already been updated at this point
                non_overlap_set = db.macros.get_title_series_ids_set(book_id) - set(series_id)
                for remove_series_id in non_overlap_set:
                    library_unset_series(db=db, title_id=book_id, series_id=remove_series_id)

                # Write the series back to the database - reordering the surviving series as required
                series_id = deepcopy(series_id)
                series_id.reverse()
                for true_series_id in series_id:
                    library_set_series(db=db, title_id=book_id, series=None, series_id=true_series_id)
            else:
                library_set_series(db=db, title_id=book_id, series=None, series_id=series_id)

        # For every element in the deleted set, nullify each of the title series
        if deleted is not None:
            for book_id in deleted:
                library_set_series(db=db, title_id=book_id, series=None, series_id=None)

        return None, None

    @staticmethod
    def generic_many_many_db_update(db, table, updated, deleted, is_authors, field=None, is_custom_series=False):
        """
        Preform update on a multiply linked table. Currently can only deal with authors.
        :param db:
        :param table:
        :param updated:
        :param is_authors:
        :return:
        """
        db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, tuple(k for k in deleted))

        vals = tuple((book_id, val) for book_id, vals in iteritems(updated) for val in vals)

        db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, tuple(k for k in updated))

        db.macros.make_generic_link_no_priority(
            table.link_table,
            table.link_table_table_id_column,
            table.link_table_bt_id_column,
            id_pairs=vals,
        )

    @staticmethod
    def language_many_many_db_clean_links(db, table, deleted):
        """
        Remove primary language links from the table.
        :param db:
        :param table:
        :param deleted:
        :return:
        """
        db.macros.break_lang_title_primary_link((k for k in deleted))

    @staticmethod
    def generic_many_many_db_clean_links(db, table, deleted):
        """
        Remove now unused links from the link table.
        :param db:
        :param table:
        :param deleted:
        :return:
        """
        db.macros.generic_clean_update(table.link_table, table.link_table_bt_id_column, (k for k in deleted))

    def generic_many_many_db_remove_links(self, db, table, field, remove, is_authors):
        """
        Used for removing all links to the target table. Used when the entries are being removed.
        :param db:
        :param table:
        :param field:
        :param remove:
        :param is_authors:
        :return:
        """
        if not is_authors:
            db.macros.break_generic_link(
                table.lx_table_name,
                table.table_id_col,
                ((item_id,) for item_id in remove),
            )
        else:
            self.do_creators_many_many_clear_unused(db, table=table, field=field)

    @staticmethod
    def do_creators_many_many_clear_unused(db, table, field):
        """
        Clear the unused entries from the creators table.
        :param db:
        :param table:
        :param field:
        :return:
        """
        db.macros.creator_clear_unused()

    @staticmethod
    def get_language_id(
        val,
        db,
        m,
        table,
        kmap,
        rid_map,
        allow_case_change,
        case_changes,
        val_map,
        is_authors=False,
    ):
        """
        Attempts to match the given val to a valid entry in the languages table.
        :param val:
        :param db:
        :param m:
        :param table:
        :param kmap:
        :param rid_map:
        :param allow_case_change:
        :param case_changes:
        :param val_map:
        :param is_authors:
        :return:
        """
        if val not in rid_map.keys():
            lang_row = db.ensure.language(val, lang_code="either")
            val_map[val] = lang_row["language_id"]
        else:
            val_map[val] = rid_map[val]

    @staticmethod
    def get_series_id(
        val,
        db,
        m,
        table,
        kmap,
        rid_map,
        allow_case_change,
        case_changes,
        val_map,
        is_authors=False,
    ):
        """
        Attempts to match the given val to a valid entry in the languages table.
        :param val:
        :param db:
        :param m:
        :param table:
        :param kmap:
        :param rid_map:
        :param allow_case_change:
        :param case_changes:
        :param val_map:
        :param is_authors:
        :return:
        """
        if val not in rid_map.keys():
            series_row = db.ensure.series_blind(creator_rows=[], series_name=val, use_phash=False)
            val_map[val] = series_row["series_id"]
        else:
            val_map[val] = rid_map[val]

    # Todo: Merge into a single generic method with the creators version
    @staticmethod
    def do_publisher_many_one_clear_unused(db, table, field):
        """
        Clear the unused entries from the publisher's table.
        :param db:
        :param table:
        :param field:
        :return:
        """
        db.macros.publisher_clear_unused()

    @staticmethod
    def dummy_many_one_clear_unused(db, table, field):
        """
        Remove unused elements from the ratings table.
        Currently not used - as that table should be preserved.
        :param db:
        :param table:
        :param field:
        :return:
        """
        pass

    @staticmethod
    def series_many_many_db_remove_links(db, table, field, remove, is_authors):
        """
        At the moment a dummy - as it's assumed series will actually be managed elesewhere.
        :param db:
        :param table:
        :param field:
        :param remove:
        :param is_authors:
        :return:
        """
        return
