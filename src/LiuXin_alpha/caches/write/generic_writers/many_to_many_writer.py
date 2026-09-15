"""
Coordinate legacy many-to-many cache updates and specialized Catalog link writes.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from copy import deepcopy

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api.common import MetadataCandidate
from LiuXin_alpha.caches.write.utils import UpdateDict
from LiuXin_alpha.databases.macro_types import LinkValue
from LiuXin_alpha.errors import InvalidUpdate, NotInCache
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, dict_itervalues as itervalues, \
    basestring
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.python_tools import uniq
from LiuXin_alpha.utils.text.icu import safe_lower, strcmp


class ManyToManyWriter(BaseWriter):
    """
    Resolve legacy relation values, update caches, and dispatch link persistence.

    Public set_books bypasses BaseWriter scalar adaptation and returns the hook result dictionary. Hook selection depends on field name; several cleanup hooks are retained but not invoked by generic_many_many.

    Example:
        For a configured relation field, ``ManyToManyWriter(field).set_books(updates, db)`` returns a dictionary containing dirtied IDs and cache maps.
    """

    def __init__(self, field):
        """
        Bind unadapted many-to-many updates and field-specific persistence helpers.

        Select publisher, author, language and series persistence by name; language/series get dedicated value matchers. Both table.priority and table.typed must be the literal booleans True or False. All four supported combinations use generic_many_many.

        Example:
            A field named "publisher" binds ``do_publisher_many_many_db_update``; an ordinary field binds the inherited generic link updater.


        :param field: Legacy field supplying metadata and the relation cache/update hooks.
        :return: None; initializes adapter state and binds lookup, persistence and cleanup hooks.
        :raises NotImplementedError: Either table capability flag is not a literal boolean.
        """

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
        Resolve values, update the relation cache, then persist the resulting link changes.

        Build a normalized reverse value map and repair case duplicates, then run optional field preflight and mandatory table precheck. Tag set updates clear existing tag links before value resolution. Deduplicate most fields, resolve/create values, rerun preflight for series/authors/publishers, and apply case changes. Convert values to IDs, discard updates equal to field.ids_for_book, precheck again, and call internal_update_cache before db_update_links. Cleanup of unused items is currently inactive. There is no whole-operation rollback, and cache changes or newly created values can precede persistence failures. The language updater requires an is_authors argument that this generic call does not supply; that legacy hook cannot complete through this call as written.

        Example:
            With a compatible tags field, ``writer.generic_many_many({7: ["fiction"]}, db, field, True)`` resolves the tag, updates the cache and writes links, returning a result dictionary.


        :param book_id_val_map: Book-to-update mapping accepted by the legacy field preflight and conversion hooks.
        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param field: Legacy field supplying metadata and the relation cache/update hooks.
        :param allow_case_change: Whether matched values may cause case changes.
        :param args: Extra compatibility arguments, logged and otherwise ignored.
        :return: Dictionary with dirtied, cache_update_needed=False, id_map and book_col_map entries.
        :raises InvalidUpdate: Initial field preflight raises NotImplementedError, or the comparison lookup raises NotInCache.
        :raises NotImplementedError: A conversion helper encounters an unsupported payload shape.
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
                    db.metadata_sql.break_generic_link(
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
        Build fresh nested update containers by replacing values with their IDs.

        Integer elements, including booleans, pass through unchanged. Dictionaries recurse using their own keys, so typed updates retain type keys. Scalar outer strings/integers are unsupported; unresolved elements raise KeyError.

        Example:
            >>> writer = object.__new__(ManyToManyWriter)
            >>> writer._do_vals_to_ids({7: ["tag", 9], 8: {"role": None}}, {"tag": 4})
            {7: [4, 9], 8: {'role': None}}


        :param book_id_val_map: Mapping whose values are None, lists/tuples, sets or nested dictionaries.
        :param val_map: Mapping from non-integer values to resolved IDs.
        :return: New dictionary; sequences become lists, sets remain sets and None is preserved.
        :raises KeyError: A non-integer element is absent from val_map.
        :raises NotImplementedError: An update value has an unsupported container shape.
        """

        def _val_to_id(_id, val_map):
            """
            Preserve an integer element or look up its resolved ID.

            Example:
                Inside the surrounding conversion, integer 9 remains 9, while "tag" resolves to 4 when ``val_map == {"tag": 4}``.


            :param _id: One sequence/set element to translate.
            :param val_map: Mapping from non-integer elements to resolved IDs.
            :return: The original integer, or val_map[_id].
            :raises KeyError: A non-integer element has no resolved entry.
            """

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
        Deduplicate sequences recursively while preserving None and set objects.

        The first occurrence of each normalized sequence key wins. Sets are passed through without applying kmap, so case-equivalent set entries are not merged.

        Example:
            >>> writer = object.__new__(ManyToManyWriter)
            >>> writer._do_duplicate_elimination({7: ["Tag", "tag", "Other"]}, str.lower)
            {7: ('Tag', 'Other')}


        :param book_id_val_map: Mapping with None, set, list/tuple or nested dictionary values.
        :param kmap: Callable producing a hashable comparison key for each sequence element.
        :return: New nested dictionary; sequence results are tuples and original set objects are shared.
        :raises NotImplementedError: A mapping value has an unsupported shape.
        :raises TypeError: A normalized sequence key is unhashable.
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

        """
        Populate shared resolution maps for non-integer values in nested updates.

        None is skipped; strings are matched whole, list/tuple/set elements are matched unless integers, and dictionaries recurse. Matcher return values are discarded. Scalar integers at the mapping-value level are unsupported, even though integer sequence members are accepted. Resolution may create persistent rows before a later value fails.

        Example:
            For ``{7: ["new tag", 4]}``, the bound matcher receives "new tag"; integer 4 needs no lookup.


        :param book_id_val_map: Mapping of strings, sequences, sets, None or nested dictionaries.
        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param m: Field metadata forwarded to the bound matcher.
        :param table: Legacy relation table supplying cached value/link maps and schema attributes.
        :param kmap: Value normalizer forwarded to the matcher.
        :param rid_map: Mutable normalized-value-to-ID lookup forwarded to the matcher.
        :param allow_case_change: Case-change permission forwarded to the matcher.
        :param case_changes: Mutable case-change output mapping.
        :param val_map: Mutable raw-value-to-ID output mapping.
        :param is_authors: Author-specific lookup flag forwarded to each matcher call.
        :return: None; any described database/cache mutations happen in place.
        :raises NotImplementedError: A top-level or nested mapping value has an unsupported shape.
        """

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
        Replace Work publisher credits and return the first publisher projection.

        None updates become empty. For each update, replace role "pbl" credits, then require the first Agent and read its canonical name. Deletions run afterward and can override an updated Work. An empty list clears credits but then raises IndexError when selecting the primary ID. Storage and projection reads are sequential without a batch rollback.

        Example:
            ``do_publisher_many_many_db_update(db, updated={7: [4, 5]}, table=table)`` retains both publisher credits and returns publisher 4 as the display projection.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :param is_custom_series: Compatibility argument not used by this helper.
        :param updated: Work-to-publisher-ID mapping; lists are expanded, other values become one element.
        :param deleted: Work IDs whose publisher-role credits are cleared; None means no deletions.
        :param is_authors: Compatibility argument not used by this helper.
        :param link_type: Compatibility argument not used by this helper.
        :return: Pair (book_col_map, id_map) containing primary publisher IDs/names and None for deleted Works.
        :raises IndexError: An updated publisher list is empty, after its storage replacement.
        """
        deleted = deleted if deleted is not None else {}
        updated = updated if updated is not None else {}

        id_map = dict()
        book_col_map = dict()

        catalog = Catalog(db)

        # Calibre exposes one display publisher while Catalog retains an
        # ordered publisher-role credit set on the Work.
        for book_id in updated:
            pub_val = updated[book_id]
            publisher_ids = tuple(pub_val) if isinstance(pub_val, list) else (pub_val,)
            catalog.agents.replace_for_wemi(
                level="work",
                entity_id=book_id,
                role="pbl",
                agent_ids=publisher_ids,
            )
            primary_id = publisher_ids[0]
            primary_row = catalog.agents.require(primary_id)
            book_col_map[book_id] = primary_id
            id_map[primary_id] = primary_row["agent_canonical_name"]

        # For every element in the deleted set, nullify each of the elements
        for book_id in deleted:
            catalog.agents.replace_for_wemi(
                level="work",
                entity_id=book_id,
                role="pbl",
                agent_ids=(),
            )

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
        Replace author-role Agent credits for each Work, then clear deleted Works.

        Each update is converted to a tuple and passed to Catalog at level "work", role "aut". Deletions run afterward. No cache maps are returned or updated by this helper; earlier Work replacements survive a later failure.

        Example:
            ``authors_many_many_db_update(db, table, updated={7: [4, 5]}, deleted={8})`` replaces Work 7 authors and clears Work 8 author credits.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :param is_custom_series: Compatibility argument not used by this helper.
        :param updated: Work IDs mapped to iterable Agent IDs; None means no updates.
        :param deleted: Work IDs whose author-role credits are cleared; None means none.
        :param is_authors: Compatibility argument not used by this helper.
        :param link_type: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        """
        deleted = deleted if deleted is not None else {}
        updated = updated if updated is not None else {}

        catalog = Catalog(db)
        for book_id, agent_ids in iteritems(updated):
            catalog.agents.replace_for_wemi(
                level="work",
                entity_id=book_id,
                role="aut",
                agent_ids=tuple(agent_ids),
            )
        for book_id in deleted:
            catalog.agents.replace_for_wemi(
                level="work",
                entity_id=book_id,
                role="aut",
                agent_ids=(),
            )

    # Todo: What about the nullified elements?
    # Todo: What about all the OTHER languages? Are they being handled correctly?
    # Todo: This should ALL be in the languages table!?
    @staticmethod
    def language_many_many_db_update(
        db,
        table,
        updated,
        is_authors,
        field=None,
        is_custom_series=False,
        deleted=None,
        link_type=None,
    ):
        """
        Write the first supplied language as primary and clear requested primary links.

        Require each first language ID through Catalog and delegate a primary Work-language write. Other supplied IDs are ignored. The required is_authors parameter has no default even though it is unused; generic_many_many currently omits it, so callers of this hook must supply it explicitly.

        Example:
            ``language_many_many_db_update(db, table, {7: [4, 5]}, False)`` validates and writes language 4 only.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param updated: Work IDs mapped to nonempty indexable language-ID collections.
        :param is_authors: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :param is_custom_series: Compatibility argument not used by this helper.
        :param deleted: Work IDs whose primary language is cleared; None means none.
        :param link_type: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        :raises IndexError: An updated language sequence is empty.
        """
        catalog = Catalog(db)
        writer = catalog.create_writer("works", "language")
        for book_id in updated:
            lang_id = updated[book_id][0]
            catalog.languages.require(lang_id)
            writer.write(
                {book_id: LinkValue(lang_id)},
                link_type="primary",
            )
        for book_id in deleted or ():
            writer.write({book_id: ()}, link_type="primary")

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
        Replace Work-series links while copying the current primary index to each new link.

        Find the first extra link column ending in "_index". When both that column and the current primary series index exist, include that index in every replacement LinkValue for the Work. Only lists expand; other values form a single element. Deletions override updates for the same Work, and all replacements are sent in one writer.write call if nonempty.

        Example:
            With primary index 2.0, ``do_series_many_many_db_update(db, updated={7: [4, 5]})`` attaches that index to both new links when the schema exposes an index column.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :param is_custom_series: Compatibility argument not used by this helper.
        :param is_authors: Compatibility argument not used by this helper.
        :param updated: Required mapping of Work IDs to a series ID or list of IDs; the None default is not handled.
        :param deleted: Work IDs whose series links are cleared; None means none.
        :param link_type: Compatibility argument not used by this helper.
        :return: (None, None); this helper returns no replacement cache maps.
        """
        catalog = Catalog(db)
        writer = catalog.create_writer("works", "series")
        index_column = next(
            (
                column.name
                for column in writer.link_spec.extra_link_columns
                if column.name.endswith("_index")
            ),
            None,
        )
        replacements = {}
        for book_id, raw_ids in iteritems(updated):
            series_ids = tuple(raw_ids) if isinstance(raw_ids, list) else (raw_ids,)
            series_index = db.metadata_sql.get_primary_series_index(book_id)
            extra = (
                {index_column: series_index}
                if index_column is not None and series_index is not None
                else {}
            )
            replacements[book_id] = tuple(
                LinkValue(series_id, extra=extra)
                for series_id in series_ids
            )
        for book_id in deleted or ():
            replacements[book_id] = ()
        if replacements:
            writer.write(replacements)

        return None, None

    @staticmethod
    def generic_many_many_db_update(db, table, updated, deleted, is_authors, field=None, is_custom_series=False):
        """
        Clear deleted and updated owner links, then insert flattened unprioritized pairs.

        Call break_generic_link for deleted IDs first, flatten updated values into (book_id, value) pairs, then clear updated IDs and call make_generic_link_no_priority with the table column arguments in their existing order. No value resolution, cache update or transaction is supplied here. This compatibility helper is distinct from the inherited default db_update_links hook.

        Example:
            For ``updated={7: [4, 5]}``, the insertion payload contains (7, 4) and (7, 5) after existing links for Work 7 are cleared.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Legacy table exposing link-table and endpoint-column names.
        :param updated: Book IDs mapped to iterable target values.
        :param deleted: Book IDs whose links are cleared.
        :param is_authors: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :param is_custom_series: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        """
        db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, tuple(k for k in deleted))

        vals = tuple((book_id, val) for book_id, vals in iteritems(updated) for val in vals)

        db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, tuple(k for k in updated))

        db.macros.make_generic_link_no_priority(
            table.link_table,
            table.link_table_table_id_column,
            table.link_table_bt_id_column,
            id_pairs=vals,
        )

    @staticmethod
    def language_many_many_db_clean_links(db, table, deleted):
        """
        Delegate removal of primary language links for the supplied book IDs.

        Example:
            ``language_many_many_db_clean_links(db, table, {7, 8})`` passes IDs 7 and 8 to break_lang_title_primary_link.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param deleted: Iterable of book IDs forwarded as a generator.
        :return: None; any described database/cache mutations happen in place.
        """
        db.metadata_sql.break_lang_title_primary_link((k for k in deleted))

    @staticmethod
    def generic_many_many_db_clean_links(db, table, deleted):
        """
        Delegate generic link cleanup using the table owner-ID column.

        Example:
            ``generic_many_many_db_clean_links(db, table, {7})`` calls generic_clean_update for owner 7.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Table providing link_table and link_table_bt_id_column.
        :param deleted: Iterable of owner IDs forwarded as a generator.
        :return: None; any described database/cache mutations happen in place.
        """
        db.macros.generic_clean_update(table.link_table, table.link_table_bt_id_column, (k for k in deleted))

    def generic_many_many_db_remove_links(self, db, table, field, remove, is_authors):
        """
        Delegate target-ID cleanup or the creator-wide unused-item cleanup hook.

        The non-author branch passes table.lx_table_name directly to break_generic_link; it does not derive a link-table name. The author branch ignores remove. generic_many_many retains this hook but its removal call is currently commented out.

        Example:
            With is_authors=True, ``writer.generic_many_many_db_remove_links(db, table, field, ids, True)`` invokes creator_clear_unused regardless of ids.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Table providing lx_table_name and table_id_col for non-author cleanup.
        :param field: Field forwarded only to the creator cleanup helper.
        :param remove: Iterable of target IDs, wrapped as singleton tuples for non-author cleanup.
        :param is_authors: Whether to call the creator cleanup helper instead of using remove.
        :return: None; any described database/cache mutations happen in place.
        """
        if not is_authors:
            db.metadata_sql.break_generic_link(
                table.lx_table_name,
                table.table_id_col,
                ((item_id,) for item_id in remove),
            )
        else:
            self.do_creators_many_many_clear_unused(db, table=table, field=field)

    @staticmethod
    def do_creators_many_many_clear_unused(db, table, field):
        """
        Invoke the database creator cleanup operation.

        Example:
            ``do_creators_many_many_clear_unused(db, table, field)`` delegates to db.metadata_sql.creator_clear_unused().


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        """
        db.metadata_sql.creator_clear_unused()

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
        Use a cached raw value or resolve an exact Catalog language match.

        Store the resolved ID under the original value in val_map. The reverse map, table caches and case-change map are not updated; kmap is unused.

        Example:
            >>> values = {}
            >>> ManyToManyWriter.get_language_id("eng", None, None, None, None, {"eng": 4}, False, {}, values)
            >>> values
            {'eng': 4}


        :param val: Value to resolve.
        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param m: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :param kmap: Compatibility argument not used by this helper.
        :param rid_map: Existing raw-value-to-ID lookup; this helper does not normalize its keys.
        :param allow_case_change: Compatibility argument not used by this helper.
        :param case_changes: Compatibility argument not used by this helper.
        :param val_map: Mutable value-to-ID output map.
        :param is_authors: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        :raises InvalidUpdate: Catalog exact lookup has no matched entity ID.
        """
        if val not in rid_map.keys():
            language_match = Catalog(db).languages.exact(val)
            if not language_match.is_match or language_match.entity_id is None:
                raise InvalidUpdate("Language could not be resolved: {!r}".format(val))
            val_map[val] = language_match.entity_id
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
        Use a cached raw series name or match/create a Catalog series identity.

        On a cache miss pass MetadataCandidate({"name": val}) to Catalog.series.match_or_create and store its result in val_map. No reverse-map or table-cache update is performed here; kmap and case-change flags are unused.

        Example:
            >>> values = {}
            >>> ManyToManyWriter.get_series_id("Cycle", None, None, None, None, {"Cycle": 4}, False, {}, values)
            >>> values
            {'Cycle': 4}


        :param val: Value to resolve.
        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param m: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :param kmap: Compatibility argument not used by this helper.
        :param rid_map: Existing raw-value-to-ID lookup; this helper does not normalize its keys.
        :param allow_case_change: Compatibility argument not used by this helper.
        :param case_changes: Compatibility argument not used by this helper.
        :param val_map: Mutable value-to-ID output map.
        :param is_authors: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        """
        if val not in rid_map.keys():
            val_map[val] = Catalog(db).series.match_or_create(
                MetadataCandidate({"name": val})
            )
        else:
            val_map[val] = rid_map[val]

    # Todo: Merge into a single generic method with the creators version
    @staticmethod
    def do_publisher_many_one_clear_unused(db, table, field):
        """
        Invoke the database publisher cleanup operation.

        Selected as a publisher cleanup hook, but generic_many_many currently does not call it.

        Example:
            ``do_publisher_many_one_clear_unused(db, table, field)`` delegates to publisher_clear_unused().


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        """
        db.metadata_sql.publisher_clear_unused()

    @staticmethod
    def dummy_many_one_clear_unused(db, table, field):
        """
        Leave all entries unchanged when unused-item cleanup is disabled.

        Example:
            >>> ManyToManyWriter.dummy_many_one_clear_unused(None, None, None) is None
            True


        :param db: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :return: None; no collaborators are accessed.
        """
        pass

    @staticmethod
    def series_many_many_db_remove_links(db, table, field, remove, is_authors):
        """
        Leave series links unchanged through the retained no-op removal hook.

        Example:
            >>> ManyToManyWriter.series_many_many_db_remove_links(None, None, None, {4}, False) is None
            True


        :param db: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :param remove: Compatibility argument not used by this helper.
        :param is_authors: Compatibility argument not used by this helper.
        :return: None; this hook performs no cleanup.
        """
        return
