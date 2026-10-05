
"""
Declare metadata-aware SQL operations with legacy variadic signatures.

MetadataSQLAPI is abstract. The concrete MetadataSQL mixins accept the narrower signatures documented below; *args/**kwargs here do not promise arbitrary arguments. Most methods target legacy titles/books/creators tables, while identifier replacement writes FRBR Work ownership. Schema compatibility, transaction boundaries and return shapes differ by method. Raw driver-connection writes and driver-wrapper calls are not interchangeable; no common atomicity or cache-refresh guarantee is provided.
"""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


# Todo: This is not a great API. It needs parameters.
class MetadataSQLAPI(abc.ABC):
    """
    Specify database-owned metadata SQL helpers without providing executable defaults.

    MetadataSQL combines concrete mixins implementing this surface. Each method documents its concrete call shape because this ABC retains broad *args/**kwargs declarations. Operations generally expect existing schema objects and IDs; they do not create a complete library model. Pure SQL deletion/update methods affect database rows rather than moving or removing physical files.

    Example:
        >>> import inspect
        >>> inspect.isabstract(MetadataSQLAPI)
        True
    """

    @abc.abstractmethod
    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Attach a concrete metadata SQL helper to a database host.

        Example:
            metadata_sql = MetadataSQL(db)


        :param db: Database exposing get, driver_wrapper, driver connections, macros and row operations.
        :return: None; the concrete MetadataSQL constructor stores db only.
        """

    @property
    @abc.abstractmethod
    def get(self):
        """
        Expose the database host’s query-result helper.

        Reading the property executes no SQL. Query arguments and returned row/scalar shape belong to the host helper.

        Example:
            rows = db.metadata_sql.get("SELECT tag_id FROM tags")


        :return: Bound db.get callable in MetadataSQL.
        """

        ...

    @property
    @abc.abstractmethod
    def execute(self):
        """
        Expose the driver-wrapper executor for one SQL statement.

        The current SQL driver returns a query cursor despite legacy None annotations on wrapper signatures. Its connection context and driver refresh behavior apply to delegated operations.

        Example:
            rows = db.metadata_sql.execute("SELECT tag_id FROM tags")


        :return: Bound db.driver_wrapper.execute callable in MetadataSQL.
        """

        ...

    @property
    @abc.abstractmethod
    def executemany(self):
        """
        Expose the driver-wrapper executor for repeated SQL bindings.

        Parameter-shape compatibility and transaction handling belong to the wrapper/driver. This property does not itself run statements.

        Example:
            db.metadata_sql.executemany("UPDATE tags SET tag=? WHERE tag_id=?", [("History", 7)])


        :return: Bound db.driver_wrapper.executemany callable in MetadataSQL.
        """

        ...

    @abc.abstractmethod
    def add_creator_tag_link(self, *args, **kwargs):
        """
        Insert a legacy creator/tag association.

        Concrete call: add_creator_tag_link(creator_id, tag_id).

        Write directly through db.driver.conn without an explicit commit or duplicate check.

        Example:
            db.metadata_sql.add_creator_tag_link(creator_id, tag_id)


        :param args: creator_id, tag_id: IDs bound to the two endpoint columns.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def add_feed(self, *args, **kwargs):
        """
        Insert a feed title and script.

        Concrete call: add_feed(title: str, script: str).

        Delegate to execute; no feed ID is returned and no duplicate check is performed.

        Example:
            db.metadata_sql.add_feed("Daily", script)


        :param args: title, script: Text stored in feed_title and feed_script.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def add_series_tag_link(self, *args, **kwargs):
        """
        Insert a legacy series/tag association.

        Concrete call: add_series_tag_link(series_id, tag_id).

        Write directly through db.driver.conn without an explicit commit or duplicate check.

        Example:
            db.metadata_sql.add_series_tag_link(series_id, tag_id)


        :param args: series_id, tag_id: IDs bound to the series and tag endpoint columns.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def add_tag(self, *args, **kwargs):
        """
        Resolve or create a tag through portable value policy.

        Concrete call: add_tag(tag_value).

        Use the tags/tag policy-aware macro rather than inserting directly into the table.

        Example:
            tag_id = db.metadata_sql.add_tag("History")


        :param args: tag_value: Display value passed unchanged to macros.ensure_table_value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Resolved or inserted tag row ID.
        """

        ...

    @abc.abstractmethod
    def add_tag_title_link(self, *args, **kwargs):
        """
        Insert a legacy tag/title association.

        Concrete call: add_tag_title_link(title_id, tag_id).

        Write directly through db.driver.conn without an explicit commit or duplicate check.

        Example:
            db.metadata_sql.add_tag_title_link(title_id, tag_id)


        :param args: title_id, tag_id: IDs bound in title-then-tag order.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def add_title_identifier(self, *args, **kwargs):
        """
        Create a legacy identifier row and link it to a title.

        Concrete call: add_title_identifier(title_id, id_type, id_val).

        Resolve the title, create/sync an identifiers row, then interlink with type=id_type. It does not deduplicate identifiers or wrap creation and linking in one transaction. This is separate from set_title_identifier’s Work/entity_identifiers path.

        Example:
            db.metadata_sql.add_title_identifier(title_id, "doi", "10.1000/example")


        :param args: title_id, id_type, id_val: Existing title ID, identifier type and stored value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def break_creator_tag_link(self, *args, **kwargs):
        """
        Delete a specific legacy creator/tag association.

        Concrete call: break_creator_tag_link(tag_id, creator_id).

        Delete matching link rows through the raw driver connection without an explicit commit.

        Example:
            db.metadata_sql.break_creator_tag_link(tag_id, creator_id)


        :param args: tag_id, creator_id: Endpoint IDs, in tag-then-creator order.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def break_creator_title_links(self, *args, **kwargs):
        """
        Delete selected creator-role links for one or more titles.

        Concrete call: break_creator_title_links(title_id, creator_type=('author', 'authors')).

        Creator roles are interpolated into an SQL IN expression using the supplied object representation; use trusted SQL-compatible tuple content. Integer IDs use execute, while an iterable becomes one-item executemany bindings. Batch failures are logged and wrapped as DatabaseDriverError.

        Example:
            db.metadata_sql.break_creator_title_links([1, 2])


        :param args: title_id: Integer ID or iterable of IDs; creator_type: Role tuple, defaulting to author/authors.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        :raises DatabaseDriverError: The batch deletion path fails, or the delegated driver reports an execution error.
        """

        ...

    @abc.abstractmethod
    def break_generic_link(self, *args, **kwargs):
        """
        Delete link rows by one endpoint, optionally restricted to one type.

        Concrete call: break_generic_link(link_table, link_col, remove_id, link_type=None).

        With no type filter, non-integer input passes directly to executemany. A type filter derives the conventional type column and supports only an integer ID. There is no automatic wrapping of an arbitrary ID iterable here.

        Example:
            db.metadata_sql.break_generic_link(link_table, owner_column, [(1,), (2,)])


        :param args: link_table, link_col: Trusted identifiers; remove_id: Integer ID or batch bindings; link_type: Optional type filter.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        :raises NotImplementedError: A type filter is combined with non-integer remove_id.
        """

        ...

    @abc.abstractmethod
    def break_generic_single_link(self, *args, **kwargs):
        """
        Delete link rows matching both endpoint values.

        Concrete call: break_generic_single_link(link_table, left_link_col, right_link_col, left_id, right_id).

        Example:
            db.metadata_sql.break_generic_single_link(link_table, left_column, right_column, 1, 10)


        :param args: link_table, left_link_col, right_link_col: Trusted identifiers; left_id, right_id: Bound endpoint values.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def break_lang_title_links(self, *args, **kwargs):
        """
        Delete legacy language links for a title and optional role.

        Concrete call: break_lang_title_links(title_id, link_type=None).

        The title ID is bound, but a non-None link_type is interpolated into quoted SQL and must be trusted. This does not add a replacement language link.

        Example:
            db.metadata_sql.break_lang_title_links(title_id, "primary")


        :param args: title_id: Title ID; link_type: Optional role name, with None selecting all roles.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def break_lang_title_primary_link(self, *args, **kwargs):
        """
        Delete primary-language links for selected titles.

        Concrete call: break_lang_title_primary_link(title_id).

        Use the literal primary role. Integer IDs are forwarded as scalar execute arguments; other inputs pass directly to executemany. Binding conversion depends on the host wrapper.

        Example:
            db.metadata_sql.break_lang_title_primary_link(title_id)


        :param args: title_id: Integer scalar or batch parameter bindings for title IDs.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def break_series_title_link(self, *args, **kwargs):
        """
        Delete matching legacy series/title links.

        Concrete call: break_series_title_link(title_id: int, series_id: int).

        Issue one driver-wrapper deletion without selecting by priority or index.

        Example:
            db.metadata_sql.break_series_title_link(title_id, series_id)


        :param args: title_id, series_id: IDs identifying the title and series pair.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def break_tag_title_link(self, *args, **kwargs):
        """
        Delete matching legacy tag/title links.

        Concrete call: break_tag_title_link(tag_id, title_id).

        Use the raw driver connection without an explicit commit.

        Example:
            db.metadata_sql.break_tag_title_link(tag_id, title_id)


        :param args: tag_id, title_id: IDs in tag-then-title order.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def check_for_creator_tag_link(self, *args, **kwargs):
        """
        Read the creator scalar for a matching legacy creator/tag link.

        Concrete call: check_for_creator_tag_link(creator_id, tag_id).

        Example:
            exists = db.metadata_sql.check_for_creator_tag_link(creator_id, tag_id) is not None


        :param args: creator_id, tag_id: Endpoint IDs to match.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Matching creator ID or None from connection.get(all=False); no bool conversion.
        """

        ...

    @abc.abstractmethod
    def check_for_series_tag_link(self, *args, **kwargs):
        """
        Read the series scalar for a matching legacy series/tag link.

        Concrete call: check_for_series_tag_link(series_id, tag_id).

        Example:
            exists = db.metadata_sql.check_for_series_tag_link(series_id, tag_id) is not None


        :param args: series_id, tag_id: Endpoint IDs to match.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Matching series ID or None from connection.get(all=False); no bool conversion.
        """

        ...

    @abc.abstractmethod
    def check_for_series_title_link(self, *args, **kwargs):
        """
        Read the highest-priority link ID and index for a series/title pair.

        Concrete call: check_for_series_title_link(series_id: int, title_id: int).

        Use the project connection.get_row helper and order by descending priority.

        Example:
            link = db.metadata_sql.check_for_series_title_link(series_id, title_id)


        :param args: series_id, title_id: Endpoint IDs in series-then-title order.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: A (link_id, series_index) row or None, despite the concrete bool annotation.
        """

        ...

    @abc.abstractmethod
    def check_for_tag_title_link(self, *args, **kwargs):
        """
        Read the title scalar for a matching legacy tag/title link.

        Concrete call: check_for_tag_title_link(title_id, tag_id).

        Example:
            exists = db.metadata_sql.check_for_tag_title_link(title_id, tag_id) is not None


        :param args: title_id, tag_id: Endpoint IDs in title-then-tag order.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Matching title ID or None; no bool conversion.
        """

        ...

    @abc.abstractmethod
    def check_for_title_author_link(self, *args, **kwargs):
        """
        Read a matching title/creator link with the exact authors role.

        Concrete call: check_for_title_author_link(title_id: int, creator_id: int).

        The singular author role is not included, and no explicit ordering chooses among duplicate matches.

        Example:
            link_id = db.metadata_sql.check_for_title_author_link(title_id, creator_id)


        :param args: title_id, creator_id: Endpoint IDs to match.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: First matching link ID or None, despite the concrete bool annotation.
        """

        ...

    @abc.abstractmethod
    def check_for_title_id_publisher_id_link(self, *args, **kwargs):
        """
        Read the highest-priority legacy publisher/title link ID.

        Concrete call: check_for_title_id_publisher_id_link(pub_id, title_id).

        Example:
            link_id = db.metadata_sql.check_for_title_id_publisher_id_link(publisher_id, title_id)


        :param args: pub_id, title_id: Publisher and title IDs, in that order.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Matching link ID or None from connection.get(all=False).
        """

        ...

    @abc.abstractmethod
    def clear_creator_tag_links_for_creator(self, *args, **kwargs):
        """
        Delete every legacy tag link belonging to one creator.

        Concrete call: clear_creator_tag_links_for_creator(creator_id).

        Use the raw driver connection without an explicit commit; creator and tag rows remain.

        Example:
            db.metadata_sql.clear_creator_tag_links_for_creator(creator_id)


        :param args: creator_id: Creator endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def clear_null_publisher_links_from_title(self, *args, **kwargs):
        """
        Delete publisher-zero placeholder links for one title.

        Concrete call: clear_null_publisher_links_from_title(title_id).

        Match publisher ID 0 rather than SQL NULL; other publisher links remain.

        Example:
            db.metadata_sql.clear_null_publisher_links_from_title(title_id)


        :param args: title_id: Title endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def clear_publisher_title_links_by_title_id(self, *args, **kwargs):
        """
        Delete all legacy publisher links for one title.

        Concrete call: clear_publisher_title_links_by_title_id(title_id).

        Delete links through driver_wrapper without deleting publisher rows.

        Example:
            db.metadata_sql.clear_publisher_title_links_by_title_id(title_id)


        :param args: title_id: Title endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def clear_series_tag_links_for_series(self, *args, **kwargs):
        """
        Delete every legacy tag link belonging to one series.

        Concrete call: clear_series_tag_links_for_series(series_id).

        Use the raw connection without an explicit commit; series and tag rows remain.

        Example:
            db.metadata_sql.clear_series_tag_links_for_series(series_id)


        :param args: series_id: Series endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def clear_tag_title_links_for_title(self, *args, **kwargs):
        """
        Delete every legacy tag link belonging to one title.

        Concrete call: clear_tag_title_links_for_title(title_id).

        Use the raw connection without an explicit commit; tag rows remain.

        Example:
            db.metadata_sql.clear_tag_title_links_for_title(title_id)


        :param args: title_id: Title endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def clear_title_comments_from_title_id(self, *args, **kwargs):
        """
        Delete legacy comment/title links for one title.

        Concrete call: clear_title_comments_from_title_id(title_id).

        Delete from comment_title_links, not comments, through driver_wrapper.

        Example:
            db.metadata_sql.clear_title_comments_from_title(title_id)


        :param args: title_id: Title endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def clear_title_creator_links_for_given_type_and_title(self, *args, **kwargs):
        """
        Delete and commit a title’s links with the fixed authors role.

        Concrete call: clear_title_creator_links_for_given_type_and_title(title_id: str).

        Despite the name, the role is hard-coded to authors. The raw driver connection is explicitly committed.

        Example:
            db.metadata_sql.clear_title_creator_links_for_given_type_and_title(title_id)


        :param args: title_id: Title ID; the concrete method accepts no separate type argument.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def creator_clear_unused(self, *args, **kwargs):
        """
        Delete creators absent from legacy creator/title links.

        Concrete call: creator_clear_unused().

        Use NOT IN over creator_title_links only; other relations do not protect a creator here. NULLs in that subquery follow SQL NOT IN semantics. Execution delegates to the host helper.

        Example:
            db.metadata_sql.creator_clear_unused()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_book(self, *args, **kwargs):
        """
        Delete a legacy books row by book_id.

        Concrete call: delete_book(book_id).

        This SQL helper neither resolves title relationships nor removes physical files itself; schema cascades remain database-defined.

        Example:
            db.metadata_sql.delete_book(book_id)


        :param args: book_id: Book row ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_conversion_options(self, *args, **kwargs):
        """
        Delete one book/format conversion-options record and optionally commit.

        Concrete call: delete_conversion_options(book_id, fmt, commit=True).

        Delete using the raw connection. commit=False leaves final transaction ownership to the caller.

        Example:
            db.metadata_sql.delete_conversion_options(book_id, "epub", commit=False)


        :param args: book_id: Book ID; fmt: Text format uppercased for matching; commit: Whether to commit, default True.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_feed(self, *args, **kwargs):
        """
        Delete feed rows by integer ID or supplied batch bindings.

        Concrete call: delete_feed(feed_id).

        Non-integer input is forwarded directly; this method does not wrap a flat ID list.

        Example:
            db.metadata_sql.delete_feed([(1,), (2,)])


        :param args: feed_id: Integer scalar or iterable of one-item executemany parameter sequences.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_file_by_id(self, *args, **kwargs):
        """
        Delete one files metadata row by ID.

        Concrete call: delete_file_by_id(file_id).

        This performs a database DELETE through execute and does not unlink a physical file.

        Example:
            db.metadata_sql.delete_file_by_id(file_id)


        :param args: file_id: Stored file row ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_files_by_id(self, *args, **kwargs):
        """
        Delete files metadata rows using batch parameter bindings.

        Concrete call: delete_files_by_id(file_ids).

        Forward bindings unchanged to executemany; physical files are not deleted here.

        Example:
            db.metadata_sql.delete_files_by_id([(1,), (2,)])


        :param args: file_ids: Iterable of one-item bindings, such as [(1,), (2,)].
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_item_by_id(self, *args, **kwargs):
        """
        Delete rows in a trusted table by scalar ID or batch bindings.

        Concrete call: delete_item_by_id(item_table, item_id_col, item_id).

        Identifiers are interpolated. An integer becomes one bound value; non-integer input passes directly to executemany.

        Example:
            db.metadata_sql.delete_item_by_id("tags", "tag_id", [(1,), (2,)])


        :param args: item_table, item_id_col: Trusted table/ID-column names; item_id: Integer scalar or executemany bindings.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_tag_by_value(self, *args, **kwargs):
        """
        Delete the canonical tag row resolved from a display value.

        Concrete call: delete_tag_by_value(tag).

        Return quietly when no identity resolves. Otherwise delete that tag ID through the raw connection and commit. Link effects depend on database constraints/triggers.

        Example:
            db.metadata_sql.delete_tag_by_value("History")


        :param args: tag: Value passed to db.get_canonical_identity for tags/tag.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_title(self, *args, **kwargs):
        """
        Delete a legacy title and the book row with the same numeric ID.

        Concrete call: delete_title(title_id).

        Issue two execute calls. This assumes equal title/book IDs rather than traversing a relation, and provides no enclosing transaction or filesystem deletion.

        Example:
            db.metadata_sql.delete_title(title_id)


        :param args: title_id: ID used for both titles.title_id and books.book_id.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def delete_title_identifiers(self, *args, **kwargs):
        """
        Delete legacy identifier rows linked to a title, optionally filtered by type.

        Concrete call: delete_title_identifiers(title_id, id_type=None).

        Delete identifier rows themselves, not just link rows. Shared identifiers can therefore affect other owners depending on schema cascades. The unfiltered branch forwards title_id as a scalar binding; the filtered branch supplies a tuple. This is separate from Work/entity_identifiers ownership.

        Example:
            db.metadata_sql.delete_title_identifiers(title_id, id_type="doi")


        :param args: title_id: Title owner ID; id_type: Optional identifiers.identifier_type filter.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def get_creator_link(self, *args, **kwargs):
        """
        Read one creator’s stored external link value.

        Concrete call: get_creator_link(creator_id).

        Use db.get and catch IndexError only; other query/result-shape failures propagate.

        Example:
            link = db.metadata_sql.get_creator_link(creator_id)


        :param args: creator_id: ID matched in creators.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: First creator_link value, or None when the result/first row is empty.
        """

        ...

    @abc.abstractmethod
    def get_creator_sort(self, *args, **kwargs):
        """
        Read one creator’s stored sort value.

        Concrete call: get_creator_sort(creator_id).

        Use db.get and catch IndexError only; no sort value is generated.

        Example:
            sort_value = db.metadata_sql.get_creator_sort(creator_id)


        :param args: creator_id: ID matched in creators.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: First creator_sort value, or None when the result/first row is empty.
        """

        ...

    @abc.abstractmethod
    def get_primary_series_index(self, *args, **kwargs):
        """
        Read the index from a title’s highest-priority legacy series link.

        Concrete call: get_primary_series_index(title_id: int).

        Order all matching links by priority descending; there is no type filter.

        Example:
            index = db.metadata_sql.get_primary_series_index(title_id)


        :param args: title_id: Title endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: First series_title_link_index scalar or None; stored values may be floats despite the concrete Optional[int] annotation.
        """

        ...

    @abc.abstractmethod
    def get_series_id_from_value(self, *args, **kwargs):
        """
        Resolve the first legacy series ID by stored text equality.

        Concrete call: get_series_id_from_value(series: str).

        No explicit ordering or policy-aware normalization is applied here.

        Example:
            series_id = db.metadata_sql.get_series_id_from_value("Collected Works")


        :param args: series: Value matched against series.series.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: First matching series ID or None, despite the concrete int annotation.
        """

        ...

    @abc.abstractmethod
    def get_tag_id_from_value(self, *args, **kwargs):
        """
        Resolve a tag ID through its canonical identity policy.

        Concrete call: get_tag_id_from_value(tag).

        No tag row is inserted.

        Example:
            tag_id = db.metadata_sql.get_tag_id_from_value("History")


        :param args: tag: Display value passed to db.get_canonical_identity.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Resolved canonical tag row ID or None.
        """

        ...

    @abc.abstractmethod
    def get_title_series_ids_set(self, *args, **kwargs):
        """
        Collect distinct legacy series IDs linked to one title.

        Concrete call: get_title_series_ids_set(title_id).

        Example:
            series_ids = db.metadata_sql.get_title_series_ids_set(title_id)


        :param args: title_id: Title endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Set of retrieved series IDs, without filtering placeholder zero values.
        """

        ...

    @abc.abstractmethod
    def library_unset_series(self, *args, **kwargs):
        """
        Delete one title/series pair’s legacy link rows.

        Concrete call: library_unset_series(title_id, series_id).

        No replacement null-series link is inserted and the series row itself is retained.

        Example:
            db.metadata_sql.library_unset_series(title_id, series_id)


        :param args: title_id, series_id: IDs of the endpoint pair to unlink.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def link_null_series_to_title(self, *args, **kwargs):
        """
        Attempt to insert a series-zero placeholder link for a title.

        Concrete call: link_null_series_to_title(title_id: int, series_index: Optional[Union[int, float]]).

        Compute priority as MAX over the entire link table plus one, not just this title. Empty-table MAX is not coalesced and may yield NULL. Suppress DatabaseDriverError from the insert; callers receive no success indicator.

        Example:
            db.metadata_sql.link_null_series_to_title(title_id, 1.0)


        :param args: title_id: Title ID; series_index: Index stored on the new link.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def link_publisher_to_null_publisher_row(self, *args, **kwargs):
        """
        Attempt to insert a publisher-zero placeholder link for a title.

        Concrete call: link_publisher_to_null_publisher_row(title_id).

        Use the entire publisher link table’s MAX priority plus one, without a zero fallback for an empty table. Suppress DatabaseDriverError and return no success indicator.

        Example:
            db.metadata_sql.link_publisher_to_null_publisher_row(title_id)


        :param args: title_id: Title endpoint ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def make_creator_title_links(self, *args, **kwargs):
        """
        Insert legacy title/creator pairs with the fixed authors role.

        Concrete call: make_creator_title_links(title_id=None, creator_id=None, id_pairs=None, creator_type='authors').

        Always insert type=authors and use the whole link table’s MIN priority minus one, without an empty-table fallback. Batch pairs pass directly to executemany. The advertised creator_type does not alter SQL.

        Example:
            db.metadata_sql.make_creator_title_links(id_pairs=[(title_id, creator_id)])


        :param args: title_id, creator_id: Single pair when id_pairs is None; id_pairs: Optional pair bindings; creator_type: Retained but ignored argument.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def publisher_clear_unused(self, *args, **kwargs):
        """
        Delete publishers absent from legacy publisher/title links.

        Concrete call: publisher_clear_unused().

        Only publisher_title_links protects a publisher here. Use SQL NOT IN semantics, including their behavior for NULL subquery values; other relations are not inspected.

        Example:
            db.metadata_sql.publisher_clear_unused()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def read_all_identifiers(self, *args, **kwargs):
        """
        Read legacy title ownership, link type and identifier text for every linked identifier.

        Concrete call: read_all_identifiers().

        Join identifier_title_links with identifiers and order by descending link priority. This reads legacy ownership, not entity_identifiers.

        Example:
            rows = db.metadata_sql.read_all_identifiers()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (title_id, link_type, identifier) rows.
        """

        ...

    @abc.abstractmethod
    def read_book_id_with_cover_id_and_cover_nmame(self, *args, **kwargs):
        """
        Read every directly linked book/cover pair in priority order.

        Concrete call: read_book_id_with_cover_id_and_cover_nmame().

        Retain the historical nmame spelling. Inner joins through book_cover_links omit books without linked covers; multiple covers can produce multiple rows.

        Example:
            rows = db.metadata_sql.read_book_id_with_cover_id_and_cover_nmame()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (book_id, cover_id, cover_name) rows.
        """

        ...

    @abc.abstractmethod
    def read_book_id_with_file_id_file_ext_file_name_and_file_size(self, *args, **kwargs):
        """
        Read directly linked book/file metadata in descending link priority.

        Concrete call: read_book_id_with_file_id_file_ext_file_name_and_file_size().

        Inner joins through book_file_links omit unlinked books/files and do not inspect physical file contents.

        Example:
            rows = db.metadata_sql.read_book_id_with_file_id_file_ext_file_name_and_file_size()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (book_id, file_id, file_extension, file_name, file_size) rows.
        """

        ...

    @abc.abstractmethod
    def read_book_sizes_max_mode(self, *args, **kwargs):
        """
        Read each book’s maximum of stored file sizes reached through folders.

        Concrete call: read_book_sizes_max_mode().

        Resolve file IDs through book_folder_links and file_folder_links, independently of direct book_file_links. SQL IN deduplicates matching file IDs. Stored file_size values are aggregated without reading files, and SQL returns NULL when there are no non-NULL sizes. No result ordering is specified.

        Example:
            rows = db.metadata_sql.read_book_sizes_max_mode()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (book_id, maximum_size) rows; the aggregate may be None.
        """

        ...

    @abc.abstractmethod
    def read_book_sizes_min_mode(self, *args, **kwargs):
        """
        Read each book’s minimum of stored file sizes reached through folders.

        Concrete call: read_book_sizes_min_mode().

        Resolve file IDs through book_folder_links and file_folder_links, independently of direct book_file_links. SQL IN deduplicates matching file IDs. Stored file_size values are aggregated without reading files, and SQL returns NULL when there are no non-NULL sizes. No result ordering is specified.

        Example:
            rows = db.metadata_sql.read_book_sizes_min_mode()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (book_id, minimum_size) rows; the aggregate may be None.
        """

        ...

    @abc.abstractmethod
    def read_book_sizes_sum_mode(self, *args, **kwargs):
        """
        Read each book’s sum of stored file sizes reached through folders.

        Concrete call: read_book_sizes_sum_mode().

        Resolve file IDs through book_folder_links and file_folder_links, independently of direct book_file_links. SQL IN deduplicates matching file IDs. Stored file_size values are aggregated without reading files, and SQL returns NULL when there are no non-NULL sizes. No result ordering is specified.

        Example:
            rows = db.metadata_sql.read_book_sizes_sum_mode()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (book_id, sum_size) rows; the aggregate may be None.
        """

        ...

    @abc.abstractmethod
    def read_creator_with_sort_and_link(self, *args, **kwargs):
        """
        Read stored creator names together with IDs, sort values and links.

        Concrete call: read_creator_with_sort_and_link().

        Select all creators without ordering or regeneration of derived values.

        Example:
            rows = db.metadata_sql.read_creator_with_sort_and_link()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (creator_id, creator, creator_sort, creator_link) rows.
        """

        ...

    @abc.abstractmethod
    def read_file_backups_for_book(self, *args, **kwargs):
        """
        Read file-intralink endpoint pairs for files directly linked to a book.

        Concrete call: read_file_backups_for_book(book_id).

        Join through book_file_links and match files as primary intralink endpoints. No backup-type or reverse-direction filter is applied, despite the method name. Order by descending book/file link priority.

        Example:
            rows = db.metadata_sql.read_file_backups_for_book(book_id)


        :param args: book_id: Book endpoint ID, forwarded as a scalar execution binding.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (primary_file_id, secondary_file_id) pairs.
        """

        ...

    @abc.abstractmethod
    def read_file_properties_for_book(self, *args, **kwargs):
        """
        Read metadata for files directly linked to one book.

        Concrete call: read_file_properties_for_book(book_id).

        Join through book_file_links and order by its descending priority. Do not read physical files or traverse folder links.

        Example:
            rows = db.metadata_sql.read_file_properties_for_book(book_id)


        :param args: book_id: Book endpoint ID, forwarded as a scalar execution binding.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: Execute result yielding (file_id, file_extension, file_name, file_size) rows.
        """

        ...

    @abc.abstractmethod
    def read_primary_title_series_id_from_meta(self, *args, **kwargs):
        """
        Read the series_id projection from the legacy meta view.

        Concrete call: read_primary_title_series_id_from_meta(title_id: int).

        Requires an existing meta view exposing series_id; this helper neither builds the view nor derives priority itself.

        Example:
            series_id = db.metadata_sql.read_primary_title_series_id_from_meta(title_id)


        :param args: title_id: ID matched against meta.id.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: First series_id scalar or None from connection.get(all=False).
        """

        ...

    @abc.abstractmethod
    def remove_unused_series(self, *args, **kwargs):
        """
        Delete series with no legacy series/title link and commit.

        Concrete call: remove_unused_series().

        Inspect each series ID with a separate link query, delete unreferenced IDs and commit the raw connection once at the end. Other relation tables do not protect a series, and placeholder ID zero is not specially exempted.

        Example:
            db.metadata_sql.remove_unused_series()


        :param args: No positional arguments; the concrete method is called without arguments.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def replace_in_cover_path(self, *args, **kwargs):
        """
        Replace literal text throughout stored cover_path values.

        Concrete call: replace_in_cover_path(target_str, replacement).

        Apply SQL replace to every covers row through execute. This is neither path-prefix validation nor filesystem relocation; occurrences anywhere in the stored string are eligible.

        Example:
            db.metadata_sql.replace_in_cover_path("/old/root", "/new/root")


        :param args: target_str: Substring to replace; replacement: New substring.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def replace_in_file_path(self, *args, **kwargs):
        """
        Replace literal text throughout stored file_path values.

        Concrete call: replace_in_file_path(target_str, replacement).

        Apply SQL replace to every files row through execute. This is neither path-prefix validation nor filesystem relocation; occurrences anywhere in the stored string are eligible.

        Example:
            db.metadata_sql.replace_in_file_path("/old/root", "/new/root")


        :param args: target_str: Substring to replace; replacement: New substring.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def replace_in_folder_path(self, *args, **kwargs):
        """
        Replace literal text throughout stored folder_path values.

        Concrete call: replace_in_folder_path(target_str, replacement).

        Apply SQL replace to every folders row through execute. This is neither path-prefix validation nor filesystem relocation; occurrences anywhere in the stored string are eligible.

        Example:
            db.metadata_sql.replace_in_folder_path("/old/root", "/new/root")


        :param args: target_str: Substring to replace; replacement: New substring.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def replace_in_folder_store_marker_path(self, *args, **kwargs):
        """
        Replace literal text throughout stored folder_store_marker_path values.

        Concrete call: replace_in_folder_store_marker_path(target_str: str, replacement: str).

        Apply SQL replace to every folder_stores row through execute. This is neither path-prefix validation nor filesystem relocation; occurrences anywhere in the stored string are eligible.

        Example:
            db.metadata_sql.replace_in_folder_store_marker_path("/old/root", "/new/root")


        :param args: target_str: Substring to replace; replacement: New substring.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def replace_in_folder_store_path(self, *args, **kwargs):
        """
        Replace literal text throughout stored folder_store_path values.

        Concrete call: replace_in_folder_store_path(target_str: str, replacement: str).

        Apply SQL replace to every folder_stores row through execute. This is neither path-prefix validation nor filesystem relocation; occurrences anywhere in the stored string are eligible.

        Example:
            db.metadata_sql.replace_in_folder_store_path("/old/root", "/new/root")


        :param args: target_str: Substring to replace; replacement: New substring.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_author_sort(self, *args, **kwargs):
        """
        Write and commit a title’s stored creator-sort text.

        Concrete call: set_author_sort(title_id, sort).

        Use the raw connection, without deriving the text from creator links.

        Example:
            db.metadata_sql.set_author_sort(title_id, "Example, Ada")


        :param args: title_id: Title row ID; sort: Replacement title_creator_sort value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_conversion_options(self, *args, **kwargs):
        """
        Expose the legacy conversion-option serialization and upsert path.

        Concrete call: set_conversion_options(book_id, fmt, options).

        The concrete mixin references sqlite and cPickle without importing either, so normal execution fails before SQL. The remaining body would serialize a binary pickle, update the first existing book/format record or insert one, then commit. This documentation does not certify that unfinished path as operational.

        Example:
            db.metadata_sql.set_conversion_options(book_id, "epub", options)  # Currently raises NameError.


        :param args: book_id: Book ID; fmt: Format text intended to be uppercased; options: Object intended for pickle serialization.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        :raises NameError: The current concrete implementation lacks its sqlite/cPickle globals.
        """

        ...

    @abc.abstractmethod
    def set_feeds(self, *args, **kwargs):
        """
        Replace the entire feed table from title/script pairs and commit.

        Concrete call: set_feeds(feeds).

        Delete every existing feed before consuming the iterable. Insert pairs through the raw connection, then commit; there is no enclosing rollback wrapper if iteration or insertion fails.

        Example:
            db.metadata_sql.set_feeds([("Daily", script)])


        :param args: feeds: Iterable of (title, script) pairs.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_file_name(self, *args, **kwargs):
        """
        Update a files row’s stored name.

        Concrete call: set_file_name(file_id, new_fname).

        Delegate to execute; no physical rename or path update occurs.

        Example:
            db.metadata_sql.set_file_name(file_id, "book.epub")


        :param args: file_id: File row ID; new_fname: Replacement file_name value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_file_size(self, *args, **kwargs):
        """
        Update a files row’s stored size.

        Concrete call: set_file_size(file_id, size).

        No filesystem measurement or range validation is performed here.

        Example:
            db.metadata_sql.set_file_size(file_id, 2048)


        :param args: file_id: File row ID; size: Replacement file_size value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_file_size_and_name(self, *args, **kwargs):
        """
        Update stored file size and name in one statement.

        Concrete call: set_file_size_and_name(file_id, size, fname).

        Only the database metadata changes; no file is moved, renamed or measured.

        Example:
            db.metadata_sql.set_file_size_and_name(file_id, 2048, "book.epub")


        :param args: file_id: File row ID; size: Replacement size; fname: Replacement name.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_has_cover(self, *args, **kwargs):
        """
        Set and commit the book’s stored cover flag.

        Concrete call: set_has_cover(book_id, value).

        No bool conversion, cover existence check or cover-link mutation is performed here.

        Example:
            db.metadata_sql.set_has_cover(book_id, True)


        :param args: book_id: Book row ID; value: Value bound directly to book_has_cover.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_override_book_path(self, *args, **kwargs):
        """
        Replace the legacy book_paths value for one book.

        Concrete call: set_override_book_path(book_id, path).

        Delegate to execute without validating or creating a filesystem path.

        Example:
            db.metadata_sql.set_override_book_path(book_id, new_path)


        :param args: book_id: Book row ID; path: Replacement stored path value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_title_identifier(self, *args, **kwargs):
        """
        Replace the primary Work identifier for a scheme while retaining prior values.

        Concrete call: set_title_identifier(title_id, id_type, id_val).

        Use portable macros.transaction and require an existing works row. Query exact scheme matches, demote all matching rows, then reuse the first exact value match or insert a new primary identifier. A false value deletes matched scheme rows. For the isbn alias (casefolded with hyphens/underscores removed), digit/X count chooses isbn10 or isbn13; deletion checks both compact and underscored spellings. Scheme/value text is otherwise preserved, with no checksum validation or canonical value normalization. Promotion affects only the queried scheme set, not every ISBN family.

        Example:
            db.metadata_sql.set_title_identifier(work_id, "doi", "10.1000/example")


        :param args: title_id: Existing Work ID; id_type: Nonblank string scheme; id_val: String value or any false value to delete matches.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        :raises TypeError: id_type is not a nonblank string, or a truthy id_val is not a string.
        :raises ValueError: The isbn alias value has neither 10 nor 13 digit/X characters.
        :raises DatabaseIntegrityError: The target Work does not exist.
        """

        ...

    @abc.abstractmethod
    def set_title_isbn(self, *args, **kwargs):
        """
        Set or clear the Work ISBN through the identifier compatibility path.

        Concrete call: set_title_isbn(title_id, isbn).

        Delegate to set_title_identifier with id_type=isbn. Preserve the supplied value text; its compact digit/X count selects the scheme without checking the checksum. Clearing removes compact and underscored ISBN-10/13 schemes. Other identifier schemes are untouched.

        Example:
            db.metadata_sql.set_title_isbn(work_id, "978-0-306-40615-7")


        :param args: title_id: Existing Work ID; isbn: String value, or a false value to remove ISBN-scheme rows.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_title_primary_language(self, *args, **kwargs):
        """
        Replace a title’s legacy primary-language link with an existing language row.

        Concrete call: set_title_primary_language(title_id, lang_id).

        Delete primary links first, then resolve title/language rows and interlink at highest priority. On DatabaseIntegrityError, delete every link for that endpoint pair and retry once as primary. There is no outer transaction, so a failed lookup or retry can leave prior links removed.

        Example:
            db.metadata_sql.set_title_primary_language(title_id, language_id)


        :param args: title_id: Existing title ID; lang_id: Existing language row ID.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def set_title_rating(self, *args, **kwargs):
        """
        Replace legacy user-rating links using a rating-plus-one row-ID convention.

        Concrete call: set_title_rating(title_id, rating).

        Delete existing user links before converting the new rating. A false value returns without an explicit commit. A truthy value selects rating row int(rating)+1, inserts a user link and commits. No rating range validation or rating-row creation occurs; invalid input can fail after deletion.

        Example:
            db.metadata_sql.set_title_rating(title_id, 8)


        :param args: title_id: Title ID; rating: False value to clear, otherwise converted with int and incremented.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def unapply_series_tags(self, *args, **kwargs):
        """
        Remove legacy series/tag pairs resolved from exact tag values.

        Concrete call: unapply_series_tags(series_id, tags).

        Look up each tag via connection.get and skip false IDs, then delete its pair. Lookup does not use canonical identity policy. The concrete implementation calls commit twice after the loop.

        Example:
            db.metadata_sql.unapply_series_tags(series_id, ["History"])


        :param args: series_id: Series endpoint ID; tags: Iterable of stored tag values to resolve.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def update_book_last_modified(self, *args, **kwargs):
        """
        Write and commit the supplied last-modified value for a book.

        Concrete call: update_book_last_modified(book_id: int, last_modified: str).

        The helper does not generate the current time or parse the timestamp.

        Example:
            db.metadata_sql.update_book_last_modified(book_id, timestamp)


        :param args: book_id: Converted with int; last_modified: Replacement stored timestamp value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def update_creator_links(self, *args, **kwargs):
        """
        Replace creator external-link values from batch bindings.

        Concrete call: update_creator_links(values).

        Pass bindings unchanged to executemany; this does not update creator/title relations.

        Example:
            db.metadata_sql.update_creator_links([("https://example.org/author", creator_id)])


        :param args: values: Iterable of (new_creator_link, creator_id) pairs.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def update_creator_sorts(self, *args, **kwargs):
        """
        Replace stored creator-sort values from batch bindings.

        Concrete call: update_creator_sorts(values).

        Pass bindings unchanged to executemany without deriving sort text.

        Example:
            db.metadata_sql.update_creator_sorts([("Example, Ada", creator_id)])


        :param args: values: Iterable of (new_creator_sort, creator_id) pairs.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def update_feed(self, *args, **kwargs):
        """
        Update feed title and script in two statements, then commit.

        Concrete call: update_feed(feed_id, script, title).

        Write title first, then script, through the raw connection. There is no explicit rollback wrapper or row-existence check.

        Example:
            db.metadata_sql.update_feed(feed_id, script, "Daily")


        :param args: feed_id: Feed row ID; script: Replacement script; title: Replacement title.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def update_index_for_series_title_link(self, *args, **kwargs):
        """
        Set and commit the index for matching legacy series/title links.

        Concrete call: update_index_for_series_title_link(title_id: int, series_id: int, index: Optional[Union[int, float]]).

        Update every link for the endpoint pair. Although the concrete annotation permits None, float(None) raises TypeError before SQL; the method cannot clear the index with None.

        Example:
            db.metadata_sql.update_index_for_series_title_link(title_id, series_id, 2.5)


        :param args: title_id, series_id: Endpoint IDs; index: Value converted with float before execution.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        :raises TypeError: index cannot be converted with float, including None.
        :raises ValueError: Text index is not a valid float.
        """

        ...

    @abc.abstractmethod
    def update_title(self, *args, **kwargs):
        """
        Replace a legacy title value, storing SQL NULL for false input.

        Concrete call: update_title(title_id, title).

        Delegate to execute. This does not automatically update title sort fields or other metadata.

        Example:
            db.metadata_sql.update_title(title_id, "New title")


        :param args: title_id: Title row ID; title: Replacement value, with any false value treated as missing.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def update_title_author_link_priority(self, *args, **kwargs):
        """
        Set and commit priority on a title/creator pair with the exact authors role.

        Concrete call: update_title_author_link_priority(title_id: int, creator_id: int, new_priority: int).

        The singular author role is not matched. No priority allocation, reordering of other links or uniqueness repair occurs here.

        Example:
            db.metadata_sql.update_title_author_link_priority(title_id, creator_id, 10)


        :param args: title_id, creator_id: Endpoint IDs; new_priority: Replacement priority bound unchanged.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...

    @abc.abstractmethod
    def update_title_creator_sort(self, *args, **kwargs):
        """
        Replace a title’s stored creator-sort field through the host executor.

        Concrete call: update_title_creator_sort(title_id, creator_val).

        Do not derive the sort text from creator links. Unlike set_author_sort, this method delegates to execute instead of committing the raw connection itself.

        Example:
            db.metadata_sql.update_title_creator_sort(title_id, "Example, Ada")


        :param args: title_id: Title row ID; creator_val: Replacement title_creator_sort value.
        :param kwargs: Named arguments from the concrete signature below; no additional keywords are accepted.
        :return: None; no row count or inserted identifier is returned.
        """

        ...
