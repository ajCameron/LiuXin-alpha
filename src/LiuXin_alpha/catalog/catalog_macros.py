
"""
Frozen reference helpers for the pre-facade catalog write path.

Existing compatibility consumers require legacy tables and helper attributes.
New Catalog code must use repositories, coordinated mutations or normalized
writers. Retain these direct-SQL operations as reference material without new
callers or features. Helpers do not open a transaction or commit themselves;
atomicity and commit policy belong to the caller or delegated operation.
Multi-step helpers can leave partial effects when a later operation fails.
"""
from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from copy import deepcopy

from typing import TYPE_CHECKING, Union, Iterable, Optional

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text import isbytestring

# Todo: Wrap these up in a "catalog_macros" class.

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI


def library_set_title(db: "CatalogAPI", title_id: int, title: str) -> None:
    """
    Forward a legacy title change to metadata_sql.update_title.

    Example:
        A title update delegates validation and any related book-row changes to update_title.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param title: Title value passed unchanged to the SQL helper.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.update_title(title_id=title_id, title=title)


def library_add_feed(db: "CatalogAPI", title: Union[bytes, str], script: Union[bytes, str]) -> None:
    """
    Decode byte feed values as UTF-8 before inserting the feed.

    Decoding failures propagate before insertion; nonbytes values pass through unchanged.

    Example:
        UTF-8 bytes for the title and script become strings before add_feed is called.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title: Feed title as text or UTF-8 bytes.
    :param script: Feed script as text or UTF-8 bytes.
    :return: None; delegated return values are discarded and failures propagate.
    """
    if isbytestring(title):
        title = title.decode("utf-8")
    if isbytestring(script):
        script = script.decode("utf-8")
    db.metadata_sql.add_feed(title, script)


def library_remove_feeds(db: "CatalogAPI", ids: set[int]) -> None:
    """
    Forward feed IDs to the legacy feed deletion helper.

    Example:
        An empty ID set is forwarded too; the SQL helper determines its effect.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param ids: Feed IDs to delete; not copied or validated here.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.delete_feed(ids)


def library_unapply_series_tags(db: "CatalogAPI", series_id: int, tags: Iterable[str]):
    """
    Delegate removal of named tags from one Series.

    Example:
        Removing Series tags leaves matching and missing-tag policy to metadata_sql.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param series_id: Legacy Series ID.
    :param tags: Tag texts consumed by the delegated helper.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.unapply_series_tags(series_id, tags)


def library_update_feed(db: "CatalogAPI", feed_id: int, script: Union[bytes, str], title: str) -> None:
    """
    Forward a feed replacement script and title without decoding.

    Example:
        Unlike feed insertion, this wrapper passes a byte script directly to update_feed.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param feed_id: Existing feed ID.
    :param script: Replacement script passed unchanged, including bytes.
    :param title: Replacement title passed unchanged.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.update_feed(feed_id, script, title)


def library_set_feeds(db: "CatalogAPI", feeds: Iterable[Union[bytes, str]]) -> None:
    """
    Delegate complete replacement of the legacy feed collection.

    Example:
        Provide title/script pairs to replace feeds; this wrapper does not validate their shape.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param feeds: Iterable of title/script pairs expected by set_feeds; the annotation is narrower.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.set_feeds(feeds)


def library_set_author_sort(db: "CatalogAPI", title_id, sort):
    """
    Forward the legacy title author-sort value.

    Example:
        Pass the already prepared sort string to the SQL helper; no normalization occurs here.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param sort: Replacement author-sort value.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.set_author_sort(title_id, sort)


# Todo: I guess this would be in items now? Does it still exist?
def library_set_cover(db: "CatalogAPI", book_id: int, value: bool) -> None:
    """
    Forward the legacy book cover-presence flag.

    Example:
        Setting the flag does not create or inspect a cover file in this wrapper.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param book_id: Legacy book ID.
    :param value: Cover-presence value passed to set_has_cover.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.set_has_cover(book_id, value)


def library_remove_unused_series(db: "CatalogAPI") -> None:
    """
    Delegate removal of Series considered unused by the SQL layer.

    Example:
        The SQL helper decides which unlinked Series rows qualify for removal.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.remove_unused_series()


# Todo: Prrroobably an expressions level thing?
# Todo: We need a policy, written down on item boundaries
#       Things to consider
#       - Is each format of a book it's own item? (depends what you mean by format)
#       - Is an auto-generated conversion of a file in an item still in the item (probably yes)
#       - Is an annotated copy of an file still in the same item (erggh. Technically no.)
#       -
# Todo: I've never been clear why this isn a book level thing anyways - or what these options are
# Todo: Formats can definitely be typed fully
# Todo: If this means, really, conversion policy, then we should be able to set it at multiple levels
def library_set_conversion_options(db: "CatalogAPI", book_id: int, fmt: str, options):
    """
    Delegate storage of per-book, per-format conversion options.

    Example:
        A caller may store EPUB options under its book ID; this wrapper does not serialize them.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param book_id: Legacy book ID.
    :param fmt: Format key passed unchanged.
    :param options: Options object; serialization belongs to the SQL helper.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.set_conversion_options(book_id=book_id, fmt=fmt, options=options)


def library_delete_conversion_options(db: "CatalogAPI", book_id: int, fmt: str, commit: bool = True) -> None:
    """
    Forward conversion-option deletion and its commit preference.

    Example:
        Use ``commit=False`` when the delegated operation must participate in caller-owned work.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param book_id: Legacy book ID.
    :param fmt: Format key to delete.
    :param commit: Commit flag forwarded as the third SQL-helper argument.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.delete_conversion_options(book_id, fmt, commit)


# Todo: Sensible, but there are, again, multiple levels this could be applied to.
def library_set_isbn(db: "CatalogAPI", title_id: int, isbn: str) -> bool:
    """
    Delegate the ISBN update and return its result.

    Example:
        The result of set_title_isbn is returned unchanged.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param isbn: ISBN text; normalization belongs to the SQL helper.
    :return: Boolean result reported by metadata_sql.set_title_isbn.
    """
    return db.metadata_sql.set_title_isbn(title_id, isbn)


def library_set_publisher(
        db: "CatalogAPI",
        title_id: int,
        publisher: Optional[str] = None,
        publisher_id: Optional[int] = None) -> tuple[Optional[int], Optional[str]]:
    """
    Promote, create or clear legacy title-publisher relationships.

    A list is deep-copied and processed in reverse. The returned pair describes
    the first processed entry (the original last ID), while later recursive calls
    may change priority again. A truthy scalar ID wins over the name. Otherwise
    ensure.publisher is called without standardization. Existing links are moved
    above the global maximum priority; new links are interlinked, then null links
    are cleared. Failures may leave earlier operations applied.

    Example:
        An empty publisher-ID list clears publisher links; absent name and ID instead link the null publisher.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param publisher: Publisher name used only when no truthy publisher ID is supplied.
    :param publisher_id: Publisher ID; also accepts a list at runtime despite the annotation.
    :return: Publisher ID/name pair, or (None, None) for clearing/null selection.
    """
    if isinstance(publisher_id, list):
        publisher_id = deepcopy(publisher_id)
        publisher_id.reverse()
        pub_pairs = []
        for pub_id in publisher_id:
            pub_pairs.append(library_set_publisher(db=db, title_id=title_id, publisher_id=pub_id))

        try:
            return pub_pairs[0]
        except IndexError:
            # Todo: Spin this off into a delete method - which is where it should be being handled
            db.metadata_sql.clear_publisher_title_links_by_title_id(title_id)
            return None, None

    # Check to see if there is already a link between the publisher and the title
    # If there is one, then update that link to make it primary
    # If there isn't one then create the link as primary
    if publisher or publisher_id:

        # Check to see if there is already a link to the publisher in the stack - if there is then pop it to the
        # top of the stack - otherwise add it
        pub_row = None
        if publisher_id:
            pub_id = publisher_id
        else:
            try:
                pub_row = db.ensure.publisher(publisher=publisher, standardize=False)
            except AttributeError:
                err_str = (
                    "AttributeError while called ensure - be sure that the database has had the metadata helper"
                    "functions declared for use"
                )
                err_str = default_log.log_variables(err_str, "ERROR", ("type(db)", type(db)))
                raise AttributeError(err_str)

            pub_id = pub_row["publisher_id"]

        pt_id = db.metadata_sql.check_for_title_id_publisher_id_link(pub_id=pub_id, title_id=title_id)

        if pt_id:

            pub_row = pub_row if pub_row is not None else db.get_row_from_id("publishers", pub_id)

            pt_link_row = db.get_row_from_id("publisher_title_links", pt_id)
            # Set the priority to maximum
            pt_link_row["publisher_title_link_priority"] = db.get_max("publisher_title_link_priority") + 1
            pt_link_row.sync()

        else:

            pub_row = pub_row if pub_row is not None else db.get_row_from_id("publishers", pub_id)

            title_row = db.get_row_from_id(table="titles", row_id=title_id)
            db.interlink_rows(primary_row=title_row, secondary_row=pub_row)

        # Ensure that there isn't a reference to the null publisher anywhere in the stack
        db.metadata_sql.clear_null_publisher_links_from_title(title_id)

        return pub_row["publisher_id"], pub_row["publisher"]

    else:

        # Nullify the publisher - by linking it to the null pub row
        db.metadata_sql.link_publisher_to_null_publisher_row(title_id)

        return None, None


# Todo: Again, we have a problem re. where this comment should be set by default
# Todo: Probably the answer is items. By default, I think the answer is items
def library_set_comment(db: "CatalogAPI", title_id: int, text: Optional[str]) -> Optional[int]:
    """
    Add and link a truthy comment, or clear comments for a title.

    Nonempty text is passed to add.comment, then interlinked to the title;
    the wrapper reads comment_id from the returned row and does not remove older
    comments in that branch. It relies on the legacy comment-row contract.

    Example:
        An empty string clears existing title comments without creating a comment row.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param text: Comment text; any false-valued input requests clearing.
    :return: Created comment_id, or None after clearing.
    """
    if text:
        comment_row = db.add.comment(text)
        title_row = db.get_row_from_id(table="titles", row_id=title_id)
        db.interlink_rows(primary_row=title_row, secondary_row=comment_row)
        return comment_row["comment_id"]
    else:
        db.metadata_sql.clear_title_comments_from_title_id(title_id)
        return None


# Todo: This should probably be a bool - as the tag might not match
def library_delete_tag(db: "CatalogAPI", tag: str) -> None:
    """
    Delegate deletion of a tag by its stored value.

    Example:
        Use the stored spelling when calling this exact-value deletion wrapper.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param tag: Tag text, passed without lowercasing or stripping.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.delete_tag_by_value(tag)


def library_delete_tags(db: "CatalogAPI", tags: Iterable[str]) -> None:
    """
    Delete tag values one at a time in iterable order.

    Example:
        If a later deletion fails, earlier deletions are not rolled back by this helper.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param tags: Tag texts; duplicates are not removed.
    :return: None; delegated return values are discarded and failures propagate.
    """
    for tag in tags:
        library_delete_tag(db, tag)


def library_unapply_tags(db: "CatalogAPI", book_id: int, tags: Iterable[str]) -> set[int]:
    """
    Resolve tag values and remove truthy-ID links from a title.

    Example:
        An unknown tag still contributes its lookup result (often None) to the returned set.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param book_id: Legacy title/book ID.
    :param tags: Exact tag texts to resolve; no normalization is applied.
    :return: Set of lookup results, including false-valued misses despite the int annotation.
    """
    tag_ids = set()
    for tag in tags:
        tag_id = db.metadata_sql.get_tag_id_from_value(tag)
        if tag_id:
            db.metadata_sql.break_tag_title_link(tag_id=tag_id, title_id=book_id)
        tag_ids.add(tag_id)
    return tag_ids


def library_unapply_creator_tags(db: "CatalogAPI", creator_id: int, tags: Iterable[str]) -> None:
    """
    Resolve exact tag values and unlink truthy matches from a Creator.

    Example:
        Unknown tag values cause no unlink; the locally collected IDs are not returned.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param creator_id: Legacy Creator ID.
    :param tags: Exact tag texts to resolve.
    :return: None; delegated return values are discarded and failures propagate.
    """
    tag_ids = set()
    for tag in tags:
        tag_id = db.metadata_sql.get_tag_id_from_value(tag)
        if tag_id:
            db.metadata_sql.break_creator_tag_link(tag_id, creator_id)
        tag_ids.add(tag_id)


def library_unapply_title_tags(db: "CatalogAPI", book_id: int, tags: Iterable[str]) -> set[int]:
    """
    Apply the title-named alias of library_unapply_tags.

    Example:
        The alias preserves false-valued lookup results in the returned set.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param book_id: Legacy title/book ID.
    :param tags: Exact tag texts to resolve.
    :return: Set returned unchanged by library_unapply_tags, including lookup misses.
    """
    return library_unapply_tags(db, book_id, tags)


def library_set_tags(db: "CatalogAPI", title_id: int, tags: Iterable[str], append: bool = False) -> set[int]:
    """
    Replace or append normalized tag links for one title.

    Tags are lowercased and stripped; blank results are ignored. Reuse exact
    stored values or create missing tags, then add only absent links. Set iteration
    does not preserve caller order. Clearing occurs before iteration/validation,
    so a bad tag may leave a partial replacement.

    Example:
        With append=False, even an empty tag iterable clears existing links.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param tags: Iterable of tag strings; raw values are deduplicated before normalization.
    :param append: True retains existing links; false clears them before consuming tags.
    :return: Set of retained or created tag IDs.
    """
    # If not append - clear all the tags linked to the book/title out - then run the add as normal
    if not append:
        db.metadata_sql.clear_tag_title_links_for_title(title_id)

    tag_ids = set()

    # Add the given tags
    for tag in set(tags):
        tag = tag.lower().strip()
        if not tag:
            continue
        t = db.metadata_sql.get_tag_id_from_value(tag)
        # Todo: Need to replace this with some species of ensure tag
        if t:
            tid = t
        else:
            tid = db.metadata_sql.add_tag(tag)

        if not db.metadata_sql.check_for_tag_title_link(title_id, tid):
            db.metadata_sql.add_tag_title_link(title_id, tid)

        tag_ids.add(tid)
    return tag_ids


def library_set_creator_tags(db: "CatalogAPI", creator_id: int, tags: Iterable[str], append: bool = False) -> None:
    """
    Replace or append normalized tag links for one Creator.

    Tags are lowercased and stripped; blank results are ignored. Reuse exact
    stored values or create missing tags, then add only absent links. Set iteration
    does not preserve caller order. Clearing occurs before iteration/validation,
    so a bad tag may leave a partial replacement.

    Example:
        With append=False, even an empty tag iterable clears existing links.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param creator_id: Legacy Creator ID.
    :param tags: Iterable of tag strings; raw values are deduplicated before normalization.
    :param append: True retains existing links; false clears them before consuming tags.
    :return: None; delegated return values are discarded and failures propagate.
    """
    if not append:
        db.metadata_sql.clear_creator_tag_links_for_creator(creator_id)

    # Add back the tags
    for tag in set(tags):
        tag = tag.lower().strip()
        if not tag:
            continue
        t = db.metadata_sql.get_tag_id_from_value(tag)
        if t:
            tid = t
        else:
            tid = db.metadata_sql.add_tag(tag)

        if not db.metadata_sql.check_for_creator_tag_link(creator_id, tid):
            db.metadata_sql.add_creator_tag_link(creator_id=creator_id, tag_id=tid)



def library_set_series_tags(db: "CatalogAPI", series_id: int, tags: Iterable[str], append: bool = False) -> None:
    """
    Replace or append normalized tag links for one Series.

    Tags are lowercased and stripped; blank results are ignored. Reuse exact
    stored values or create missing tags, then add only absent links. Set iteration
    does not preserve caller order. Clearing occurs before iteration/validation,
    so a bad tag may leave a partial replacement.

    Example:
        With append=False, even an empty tag iterable clears existing links.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param series_id: Legacy Series ID.
    :param tags: Iterable of tag strings; raw values are deduplicated before normalization.
    :param append: True retains existing links; false clears them before consuming tags.
    :return: None; delegated return values are discarded and failures propagate.
    """
    if not append:
        db.metadata_sql.clear_series_tag_links_for_series(series_id)

    # Add back the tags
    for tag in set(tags):
        tag = tag.lower().strip()
        if not tag:
            continue
        t = db.metadata_sql.get_tag_id_from_value(tag)
        if t:
            tid = t
        else:
            tid = db.metadata_sql.add_tag(tag)

        if not db.metadata_sql.check_for_series_tag_link(series_id=series_id, tag_id=tid):
            db.metadata_sql.add_series_tag_link(series_id, tid)



def library_set_title_tags(db: "CatalogAPI", title_id: int, tags: Iterable[str], append: bool = False) -> set[int]:
    """
    Replace or append normalized tag links for one title.

    Tags are lowercased and stripped; blank results are ignored. Reuse exact
    stored values or create missing tags, then add only absent links. Set iteration
    does not preserve caller order. Clearing occurs before iteration/validation,
    so a bad tag may leave a partial replacement.

    Example:
        With append=False, even an empty tag iterable clears existing links.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param tags: Iterable of tag strings; raw values are deduplicated before normalization.
    :param append: True retains existing links; false clears them before consuming tags.
    :return: Set of retained or created tag IDs.
    """
    return library_set_tags(db, title_id, tags, append=append)


# Todo: How do we determine what series an item is in? So we can unset it.
def library_unset_series(
        db: "CatalogAPI",
        title_id: int,
        series: Optional[Union[int, str]] = None,
        series_id: int = None) -> None:
    """
    Resolve an optional Series selector before delegating unlinking.

    Integer selectors, including bool, pass through as IDs. A name lookup may
    return None; the SQL helper determines the resulting unlink scope. No local
    existence check is performed.

    Example:
        Conflicting resolved and explicit IDs raise ValueError before the unlink helper is called.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param series: Series ID or exact name; None uses series_id directly.
    :param series_id: Explicit Series ID, which must agree with a supplied series selector.
    :return: None; delegated return values are discarded and failures propagate.
    """
    if series is not None:
        resolved_id = (
            series
            if isinstance(series, int)
            else db.metadata_sql.get_series_id_from_value(series)
        )
        if series_id is not None and resolved_id != series_id:
            raise ValueError("series and series_id identify different rows")
        series_id = resolved_id
    db.metadata_sql.library_unset_series(title_id=title_id, series_id=series_id)


def library_set_series(
    db: "CatalogAPI",
    title_id: int,
    series: Optional[Union[int, str]] = None,
    series_id: Optional[int] = None,
    update_cache_series=None,
    update_cache_series_idx=None,
) -> tuple[None, None]:
    """
    Promote or create a legacy title-Series link while preserving its index.

    Existing links gain a priority above the global maximum. New links inherit
    the title's primary Series index; missing names are ensured without
    standardization. Successful non-null selection removes the null Series link.
    With neither selector, link the null Series while preserving the index.
    Callbacks run after their associated writes and may fail after mutations;
    the ID branch tests the index callback for truthiness, the name branch for
    non-None. No enclosing transaction is opened.

    Example:
        When both a name and ID are given, the name branch replaces series_id with its lookup result.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param series: Any non-None value is looked up as a Series name, even integers.
    :param series_id: Used only when series is None; a supplied name takes precedence.
    :param update_cache_series: Optional callback invoked last with title_id and the original series argument.
    :param update_cache_series_idx: Optional callback for an existing link, receiving title_id and series_idx.
    :return: Always (None, None) after successful writes and callbacks.
    """
    # If there is already a link between the title and the series then promote it to the highest priority
    # If there is no link then create it
    # If the series to update is None then set the series to null and continue
    if series is not None:

        title_row = db.get_row_from_id(table="titles", row_id=title_id)
        series_id = db.metadata_sql.get_series_id_from_value(series)

        if series_id:
            series_row = db.get_row_from_id(table="series", row_id=series_id)

            # Check to see if there is already a link which will need updating
            st_status = db.metadata_sql.check_for_series_title_link(series_id, title_id)

            # Link exists and has to be updated
            if st_status:
                series_title_link_id, series_title_link_index = st_status
                # Retrieve the row to update
                st_link_row = db.get_row_from_id("series_title_links", series_title_link_id)
                # Set the priority to maximum
                st_link_row["series_title_link_priority"] = db.get_max("series_title_link_priority") + 1
                # Transfer the index across
                st_link_row["series_title_link_index"] = series_title_link_index
                st_link_row.sync()

                # Set the index in the cache to be the new index
                if update_cache_series_idx is not None:
                    update_cache_series_idx(title_id=title_id, series_idx=series_title_link_index)

            # Link doesn't exist and has to be created
            else:

                # Retrieve the index to copy across
                st_index = db.metadata_sql.get_primary_series_index(title_id)

                db.interlink_rows(primary_row=title_row, secondary_row=series_row, index=st_index)

        else:
            # Make the series row that will be associated with the title
            series_row = db.ensure.series_blind(creator_rows=[], series_name=series, stand=False)

            # Retrieve the index to copy across
            st_index = db.metadata_sql.get_primary_series_index(title_id=title_id)

            # Create the new row with the index
            # Todo: Might be nice to set where the series came from - a source column
            db.interlink_rows(primary_row=title_row, secondary_row=series_row, index=st_index)

        # Ensure that there isn't a reference to the null series elsewhere in the stack
        db.metadata_sql.break_series_title_link(title_id=title_id, series_id=0)

    elif series_id is not None:

        series_row = db.get_row_from_id(table="series", row_id=series_id)
        # Check to see if there is already a link for updating
        st_status = db.metadata_sql.check_for_series_title_link(series_id=series_id, title_id=title_id)

        # Link exists and has to be updated
        if st_status:

            series_title_link_id, series_title_link_index = st_status
            # Retrieve the row to update
            st_link_row = db.get_row_from_id("series_title_links", series_title_link_id)
            # Set the priority to maximum
            st_link_row["series_title_link_priority"] = db.get_max("series_title_link_priority") + 1
            # Transfer the index across
            st_link_row["series_title_link_index"] = series_title_link_index
            st_link_row.sync()

            # Set the index in the cache to be the new index
            if update_cache_series_idx:
                update_cache_series_idx(title_id=title_id, series_idx=series_title_link_index)

        # Link doesn't exist and has to be created
        else:

            # Retrieve the index to copy across
            st_index = db.metadata_sql.get_primary_series_index(title_id=title_id)

            title_row = db.get_row_from_id("titles", title_id)

            # Todo: source="user_set" would be nice - if true
            db.interlink_rows(primary_row=title_row, secondary_row=series_row, index=st_index)

        # Ensure that there isn't a reference to the null series elsewhere in the stack
        db.metadata_sql.break_series_title_link(title_id=title_id, series_id=0)

    else:

        # Check to see if there is already a link to any series - if there is then use the index from that link
        # so that it's preserved in the top entry of the stack - statement will return None if there isn't - which
        # is fine
        series_index = db.metadata_sql.get_primary_series_index(title_id)

        # Nullify the series - by linking it to the null series row
        db.metadata_sql.link_null_series_to_title(title_id=title_id, series_index=series_index)

        # Series index is not changed - so doesn't have to be updated in the cache

    # Todo: This should not happen here - instead should propogate back and be taken care of in the cache
    if update_cache_series is not None:
        update_cache_series(title_id=title_id, series=series)

    return None, None


def library_set_series_index(
        db: "CatalogAPI",
        title_id: int,
        idx: Optional[Union[float, int]],
        series_id = None,
        update_cache_series_idx = None) -> None:
    """
    Resolve a title's Series and update its link index.

    If resolution yields None, create a null-Series link with the index instead.
    A callback failure occurs after the SQL mutation and is not rolled back here.

    Example:
        A callable resolver receives ``(title_id, index_is_id=True)``.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param idx: Replacement numeric index or None, passed unchanged.
    :param series_id: Explicit Series ID, callable resolver, or None for primary-ID lookup.
    :param update_cache_series_idx: Optional callback invoked positionally with title_id and idx after writing.
    :return: None; delegated return values are discarded and failures propagate.
    """
    # Get the id of the series currently linked to the given book
    if callable(series_id):
        series_id = series_id(title_id, index_is_id=True)
    elif series_id is None:
        series_id = db.metadata_sql.read_primary_title_series_id_from_meta(title_id)

    if series_id is not None:
        # Update the link's index
        db.metadata_sql.update_index_for_series_title_link(title_id, series_id, idx)
    else:
        # No links where found - insert a link to the null series including the index information
        db.metadata_sql.link_null_series_to_title(title_id, idx)

    if update_cache_series_idx is not None:
        update_cache_series_idx(title_id, idx)


def library_set_last_modified(
        db: "CatalogAPI",
        book_id: int,
        last_modified) -> None:
    """
    Forward a legacy book last-modified value.

    Example:
        No clock is read here; the caller supplies the replacement timestamp.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param book_id: Legacy book ID.
    :param last_modified: Timestamp value passed unchanged; validation belongs to metadata_sql.
    :return: None; delegated return values are discarded and failures propagate.
    """
    db.metadata_sql.update_book_last_modified(book_id=book_id, last_modified=last_modified)


def library_set_authors_from_ids(
        db: "CatalogAPI",
        title_id: int,
        author_ids: Union[list[int], tuple[int]],
        append: bool = False) -> None:
    """
    Replace or append ordered legacy author links for a title.

    Append starts below the global minimum creator-title priority and decrements
    for each supplied ID, including existing links. Replacement clears before
    constructing/inserting rows; append may partially apply. Neither path validates
    all authors upfront or opens an enclosing transaction.

    Example:
        Replacement assigns descending priorities from len(author_ids)+1.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param author_ids: Sized ordered author-ID sequence; duplicates are retained.
    :param append: False clears author links and bulk-inserts; true creates or reprioritizes each link.
    :return: None; delegated return values are discarded and failures propagate.
    """
    # If not append then clear the author type creator links to the book and add the new set back in
    if not append:
        db.metadata_sql.clear_title_creator_links_for_given_type_and_title(title_id)

        priority = len(author_ids) + 1
        link_row_dicts = []
        for author_id in author_ids:
            link_row_dict = {
                "creator_title_link_creator_id": author_id,
                "creator_title_link_title_id": title_id,
                "creator_title_link_type": "authors",
                "creator_title_link_priority": priority,
            }
            priority -= 1
            link_row_dicts.append(link_row_dict)

        db.driver.direct_add_multiple_simple_row_dicts(link_row_dicts)
        return

    # If there are links already present, then place them in order - if not just add them
    title_row = db.get_row_from_id("titles", title_id)

    ct_link_priority = db.get_min("creator_title_link_priority") - 1
    for author_id in author_ids:

        ct_link_id = db.metadata_sql.check_for_title_author_link(title_id=title_id, creator_id=author_id)

        # If there is no link then create one
        if ct_link_id is None:
            author_row = db.get_row_from_id("creators", author_id)
            db.interlink_rows(
                primary_row=title_row,
                secondary_row=author_row,
                priority=ct_link_priority,
                type="authors",
            )
        # If there is a link then update it's priority
        else:
            db.metadata_sql.update_title_author_link_priority(
                title_id=title_id, creator_id=author_id, new_priority=ct_link_priority
            )

        ct_link_priority -= 1


def library_set_language(db: "CatalogAPI", title_id: int, lang_string: str) -> None:
    """
    Ensure a language from a name/code and set it as primary.

    Example:
        The ensured row's language_id is forwarded to set_title_primary_language.


    :param db: Legacy Catalog/database facade supplying metadata_sql and any row helpers used here.
    :param title_id: Legacy title ID.
    :param lang_string: Language input accepted by ensure.language with lang_code="either".
    :return: None; delegated return values are discarded and failures propagate.
    """
    lang_row = db.ensure.language(lang_string, lang_code="either")
    lang_id = lang_row["language_id"]

    db.metadata_sql.set_title_primary_language(title_id, lang_id)
