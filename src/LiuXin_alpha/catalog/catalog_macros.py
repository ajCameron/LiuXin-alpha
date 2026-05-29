
"""
Simplified front ends for the write system which should be (mostly) used instead of actual writers.

Macros are common functions which can best be expressed in pure SQL.
(Either in principle, but currently there's a shim, or in practice).
Catalog macros are aware of metadata, and thus operate at a higher level than database macros.
"""
from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from copy import deepcopy

from typing import TYPE_CHECKING, Union, Iterable, Optional

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text import isbytestring

# Todo: Wrap these up in a "catalog_macros" class.

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


def library_set_title(db: "DatabaseAPI", title_id: int, title: str) -> None:
    """
    Set the title of the work - updates both the title table and the books table.

    If you attempt to set the title to something with evaluates as False the attempted update will be ignored.
    :param db: The database to preform the set in
    :param title_id: The id the title to update the title for
    :param title: The title string to set the title too
    :return:
    """
    db.macros.update_title(title_id=title_id, title=title)


def library_add_feed(db: "DatabaseAPI", title: Union[bytes, str], script: Union[bytes, str]) -> None:
    """
    Add to the field table - assume that the title and script is encoded in utf-8.

    :param db: The database to do the update on
    :param title: The title of the feed
    :param script: The script to fetch the feed
    :return:
    """
    if isbytestring(title):
        title = title.decode("utf-8")
    if isbytestring(script):
        script = script.decode("utf-8")
    db.macros.add_feed(title, script)


def library_remove_feeds(db: "DatabaseAPI", ids: set[int]) -> None:
    """
    Remove feeds from the feeds table.

    :param db: The database to do the update on
    :param ids: The ids of the feeds to remove
    :return:
    """
    db.macros.delete_feed(ids)


def library_unapply_series_tags(db: "DatabaseAPI", series_id: int, tags: Iterable[str]):
    """
    Remove every tag in the given iterator of tags from the given series with the given series_id.

    If the tag is not linked to the series no change is made.
    :param db: The database to preform the changes to
    :param series_id: The id of the seris to remove the tags from
    :param tags: Text of the tags to remove from the series
    :return:
    """
    db.macros.unapply_series_tags(series_id, tags)


def library_update_feed(db: "DatabaseAPI", feed_id: int, script: Union[bytes, str], title: str) -> None:
    """
    Update the feed table with a new script and title.

    :param db: The database to do the update on
    :param feed_id: Ids from the feed table
    :param script: The script to update the table with
    :param title: The title of the feed
    :return:
    """
    db.macros.update_feed(feed_id, script, title)


def library_set_feeds(db: "DatabaseAPI", feeds: Iterable[Union[bytes, str]]) -> None:
    """
    Clears the entire feed table and updates it with entirely new feeds.

    :param db:
    :param feeds: An iterable of tuples - title, script. These will be set as the new feed_title, feed_script fields
                  of the feeds table. The feeds table will be cleared otherwise.
    :return:
    """
    db.macros.set_feeds(feeds)


def library_set_author_sort(db: "DatabaseAPI", title_id, sort):
    """
    Sets the author sort field for the given book/title id.

    :param db:
    :param title_id:
    :param sort:
    :return:
    """
    db.macros.set_author_sort(title_id, sort)


# Todo: I guess this would be in items now? Does it still exist?
def library_set_cover(db: "DatabaseAPI", book_id: int, value: bool) -> None:
    """
    Update the flag stored in the books table - in book_has_cover

    :param db:
    :param book_id:
    :param value:
    :return:
    """
    db.macros.set_has_cover(book_id, value)


def library_remove_unused_series(db: "DatabaseAPI") -> None:
    """
    Remove series that are not currently in use from the specified database.

    "in use" means series linked to works.
    :param db:
    :return:
    """
    db.macros.remove_unused_series()


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
def library_set_conversion_options(db: "DatabaseAPI", book_id: int, fmt: str, options):
    """
    Sets a conversion option for a book.

    :param db: The database to preform the update on
    :param book_id: The id of the book to set the conversion option for (not the id of the entry in the conversion
                    option table)
    :param fmt: Format to update the conversion option for
    :param options: This wil be stored as a CPickle.dump in the conversion_option_data column
    :return:
    """
    db.macros.set_conversion_options(book_id=book_id, fmt=fmt, options=options)


def library_delete_conversion_options(db: "DatabaseAPI", book_id: int, fmt: str, commit: bool = True) -> None:
    """
    Remove a conversion option for a given format from a given id

    :param db: The database to preform the update on
    :param book_id: The id of the book to remove the conversion option from
    :param fmt: The format to remove the conversion option for
    :param commit: Commit the change once it's been made
    :return:
    """
    db.macros.delete_conversion_options(book_id, fmt, commit)


# Todo: Sensible, but there are, again, multiple levels this could be applied to.
def library_set_isbn(db: "DatabaseAPI", title_id: int, isbn: str) -> bool:
    """
    Set an isbn in the identifiers table.

    :param db: The database to preform the update on
    :param title_id: The id of the book to update.
    :param isbn: The isbn of the book to update.
    :return
    """
    return db.macros.set_title_isbn(title_id, isbn)


def library_set_publisher(
        db: "DatabaseAPI",
        title_id: int,
        publisher: Optional[str] = None,
        publisher_id: Optional[int] = None) -> tuple[Optional[int], Optional[str]]:
    """
    Changes the primary publisher of the title to be the given publisher.

    If the publisher row is None, then the book_publisher column will be set None.
    :param db: The database to preform the update on
    :param title_id: The id of the book row to set the publisher for
    :param publisher: The publisher string to set - the publisher will be trivially matched to a row in the
                      publisher row
    :param publisher_id: If provided, will preform the link to the publisher represented by this id, rather than the
                         one named in the :param publisher: string.
                         publisher_id will take precedence over publisher if both are provided.

    :return:
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
            db.macros.clear_publisher_title_links_by_title_id(title_id)
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

        pt_id = db.macros.check_for_title_id_publisher_id_link(pub_id=pub_id, title_id=title_id)

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
        db.macros.clear_null_publisher_links_from_title(title_id)

        return pub_row["publisher_id"], pub_row["publisher"]

    else:

        # Nullify the publisher - by linking it to the null pub row
        db.macros.link_publisher_to_null_publisher_row(title_id)

        return None, None


# Todo: Again, we have a problem re. where this comment should be set by default
# Todo: Probably the answer is items. By default, I think the answer is items
def library_set_comment(db: "DatabaseAPI", title_id: int, text: Optional[str]) -> Optional[int]:
    """
    Set the primary comment/note on a title (and thus on a book) to be this text.

    Multiple comments can be set for a title - this just sets the primary comment.
    Note - comments are a type of note - so the text will be stored in the notes table and linked to the title with
    the link type "comment"
    :param db: The database to preform the update on
    :param title_id: The id of the title/book to deal with.
    :param text: The text of the comment to set.
    :return: Optional new comment id
    """
    if text:
        comment_row = db.add.comment(text)
        title_row = db.get_row_from_id(table="titles", row_id=title_id)
        db.interlink_rows(primary_row=title_row, secondary_row=comment_row)
        return comment_row["comment_id"]
    else:
        db.macros.clear_title_comments_from_title_id(title_id)
        return None


# Todo: This should probably be a bool - as the tag might not match
def library_delete_tag(db: "DatabaseAPI", tag: str) -> None:
    """
    Delete a tag from the tag text.

    :param db:
    :param tag:
    :return:
    """
    db.macros.delete_tag_by_value(tag)


def library_delete_tags(db: "DatabaseAPI", tags: Iterable[str]) -> None:
    """
    Delete every tag from an iterable of tags.

    No update is made to the cache - presumably this is handled at a higher level.
    :param db: The database to preform the delete on
    :param tags: An iterable of tag texts to be deleted.
    :return:
    """
    for tag in tags:
        library_delete_tag(db, tag)


def library_unapply_tags(db: "DatabaseAPI", book_id: int, tags: Iterable[str]) -> set[int]:
    """
    Remove every tag in the given tags from the given book_id.

    If the tag is not linked to the book no change is made.
    :param db: The database to apply the changes to
    :param book_id: The id of the book/title to remove the tags from
    :param tags: An iterable of the exact text of each of the tags to remove.
    :return:
    """
    tag_ids = set()
    for tag in tags:
        tag_id = db.macros.get_tag_id_from_value(tag)
        if tag_id:
            db.macros.break_tag_title_link(tag_id=tag_id, title_id=book_id)
        tag_ids.add(tag_id)
    db.driver.conn.commit()
    return tag_ids


def library_unapply_creator_tags(db: "DatabaseAPI", creator_id: int, tags: Iterable[str]) -> None:
    """
    Remove every tag in the given iterator of tags from the given creator with the given creator_id.

    If the tag is not linked to the creator no change is made.
    :param db: The database to preform the update in
    :param creator_id:
    :param tags:
    :return:
    """
    tag_ids = set()
    for tag in tags:
        tag_id = db.macros.get_tag_id_from_value(tag)
        if tag_id:
            db.macros.break_creator_tag_link(tag_id, creator_id)
        tag_ids.add(tag_id)
    db.driver.conn.commit()


def library_unapply_title_tags(db: "DatabaseAPI", book_id: int, tags: Iterable[str]) -> set[int]:
    """
    Remove every tag in the given tags from the given book_id.

    If the tag is not linked to the book no change is made.
    :param db:
    :param book_id: The id of the book/title to remove the tags from
    :param tags: An iterable of the exact text of each of the tags to remove.
    :return:
    """
    return library_unapply_tags(db, book_id, tags)


def library_set_tags(db: "DatabaseAPI", title_id: int, tags: Iterable[str], append: bool = False) -> set[int]:
    """
    Append or replace the given iterable of tag texts for the given book/title id.

    Use the set_creator_tags to set tags for a creator of the work, and set_series_tags to set tags for the series
    the title is in.
    tags are matched on their exact text.
    Use ensure_tags to create tags if needed.
    :param db: The database to do the update on
    :param title_id:
    :param tags: list of strings
    :param append: If True existing tags are not removed
    :return:
    """
    # If not append - clear all the tags linked to the book/title out - then run the add as normal
    if not append:
        db.macros.clear_tag_title_links_for_title(title_id)

    tag_ids = set()

    # Add the given tags
    for tag in set(tags):
        tag = tag.lower().strip()
        if not tag:
            continue
        t = db.macros.get_tag_id_from_value(tag)
        # Todo: Need to replace this with some species of ensure tag
        if t:
            tid = t
        else:
            tid = db.macros.add_tag(tag)

        if not db.macros.check_for_tag_title_link(title_id, tid):
            db.macros.add_tag_title_link(title_id, tid)

        tag_ids.add(tid)
    db.driver.conn.commit()

    return tag_ids


def library_set_creator_tags(db: "DatabaseAPI", creator_id: int, tags: Iterable[str], append: bool = False) -> None:
    """
    Set the given iterable of tag texts for the creator specified with the given id.

    :param db: The database to do the update on
    :param creator_id:
    :param tags:
    :param append:
    :return:
    """
    if not append:
        db.macros.clear_creator_tag_links_for_creator(creator_id)

    # Add back the tags
    for tag in set(tags):
        tag = tag.lower().strip()
        if not tag:
            continue
        t = db.macros.get_tag_id_from_value(tag)
        if t:
            tid = t
        else:
            tid = db.macros.add_tag(tag)

        if not db.macros.check_for_creator_tag_link(creator_id, tid):
            db.macros.add_creator_tag_link(creator_id=creator_id, tag_id=tid)

    db.driver.conn.commit()


def library_set_series_tags(db: "DatabaseAPI", series_id: int, tags: Iterable[str], append: bool = False) -> None:
    """
    Set the given iterable of tag texts for the series specified with the given id.

    :param db: The database to do the updates on
    :param series_id: The id of the series to update the tags for
    :param tags: An iterable of tags to apply to the series
    :param append:
    :return:
    """
    if not append:
        db.macros.clear_series_tag_links_for_series(series_id)

    # Add back the tags
    for tag in set(tags):
        tag = tag.lower().strip()
        if not tag:
            continue
        t = db.macros.get_tag_id_from_value(tag)
        if t:
            tid = t
        else:
            tid = db.macros.add_tag(tag)

        if not db.macros.check_for_series_tag_link(series_id=series_id, tag_id=tid):
            db.macros.add_series_tag_link(series_id, tid)

    db.driver.conn.commit()


def library_set_title_tags(db: "DatabaseAPI", title_id: int, tags: Iterable[str], append: bool = False) -> set[int]:
    """
    Sets the tags for a given title row - see the set_tags method.

    :param db: The database to do the update on
    :param title_id:
    :param tags:
    :param append:
    :return:
    """
    return library_set_tags(db, title_id, tags, append=append)


# Todo: How do we determine what series an item is in? So we can unset it.
def library_unset_series(
        db: "DatabaseAPI",
        title_id: int,
        series: Optional[Union[int, str]] = None,
        series_id: int = None) -> None:
    """
    Used when you want to break a link between a series and a title.

    :param db:
    :param title_id:
    :param series:
    :param series_id:
    :return:
    """
    if series is not None:
        raise NotImplementedError
    db.macros.library_unset_series(title_id=title_id, series_id=series_id)


def library_set_series(
    db: "DatabaseAPI",
    title_id: int,
    series: Optional[Union[int, str]] = None,
    series_id: Optional[int] = None,
    update_cache_series=None,
    update_cache_series_idx=None,
) -> tuple[None, None]:
    """
    Sets the primary series for a book_title - updates the book_series_id as well.

    Searches on the series name - no refinements are used - just the raw name.
    :param db:
    :param title_id: The id of the title to do the update for
    :param series: The name of the series
    :param series_id: The id of the entry on the series table. If this is provided it takes precedence over the series
                      which will be ignored.
    :param update_cache_series: Function to update the series field of any cache which is currently being maintained.
    :param update_cache_series_idx:
    :return:
    """
    # If there is already a link between the title and the series then promote it to the highest priority
    # If there is no link then create it
    # If the series to update is None then set the series to null and continue
    if series is not None:

        title_row = db.get_row_from_id(table="titles", row_id=title_id)
        series_id = db.macros.get_series_id_from_value(series)

        if series_id:
            series_row = db.get_row_from_id(table="series", row_id=series_id)

            # Check to see if there is already a link which will need updating
            st_status = db.macros.check_for_series_title_link(series_id, title_id)

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
                st_index = db.macros.get_primary_series_index(title_id)

                db.interlink_rows(primary_row=title_row, secondary_row=series_row, index=st_index)

        else:
            # Make the series row that will be associated with the title
            series_row = db.ensure.series_blind(creator_rows=[], series_name=series, stand=False)

            # Retrieve the index to copy across
            st_index = db.macros.get_primary_series_index(title_id=title_id)

            # Create the new row with the index
            # Todo: Might be nice to set where the series came from - a source column
            db.interlink_rows(primary_row=title_row, secondary_row=series_row, index=st_index)

        # Ensure that there isn't a reference to the null series elsewhere in the stack
        db.macros.break_series_title_link(title_id=title_id, series_id=0)

    elif series_id is not None:

        series_row = db.get_row_from_id(table="series", row_id=series_id)
        # Check to see if there is already a link for updating
        st_status = db.macros.check_for_series_title_link(series_id=series_id, title_id=title_id)

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
            st_index = db.macros.get_primary_series_index(title_id=title_id)

            title_row = db.get_row_from_id("titles", title_id)

            # Todo: source="user_set" would be nice - if true
            db.interlink_rows(primary_row=title_row, secondary_row=series_row, index=st_index)

        # Ensure that there isn't a reference to the null series elsewhere in the stack
        db.macros.break_series_title_link(title_id=title_id, series_id=0)

    else:

        # Check to see if there is already a link to any series - if there is then use the index from that link
        # so that it's preserved in the top entry of the stack - statement will return None if there isn't - which
        # is fine
        series_index = db.macros.get_primary_series_index(title_id)

        # Nullify the series - by linking it to the null series row
        db.macros.link_null_series_to_title(title_id=title_id, series_index=series_index)

        # Series index is not changed - so doesn't have to be updated in the cache

    # Todo: This should not happen here - instead should propogate back and be taken care of in the cache
    if update_cache_series is not None:
        update_cache_series(title_id=title_id, series=series)

    return None, None


def dummy_series_id(*args, **kwargs):
    raise NotImplementedError("{} - {}".format(args, kwargs))


def library_set_series_index(
        db: "DatabaseAPI",
        title_id: int,
        idx: Optional[Union[float, int]],
        series_id = dummy_series_id,
        update_cache_series_idx = None) -> None:
    """
    Sets the series index for the primary series.

    (the series associated with the book_id, stored in the books table as book_series_id) to the given index.
    Updates the database and the cache.
    :param db: The database to do the update in
    :param title_id: The id of the title/book to update (specifically book in this case, as it updates the books tables
                     column book_series_id)
    :param idx: Set the book to be this position in the series
    :param series_id: Function to get the current series id for the given title
    :param update_cache_series_idx: Function to update a cache entry of the series
    :return:
    """
    # Get the id of the series currently linked to the given book
    try:
        series_id = series_id(title_id, index_is_id=True)
    except NotImplementedError:
        series_id = db.macros.read_primary_title_series_id_from_meta(title_id)

    if series_id is not None:
        # Update the link's index
        db.macros.update_index_for_series_title_link(title_id, series_id, idx)
    else:
        # No links where found - insert a link to the null series including the index information
        db.macros.link_null_series_to_title(title_id, idx)

    if update_cache_series_idx is not None:
        update_cache_series_idx(title_id, idx)


def library_set_last_modified(
        db: "DatabaseAPI",
        book_id: int,
        last_modified) -> None:
    """
    Set the last modified field in the books table.

    :param db:
    :param book_id:
    :param last_modified:
    :return:
    """
    db.macros.update_book_last_modified(book_id=book_id, last_modified=last_modified)


def library_set_authors_from_ids(
        db: "DatabaseAPI",
        title_id: int,
        author_ids: Union[list[int], tuple[int]],
        append: bool = False) -> None:
    """
    Sets the authors for a work from a list of ids.

    The authors will be set or appended in a priority order equal to the order of the list here.
    :param db: The database to do the update on
    :param title_id: The id of the title to set from
    :param author_ids: A list of author ids - should be a list as the priority order of the authors will be
                       respected when they're applied to the title.
    :param append: Append the authors to the given title - if False then erase all the authors associated with the
                   title and replace with the given list.
    :return:
    """
    # If not append then clear the author type creator links to to the book and add the new set back in
    if not append:
        db.macros.clear_title_creator_links_for_given_type_and_title(title_id)

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

        ct_link_id = db.macros.check_for_title_author_link(title_id=title_id, creator_id=author_id)

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
            db.macros.update_title_author_link_priority(
                title_id=title_id, creator_id=author_id, new_priority=ct_link_priority
            )

        ct_link_priority -= 1


def library_set_language(db: "DatabaseAPI", title_id: int, lang_string: str) -> None:
    """
    Set the primary language of a work - preforms the set from a string value of the language.

    :param db: The database to preform the update for
    :param title_id:
    :param lang_string: The language as a string.
    :return:
    """
    lang_row = db.ensure.language(lang_string, lang_code="either")
    lang_id = lang_row["language_id"]

    db.macros.set_title_primary_language(db, title_id, lang_id)
