#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import pprint
from copy import deepcopy
from collections import defaultdict

from six import string_types

from LiuXin.customize.cache.base_tables import ONE_MANY, MANY_ONE, MANY_MANY
from LiuXin.databases.adaptors import get_adapter
from LiuXin.databases.adaptors import sqlite_datetime

from LiuXin.exceptions import DatabaseIntegrityError
from LiuXin.exceptions import InputIntegrityError
from LiuXin.exceptions import InvalidUpdate
from LiuXin.exceptions import NotInCache

from LiuXin.metadata import author_to_author_sort, title_sort

from LiuXin.utils.calibre import isbytestring
from LiuXin.utils.icu import strcmp
from LiuXin.utils.icu import safe_lower
from LiuXin.utils.logger import default_log
from LiuXin.utils.general_ops.python_tools import uniq

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import dict_iterkeys as iterkeys
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin.utils.lx_libraries.liuxin_six import dict_itervalues as itervalues
from LiuXin.utils.lx_libraries.liuxin_six import six_string_types

from past.builtins import basestring


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"

"""
Write convenience methods for items linked to the titles table.
"""


class DummyWriter(object):
    def __init__(self, field):
        self.field = field
        self.set_books_func = self.dummy

    @staticmethod
    def dummy(book_id_val_map, *args):
        return set()

    def set_books(self, book_id_val_map, db, allow_case_change=True, error=False):
        raise NotImplementedError("writer is not available for this field")

    def set_books_for_enum(self, book_id_val_map, db, field, allow_case_change):
        raise NotImplementedError("writer is not available for this field")


def library_set_title(db, title_id, title):
    """
    Set the title of the work - updates both the title table and the books table.
    If you attempt to set the title to something with evaluates as False the attempted update will be ignored.
    :param db: The database to preform the set in
    :param title_id: The id the title to update the title for
    :param title: The title string to set the title too
    :return:
    """
    db.macros.update_title(title_id=title_id, title=title)


def library_add_feed(db, title, script):
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


def library_remove_feeds(db, ids):
    """
    Remove feeds from the feeds table.
    :param db: The database to do the update on
    :param ids: The ids of the feeds to remove
    :return:
    """
    db.macros.delete_feed(ids)


def library_unapply_series_tags(db, series_id, tags):
    """
    Remove every tag in the given iterator of tags from the given series with the given series_id. If the tag is
    not linked to the series no change is made.
    :param db: The database to preform the changes to
    :param series_id: The id of the seris to remove the tags from
    :param tags: Text of the tags to remove from the series
    :return:
    """
    db.macros.unapply_series_tags(series_id, tags)


def library_update_feed(db, feed_id, script, title):
    """
    Update the feed table with a new script and title
    :param db: The database to do the update on
    :param feed_id: Ids from the feed table
    :param script: The script to update the table with
    :param title: The title of the feed
    :return:
    """
    db.macros.update_feed(feed_id, script, title)


def library_set_feeds(db, feeds):
    """
    Clears the entire feed table and updates it with the feeds.
    :param db:
    :param feeds: An iterable of tuples - title, script. These will be set as the new feed_title, feed_script fields
                  of the feeds table. The feeds table will be cleared otherwise.
    :return:
    """
    db.macros.set_feeds(feeds)


def library_set_author_sort(db, title_id, sort):
    """
    Sets the author sort field for the given book/title id.
    :param db:
    :param title_id:
    :param sort:
    :return:
    """
    db.macros.set_author_sort(title_id, sort)


def library_set_cover(db, book_id, value):
    """
    Update the flag stored in the books table - in book_has_cover
    :param db:
    :param book_id:
    :param value:
    :return:
    """
    db.macros.set_has_cover(book_id, value)


def library_remove_unused_series(db):
    """
    Remove series that are not currently in use from the specified database.
    :param db:
    :return:
    """
    db.macros.remove_unused_series()


def library_set_conversion_options(db, book_id, fmt, options):
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


def library_delete_conversion_options(db, book_id, fmt, commit=True):
    """
    Remove a conversion option for a given format from a given id
    :param db: The database to preform the update on
    :param book_id: The id of the book to remove the conversion option from
    :param fmt: The format to remove the conversion option for
    :param commit: Commit the change once it's been made
    :return:
    """
    db.macros.delete_conversion_options(book_id, fmt, commit)


def library_set_isbn(db, title_id, isbn):
    """
    Set a isbn in the identifiers table.
    :param db: The database to preform the update on
    :param title_id: The id of the book to update.
    :param isbn: The isbn of the book to update.
    :return
    """
    return db.macros.set_title_isbn(title_id, isbn)


def library_set_publisher(db, title_id, publisher=None, publisher_id=None):
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


def library_set_comment(db, title_id, text):
    """
    Set the primary comment/note on a title (and thus on a book) to be this text.
    Multiple comments can be set for a title - this just sets the primary comment.
    Note - comments are a type of note - so the text will be stored in the notes table and linked to the title with
    the link type "comment"
    :param db: The database to preform the update on
    :param title_id: The id of the title/book to deal with.
    :param text: The text of the comment to set.
    :return:
    """
    if text:
        comment_row = db.add.comment(text)
        title_row = db.get_row_from_id(table="titles", row_id=title_id)
        db.interlink_rows(primary_row=title_row, secondary_row=comment_row)
        return comment_row["comment_id"]
    else:
        db.macros.clear_title_comments_from_title_id(title_id)
        return None


def library_delete_tag(db, tag):
    """
    Delete a tag from the tag text.
    :param db:
    :param tag:
    :return:
    """
    db.macros.delete_tag_by_value(tag)


def library_delete_tags(db, tags):
    """
    Delete every tag from an iterable of tags.
    No uopdate is made to the cache - presumably this is handled at a higher level.
    :param db: The database to preform the delete on
    :param tags: An iterable of tag texts to be deleted.
    :return:
    """
    for tag in tags:
        library_delete_tag(db, tag)


def library_unapply_tags(db, book_id, tags):
    """
    Remove every tag in the given tags from the given book_id. If the tag is not linked to the book no change is
    made.
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


def library_unapply_creator_tags(db, creator_id, tags):
    """
    Remove every tag in the given iterator of tags from the given creator with the given creator_id. If the tag is
    not linked to the creator no change is made.
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


def library_unapply_title_tags(db, book_id, tags):
    """
    Remove every tag in the given tags from the given book_id. If the tag is not linked to the book no change is
    made.
    :param db:
    :param book_id: The id of the book/title to remove the tags from
    :param tags: An iterable of the exact text of each of the tags to remove.
    :return:
    """
    return library_unapply_tags(db, book_id, tags)


def library_set_tags(db, title_id, tags, append=False):
    """
    Set the given iterable of tag texts for the given book/title id. Use the set_creator_tags to set tags for a
    creator of the work, and set_series_tags to set tags for the series the title is in.
    tags are matched on their exact text. Use ensure_tags
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


def library_set_creator_tags(db, creator_id, tags, append=False):
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


def library_set_series_tags(db, series_id, tags, append=False):
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


def library_set_title_tags(db, title_id, tags, append=False):
    """
    Sets the tags for a given title row - see the set_tags method.
    :param db: The database to do the update on
    :param title_id:
    :param tags:
    :param append:
    :return:
    """
    return library_set_tags(db, title_id, tags, append=append)


def library_unset_series(db, title_id, series=None, series_id=None):
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
    db,
    title_id,
    series=None,
    series_id=None,
    update_cache_series=None,
    update_cache_series_idx=None,
):
    """
    Sets the primary series for a book_title - updates the book_series_id as well.
    Searches on the series name - no refinements are used - just the raw name.
    :param db:
    :param title_id: The id of the title to do the update for
    :param series: The name of the series
    :param series_id: The id of the entry on the series table. If this is provided it takes precidence over the series
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


def library_set_series_index(db, title_id, idx, series_id=dummy_series_id, update_cache_series_idx=None):
    """
    Sets the series index for the primary series (the series associated with the book_id, stored in the books table
    as book_series_id) to the given index.
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


def library_set_last_modified(db, book_id, last_modified):
    """
    Set the last modified field in the books table.
    :param db:
    :param book_id:
    :param last_modified:
    :return:
    """
    db.macros.update_book_last_modified(book_id=book_id, last_modified=last_modified)


def library_set_authors_from_ids(db, title_id, author_ids, append=False):
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


def library_set_language(db, title_id, lang_string):
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


def get_writer(field):
    """
    Return a writer object suitable for the table.
    :param field:
    :return:
    """
    if field.metadata["datatype"] == "composite" or field.name in {
        "id",
        "size",
        "path",
        "formats",
        "news",
    }:
        return DummyWriter(field)

    elif field.name == "identifiers" or field.table.name == "identifiers":
        return IdentifiersWrite(field)

    elif field.name == "languages":
        return LanguagesWriter(field)

    elif field.name == "cover":
        return CoversWrite(field)

    elif field.name == "uuid":
        return UUIDWriter(field)

    elif field.name[0] == "#" and field.name.endswith("_index"):
        return CustomSeriesIndexWriter(field)

    elif field.name == "title":
        return TitleWriter(field)

    elif field.name == "author_sort":
        return AuthorSortWriter(field)

    # Todo: Likewise for one_one, many_one, one_many
    elif field.table.table_type == MANY_ONE:
        return ManyToOneWriter(field)

    # Todo: Remove the is_many_many and is_many entirely - table type does the same thing and is less badly named
    elif field.name == "publisher" or field.is_many_many or field.table.table_type == MANY_MANY:
        return ManyToManyWriter(field)

    # Todo: This probably doesn't work, at least not the way you expect
    elif field.is_many or field.table.table_type == ONE_MANY:
        return OneToManyWriter(field)

    else:
        return OneToOneWriter(field)


class BaseWriter(object):
    def __init__(self, field):
        """
        Operations which should be done for every field.
        :param field:
        """
        self.adapter = get_adapter(field.name, field.metadata)
        self.name = field.name
        self.field = field
        self.dt = field.metadata["datatype"]
        self.accept_vals = lambda x: True

    def set_books_func(self, book_id_val_map, db, field, allow_case_change=False):
        """
        Should be over-ridden by one of the
        :param book_id_val_map:
        :param db:
        :param field:
        :param allow_case_change:
        :return:
        """
        raise NotImplementedError

    def no_adapter_set_books(self, book_id_val_map, db, allow_case_change=True):
        """
        Used when the values in question should not be run through an adapter before being written out to the database.
        :param book_id_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """
        if not book_id_val_map:
            return set()

        try:
            dirtied = self.set_books_func(book_id_val_map, db, self.field, allow_case_change)
        except Exception as e:
            err_str = "error while calling self.set_books_func"
            default_log.log_exception(err_str, e, "ERROR", ("self.set_books_func", self.set_books_func))
            raise

        return dirtied

    def set_books(self, book_id_val_map, db, allow_case_change=True):
        """
        Preform the write for the given metadata into the books in accordance with the book_id_val_mpa.
        :param book_id_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """
        book_id_val_map = {k: self.adapter(v) for k, v in iteritems(book_id_val_map) if self.accept_vals(v)}

        if not book_id_val_map:
            return set()

        try:
            dirtied = self.set_books_func(book_id_val_map, db, self.field, allow_case_change)
        except Exception as e:
            err_str = "error while calling self.set_books_func"
            default_log.log_exception(err_str, e, "ERROR", ("self.set_books_func", self.set_books_func))
            raise

        return dirtied

    @staticmethod
    def get_db_id(
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
        id_map_update=None,
    ):
        """
        Get the db id for the value val. If the val does not exist in the db it is inserted into it.
        :param val: The value to search for
        :param db: The database to do the search in.
        :param m: field.metadata for the field being searched
        :param table:
        :param kmap: Case mapper - usually either icu_lower or the identity function
        :param rid_map: Keyed with values from the database and valued with the id corresponding to that valur
        :param allow_case_change:
        :param case_changes: A dictionary recording the required case changes to get a match
        :param val_map: A map keyed with the value and valued with it's id
        :param is_authors: Is the value from the authors table?
        :param id_map_update:
        :return None: All changes happen internally to the value passed into the function
        """
        id_map_update = id_map_update if id_map_update is not None else dict()

        # Process m to extract the table and column the value will be added into - adding flexibility
        # Todo: Account for is_authors - use the author phash search system here
        if isinstance(m, string_types):
            m_table = m
            m_col = db.get_display_column(m_table)
        else:
            m_table = m["table"]
            m_col = m["column"]

        # Tries looking the value up in the cache - if it fails starts checking the database
        kval = kmap(val)
        item_id = rid_map.get(kval, None)

        # If the item can't be found in the cache then it needs to be added to the database
        if item_id is None:

            if is_authors:

                # Todo: Use this in the add.creator method, by default
                aus = author_to_author_sort(val)
                # Todo: Why does this happen? Make sure that it happens everywhere it should. Should add to add.creator
                val_row = db.add.creator(creator=val.replace(",", "|"), creator_sort=aus).row_dict

                item_id = val_row["creator_id"]
                try:
                    table.seen_item_ids.add(item_id)
                except:
                    pass

                # Writing the values which are unique to authors into the cache
                table.asort_map[item_id] = aus
                table.alink_map[item_id] = ""

            elif m_table in db.custom_tables:

                item_id = db.macros.ensure_custom_column_value(m_table, val)

            else:

                # Deal with the generic case
                val_row = db.get_blank_row(m_table)
                val_row[m_col] = val
                val_row.sync()
                item_id = val_row.row_id
                try:
                    table.seen_item_ids.add(item_id)
                except:
                    pass

            # Store the new values for later write out into the cache
            rid_map[kval] = item_id

        # If the value is already in the cache/ the table check to see if it has the same case as the given value
        # If it doesn't register the cahnge - if it does no further action need be taken
        elif allow_case_change and val != table.id_map[item_id]:
            case_changes[item_id] = val

        # Finally writing the full analyzed value, id pair into the cache update
        id_map_update[item_id] = val
        val_map[val] = item_id

        return id_map_update

    # Generic one to one methods in other tables
    @staticmethod
    def delete_one_to_one_in_other(db, field, deleted):
        """
        Remove one to one entries in a table not of books type.
        :param db:
        :param field:
        :param deleted:
        :return:
        """
        # Todo: Why is this hack necessary? Does it do what you think it does?
        deleted_ids = tuple(de[0] for de in deleted)

        # Delete all references to the book from the link table - foreign keys should take out the value from the
        # one_to_one table as well
        db.macros.break_generic_link(field.table.link_table, field.table.link_table_bt_id_column, deleted_ids)

    @staticmethod
    def custom_delete_one_to_one_in_other(db, field, deleted):
        """
        Remove one to one entries in a table not of books type.
        :param db:
        :param field:
        :param deleted:
        :return:
        """
        deleted_ids = tuple(de[0] for de in deleted)

        db.macros.break_cc_links_by_book_id(lt=field.metadata["table"], book_id=deleted_ids)

    # Todo: Check that dirtied has an update method
    @staticmethod
    def change_case(case_changes, dirtied, db, table, m, is_authors=False):
        """
        Write case changes into the database.
        :param case_changes: A list of case changes to be applied to the database
        :param dirtied: An object containing the dirtied books
        :param db: A database to write the changes to
        :param table: A Table object to cache the changes
        :param m:
        :param is_authors: Should
        :return:
        """
        # Process the field to get the table and the column the update should happen in
        # Todo: Account for the authors-creators change
        if isinstance(m, string_types):
            m_table = m
            m_col = db.get_display_column(m)
        else:
            m_table = m["table"]
            m_col = m["column"]

        # Processing the author strings to ensure safety when written into the database
        if is_authors:
            vals = {item_id: val.replace(",", "|") for item_id, val in iteritems(case_changes)}
        else:
            vals = {item_id: val for item_id, val in iteritems(case_changes)}

        # Update the database with the case change
        db.update_columns(values_map=vals, field=m_col, table=m_table)

        # Write the case changes into the cache and dirty the appropriate books
        for item_id, val in iteritems(case_changes):
            table.id_map[item_id] = val
            dirtied.update(table.col_book_map[item_id])
            if is_authors:
                table.asort_map[item_id] = author_to_author_sort(val)

    def do_generic_one_to_many_db_update(
        self,
        db,
        table,
        field,
        is_custom_series,
        updated,
        deleted,
        clean_before_write=False,
        link_type=None,
    ):
        """
        Generic handler for applying changes to the db.
        Should be fairly general.
        :param db:
        :param table:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :param clean_before_write: If True, then all links to any given book_id in update will be broken before
                                   proceeding to write the new values out to the database.
        :param link_type: If provided, then all the links will be set to this type
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: This also doesn't seem to work - at all - needs to be fixed
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, ((k,) for k in deleted))
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, (k for k in deleted))
            for del_id in deleted:
                db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:
                m = field.metadata
                # Todo: Should trip this mess
                raise NotImplementedError
                # del_stmt = 'DELETE FROM {0} WHERE book=?; '.format(table.link_table)
                # ins_stmt = 'INSERT INTO {0}(book, {1}, extra) VALUES(?, ?, 1.0);'
                # .format(table.link_table, m['link_column'])
            else:
                pass

            # Lock the database to stop anything else from writing to it while doing the update
            with db.lock:
                # Todo: This macro just won't work in this form
                # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column,
                #                              (book_id for book_id in iterkeys(updated)))

                for book_id, item_id in iteritems(updated):

                    title_row = db.get_row_from_id("titles", row_id=book_id)

                    if isinstance(item_id, int):

                        # Done here to allow the recursive call for the dict process
                        db.macros.break_generic_link(
                            link_table=table.link_table,
                            link_col=table.link_table_bt_id_column,
                            remove_id=book_id,
                            link_type=link_type,
                        )
                        # Break any existing links to the item - they need to be repointed
                        db.macros.break_generic_link(
                            link_table=table.link_table,
                            link_col=table.link_table_table_id_column,
                            remove_id=item_id,
                            link_type=link_type,
                        )

                        item_row = db.get_row_from_id(table.name, row_id=item_id)
                        db.interlink_rows(
                            primary_row=title_row,
                            secondary_row=item_row,
                            type=link_type,
                        )

                        # Todo: Ideally do this in a MACRO
                        # if not priority:
                        #     db.macros.make_generic_link_no_priority(table.link_table, table.link_table_table_id_column,
                        #                                             table.link_table_bt_id_column,
                        #                                             book_id, item_id)
                        # else:
                        #     db.macros.make_generic_link(link_table=table.link_table,
                        #                                 left_link_col=table.link_table_table_id_column,
                        #                                 right_link_col=table.link_table_bt_id_column,
                        #                                 priority_col=table.priority_column,
                        #                                 left_id=book_id, right_id=item_id)

                    elif isinstance(item_id, (set, list, tuple)):

                        # Done here to allow the recursive call for the dict process
                        db.macros.break_generic_link(
                            link_table=table.link_table,
                            link_col=table.link_table_bt_id_column,
                            remove_id=book_id,
                            link_type=link_type,
                        )

                        item_id = deepcopy([iid for iid in item_id])
                        item_id.reverse()

                        for true_item_id in item_id:
                            # Break any existing links to the item - with any type- they need to be repointed
                            db.macros.break_generic_link(
                                link_table=table.link_table,
                                link_col=table.link_table_table_id_column,
                                remove_id=true_item_id,
                            )

                            item_row = db.get_row_from_id(table.name, row_id=true_item_id)
                            db.interlink_rows(
                                primary_row=title_row,
                                secondary_row=item_row,
                                type=link_type,
                            )

                            # Todo: Think the problem is this doesn't preserve the other properties of links
                            # if not priority:
                            #     # Todo: Think I've confused left and right here
                            #     db.macros.make_generic_link_no_priority(link_table=table.link_table,
                            #                                             left_link_col=table.link_table_table_id_column,
                            #                                             right_link_col=table.link_table_bt_id_column,
                            #                                             left_id=book_id, right_id=true_item_id)
                            # else:
                            #     db.macros.make_generic_link(link_table=table.link_table,
                            #                                 left_link_col=table.link_table_table_id_column,
                            #                                 right_link_col=table.link_table_bt_id_column,
                            #                                 priority_col=table.priority_column,
                            #                                 left_id=book_id, right_id=true_item_id)

                    # We've been passed a type dict - call recursively to handle it
                    elif isinstance(item_id, dict):

                        for local_link_type, link_vals in iteritems(item_id):
                            if link_vals is not None:
                                self.do_generic_one_to_many_db_update(
                                    db,
                                    table=table,
                                    field=field,
                                    is_custom_series=is_custom_series,
                                    updated={book_id: link_vals},
                                    deleted=set(),
                                    clean_before_write=clean_before_write,
                                    link_type=local_link_type,
                                )
                            else:
                                db.macros.break_generic_link(
                                    link_table=table.link_table,
                                    link_col=table.link_table_bt_id_column,
                                    remove_id=book_id,
                                    link_type=local_link_type,
                                )

                    else:
                        err_str = "Attempt to do_generic_one_to_many_db_update encountered an unexpected case"
                        err_str = default_log.log_variables(err_str, "ERROR", ("item_id", item_id))
                        raise NotImplementedError(err_str)

        return None, None

    def do_generic_many_to_many_db_update(
        self,
        db,
        table,
        field,
        is_custom_series,
        updated,
        deleted,
        clean_before_write=False,
        link_type=None,
    ):
        """
        Generic handler for applying changes to the db.
        Should be fairly general.
        :param db:
        :param table:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :param clean_before_write: If True, then all links to any given book_id in update will be broken before
                                   proceeding to write the new values out to the database.
        :param link_type: If provided, then all the links will be set to this type
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: This also doesn't seem to work - at all
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, ((k,) for k in deleted))
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, (k for k in deleted))
            for del_id in deleted:
                db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:
                m = field.metadata
                # Todo: Should trip this mess
                raise NotImplementedError
                # del_stmt = 'DELETE FROM {0} WHERE book=?; '.format(table.link_table)
                # ins_stmt = 'INSERT INTO {0}(book, {1}, extra) VALUES(?, ?, 1.0);'.format(table.link_table, m['link_column'])
            else:
                pass

            # Lock the database to stop anything else from writing to it while doing the update
            with db.lock:
                # Todo: This macro just won't work in this form
                # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column,
                #                              (book_id for book_id in iterkeys(updated)))

                for book_id, item_id in iteritems(updated):

                    title_row = db.get_row_from_id("titles", row_id=book_id)

                    # Todo: With how the data is currently being used, this should never be triggered
                    if isinstance(item_id, int):

                        item_row = db.get_row_from_id(table.name, row_id=item_id)
                        try:
                            db.interlink_rows(
                                primary_row=title_row,
                                secondary_row=item_row,
                                type=link_type,
                            )
                        except DatabaseIntegrityError:
                            # The link exists - but it needs to be repointed - and, potentially, retyped
                            db.macros.reprioritize_link(
                                link_table=table.link_table,
                                left_link_col=table.link_table_bt_id_column,
                                right_link_col=table.link_table_table_id_column,
                                left_id=book_id,
                                right_id=item_id,
                                new_type=link_type,
                            )

                        # Todo: Ideally do this in a MACRO
                        # if not priority:
                        #     db.macros.make_generic_link_no_priority(table.link_table, table.link_table_table_id_column,
                        #                                             table.link_table_bt_id_column,
                        #                                             book_id, item_id)
                        # else:
                        #     db.macros.make_generic_link(link_table=table.link_table,
                        #                                 left_link_col=table.link_table_table_id_column,
                        #                                 right_link_col=table.link_table_bt_id_column,
                        #                                 priority_col=table.priority_column,
                        #                                 left_id=book_id, right_id=item_id)

                    elif isinstance(item_id, (set, list, tuple)):

                        # Need to know the links before and after - the valid links will be repointed
                        existing_item_ids = db.macros.get_linked_ids(
                            link_table=table.link_table,
                            left_id_col=table.link_table_bt_id_column,
                            right_id_col=table.link_table_table_id_column,
                            left_id=book_id,
                            type_filter=link_type,
                        )

                        item_id = deepcopy([iid for iid in item_id])
                        item_id.reverse()

                        for true_item_id in item_id:

                            # If the item is already linked to the book, then repoint it
                            # This preserves any additional data which might be associated with the link
                            if true_item_id in existing_item_ids:
                                db.macros.reprioritize_link(
                                    link_table=table.link_table,
                                    left_link_col=table.link_table_bt_id_column,
                                    right_link_col=table.link_table_table_id_column,
                                    left_id=book_id,
                                    right_id=true_item_id,
                                    new_type=link_type,
                                )
                                continue

                            # If the item is not linked to the book - then it has to be - retrieve and link
                            item_row = db.get_row_from_id(table.name, row_id=true_item_id)
                            try:
                                db.interlink_rows(
                                    primary_row=title_row,
                                    secondary_row=item_row,
                                    type=link_type,
                                )
                            except DatabaseIntegrityError:
                                # Item may already be linked to the book - but with a different type - repointing
                                # anyway
                                db.macros.reprioritize_link(
                                    link_table=table.link_table,
                                    left_link_col=table.link_table_bt_id_column,
                                    right_link_col=table.link_table_table_id_column,
                                    left_id=book_id,
                                    right_id=true_item_id,
                                    new_type=link_type,
                                )

                        # Remove the links which once existed but are no longer needed
                        for excess_item_id in set(existing_item_ids) - set(item_id):

                            db.macros.break_generic_single_link(
                                link_table=table.link_table,
                                left_link_col=table.link_table_bt_id_column,
                                right_link_col=table.link_table_table_id_column,
                                left_id=book_id,
                                right_id=excess_item_id,
                            )

                            # if not priority:
                            #     # Todo: Think I've confused left and right here
                            #     db.macros.make_generic_link_no_priority(link_table=table.link_table,
                            #                                             left_link_col=table.link_table_table_id_column,
                            #                                             right_link_col=table.link_table_bt_id_column,
                            #                                             left_id=book_id, right_id=true_item_id)
                            # else:
                            #     db.macros.make_generic_link(link_table=table.link_table,
                            #                                 left_link_col=table.link_table_table_id_column,
                            #                                 right_link_col=table.link_table_bt_id_column,
                            #                                 priority_col=table.priority_column,
                            #                                 left_id=book_id, right_id=true_item_id)

                    # We've been passed a type dict - call recursively to handle it
                    elif isinstance(item_id, dict):

                        for local_link_type, link_vals in iteritems(item_id):
                            if link_vals is not None:
                                self.do_generic_many_to_many_db_update(
                                    db,
                                    table=table,
                                    field=field,
                                    is_custom_series=is_custom_series,
                                    updated={book_id: link_vals},
                                    deleted=set(),
                                    clean_before_write=clean_before_write,
                                    link_type=local_link_type,
                                )
                            else:
                                db.macros.break_generic_link(
                                    link_table=table.link_table,
                                    link_col=table.link_table_bt_id_column,
                                    remove_id=book_id,
                                    link_type=local_link_type,
                                )

                    else:
                        err_str = "Cannot parse item_id to update"
                        err_str = default_log.log_variables(err_str, "ERROR", ("item_id", item_id))
                        raise NotImplementedError(err_str)

        return None, None

    def _do_vals_to_ids(
        self,
        book_id_val_map,
        db_id_matcher,
        db,
        m,
        table,
        kmap,
        rid_map,
        allow_case_change,
        case_changes,
        val_map,
        id_map_update,
    ):
        def _process_list_set_str_val(val):
            # We have a list or set of values
            if isinstance(val, (set, list)):
                # To keep compatibility with other methods
                if isinstance(val, list):
                    true_vals = deepcopy(val)
                    true_vals.reverse()
                else:
                    true_vals = val

                for true_val in true_vals:
                    if isinstance(true_val, int):
                        pass
                    else:
                        db_id_matcher(
                            true_val,
                            db,
                            m,
                            table,
                            kmap,
                            rid_map,
                            allow_case_change,
                            case_changes,
                            val_map,
                            id_map_update=id_map_update,
                        )

            elif isinstance(val, basestring):
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
                    id_map_update=id_map_update,
                )

            elif isinstance(val, int):
                pass

            else:
                raise NotImplementedError

        for val in itervalues(book_id_val_map):
            if val is not None:
                if isinstance(val, (basestring, set, list)):
                    _process_list_set_str_val(val)
                # Presumably match has occurred already. Or something has gone terribly wrong.
                elif isinstance(val, int):
                    pass
                elif isinstance(val, dict):
                    for nested_vals in itervalues(val):
                        if nested_vals:
                            _process_list_set_str_val(nested_vals)
                else:
                    raise NotImplementedError(self._unexpected_val_in_book_id_val_map(book_id_val_map, val))

    def _unexpected_val_in_book_id_val_map(self, book_id_val_map, val):
        """
        Err msg
        :param book_id_val_map:
        :param val:
        :return:
        """
        err_msg = [
            "Unexpected value found in book_id_val_map",
            "book_id_val_map: \n{}\n".format(pprint.pformat(book_id_val_map)),
            "val: {}".format(val),
            "type(val): {}".format(type(val)),
        ]
        return "\n".join(err_msg)


class AuthorSortWriter(BaseWriter):
    """
    Class for writing information out to the AuthorSort table.
    """

    def __init__(self, field):
        super(AuthorSortWriter, self).__init__(field)
        self.set_books_func = self.set_author_sort

    @staticmethod
    def set_author_sort(book_id_val_map, db, field, *args):
        """
        Set the author sort for the given books.
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        for book_id, creator_val in iteritems(book_id_val_map):
            db.macros.update_title_creator_sort(title_id=book_id, creator_val=creator_val)
        return set(book_id_val_map)


class CoversWrite(BaseWriter):
    """
    Class for writing covers information out to the table.
    """

    def __init__(self, field):
        super(CoversWrite, self).__init__(field=field)
        self.set_books_func = self.set_cover_exists

    @staticmethod
    def set_cover_exists(book_id_val_map, db, field, *args):
        """
        Set a flag to indicate if the works have a cover or not
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        for book_id, cover_status in iteritems(book_id_val_map):
            library_set_cover(db, book_id, cover_status)

        return set(book_id_val_map)


class CustomSeriesIndexWriter(BaseWriter):
    """
    Class for writing data out to custom series index tables.
    """

    def __init__(self, field):
        super(CustomSeriesIndexWriter, self).__init__(field)
        self.set_books_func = self.custom_series_index

    @staticmethod
    def custom_series_index(book_id_val_map, db, field, *args):
        """
        Table of type series have an extra column in their link table - which is the index of that custom series.
        This method writes new values for the custom index out to the database.
        :param book_id_val_map: Keyed with the id of the book and valued with the new index value for that book.
        :param db: The database to preform the update in
        :param field: The base field - the name of the index field will be constructed from that
        :param args: Any additional arguments are ignored
        :return:
        """
        series_field = field.series_field
        sequence = []
        for book_id, sidx in iteritems(book_id_val_map):
            if sidx is None:
                sidx = 1.0
            ids = series_field.ids_for_book(book_id)
            if ids:
                if isinstance(ids, int):
                    ids = (ids,)
                sequence.append((sidx, book_id, ids[0]))
            field.table.book_col_map[book_id] = sidx

        if sequence:
            db.macros.update_custom_column_additional_column_many(
                table=field.metadata["table"],
                column=field.metadata["column"],
                sequence=sequence,
            )

        return {s[1] for s in sequence}


class LanguagesWriter(BaseWriter):
    """
    Class for writing languages information out to the table.
    """

    def __init__(self, field):
        super(LanguagesWriter, self).__init__(field=field)

        self.set_books = self.no_adapter_set_books
        self.set_books_func = self.set_languages

    @staticmethod
    def set_languages(book_id_val_map, db, field, *args):
        """
        Preforms a set into the languages table.
        Parses the :param book_id_val_map: and uses the information it provides to update the links between the titles
        table and the languages table.
        :param book_id_val_map: Assume that we receive a directory keyed with the book id and valued with the language CODE.
        :param db:
        :param field:
        :param args:
        :return:
        """
        for book_id, lang_code in iteritems(book_id_val_map):

            if isinstance(lang_code, six_string_types):

                # Scrub any primary languages from the languages table - if they exist
                db.macros.break_lang_title_links(book_id, link_type="primary")

                title_row = db.get_row_from_id("titles", row_id=book_id)
                # Todo: ensure.language is being called at least three times in this module - does it need to be?
                lang_row = db.ensure.language(lang_code, lang_code="either")
                db.interlink_rows(primary_row=title_row, secondary_row=lang_row, type="primary")
                continue

            elif isinstance(lang_code, dict):

                # Scrub all language_title links for the given id from the database - the ones in use will be recreated
                db.macros.break_lang_title_links(book_id)

                # Going to need the title row to link language rows to it
                title_row = db.get_row_from_id("titles", row_id=book_id)

                # Todo: Spin primary language off into a different table
                # Check that we're not trying to try and set multiple primary languages
                if "primary" in lang_code and len(lang_code["primary"]) not in [0, 1]:
                    raise AssertionError("Trying to set multiple languages primary - stop it!")

                # Todo: Now should be done in the languages table
                for link_type, language_ids in iteritems(lang_code):
                    # Check the status of the link dict before trying to write it out onto the database
                    assert isinstance(
                        link_type, six_string_types
                    ), "link type not a basestring - link update probably malformed"
                    assert isinstance(language_ids, list), "link_ids are not a list - link update probably malformed"

                    for language_id in language_ids:

                        if isinstance(language_id, int):
                            lang_row = db.get_row_from_id("languages", row_id=language_id)
                        elif isinstance(language_id, six_string_types):
                            lang_row = db.ensure.language(language_id, lang_code="either")
                        else:
                            raise NotImplementedError

                        try:
                            db.interlink_rows(
                                primary_row=title_row,
                                secondary_row=lang_row,
                                type=link_type,
                                priority="lowest",
                            )
                        except AttributeError:
                            err_str = "AttributeError while trying to link rows"
                            err_str = default_log.log_variables(
                                err_str,
                                "ERROR",
                                ("language_id", language_id),
                                ("lang_row", lang_row),
                            )
                            raise NotImplementedError(err_str)

                continue

            else:

                raise NotImplementedError("Cannot preformed update - book_id_val_map is not well formed")

        # Just assume that every indicated book has been touched
        return set(book_id_val_map)


class IdentifiersWrite(BaseWriter):
    """
    Class for writing identifier information out to the table
    """

    def __init__(self, field):
        super(IdentifiersWrite, self).__init__(field=field)

        self.set_books_func = self.identifiers
        self.set_books = self.no_adapter_set_books

    @staticmethod
    def identifiers(book_id_val_map, db, field, *args):  # {{{
        """
        Write identifiers out to the table.
        Unless this is called with append this will overwrite all the identifiers currently associated with the book.
        :param book_id_val_map: Keyed with the id of the book and valued with the identifiers to update that book with
        :param db: The database to do the update on
        :param field: A field with identifier like structure (theoretically - only currently working for the identifiers
                      table).
        :param args: Ignored
        :return:
        """
        table = field.table
        updates = set()
        for book_id, ids in iteritems(book_id_val_map):

            # If the book does not currently have an entry in the ids cache, add it
            if book_id not in table.book_col_map:
                table.book_col_map[book_id] = {}

            current_ids = table.book_col_map[book_id]
            remove_keys = set(current_ids) - set(ids)

            for key in remove_keys:
                table.col_book_map.get(key, set()).discard(book_id)
                current_ids.pop(key, None)
            current_ids.update(ids)

            for key, val in iteritems(ids):
                if key not in table.col_book_map:
                    table.col_book_map[key] = set()
                table.col_book_map[key].add(book_id)
                updates.add((book_id, key, val))

        # Write the updated identifiers out to the database
        for book_id, id_type, id_val in updates:
            db.macros.set_title_identifier(title_id=book_id, id_type=id_type, id_val=id_val)

        return set(book_id_val_map)


class OneToOneWriter(BaseWriter):
    def __init__(self, field):
        super(OneToOneWriter, self).__init__(field)
        self.set_books_func = self.one_one_in_books if field.metadata["table"] == "books" else self.one_one_in_other

        if self.name in {"timestamp", "uuid", "sort"}:
            self.accept_vals = bool

    # Todo: Cache updates should be handled by a seperate process (with reference to the docstring)
    def one_one_in_books(self, book_id_val_map, db, field, *args):
        """
        Set fields for a one-one field in the books/title table.
        Preform an update of the database and cache for a generic database field.
        :param book_id_val_map: Keyed with the id of the book and valued with the value to set for in the database.
        :param db: db object to preform the update on
        :param field: Object representing the field to update (must have a column item - which describes the column to
                      update)
                      Typically an in memory store of the item.
        :param args:
        :return affects_ids: The book ids which have been changed by this operation
        """
        if args:
            info_str = "unexpected args passed to one_one_in_books"
            default_log.log_variables(info_str, "INFO", ("args", args))

        db_updater = {
            "series_index": self.series_index_one_one_db_updater,
            "last_modified": self.last_modified_one_one_db_updater,
        }.get(self.name, self.generic_one_one_db_updater)

        # Check that the given field is allowed in the books table and error if it isn't
        col = (
            field.metadata["column"]
            if "liuxin_table_name" not in field.metadata
            else field.metadata["liuxin_table_name"]
        )

        if "in_table" in field.metadata.keys():
            dst_table = field.metadata["in_table"]
        else:
            if col.startswith("book") or col.startswith("title"):
                dst_table = "books" if col.startswith("book") else "titles"
            else:
                dst_table = "titles"

        if dst_table == "titles" and not col.startswith("title"):
            table_col = "title_{}".format(col)
        elif dst_table == "books" and not col.startswith("book"):
            table_col = "book_{}".format(col)
        else:
            table_col = col

        # Todo: This is a stupid patch - fix it by renaming the column
        if table_col == "title_pubdate":
            table_col = "book_pubdate"

        if book_id_val_map:

            # Writing the changes out the database
            book_val_map = {k: sqlite_datetime(v) for k, v in iteritems(book_id_val_map)}

            db_updater(db=db, values_map=book_val_map, field=table_col, table=dst_table)

            # Updating the cache - if one is present in the field
            try:
                field.table.book_col_map.update(book_id_val_map)
            except AttributeError:
                pass

        # Return a set of the touched ids
        return set(book_id_val_map)

    # Todo: Comments should really be "one_many" - and need to test that this works properly with
    #       actualy one_one in other
    def one_one_in_other(self, book_id_val_map, db, field, *args):
        """
        Set a one-one field in a non-books table.
        If a field is not one-one, then the new value is guaranteed to be the highest priority of the item type linked
        to that book record - but old max value won't be deleted by default.
        This should provide calibre emulation - while retaining data for later use.
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        field.table.update_precheck(
            book_id_item_id_map=book_id_val_map,
            id_map_update=dict(),
            acceptance_functions=[self.accept_vals, self.adapter],
        )

        if args:
            info_str = "Unexpected arguments passed to LiuXin.databases.write:one_one_in_other.\n"
            default_log.log_variables(info_str, "INFO", ("args", args))

        if not field.table.custom:
            db_updater = {"comments": self.comments_one_one_in_other_updater}.get(
                field.table.name, self.generic_one_one_in_other_updater
            )

        else:
            db_updater = self.cc_one_one_updater

        id_map = None

        # Process the book_id_val_map - if the value is set to None then all the entries in the other table should be
        # deleted
        deleted = tuple((k, None) for k, v in iteritems(book_id_val_map) if v is None)
        if deleted:

            if not field.table.custom:
                self.delete_one_to_one_in_other(db, field, deleted)
            else:
                self.custom_delete_one_to_one_in_other(db, field, deleted)

            # Todo: See below AND DO NOT DO THIS HERE - SEPERATION OF CONCERNS. THIS IS THE WRITER! IT WRITES TO THE DB!

            # Remove the deleted values form the cache - if the passed in field is a cache like object
            if hasattr(field, "table") and hasattr(field, "complex_update") and not field.complex_update:
                for book_id in deleted:
                    field.table.book_col_map.pop(book_id[0], None)

        # Make the text which will be written to the database - the cases where the comment are to be set None have
        # already been acted on
        updated = {k: v for k, v in iteritems(book_id_val_map) if v is not None}
        book_col_map = None
        if updated:

            id_map, book_col_map = db_updater(db, field, updated)

            # Todo: This is REALLY stupid - there is a call to a cache update method in the set_field function in the
            #       cache
            # which probably triggered all these calls - UPDATE THE DATABASE. THEN UPDATE THE CACHE. DO EACH with the
            # FUNCTIONS WHICH CLAIM TO DO THAT!

            # Update the cache - if the passed in field has a cache like structure
            if field.table.name != "comments":
                if hasattr(field, "table") and hasattr(field, "complex_update") and not field.complex_update:
                    field.table.book_col_map.update(updated)

        if id_map is None and not deleted:
            return set(book_id_val_map)

        elif id_map is None and deleted:
            rtn_info = dict()
            rtn_info["dirtied"] = set(book_id_val_map)
            rtn_info["id_map"] = None
            rtn_info["book_col_map"] = dict(did for did in deleted)
            return rtn_info

        else:
            # Todo: Need to rename id_map
            rtn_info = dict()
            rtn_info["dirtied"] = set(book_id_val_map)
            rtn_info["id_map"] = id_map
            rtn_info["book_col_map"] = book_col_map
            return rtn_info

    @staticmethod
    def generic_one_one_db_updater(db, values_map, field, table):
        """
        Generic update method - applies the book_id_val_map to the database.
        :param db:
        :param values_map:
        :param field:
        :param table:
        :return:
        """
        db.update_columns(values_map=values_map, field=field, table=table)

    @staticmethod
    def series_index_one_one_db_updater(db, values_map, field, table):
        """
        Do an update on the series_index - this should update the series index for the primary index of all entries in
        the values_map - creating a link to the null series if required.
        :param db: The database to do the update on
        :param values_map: Keyed with the id of the book and valued with the new series index
        :param field:
        :param table:
        :return:
        """
        for book_id in values_map:
            series_index_val = values_map[book_id]
            library_set_series_index(db=db, title_id=book_id, idx=series_index_val)

    @staticmethod
    def last_modified_one_one_db_updater(db, values_map, field, table):
        """
        Do an update on the last_modified field in the books table.
        :param db: The database to do the update on.
        :param values_map: Keyed with the book id and valued with the new last_modified value.
        :param field:
        :param table:
        :return:
        """
        for book_id in values_map:
            library_set_last_modified(db, book_id, values_map[book_id])

    @staticmethod
    def comments_one_one_in_other_updater(db, field, updated):
        """
        Updater for the comments table
        :param db:
        :param field:
        :param updated:
        :return:
        """
        id_map = dict()
        book_col_map = dict()

        for book_id in updated:
            comment_val = updated[book_id]
            book_comment_id = library_set_comment(db, book_id, comment_val)

            id_map[book_comment_id] = comment_val
            book_col_map[book_id] = book_comment_id

        return id_map, book_col_map

    @staticmethod
    def generic_one_one_in_other_updater(db, field, updated):
        """
        Generic one-one in other table updater.
        :return:
        """
        # Update the database - unlinking the records in the other database from the books - they should be fielded by
        # the maintenance bot
        # Todo: What? Probably shouldn't be comments
        for book_id, val in iteritems(updated):
            comment_row = db.get_blank_row("comments")
            comment_row["comment"] = val
            comment_row.sync()
            db.macros.make_generic_link(
                field.table.link_table,
                field.table.link_table_bt_id_column,
                field.table.link_table_table_id_column,
                field.table.link_table_priority_col,
                book_id,
                comment_row["comment_id"],
            )

        return None, None

    @staticmethod
    def cc_one_one_updater(db, field, updated):
        """
        Updater for the comments table
        :param db:
        :param field:
        :param updated:
        :return:
        """
        for book_id, val in iteritems(updated):

            # break the old link - if one exists
            db.macros.break_cc_lt_link(lt=field.metadata["table"], book=book_id)

            # write the new value to the custom column table
            db.macros.add_cc_link_with_extra(lt=field.metadata["table"], book_id=book_id, value_id=val)

        return None, None


# Todo: This really needs to be split down into modues - do this ASAP
class TitleWriter(OneToOneWriter):
    def __init__(self, field):
        super(TitleWriter, self).__init__(field)
        self.set_books_func = self.set_title

    def set_title(self, book_id_val_map, db, field, *args):
        """
        Set the title and update the title_sort field
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        # Update the titles in the database
        ans = self.one_one_in_books(book_id_val_map, db, field, *args)

        # Update the title sort field
        field.title_sort_field.writer.set_books({k: title_sort(v) for k, v in iteritems(book_id_val_map)}, db)
        return ans


class UUIDWriter(OneToOneWriter):
    def __init__(self, field):
        super(UUIDWriter, self).__init__(field)
        self.set_books_func = self.set_uuid

    def set_uuid(self, book_id_val_map, db, field, *args):
        """
        Update the uuid for the book.
        :param book_id_val_map: Keyed with the id of the book and valued with the new uuid value
        :param db: The database to preform the update in
        :param field: In memory field representing data from the database
        :param args:
        :return:
        """
        # Todo: This should not have to happen here
        # Update the cache
        field.table.update_uuid_cache(book_id_val_map)

        # Update the database through the uuid field
        return self.one_one_in_books(book_id_val_map, db, field, *args)


class ManyToOneWriter(BaseWriter):
    """
    Write in to a many to one table.
    """

    def __init__(self, field):
        super(ManyToOneWriter, self).__init__(field)
        self.set_books_func = self.many_one
        self.set_books = self.no_adapter_set_books

        if field.table.typed:
            self._make_book_id_item_id_map = self._typed_make_book_id_item_id_map

    # Todo: Normalize names inside this function
    def many_one(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Update fields where many books are linked to one item.
        No examples of this exist in the canonical database. Custom examples might include "character_introductions"
        (characters can be introduced, at most, once) or shelf locations in a physical library (a book can be on, at
        most, one shelf).
        Retrieves the appropriate handler for the database upate for the particular field. Passes that into the update
        handler which is also responsible for updating the cache and running clean operations on the table.
        :param book_id_val_map: A map from the book ids to the update values
        :param db: The database to run the update on
        :param field: The field being updated
        :param allow_case_change: If True allows case changes when trying to match the updated value to existing values
                                  on the database.
        :param args:
        :return:
        """
        if args:
            info_str = "many_one had unexpected arguments passed into it"
            default_log.log_variables(info_str, "INFO", ("args", args))

        if not field.table.custom:
            db_update_links = {"rating": self.do_rating_many_one_db_update}.get(
                self.name, self.do_generic_many_one_db_update
            )
        else:
            db_update_links = self.do_custom_many_one_db_update

        db_clean_unused_items = {"rating": self.dummy_many_one_clear_unused}.get(
            self.name, self.generic_many_one_clear_unused
        )

        db_id_matcher = {"rating": self.get_rating_id}.get(self.name, self.get_db_id)

        dirtied = set()
        m = field.metadata
        table = field.table
        dt = m["datatype"]

        table.update_precheck(book_id_val_map, {})

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        # Map values to db ids - including any new values
        # Creating a map which will, in turn, be used to actually find all the ids in the table - by turning the id:item
        # relation around and applying a normalization function to every element in the table
        kmap = safe_lower if dt in {"text", "series"} else lambda x: x
        rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # table has some entries which differ only in case, fix that
        if len(rid_map) != len(table.id_map):
            table.fix_case_duplicates(db)
            rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # Clean the val map and make a note of the case changes - then match the given string to an entry on the
        # database
        case_changes = {}
        id_map_update = dict()
        val_map = {None: None}

        self._do_vals_to_ids(
            book_id_val_map,
            db_id_matcher,
            db,
            m,
            table,
            kmap,
            rid_map,
            allow_case_change,
            case_changes,
            val_map,
            id_map_update,
        )

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating an in-memory map between the book ids and the item ids
        book_id_item_id_map = self._make_book_id_item_id_map(book_id_val_map, val_map)

        # Todo: Need to implement a per-table method to whether update is even required
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != table.book_col_map.get(k, None)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        updated, deleted = table.internal_update_cache(book_id_item_id_map, id_map_update)

        book_col_map, id_map = db_update_links(db, table, field, is_custom_series, updated, deleted)

        rtn_info = dict()
        rtn_info["dirtied"] = set(updated.keys()).union(deleted)
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update
        # Todo: Not being respected by the fields update method - contradictory methods used - c.f. internal_update_used
        rtn_info["cache_update_needed"] = False

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = False

        if clear_unused:
            db_clean_unused_items(db, table, field)

        return rtn_info

    def _make_book_id_item_id_map(self, book_id_val_map, val_map):
        """
        Transform the book_id_val_map to a book_id_item_id_map
        :param book_id_val_map:
        :param val_map:
        :return:
        """
        book_id_item_id_map = dict()
        for book_id, item_val in iteritems(book_id_val_map):
            if item_val in val_map:
                book_id_item_id_map[book_id] = val_map[item_val]
            else:
                book_id_item_id_map[book_id] = item_val
        return book_id_item_id_map

    def _typed_make_book_id_item_id_map(self, book_id_val_map, val_map):
        """
        Transform the book_id_val_map to a book_id_item_id_map - in the case where the map contains type information as
        well.
        :param book_id_val_map:
        :param val_map:
        :return:
        """
        book_id_item_id_map = defaultdict(dict)
        for book_id, item_val in iteritems(book_id_val_map):
            if item_val is None:
                book_id_item_id_map[book_id] = None

            elif isinstance(item_val, dict):
                for link_type, link_val in iteritems(item_val):
                    if link_val is None:
                        book_id_item_id_map[book_id][link_type] = None
                    elif isinstance(link_val, basestring):
                        book_id_item_id_map[book_id][link_type] = val_map[link_val]
                    elif isinstance(link_val, int):
                        book_id_item_id_map[book_id][link_type] = link_val
                    else:
                        raise NotImplementedError

            else:
                err_str = "Unexpected form of book_id_val_map"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("book_id", book_id),
                    ("item_val", item_val),
                    ("book_id_val_map", book_id_val_map),
                )
                raise NotImplementedError(err_str)

        return book_id_item_id_map

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
    def do_rating_many_one_db_update(db, table, field, is_custom_series, updated, deleted):
        """
        Preform updates of the title-ratings link table.
        :param db:
        :param table:
        :param field:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Preform updates on all the links which haven't been broken
        for book_id in updated:
            book_val = updated[book_id]
            db.macros.set_title_rating(book_id, book_val)

        # Break any links which have been marked to be deleted
        for book_id in deleted:
            db.macros.set_title_rating(db, book_id, 0)

        # Todo: This is a problem that needs to be fixed - by returning the maps - later
        return None, None

    def do_generic_many_one_db_update(self, db, table, field, is_custom_series, updated, deleted, link_type=None):
        """
        Use the generic database update handler to apply the changes to the database.
        :param db:
        :param table:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: Neither of these forms seem to actually work - fix this
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, ((k,) for k in deleted))
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, (k for k in deleted))
            for del_id in deleted:
                db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:
                m = field.metadata
                # Todo: Should trip this mess
                raise NotImplementedError
                # del_stmt = 'DELETE FROM {0} WHERE book=?; '.format(table.link_table)
                # ins_stmt = 'INSERT INTO {0}(book, {1}, extra) VALUES(?, ?, 1.0);'.format(table.link_table, m['link_column'])
            else:
                pass

            # Lock the database to stop anything else from writing to it while doing the update
            with db.lock:

                for book_id, book_val in iteritems(updated):

                    if isinstance(book_val, int):

                        # About to write a new link - so all old links - regardless of type - must be broken
                        db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, book_id)

                        title_row = db.get_row_from_id("titles", row_id=book_id)
                        book_row = db.get_row_from_id(table.name, row_id=book_val)
                        db.interlink_rows(
                            primary_row=title_row,
                            secondary_row=book_row,
                            type=link_type,
                        )

                        # db.macros.make_generic_link_no_priority(table.link_table, table.link_table_table_id_column,
                        #                                         table.link_table_bt_id_column,
                        #                                         book_id, item_id)
                    elif isinstance(book_val, dict):

                        for book_link_type, book_link_val in iteritems(book_val):
                            # Recurse to deal with the case where
                            self.do_generic_many_one_db_update(
                                db,
                                table,
                                field,
                                is_custom_series,
                                updated={book_id: book_link_val},
                                deleted=dict(),
                                link_type=book_link_type,
                            )

                    elif book_val is None:

                        # Nullify the link for the specified type - or the whole thing
                        db.macros.break_generic_link(
                            table.link_table,
                            table.link_table_bt_id_column,
                            book_id,
                            link_type=link_type,
                        )

                    else:
                        raise NotImplementedError(self._book_val_has_unexpected_form(updated, book_val))

        return None, None

    def _book_val_has_unexpected_form(self, updated, book_val):
        """
        Err msg
        :param updated:
        :param book_val:
        :param book_val:
        :return:
        """
        err_msg = [
            "book_val was found to have unexpected form",
            "update: \n{}\n".format(pprint.pformat(updated)),
            "book_val: {}".format(book_val),
            "type(book_val): {}".format(type(book_val)),
        ]
        return "\n".join(err_msg)

    # Todo: This is probably going to lead to unpredictable results - especially with the cache rewrite - fix it
    @staticmethod
    def generic_many_one_clear_unused(db, table, field):
        """
        Clear now unused items from a many-one table on the database.
        :return:
        """
        remove = {item_id for item_id in table.id_map if not table.col_book_map.get(item_id, False)}
        if remove:
            m = field.metadata
            table_id = m["table_id"] if "table_id" in m.keys() else "id"
            db.macros.break_generic_link(m["table"], table_id, ((item_id,) for item_id in remove))

            # Todo: This needs to be in the table rather than in write - seperation of concerns
            for item_id in remove:
                del table.id_map[item_id]
                table.col_book_map.pop(item_id, None)

    @staticmethod
    def do_custom_many_one_db_update(db, table, field, is_custom_series, updated, deleted):
        """
        Update a many to one entry in a custom table.
        :param db:
        :param table:
        :param field:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Update the db link table

        # delete all links to the books which have references cleared for them
        if deleted:
            try:
                cc_table = table.link_table
            except AttributeError:
                cc_table = table.metadata["table"]
            target_ids = set([k for k in deleted])
            db.macros.break_cc_links_by_book_id(lt=cc_table, book_id=target_ids)

        if updated:

            if is_custom_series:
                m = field.metadata
                try:
                    cc_table = table.link_table
                except AttributeError:
                    cc_table = table.metadata["table"]

                # Lock the database to stop anything else from writing to it while doing the update
                with db.lock:

                    db.macros.break_cc_links_by_book_id(
                        lt=cc_table,
                        book_id=((book_id,) for book_id in iterkeys(updated)),
                    )
                    db.macros.add_cc_link_with_extra_multi(
                        lt=cc_table,
                        sequence=((book_id, item_id, 1.0) for book_id, item_id in iteritems(updated)),
                        extra=True,
                        target_column=m["link_column"],
                    )

            else:

                try:
                    cc_table = table.link_table
                except AttributeError:
                    cc_table = table.metadata["table"]

                # Lock the database to stop anything else from writing to it while doing the update
                with db.lock:

                    db.macros.break_cc_links_by_book_id(
                        lt=cc_table,
                        book_id=((book_id,) for book_id in iterkeys(updated)),
                    )

                    db.macros.add_cc_link_with_extra_multi(
                        lt=cc_table,
                        sequence=(
                            (
                                book_id,
                                item_id,
                            )
                            for book_id, item_id in iteritems(updated)
                        ),
                        extra=False,
                    )

        return None, None

    # Todo: Probably needs to be in the ensure method
    @staticmethod
    def get_rating_id(
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
        id_map_update=None,
    ):
        """
        Attempts to match the given rating value to an entry in the ratings table.
        :param val: The value to match - will fail if it's not an integer in the range 1-10.
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
        # Todo: Needs to do cache update - doesn't currently
        # Todo: These should really be methods in the cache - it'd be a whole lot more elegant

        old_val = deepcopy(val)

        # Pass False to set the rating for the title null
        if not val:
            val_map[val] = None
            return

        val = int(val)
        if val not in range(1, 11):
            err_str = "Cannot set rating - rating must be an integer in the range 1-10"
            err_str = default_log.log_variables(err_str, "ERROR", ("val", val))
            raise InputIntegrityError(err_str)

        val_map[old_val] = int(val)


# Todo: When you say many_one, do you actually mean one_many - which would make a lot more sense in the context
class OneToManyWriter(ManyToOneWriter):
    """
    Writer for objects with a One To Many relationship.
    """

    def __init__(self, field):
        super(OneToManyWriter, self).__init__(field)

        self.m_table = self.field.metadata["table"]
        self.m_column = self.field.metadata["column"]

        # Is the value being linked to unique? Default assumption is no - as in the case of notes - where many different
        # notes may be linked to a single title e.t.c
        try:
            self.val_unique = bool(self.field.metadata["val_unique"])
        except KeyError:
            self.val_unique = False

        if self.val_unique:
            self.set_books_func = (
                self.set_books_for_enum if field.metadata["datatype"] == "enumeration" else self.set_books_func_one_many
            )
        else:
            self.set_books_func = (
                self.set_books_for_enum
                if field.metadata["datatype"] == "enumeration"
                else self.set_books_function_one_many_not_unique
            )

    def set_books_for_enum(self, book_id_val_map, db, field, allow_case_change):
        allowed = set(field.metadata["display"]["enum_values"])
        book_id_val_map = {k: v for k, v in iteritems(book_id_val_map) if v is None or v in allowed}
        if not book_id_val_map:
            return set()
        return self.set_books_func_one_many(book_id_val_map, db, field, False)

    def set_books_function_one_many_not_unique(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Responsible for returning enough information to preform a cache update.
        :param book_id_val_map:
        :param db:
        :param field:
        :param allow_case_change: Irrelevant here
        :param args:
        :return:
        """
        if args:
            info_str = "set_books_func_one_many had unexpected arguments passed into it"
            default_log.log_variables(info_str, "INFO", ("args", args))

        # Transform the book_id_val_map into a form which can be written out to the database
        new_book_id_val_map = dict()

        # Todo: This information HAS to be availble elsewhere
        link_table = db.driver_wrapper.get_link_table_name(table1="titles", table2=self.m_table)
        link_col = db.driver_wrapper.get_link_column(
            table1="titles",
            table2=self.m_table,
            column_type=db.driver_wrapper.get_id_column("titles"),
        )
        right_link_col = db.driver_wrapper.get_link_column(
            table1="titles",
            table2=self.m_table,
            column_type=db.driver_wrapper.get_id_column(self.m_table),
        )
        left_link_col = db.driver_wrapper.get_link_column(
            table1="titles",
            table2=self.m_table,
            column_type=db.driver_wrapper.get_id_column("titles"),
        )

        book_id_val_map, id_map_update = field.update_preflight(
            book_id_item_id_map=book_id_val_map, id_map_update=dict()
        )

        field.table.update_precheck(book_id_val_map, id_map_update)

        if field.table.priority is False and field.table.typed is False:
            return self._do_not_unique_not_priority_and_not_typed_db_update(
                db, book_id_val_map, link_table, link_col, right_link_col
            )

        elif field.table.priority is True and field.table.typed is False:
            return self._do_not_unique_priority_and_not_typed_db_update(
                db, book_id_val_map, link_table, link_col, right_link_col
            )

        elif field.table.priority is False and field.table.typed is True:
            return self._do_not_unique_not_priority_and_typed_db_update(
                db,
                book_id_val_map,
                link_table,
                link_col,
                right_link_col,
                left_link_col,
                field,
            )

        elif field.table.priority is True and field.table.typed is True:
            return self._do_not_unique_priority_and_typed_db_update(
                db, book_id_val_map, link_table, link_col, right_link_col, field
            )

        else:
            raise NotImplementedError

    def _do_not_unique_not_priority_and_not_typed_db_update(
        self, db, book_id_val_map, link_table, link_col, right_link_col
    ):
        """
        Do db update in the case where the link does not have priority or type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:
        :param right_link_col:
        :return:
        """
        id_map = dict()

        final_book_id_val_map = defaultdict(set)

        # Todo: Does not deal with ids being passed in as integers
        # Todo: Note this WILL NOT WORK on typed tables - though the modification is easy
        # Nothing fancy is needed - just need to preformm the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, book_vals in iteritems(book_id_val_map):

            # Todo: Need to generalize this - and rationalize the metadata
            bt_row = db.get_row_from_id("titles", book_id)

            # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
            # If we're being passed a string, then add it as the only value
            db.macros.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)

            # If the link has the concept of priority this should set it correctly - if not it doesn't matter
            book_vals = list(bv for bv in book_vals)
            book_vals.reverse()

            # Add the links back in
            for book_val in book_vals:

                # If we're being passed an iterable of strings, then we just need to add, link and return
                if isinstance(book_val, six_string_types):
                    new_val_row = db.get_blank_row(self.m_table)
                    new_val_row[self.m_column] = book_val
                    new_val_row.sync()

                    db.interlink_rows(primary_row=bt_row, secondary_row=new_val_row)
                    id_map[new_val_row.row_id] = book_val
                    final_book_id_val_map[book_id].add(new_val_row.row_id)

                elif isinstance(book_val, int):
                    # We're being passed an integer - assume this is a note_id - move the note association to the
                    # specified title

                    # Break an existing link to the item
                    db.macros.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=book_val,
                    )

                    # Link the note back to the title
                    book_val_row = db.get_row_from_id(self.m_table, book_val)
                    db.interlink_rows(primary_row=bt_row, secondary_row=book_val_row)

                    final_book_id_val_map[book_id].add(book_val)
                else:
                    raise NotImplementedError

            if not book_vals:
                final_book_id_val_map[book_id] = None

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _do_not_unique_priority_and_not_typed_db_update(
        self, db, book_id_val_map, link_table, link_col, right_link_col
    ):
        """
        Do db update in the case where the link does not have priority or type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:
        :param right_link_col:
        :return:
        """
        id_map = dict()

        final_book_id_val_map = defaultdict(list)

        # Todo: Does not deal with ids being passed in as integers
        # Todo: Note this WILL NOT WORK on typed tables - though the modification is easy
        # Nothing fancy is needed - just need to preformm the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, book_vals in iteritems(book_id_val_map):

            # Todo: Need to generalize this - and rationalize the metadata
            bt_row = db.get_row_from_id("titles", book_id)

            # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
            # If we're being passed a string, then add it as the only value
            db.macros.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)

            # If the link has the concept of priority this should set it correctly - if not it doesn't matter
            book_vals = list(bv for bv in book_vals) if book_vals is not None else []
            book_vals.reverse()

            # Add the links back in
            for book_val in book_vals:

                # If we're being passed an iterable of strings, then we just need to add, link and return
                if isinstance(book_val, six_string_types):
                    new_val_row = db.get_blank_row(self.m_table)
                    new_val_row[self.m_column] = book_val
                    new_val_row.sync()

                    db.interlink_rows(primary_row=bt_row, secondary_row=new_val_row)
                    id_map[new_val_row.row_id] = book_val
                    final_book_id_val_map[book_id] = [
                        new_val_row.row_id,
                    ] + final_book_id_val_map[book_id]

                elif isinstance(book_val, int):
                    # We're being passed an integer - assume this is a note_id - move the note association to the
                    # specified title

                    # Break an existing link to the item
                    db.macros.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=book_val,
                    )

                    # Link the note back to the title
                    book_val_row = db.get_row_from_id(self.m_table, book_val)
                    db.interlink_rows(primary_row=bt_row, secondary_row=book_val_row)

                    final_book_id_val_map[book_id] = [
                        book_val,
                    ] + final_book_id_val_map[book_id]

                else:
                    raise NotImplementedError

            if not book_vals:
                final_book_id_val_map[book_id] = None

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _do_not_unique_not_priority_and_typed_db_update(
        self,
        db,
        book_id_val_map,
        link_table,
        link_col,
        right_link_col,
        left_link_col,
        field,
    ):
        """
        Do db update in the case where the link does not have priority but does have type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:

        :param right_link_col:
        :param left_link_col:

        :param field:
        :return:
        """
        id_map = dict()
        new_ids = set()

        final_book_id_val_map = defaultdict(self._default_dict_list_factory)

        # Nothing fancy is needed - just need to preform the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, type_dict in iteritems(book_id_val_map):

            if type_dict is None:
                final_book_id_val_map[book_id] = None
                continue

            for link_type, book_vals in iteritems(type_dict):

                # Todo: This destroys information which has been added to the link
                # After this, there should be no links of any kind to the book - they all need to be re-added
                db.macros.break_generic_link(
                    link_table=link_table,
                    link_col=link_col,
                    remove_id=book_id,
                    link_type=link_type,
                )

                # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
                # If we're being passed a string, then add it as the only value

                # Check to see if fields actually need to be nullified - and note if they do
                if book_vals is None:
                    final_book_id_val_map[book_id][link_type] = None
                    continue

                # If the link has the concept of priority this should set it correctly - if not it doesn't matter
                book_vals = list(bv for bv in book_vals) if book_vals is not None else []
                book_vals.reverse()

                # Add the links back in
                for book_val in book_vals:

                    # If we're being passed an iterable of strings, then we just need to add, link and return
                    if isinstance(book_val, six_string_types):
                        new_val_row = db.get_blank_row(self.m_table)
                        new_val_row[self.m_column] = book_val
                        new_val_row.sync()

                        id_map[new_val_row.row_id] = book_val
                        final_book_id_val_map[book_id][link_type] = [new_val_row.row_id,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                        new_ids.add(new_val_row.row_id)

                    elif isinstance(book_val, int):
                        # We're being passed an integer - assume this is a note_id - move the note association to the
                        # specified title

                        final_book_id_val_map[book_id][link_type] = [book_val,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                    else:
                        raise NotImplementedError

        try:
            field.table.cache_update_precheck(final_book_id_val_map, id_map)
        except Exception as e:
            for item_id in new_ids:
                db.driver_wrapper.delete_by_id(target_table=self.m_table, row_id=item_id)
            raise

        for book_id, type_dict in iteritems(final_book_id_val_map):

            if type_dict is None:
                db.macros.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)
                continue

            for link_type, book_vals in iteritems(type_dict):

                # Break any links which exist between the book and the item with that type
                if book_vals is None:
                    db.macros.break_generic_link(
                        link_table=link_table,
                        link_col=left_link_col,
                        remove_id=book_id,
                        link_type=link_type,
                    )
                    continue

                # Todo: Need to generalize this - and rationalize the metadata
                bt_row = db.get_row_from_id("titles", book_id)

                book_vals = list(book_vals)
                book_vals.reverse()
                for item_id in book_vals:
                    # Break any existing link to the item
                    db.macros.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=item_id,
                    )

                    item_row = db.get_row_from_id(self.m_table, item_id)
                    db.interlink_rows(primary_row=bt_row, secondary_row=item_row, type=link_type)

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _do_not_unique_priority_and_typed_db_update(
        self, db, book_id_val_map, link_table, link_col, right_link_col, field
    ):
        """
        Do db update in the case where the link does not have priority or type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:
        :param right_link_col:
        :return:
        """
        id_map = dict()
        new_ids = set()

        final_book_id_val_map = defaultdict(self._default_dict_list_factory)

        # Nothing fancy is needed - just need to preform the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, type_dict in iteritems(book_id_val_map):

            if type_dict is None:
                final_book_id_val_map[book_id] = None
                continue

            for link_type, book_vals in iteritems(type_dict):

                # Todo: This destroys information which has been added to the link
                # After this, there should be no links of any kind to the book - they all need to be re-added
                db.macros.break_generic_link(
                    link_table=link_table,
                    link_col=link_col,
                    remove_id=book_id,
                    link_type=link_type,
                )

                # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
                # If we're being passed a string, then add it as the only value

                # Check to see if fields actually need to be nullified - and note if they do
                if book_vals is None:
                    final_book_id_val_map[book_id][link_type] = None
                    continue

                # If the link has the concept of priority this should set it correctly - if not it doesn't matter
                book_vals = list(bv for bv in book_vals) if book_vals is not None else []
                book_vals.reverse()

                # Add the links back in
                for book_val in book_vals:

                    # If we're being passed an iterable of strings, then we just need to add, link and return
                    if isinstance(book_val, six_string_types):
                        new_val_row = db.get_blank_row(self.m_table)
                        new_val_row[self.m_column] = book_val
                        new_val_row.sync()

                        id_map[new_val_row.row_id] = book_val
                        final_book_id_val_map[book_id][link_type] = [new_val_row.row_id,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                        new_ids.add(new_val_row.row_id)

                    elif isinstance(book_val, int):
                        # We're being passed an integer - assume this is a note_id - move the note association to the
                        # specified title

                        final_book_id_val_map[book_id][link_type] = [book_val,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                    else:
                        raise NotImplementedError

        try:
            field.table.cache_update_precheck(final_book_id_val_map, id_map)
        except Exception as e:
            for item_id in new_ids:
                db.driver_wrapper.delete_by_id(target_table=self.m_table, row_id=item_id)
            raise

        for book_id, type_dict in iteritems(final_book_id_val_map):

            if type_dict is None:
                db.macros.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)
                continue

            for link_type, book_vals in iteritems(type_dict):

                if book_vals is None:
                    continue

                # Todo: Need to generalize this - and rationalize the metadata
                bt_row = db.get_row_from_id("titles", book_id)

                book_vals = list(book_vals)
                book_vals.reverse()
                for item_id in book_vals:

                    # Break any existing link to the item
                    db.macros.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=item_id,
                    )

                    item_row = db.get_row_from_id(self.m_table, item_id)
                    db.interlink_rows(primary_row=bt_row, secondary_row=item_row, type=link_type)

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _default_dict_list_factory(self):
        return defaultdict(list)

    def _default_dict_set_factory(self):
        return defaultdict(set)

    def set_books_func_one_many(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Responsible for returning enough information to preform a cache update.
        :param book_id_val_map: A map from the book ids to the update values
        :param db: The database to run the update on
        :param field: The field being updated
        :param allow_case_change: If True allows case changes when trying to match the updated value to existing values
                                  on the database.
        :param args:
        :return:
        """
        if args:
            info_str = "set_books_func_one_many had unexpected arguments passed into it"
            default_log.log_variables(info_str, "INFO", ("args", args))

        book_id_val_map, id_map_update = field.table.update_preflight_unique(
            book_id_item_id_map=book_id_val_map, id_map_update=dict()
        )

        # Todo: Check that this is also being done first for all the  other update method
        # Want to do a gross check to make sure the update isn't totally invalid before going any further
        field.table.update_precheck_unique(book_id_val_map, id_map_update)

        available_db_matchers = {None: None}
        db_id_matcher = available_db_matchers.get(self.name, self.get_db_id)

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # Map values to db ids - including any new values
        # Creating a map which will, in turn, be used to actually find all the ids in the table - by turning the id:item
        # relation around and applying a normalization function to every element in the table
        kmap = safe_lower if dt in {"text", "series"} else lambda x: x
        rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # table has some entries which differ only in case, fix that
        if len(rid_map) != len(table.id_map):
            table.fix_case_duplicates(db)
            rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # Clean the val map and make a note of the case changes - then match the given string to an entry on the
        # database
        val_map = {None: None}
        case_changes = {}

        id_map_update = dict()

        self._do_vals_to_ids(
            book_id_val_map,
            db_id_matcher,
            db,
            m,
            table,
            kmap,
            rid_map,
            allow_case_change,
            case_changes,
            val_map,
            id_map_update,
        )

        if field.table.priority is False and field.table.typed is False:
            return self._do_unique_not_priority_and_not_typed_db_update(
                db, book_id_val_map, field, val_map, id_map_update
            )

        elif field.table.priority is True and field.table.typed is False:
            return self._do_unique_priority_and_not_typed_db_update(db, book_id_val_map, field, val_map, id_map_update)

        elif field.table.priority is False and field.table.typed is True:
            return self._do_unique_not_priority_and_typed_db_update(db, book_id_val_map, field, val_map, id_map_update)

        elif field.table.priority is True and field.table.typed is True:
            # The only difference from above is the dictionary is finally valued with a list not a set - should all
            # still work
            return self._do_unique_priority_and_typed_db_update(db, book_id_val_map, field, val_map, id_map_update)
        else:
            raise NotImplementedError

    def _do_unique_not_priority_and_not_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = defaultdict(set)
        for b_id, item_ids_set in iteritems(book_id_val_map):
            if not item_ids_set:
                clean_book_id_item_id_map[b_id] = set()
                continue
            for item_id in item_ids_set:
                if isinstance(item_id, int):
                    clean_book_id_item_id_map[b_id].add(item_id)
                elif isinstance(item_id, basestring):
                    clean_book_id_item_id_map[b_id].add(val_map[item_id])
                else:
                    raise NotImplementedError
        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != table.book_col_map.get(k, None)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps

        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        if clear_unused:
            db_clean_unused_items(db, table, field)

        return rtn_info

    def _do_unique_priority_and_not_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = defaultdict(list)

        def _to_id(item, val_map):
            if isinstance(item, int):
                return item
            else:
                return val_map[item]

        for b_id, item_ids_list in iteritems(book_id_val_map):
            if not item_ids_list:
                clean_book_id_item_id_map[b_id] = []
                continue
            clean_book_id_item_id_map[b_id] = [_to_id(item, val_map) for item in item_ids_list]

        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != table.book_col_map.get(k, None)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps
        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        # Todo: Is producing unexpected results - needs a re-write
        # if clear_unused:
        #     db_clean_unused_items(db, table, field)

        return rtn_info

    def _do_unique_not_priority_and_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = dict()

        def _to_id(item, val_map):
            if isinstance(item, int):
                return item
            else:
                return val_map[item]

        for b_id, link_dict in iteritems(book_id_val_map):
            if not link_dict:
                clean_book_id_item_id_map[b_id] = None
                continue
            clean_b_link_dict = dict()
            for link_type, link_set in iteritems(link_dict):
                try:
                    clean_b_link_dict[link_type] = set([_to_id(item, val_map) for item in link_set])
                except TypeError:
                    clean_b_link_dict[link_type] = None
            clean_book_id_item_id_map[b_id] = clean_b_link_dict

        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != field.ids_for_book(k)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps

        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        # Todo: Is producing unexpected results - needs a re-write
        # if clear_unused:
        #     db_clean_unused_items(db, table, field)

        return rtn_info

    def _do_unique_priority_and_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = dict()

        def _to_id(item, val_map):
            if isinstance(item, int):
                return item
            else:
                return val_map[item]

        for b_id, link_dict in iteritems(book_id_val_map):
            if not link_dict:
                clean_book_id_item_id_map[b_id] = None
                continue
            clean_b_link_dict = dict()
            for link_type, link_list in iteritems(link_dict):
                try:
                    clean_b_link_dict[link_type] = [_to_id(item, val_map) for item in link_list]
                except TypeError:
                    clean_b_link_dict[link_type] = None
            clean_book_id_item_id_map[b_id] = clean_b_link_dict

        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != field.ids_for_book(k)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps

        # Todo: Make sure this is consistent with the other methods like this
        field.table.cache_update_precheck(book_id_item_id_map, val_map)
        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        # Todo: Is producing unexpected results - needs a re-write
        # if clear_unused:
        #     db_clean_unused_items(db, table, field)

        return rtn_info

    @staticmethod
    def do_custom_one_many_db_update(
        db,
        table,
        field,
        is_custom_series,
        updated,
        deleted,
        clean_before_write=False,
        priority=False,
    ):
        """
        Update a many to one entry in a custom table.
        :param db:
        :param table:
        :param field:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            try:
                cc_table = table.link_table
            except AttributeError:
                cc_table = table.metadata["table"]
            db.macros.break_cc_links_by_book_id(lt=cc_table, book_id=((k,) for k in deleted))

        if updated:

            if is_custom_series:
                m = field.metadata
                try:
                    cc_table = table.link_table
                except AttributeError:
                    cc_table = table.metadata["table"]

                # Lock the database to stop anything else from writing to it while doing the update
                with db.lock:

                    db.macros.break_cc_links_by_book_id(
                        lt=cc_table,
                        book_id=((book_id,) for book_id in iterkeys(updated)),
                    )
                    db.macros.add_cc_link_with_extra_multi(
                        lt=cc_table,
                        sequence=((book_id, item_id, 1.0) for book_id, item_id in iteritems(updated)),
                        extra=True,
                        target_column=m["link_column"],
                    )

            else:
                try:
                    cc_table = table.link_table
                except AttributeError:
                    cc_table = table.metadata["table"]

                # Lock the database to stop anything else from writing to it while doing the update
                with db.lock:

                    db.macros.break_cc_links_by_book_id(
                        lt=cc_table,
                        book_id=((book_id,) for book_id in iterkeys(updated)),
                    )
                    db.macros.add_cc_link_with_extra_multi(
                        lt=cc_table,
                        sequence=(
                            (
                                book_id,
                                item_id,
                            )
                            for book_id, item_id in iteritems(updated)
                        ),
                        extra=False,
                    )

        return None, None


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


class UpdateDict(dict):
    """
    Designed to hold updates to the database in dictionary form - with some additional attributes (such as have they
    been checked).
    """

    def __init__(self, *args, **kwargs):
        super(UpdateDict, self).__init__(*args, **kwargs)

        self.checked = False
