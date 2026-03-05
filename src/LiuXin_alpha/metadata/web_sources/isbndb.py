#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import pprint

from LiuXin.utils.localization import _
from LiuXin.metadata import check_isbn
from LiuXin.metadata.web_sources.base import Source, Option
from LiuXin.metadata.metadata import MetaData as Metadata

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

BASE_URL = "http://isbndb.com/api/books.xml?access_key=%s&page_number=1&results=subjects,authors,texts&"

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class ISBNDB(Source):

    name = "ISBNDB"
    description = _("Downloads metadata from isbndb.com")

    capabilities = frozenset(["identify"])
    touched_fields = frozenset(["title", "authors", "identifier:isbn", "comments", "publisher"])
    supports_gzip_transfer_encoding = True

    # Shortcut, since we have no cached cover URLS
    cached_cover_url_is_reliable = False

    options = (
        Option(
            "isbndb_key",
            "string",
            None,
            _("IsbnDB key:"),
            _("To use isbndb.com you have to sign up for a free account at isbndb.com and get an access key."),
        ),
    )

    config_help_message = (
        "<p>"
        + _(
            "To use metadata from isbndb.com you must sign"
            " up for a free account and get an isbndb key and enter it below."
            " Instructions to get the key are "
            '<a href="%s">here</a>.'
        )
    ) % "http://isbndb.com/api/v1/docs/keys"

    LIUXIN_TEST_KEY = "7L2BHNMV"

    def __init__(self, *args, **kwargs):
        Source.__init__(self, *args, **kwargs)

        if self.LIUXIN_TEST_KEY is not None:
            self.isbndb_key = self.LIUXIN_TEST_KEY

        prefs = self.prefs
        prefs.defaults["key_migrated"] = False
        prefs.defaults["isbndb_key"] = self.isbndb_key

        # if not prefs['key_migrated']:
        #     prefs['key_migrated'] = True
        #     try:
        #         from LiuXin.customize.ui import config
        #         key = config['plugin_customization']['IsbnDB']
        #         prefs['isbndb_key'] = key
        #     except:
        #         pass

    def isbndb_key(self):
        return self.prefs["isbndb_key"]

    def is_configured(self):
        return self.isbndb_key is not None

    def create_query(self, title=None, authors=None, identifiers=None):

        from urllib import quote

        if identifiers is None:
            identifiers = {}

        base_url = BASE_URL % self.isbndb_key
        isbn = check_isbn(identifiers.get("isbn", None))
        q = ""
        if isbn is not None:
            q = "index1=isbn&value1=" + isbn
        elif title or authors:
            tokens = []
            title_tokens = list(self.get_title_tokens(title))
            tokens += title_tokens
            author_tokens = self.get_author_tokens(authors, only_first_author=True)
            tokens += author_tokens
            tokens = [quote(t.encode("utf-8") if isinstance(t, unicode) else t) for t in tokens]
            q = "+".join(tokens)
            q = "index1=combined&value1=" + q

        if not q:
            return None
        if isinstance(q, unicode):
            q = q.encode("utf-8")
        print("query: {}".format(q))
        return base_url + q

    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
    ):
        """

        :param log:
        :param result_queue:
        :param abort:
        :param title:
        :param authors:
        :param identifiers:
        :param timeout:
        :return:
        """
        if identifiers is None:
            identifiers = {}

        if not self.is_configured():
            return
        query = self.create_query(title=title, authors=authors, identifiers=identifiers)
        if not query:
            err = "Insufficient metadata to construct query"
            log.error(err)
            return err

        results = []
        try:
            results = self.make_query(
                query,
                abort,
                title=title,
                authors=authors,
                identifiers=identifiers,
                timeout=timeout,
            )
        except Exception as e:
            err_str = "Failed to make query to ISBNDb, aborting."
            err_str = log.log_exception(err_str, e, "INFO", ("query", query))
            return err_str

        # Search again without the identifiers
        if not results and identifiers.get("isbn", False) and title and authors and not abort.is_set():
            return self.identify(log, result_queue, abort, title=title, authors=authors, timeout=timeout)

        for result in results:
            self.clean_downloaded_metadata(result)
            result_queue.put(result)

    def make_query(
        self,
        q,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        max_pages=10,
        timeout=30,
    ):
        """
        Query the ISBNDB website - return the xml feed for parsing.
        :param q:
        :param abort:
        :param title:
        :param authors:
        :param identifiers:
        :param max_pages:
        :param timeout:
        :return:
        """
        from lxml import etree
        from LiuXin.file_formats.chardet import xml_to_unicode
        from utils.libraries.cleantext import clean_ascii_chars

        if identifiers is None:
            identifiers = {}

        page_num = 1
        parser = etree.XMLParser(recover=True, no_network=True)
        br = self.browser()

        seen = set()

        candidates = []
        total_found = 0
        while page_num <= max_pages and not abort.is_set():

            url = q.replace("&page_number=1&", "&page_number=%d&" % page_num)

            print("--------------------")
            print(url)
            print("--------------------")

            page_num += 1
            raw = br.open_novisit(url, timeout=timeout).read()
            feed = etree.fromstring(
                xml_to_unicode(clean_ascii_chars(raw), strip_encoding_pats=True)[0],
                parser=parser,
            )
            total, found, results = self.parse_feed(feed, seen, title, authors, identifiers)

            total_found += found
            candidates += results

            if total_found >= total or len(candidates) > 9:
                break

        print(pprint.pformat(candidates))

        return candidates

    def parse_feed(self, feed, seen, orig_title, orig_authors, identifiers):
        """

        :param feed:
        :param seen:
        :param orig_title:
        :param orig_authors:
        :param identifiers:
        :return:
        """
        from lxml import etree

        def tostring(x):
            """
            Extract the text component from an xml element
            :param x:
            :return:
            """
            if x is None:
                return ""
            return etree.tostring(x, method="text", encoding=six_unicode).strip()

        # Originally had a match function which tried to determine if the feed result matched the return. Removed, as
        # it seems to be too restricting (no results are surviving the filter)

        orig_isbn = identifiers.get("isbn", None)
        results = []

        bl = feed.find("BookList")
        if bl is None:
            err = tostring(feed.find("errormessage"))
            raise ValueError("ISBNDb query failed:" + err)

        # Total number of results matching the parameters
        # Results in this page of the feeed
        total_results = int(bl.get("total_results"))
        shown_results = int(bl.get("shown_results"))
        for bd in bl.xpath(".//BookData"):

            # If title and author information can't be excttracted, something has gone vbery wrong
            title = tostring(bd.find("Title"))
            if not title:
                continue

            authors = []
            for au in bd.xpath(".//Authors/Person"):
                au = tostring(au)
                if au:
                    if "," in au:
                        ln, _, fn = au.partition(",")
                        au = fn.strip() + " " + ln.strip()
                authors.append(au)
            if not authors:
                continue

            md = Metadata(title, authors)

            # Parse the ISBN out of the feed
            isbn10 = check_isbn(bd.get("isbn", None))
            isbn13 = check_isbn(bd.get("isbn13", None))
            md.isbn10 = isbn10
            md.isbn13 = isbn13

            comments = tostring(bd.find("Summary"))

            publisher = tostring(bd.find("PublisherText"))
            if not publisher:
                publisher = None
            if publisher and "audio" in publisher.lower():
                continue

            md.publisher = publisher
            md.comments = comments
            results.append(md)

        return total_results, shown_results, results


if __name__ == "__main__":
    # To run these test use:
    # calibre-debug -e src/calibre/ebooks/metadata/sources/isbndb.py
    from LiuXin.file_formats.metadata.sources.test import (
        test_identify_plugin,
        title_test,
        authors_test,
    )

    test_identify_plugin(
        ISBNDB.name,
        [
            (
                {"title": "Great Gatsby", "authors": ["Fitzgerald"]},
                [
                    title_test("The great gatsby", exact=True),
                    authors_test(["F. Scott Fitzgerald"]),
                ],
            ),
            (
                {"title": "Flatland", "authors": ["Abbott"]},
                [title_test("Flatland", exact=False)],
            ),
        ],
    )
