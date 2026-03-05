#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import socket
import time
import re
from threading import Thread
from Queue import Queue, Empty
from urllib import urlencode

import html5lib
from lxml.html import tostring

from utils.libraries.cleantext import clean_ascii_chars
from LiuXin.file_formats.chardet import xml_to_unicode

from LiuXin.utils.localization import _

from LiuXin.utils.calibre import as_unicode
from LiuXin.utils.web.utils import random_user_agent

from LiuXin.metadata import check_isbn
from LiuXin.metadata.web_sources.base import Source, Option, fixcase, fixauthors
from LiuXin.metadata.book.base import calibreMetadata as Metadata

from LiuXin.utils.localization import canonicalize_lang

from LiuXin.utils.icu import lower as icu_lower

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

from past.builtins import basestring


__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def cssselect(expr):
    from cssselect import HTMLTranslator
    from lxml.etree import XPath

    return XPath(HTMLTranslator().css_to_xpath(expr))


class Amazon(Source):

    name = "Amazon.com"
    description = _("Downloads metadata and covers from Amazon")

    capabilities = frozenset(["identify", "cover"])
    touched_fields = frozenset(
        [
            "title",
            "authors",
            "identifier:amazon",
            "rating",
            "comments",
            "publisher",
            "pubdate",
            "languages",
            "series",
        ]
    )
    has_html_comments = True
    supports_gzip_transfer_encoding = True
    prefer_results_with_isbn = False

    AMAZON_DOMAINS = {
        "com": _("US"),
        "fr": _("France"),
        "de": _("Germany"),
        "uk": _("UK"),
        "it": _("Italy"),
        "jp": _("Japan"),
        "es": _("Spain"),
        "br": _("Brazil"),
    }

    options = (
        Option(
            "domain",
            "choices",
            "com",
            _("Amazon website to use:"),
            _("Metadata from Amazon will be fetched using this country's Amazon website."),
            choices=AMAZON_DOMAINS,
        ),
    )

    def __init__(self, *args, **kwargs):
        """
        Ported from calibre - appears to have been intended to run with the Worker thread above - now modded to run
        standalone (needed it while assemlbing a test data set).
        :param args:
        :param kwargs:
        :return:
        """
        Source.__init__(self, *args, **kwargs)

        # If the domain hasn't been properly set it, defaulting to .com
        if "domain" in kwargs:
            self.domain = kwargs["domain"]
        if not isinstance(self.domain, basestring):
            self.domain = "com"

        self.set_amazon_id_touched_fields()

    def test_fields(self, mi):
        """
        Return the first field from self.touched_fields that is null on the mi object
        :param mi:
        """
        for key in self.touched_fields:
            if key.startswith("identifier:"):
                key = key.partition(":")[-1]
                if key == "amazon":
                    if self.domain != "com":
                        key += "_" + self.domain
                if not mi.has_identifier(key):
                    return "identifier: " + key
            elif mi.is_null(key):
                return key

    def user_agent(self):
        """
        Pass in an index to random_user_agent() to test with a particular user agent
        :return:
        """
        return random_user_agent()

    def save_settings(self, *args, **kwargs):
        Source.save_settings(self, *args, **kwargs)
        self.set_amazon_id_touched_fields()

    def set_amazon_id_touched_fields(self):
        """
        Set the fields which are affected by this plugin.
        """
        id_name = "identifier:amazon"
        if self.domain != "com":
            id_name += "_" + self.domain
        tf = [x for x in self.touched_fields if not x.startswith("identifier:amazon")] + [id_name]
        self.touched_fields = frozenset(tf)

    def get_domain_and_asin(self, identifiers):
        for key, val in identifiers.iteritems():
            key = key.lower()
            if key in ("amazon", "asin"):
                return "com", val
            if key.startswith("amazon_"):
                domain = key.split("_")[-1]
                if domain and domain in self.AMAZON_DOMAINS:
                    return domain, val
        return None, None

    # {{{
    def get_book_url(self, identifiers):
        """
        Filters the identifiers to see if an asin is present - if it is, uses it to get the book's url
        :param identifiers:
        :return:
        """
        domain, asin = self.get_domain_and_asin(identifiers)
        if domain and asin:
            if domain == "com":
                url = "http://amzn.com/" + asin
            elif domain == "uk":
                url = "http://www.amazon.co.uk/dp/" + asin
            elif domain == "br":
                url = "http://www.amazon.com.br/dp/" + asin
            else:
                url = "http://www.amazon.%s/dp/%s" % (domain, asin)
            if url:
                idtype = "amazon" if domain == "com" else "amazon_" + domain
                return idtype, asin, url
        return

    def get_book_url_name(self, idtype, idval, url):
        if idtype == "amazon":
            return self.name
        return "A" + idtype.replace("_", ".")[1:]

    # }}}

    def domain(self):
        x = getattr(self, "testing_domain", None)
        if x is not None:
            return x
        domain = self.prefs["domain"]
        if domain not in self.AMAZON_DOMAINS:
            domain = "com"

        return domain

    def clean_downloaded_metadata(self, mi):
        do_case = mi.language == "eng" or (mi.is_null("language") and self.domain in {"com", "uk"})
        if mi.title and do_case:
            mi.title = fixcase(mi.title)
        mi.authors = fixauthors(mi.authors)
        if mi.tags and do_case:
            mi.tags = list(map(fixcase, mi.tags))
        mi.isbn = check_isbn(mi.isbn)

    def get_website_domain(self, domain):
        """
        Translate the shortened form of the domain name into the actual domain.
        :param domain:
        :return:
        """
        u_domain = domain
        if domain == "uk":
            u_domain = "co.uk"
        elif domain == "jp":
            u_domain = "co.jp"
        elif domain == "br":
            u_domain = "com.br"
        return u_domain

    def create_query(self, log, title=None, authors=None, identifiers=None, domain=None):
        """
        Make a query to submit to Amazon
        :param log:
        :param title:
        :param authors:
        :param identifiers:
        :param domain:
        :return:
        """
        if identifiers is None:
            identifiers = {}

        if domain is None:
            domain = self.domain

        idomain, asin = self.get_domain_and_asin(identifiers)
        if idomain is not None:
            domain = idomain

        # See the amazon detailed search page to get all options
        q = {"search-alias": "aps", "unfiltered": "1"}

        if domain == "com":
            q["sort"] = "relevanceexprank"
        else:
            q["sort"] = "relevancerank"

        isbn = check_isbn(identifiers.get("isbn", None))

        if asin is not None:
            q["field-keywords"] = asin
        elif isbn is not None:
            q["field-isbn"] = isbn
        else:
            # Only return book results
            q["search-alias"] = "digital-text" if domain == "br" else "stripbooks"
            if title:
                title_tokens = list(self.get_title_tokens(title))
                if title_tokens:
                    q["field-title"] = " ".join(title_tokens)
            if authors:
                author_tokens = self.get_author_tokens(authors, only_first_author=True)
                if author_tokens:
                    q["field-author"] = " ".join(author_tokens)

        if not ("field-keywords" in q or "field-isbn" in q or ("field-title" in q)):
            # Insufficient metadata to make an identify query
            return None, None

        # magic parameter to enable Japanese Shift_JIS encoding.
        if domain == "jp":
            q["__mk_ja_JP"] = "カタカナ"

        if domain == "jp":
            encode_to = "Shift_JIS"
        else:
            encode_to = "latin1"
        encoded_q = dict([(x.encode(encode_to, "ignore"), q[x].encode(encode_to, "ignore")) for x in q.keys()])
        url = "http://www.amazon.%s/s/?" % self.get_website_domain(domain) + urlencode(encoded_q)
        return url, domain

    # }}}

    def get_cached_cover_url(self, identifiers):  # {{{
        url = None
        domain, asin = self.get_domain_and_asin(identifiers)
        if asin is None:
            isbn = identifiers.get("isbn", None)
            if isbn is not None:
                asin = self.cached_isbn_to_identifier(isbn)
        if asin is not None:
            url = self.cached_identifier_to_cover_url(asin)

        return url

    # }}}

    def parse_results_page(self, root, domain, result_count=5):
        """
        Parse the page of results produced by searching amazon.
        :param root: The root of the html tree for that page
        :param domain: Which amazon site are we dealing with?
        :param result_count: How many results should be returned when the results page is parsed.
        :return:
        """
        from lxml.html import tostring

        if result_count > 16:
            wrn_str = (
                "Returning this many results would require parsing multiple amazon search pages"
                "which this method is not setup to do. Limiting to 16."
            )
            result_count = 16
            print(wrn_str)

        matches = []

        def title_ok(title):
            title = title.lower()
            bad = [
                "bulk pack",
                "[audiobook]",
                "[audio cd]",
                "(a book companion)",
                "( slipcase with door )",
            ]
            if self.domain == "com":
                bad.extend(["(%s edition)" % x for x in ("spanish", "german")])
            for x in bad:
                if x in title:
                    return False
            return True

        for a in root.xpath(
            r'//li[starts-with(@id, "result_")]//a[@href and contains(@class, "s-access-detail-page")]'
        ):
            title = tostring(a, method="text", encoding=six_unicode)
            if title_ok(title):
                url = a.get("href")
                if url.startswith("/"):
                    url = "http://www.amazon.%s%s" % (
                        self.get_website_domain(domain),
                        url,
                    )
                matches.append(url)

        if not matches:
            # Previous generation of results page markup
            for div in root.xpath(r'//div[starts-with(@id, "result_")]'):
                links = div.xpath(r'descendant::a[@class="title" and @href]')
                if not links:
                    # New amazon markup
                    links = div.xpath("descendant::h3/a[@href]")
                for a in links:
                    title = tostring(a, method="text", encoding=six_unicode)
                    if title_ok(title):
                        url = a.get("href")
                        if url.startswith("/"):
                            url = "http://www.amazon.%s%s" % (
                                self.get_website_domain(domain),
                                url,
                            )
                        matches.append(url)
                    break

        if not matches:
            # This can happen for some user agents that Amazon thinks are mobile/less capable
            for td in root.xpath(r'//div[@id="Results"]/descendant::td[starts-with(@id, "search:Td:")]'):
                for a in td.xpath(
                    r'descendant::td[@class="dataColumn"]/descendant::a[@href]/span[@class="srTitle"]' r"/.."
                ):
                    title = tostring(a, method="text", encoding=six_unicode)
                    if title_ok(title):
                        url = a.get("href")
                        if url.startswith("/"):
                            url = "http://www.amazon.%s%s" % (
                                self.get_website_domain(domain),
                                url,
                            )
                        matches.append(url)
                    break

        # Amazon sorts results by relevance - anything out of the top five is likely to be fairly irrelevant
        return matches[:result_count]

    def identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        sleep_interval=5,
    ):
        """
        Note this method will retry without identifiers automatically if no match is found with identifiers.
        :param log: The logger for this method
        :param result_queue: The results will be added to this object
        :param abort: A handler used to terminate the process
        :param title: Title used in the search
        :param authors: Authors to be used in the search - expected as a list of strings.
        :param identifiers: An optional dictionary of identifiers
        :param timeout: How long the process should wait for a response before timing out
        :param sleep_interval: How long should the process sleep between making repeated requests? A too short an
                               interval seems to result in even more HTTP Error 503 than usual - so lengthening it
                               slightly seems to be a good idea.
        """
        if identifiers is None:
            identifiers = {}

        testing = getattr(self, "running_a_test", False)

        query, domain = self.create_query(log, title=title, authors=authors, identifiers=identifiers)
        if query is None:
            log.error("Insufficient metadata to construct query")
            return
        if testing:
            log.info("Using user agent for amazon: %s" % self.user_agent)

        br = self.browser()
        raw = None
        for i in range(10):
            try:
                raw = br.open_novisit(query, timeout=timeout).read().strip()
                break
            except Exception as e:

                if callable(getattr(e, "getcode", None)) and e.getcode() == 404:
                    log.error("Query malformed: %r" % query)
                    return

                attr = getattr(e, "args", [None])
                attr = attr if attr else [None]

                if isinstance(attr[0], socket.timeout):
                    msg = _("Amazon timed out. Try again later.")
                    log.error(msg)
                else:
                    msg = "Failed to make identify query: %r" % query
                    log.exception(msg)
                if i == 10:
                    print("Attempt {} failed".format(six_unicode(i + 1)))
                    return as_unicode(msg)
                else:
                    print("Attempt {} failed".format(six_unicode(i + 1)))
            time.sleep(sleep_interval)

        # All the urls we're after should contain only ascii chars - so this should be fine
        raw = clean_ascii_chars(xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True)[0])

        if testing:
            import tempfile

            with tempfile.NamedTemporaryFile(prefix="amazon_results_", suffix=".html", delete=False) as f:
                f.write(raw.encode("utf-8"))
            print("Downloaded html for results page saved in", f.name)

        matches = []
        found = "<title>404 - " not in raw

        if found:
            try:
                root = html5lib.parse(raw, treebuilder="lxml", namespaceHTMLElements=False)
            except Exception as e:
                msg = "Failed to parse amazon page for query: %r" % query
                log.log_exception(msg, e, "ERROR")
                log.exception(msg)
                return msg

            # Todo: This seems to be a bug in calibre
            errmsg = root.xpath('//*[@id="errorMessage"]')
            if errmsg:
                msg = tostring(errmsg, method="text", encoding=six_unicode).strip()
                log.error(msg)
                # The error is almost always a not found error
                found = False
        else:
            root = None

        if found:
            matches = self.parse_results_page(root, domain)

        if abort.is_set():
            return

        if not matches:
            if identifiers and title and authors:
                log("No matches found with identifiers, retrying using only title and authors. Query: %r" % query)
                return self.identify(
                    log,
                    result_queue,
                    abort,
                    title=title,
                    authors=authors,
                    timeout=timeout,
                )
            log.error("No matches found with query: %r" % query)
            return

        workers = [
            Worker(url, result_queue, br, log, i, domain, self, testing=testing) for i, url in enumerate(matches)
        ]

        # Start the workers to download the results pages in parallel
        # Don't send all requests at the same time
        for w in workers:
            w.start()
            time.sleep(0.1)

        while not abort.is_set():
            a_worker_is_alive = False
            for w in workers:
                w.join(0.2)
                if abort.is_set():
                    break
                if w.is_alive():
                    a_worker_is_alive = True
            if not a_worker_is_alive:
                break

        return None

    def limited_identify(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        result_count=1,
    ):
        """
        Identify method with an optional parameter for the number of results to return. The choices are between 1 & 16.
        Note this method will retry without identifiers automatically if no match is found with identifiers.
        :param log: The logger for this method
        :param result_queue: The results will be added to this object
        :param abort: A handler used to terminate the process
        :param title: Title used in the search
        :param authors: Authors to be used in the search
        :param identifiers: An optional dictionary of identifiers
        :param timeout: How long the process should wait for a response before timing out
        :param result_count: How mayn results should be returned by the search?
        """
        if identifiers is None:
            identifiers = {}

        if 1 > result_count or 16 < result_count:
            wrn_str = "Result count out of recognized range - assuming you want one result"
            log.log_variablers(wrn_str, "WARNING", ("result_count", result_count))
            result_count = 1

        testing = getattr(self, "running_a_test", False)

        query, domain = self.create_query(log, title=title, authors=authors, identifiers=identifiers)
        if query is None:
            log.error("Insufficient metadata to construct query")
            return
        if testing:
            print("Using user agent for amazon: %s" % self.user_agent)

        br = self.browser()
        raw = None
        for i in range(5):
            try:
                raw = br.open_novisit(query, timeout=timeout).read().strip()
                break
            except Exception as e:

                if callable(getattr(e, "getcode", None)) and e.getcode() == 404:
                    log.error("Query malformed: %r" % query)
                    return

                attr = getattr(e, "args", [None])
                attr = attr if attr else [None]

                if isinstance(attr[0], socket.timeout):
                    msg = _("Amazon timed out. Try again later.")
                    log.error(msg)
                else:
                    msg = "Failed to make identify query: %r" % query
                    log.exception(msg)
                if i == 4:
                    print("Attempt {} failed".format(six_unicode(i + 1)))
                    return as_unicode(msg)
                else:
                    print("Attempt {} failed".format(six_unicode(i + 1)))
            time.sleep(1)

        raw = clean_ascii_chars(xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True)[0])

        if testing:
            import tempfile

            with tempfile.NamedTemporaryFile(prefix="amazon_results_", suffix=".html", delete=False) as f:
                f.write(raw.encode("utf-8"))
            print("Downloaded html for results page saved in", f.name)

        matches = []
        found = "<title>404 - " not in raw

        if found:
            try:
                root = html5lib.parse(raw, treebuilder="lxml", namespaceHTMLElements=False)
            except Exception as e:
                msg = "Failed to parse amazon page for query: %r" % query
                log.log_exception(msg, e, "ERROR")
                log.exception(msg)
                return msg

            # Todo: This seems to be a bug in calibre
            errmsg = root.xpath('//*[@id="errorMessage"]')
            if errmsg:
                msg = tostring(errmsg, method="text", encoding=six_unicode).strip()
                log.error(msg)
                # The error is almost always a not found error
                found = False
        else:
            root = None

        if found:
            matches = self.parse_results_page(root, domain, result_count=result_count)

        if abort.is_set():
            return

        if not matches:
            if identifiers and title and authors:
                log("No matches found with identifiers, retrying using only title and authors. Query: %r" % query)
                return self.identify(
                    log,
                    result_queue,
                    abort,
                    title=title,
                    authors=authors,
                    timeout=timeout,
                )
            log.error("No matches found with query: %r" % query)
            return

        workers = [
            Worker(url, result_queue, br, log, i, domain, self, testing=testing) for i, url in enumerate(matches)
        ]

        # Start the workers to download the results pages in parallel
        # Don't send all requests at the same time
        for w in workers:
            w.start()
            time.sleep(0.1)

        while not abort.is_set():
            a_worker_is_alive = False
            for w in workers:
                w.join(0.2)
                if abort.is_set():
                    break
                if w.is_alive():
                    a_worker_is_alive = True
            if not a_worker_is_alive:
                break

        return None

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        get_best_cover=False,
    ):
        """
        Download a cover from Amazon.
        :param log:
        :param result_queue: A Queue object
        :param abort:
        :param title: Expected as a string
        :param authors: Expected as a list of strings
        :param identifiers:
        :param timeout: How long the method will wait for a result
        :param get_best_cover:
        :return:
        """
        if identifiers is None:
            identifiers = {}

        cached_url = self.get_cached_cover_url(identifiers)
        if cached_url is None:
            log.info("No cached cover found, running identify")
            rq = Queue()
            self.identify(log, rq, abort, title=title, authors=authors, identifiers=identifiers)
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(key=self.identify_results_keygen(title=title, authors=authors, identifiers=identifiers))
            for mi in results:
                cached_url = self.get_cached_cover_url(mi.identifiers)
                if cached_url is not None:
                    break
        if cached_url is None:
            log.info("No cover found")
            return

        if abort.is_set():
            return

        log.info("Downloading cover from: ", cached_url)
        br = self.browser()
        try:
            cdata = br.open_novisit(cached_url, timeout=timeout).read()
            result_queue.put((self, cdata))
        except Exception as e:
            err_str = "Failed to download cover from: {0}".format(cached_url)
            log.log_exception(err_str, e, "DEBUG")

    # }}}


class Worker(Thread):  # Get details {{{
    """
    Get book details from amazons book page in a separate thread
    """

    def __init__(
        self,
        url,
        result_queue,
        browser,
        log,
        relevance,
        domain,
        plugin,
        timeout=20,
        testing=False,
    ):
        """
        Helper method to download details from Amazon in a seperate thread.
        :param url: url to read from
        :param result_queue: A Queue object to append the results from the query
        :param browser: A browser object which will be used to access amazon
        :param log:
        :param relevance:
        :param domain:
        :param plugin:
        :param timeout:
        :param testing:
        :return:
        """
        Thread.__init__(self)
        self.daemon = True
        self.testing = testing
        self.url, self.result_queue = url, result_queue
        self.log, self.timeout = log, timeout
        self.relevance, self.plugin = relevance, plugin
        self.browser = browser.clone_browser()
        self.cover_url = self.amazon_id = self.isbn = None
        self.domain = domain
        from lxml.html import tostring

        self.tostring = tostring

        months = {
            "de": {
                1: ["jän", "januar"],
                2: ["februar"],
                3: ["märz"],
                5: ["mai"],
                6: ["juni"],
                7: ["juli"],
                10: ["okt", "oktober"],
                12: ["dez", "dezember"],
            },
            "it": {
                1: ["enn"],
                2: ["febbr"],
                5: ["magg"],
                6: ["giugno"],
                7: ["luglio"],
                8: ["ag"],
                9: ["sett"],
                10: ["ott"],
                12: ["dic"],
            },
            "fr": {
                1: ["janv"],
                2: ["févr"],
                3: ["mars"],
                4: ["avril"],
                5: ["mai"],
                6: ["juin"],
                7: ["juil"],
                8: ["août"],
                9: ["sept"],
                12: ["déc"],
            },
            "br": {
                1: ["janeiro"],
                2: ["fevereiro"],
                3: ["março"],
                4: ["abril"],
                5: ["maio"],
                6: ["junho"],
                7: ["julho"],
                8: ["agosto"],
                9: ["setembro"],
                10: ["outubro"],
                11: ["novembro"],
                12: ["dezembro"],
            },
            "es": {
                1: ["enero"],
                2: ["febrero"],
                3: ["marzo"],
                4: ["abril"],
                5: ["mayo"],
                6: ["junio"],
                7: ["julio"],
                8: ["agosto"],
                9: ["septiembre", "setiembre"],
                10: ["octubre"],
                11: ["noviembre"],
                12: ["diciembre"],
            },
            "jp": {
                1: ["1月"],
                2: ["2月"],
                3: ["3月"],
                4: ["4月"],
                5: ["5月"],
                6: ["6月"],
                7: ["7月"],
                8: ["8月"],
                9: ["9月"],
                10: ["10月"],
                11: ["11月"],
                12: ["12月"],
            },
        }

        self.english_months = [
            None,
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        self.months = months.get(self.domain, {})

        self.pd_xpath = """
            //h2[text()="Product Details" or \
                 text()="Produktinformation" or \
                 text()="Dettagli prodotto" or \
                 text()="Product details" or \
                 text()="Détails sur le produit" or \
                 text()="Detalles del producto" or \
                 text()="Detalhes do produto" or \
                 starts-with(text(), "登録情報")]/../div[@class="content"]
            """
        # Editor: is for Spanish
        self.publisher_xpath = """
            descendant::*[starts-with(text(), "Publisher:") or \
                    starts-with(text(), "Verlag:") or \
                    starts-with(text(), "Editore:") or \
                    starts-with(text(), "Editeur") or \
                    starts-with(text(), "Editor:") or \
                    starts-with(text(), "Editora:") or \
                    starts-with(text(), "出版社:")]
            """
        self.publisher_names = {
            "Publisher",
            "Verlag",
            "Editore",
            "Editeur",
            "Editor",
            "Editora",
            "出版社",
        }

        self.language_xpath = """
            descendant::*[
                starts-with(text(), "Language:") \
                or text() = "Language" \
                or text() = "Sprache:" \
                or text() = "Lingua:" \
                or text() = "Idioma:" \
                or starts-with(text(), "Langue") \
                or starts-with(text(), "言語") \
                ]
            """
        self.language_names = {
            "Language",
            "Sprache",
            "Lingua",
            "Idioma",
            "Langue",
            "言語",
        }

        self.tags_xpath = """
            descendant::h2[
                text() = "Look for Similar Items by Category" or
                text() = "Ähnliche Artikel finden" or
                text() = "Buscar productos similares por categoría" or
                text() = "Ricerca articoli simili per categoria" or
                text() = "Rechercher des articles similaires par rubrique" or
                text() = "Procure por itens similares por categoria" or
                text() = "関連商品を探す"
            ]/../descendant::ul/li
        """

        self.ratings_pat = re.compile(
            r"([0-9.]+) ?(out of|von|su|étoiles sur|つ星のうち|de un máximo de|de) "
            r"([\d\.]+)( (stars|Sternen|stelle|estrellas|estrelas)){0,1}"
        )

        lm = {
            "eng": ("English", "Englisch"),
            "fra": ("French", "Français"),
            "ita": ("Italian", "Italiano"),
            "deu": ("German", "Deutsch"),
            "spa": ("Spanish", "Espa\xf1ol", "Espaniol"),
            "jpn": ("Japanese", "日本語"),
            "por": ("Portuguese", "Português"),
        }

        self.lang_map = {}
        for code, names in lm.iteritems():
            for name in names:
                self.lang_map[name] = code

        self.series_pat = re.compile(
            r"""
                \|\s*              # Prefix
                (Series)\s*:\s*    # Series declaration
                (?P<series>.+?)\s+  # The series name
                \((Book)\s*    # Book declaration
                (?P<index>[0-9.]+) # Series index
                \s*\)
                """,
            re.X,
        )

    def delocalize_datestr(self, raw):
        if not self.months:
            return raw
        ans = raw.lower()
        for i, vals in self.months.iteritems():
            for x in vals:
                ans = ans.replace(x, self.english_months[i])
        ans = ans.replace(" de ", " ")
        return ans

    def run(self):
        try:
            self.get_details()
        except Exception as e:
            err_str = "get_details failed for url: %r" % self.url
            self.log.log_exception(err_str, e, "ERROR")

    def get_details(self):
        from utils.libraries.cleantext import clean_ascii_chars
        from LiuXin.file_formats.chardet import xml_to_unicode
        import html5lib

        raw = None
        for i in range(5):
            try:
                raw = self.browser.open_novisit(self.url, timeout=self.timeout).read().strip()
                break
            except Exception as e:
                # If the URL is malformed, there is no real point in doing this twice
                if callable(getattr(e, "getcode", None)) and e.getcode() == 404:
                    self.log.error("URL malformed: %r" % self.url)
                    return

                attr = getattr(e, "args", [None])
                attr = attr if attr else [None]
                if isinstance(attr[0], socket.timeout):
                    msg = "Amazon timed out. Try again later."
                    self.log.error(msg)
                else:
                    msg = "Failed to make details query: %r" % self.url
                    self.log.exception(msg)
                if i == 4:
                    print("Attempt {} failed - terminating.".format(six_unicode(i + 1)))
                    return
                else:
                    print("Attempt {} failed\n".format(six_unicode(i + 1)))
            time.sleep(1)

        oraw = raw
        raw = xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True)[0]
        if "<title>404 - " in raw:
            self.log.error("URL malformed: %r" % self.url)
            return

        try:
            root = html5lib.parse(clean_ascii_chars(raw), treebuilder="lxml", namespaceHTMLElements=False)
        except Exception as e:
            msg = "Failed to parse amazon details page: %r" % self.url
            self.log.log_exception(msg, e, "DEBUG")
            return
        if self.domain == "jp":
            for a in root.xpath("//a[@href]"):
                if "black-curtain-redirect.html" in a.get("href"):
                    self.url = "http://amazon.co.jp" + a.get("href")
                    self.log("Black curtain redirect found, following")
                    return self.get_details()

        errmsg = root.xpath('//*[@id="errorMessage"]')
        if errmsg:
            msg = "Failed to parse amazon details page: %r" % self.url
            msg += self.tostring(errmsg, method="text", encoding=six_unicode).strip()
            self.log.error(msg)
            return

        self.parse_details(oraw, root)

    def parse_details(self, raw, root):
        try:
            asin = self.parse_asin(root)
        except Exception as e:
            err_str = "Error parsing asin for url: %r" % self.url
            self.log.log_exception(err_str, e, "DEBUG", ("root", root))
            asin = None
        if self.testing:
            import tempfile
            import uuid

            with tempfile.NamedTemporaryFile(
                prefix=(asin or str(uuid.uuid4())) + "_", suffix=".html", delete=False
            ) as f:
                f.write(raw)
            print("Downloaded html for", asin, "saved in", f.name)

        try:
            title = self.parse_title(root)
        except Exception as e:
            err_str = "Error parsing title for url: %r" % self.url
            self.log.log_exception(err_str, e, "DEBUG", ("root", root))
            title = None

        try:
            authors = self.parse_authors(root)
        except Exception as e:
            err_str = "Error parsing authors for url: %r" % self.url
            self.log.log_exception(err_str, e, "DEBUG", ("root", root))
            authors = []

        if not title or not authors or not asin:
            err_str = "Could not find title/authors/asin for %r" % self.url
            err_str += "ASIN: %r Title: %r Authors: %r" % (asin, title, authors)
            self.log.error(err_str)
            return

        mi = Metadata(title, authors)
        idtype = "amazon" if self.domain == "com" else "amazon_" + self.domain
        mi.set_identifier(idtype, asin)
        self.amazon_id = asin

        try:
            mi.rating = self.parse_rating(root)
        except:
            self.log.exception("Error parsing ratings for url: %r" % self.url)

        try:
            mi.comments = self.parse_comments(root)
        except Exception as e:
            err_str = "Error parsing comments for url: %r" % self.url
            self.log.log_exception(err_str, e, "DEBUG", ("root", root))

        try:
            series, series_index = self.parse_series(root)
            if series:
                mi.series, mi.series_index = series, series_index
            elif self.testing:
                mi.series, mi.series_index = "Dummy series for testing", 1
        except Exception as e:
            err_str = "Error parsing series for url: %r" % self.url
            self.log.log_exception(err_str, e, "DEBUG", ("root", root))

        try:
            mi.tags = self.parse_tags(root)
        except Exception as e:
            err_str = "Error parsing tags for url: %r" % self.url
            self.log.log_exception(err_str, e, "DEBUG", ("root", root))

        try:
            self.cover_url = self.parse_cover(root, raw)
        except Exception as e:
            err_str = "Error parsing cover for url: %r" % self.url
            self.log.log_exception(err_str, e, "DEBUG", ("root", root))
        mi.has_cover = bool(self.cover_url)

        non_hero = cssselect("div#bookDetails_container_div div#nonHeroSection")(root)
        if non_hero:
            # New style markup
            try:
                self.parse_new_details(root, mi, non_hero[0])
            except Exception as e:
                err_str = "Failed to parse new-style book details section"
                self.log.log_exception(err_str, e, "DEBUG", ("root", root))
        else:
            pd = root.xpath(self.pd_xpath)
            if pd:
                pd = pd[0]

                try:
                    isbn = self.parse_isbn(pd)
                    if isbn:
                        self.isbn = mi.isbn = isbn
                except Exception as e:
                    err_str = "Error parsing ISBN for url: %r" % self.url
                    self.log.log_exception(err_str, e, "DEBUG", ("root", root))

                try:
                    mi.publisher = self.parse_publisher(pd)
                except Exception as e:
                    err_str = "Error parsing publisher for url: %r" % self.url
                    self.log.log_exception(err_str, e, "DEBUG", ("root", root))

                try:
                    mi.pubdate = self.parse_pubdate(pd)
                except Exception as e:
                    err_str = "Error parsing publish date for url: %r" % self.url
                    self.log.log_exception(err_str, e, "DEBUG", ("root", root), ("pd", pd))

                try:
                    lang = self.parse_language(pd)
                    if lang:
                        mi.language = lang
                except Exception as e:
                    err_str = "Error parsing language for url: %r" % self.url
                    self.log.log_exception(err_str, e, "DEBUG", ("root", root), ("pd", pd))

            else:
                self.log.warning("Failed to find product description for url: %r" % self.url)

        mi.source_relevance = self.relevance

        if self.amazon_id:
            if self.isbn:
                self.plugin.cache_isbn_to_identifier(self.isbn, self.amazon_id)
            if self.cover_url:
                self.plugin.cache_identifier_to_cover_url(self.amazon_id, self.cover_url)

        self.plugin.clean_downloaded_metadata(mi)

        self.result_queue.put(mi)

    @staticmethod
    def parse_asin(root):
        link = root.xpath('//link[@rel="canonical" and @href]')
        for l in link:
            return l.get("href").rpartition("/")[-1]

    def totext(self, elem):
        return self.tostring(elem, encoding=six_unicode, method="text").strip()

    def parse_title(self, root):
        h1 = root.xpath('//h1[@id="title"]')
        if h1:
            h1 = h1[0]
            for child in h1.xpath('./*[contains(@class, "a-color-secondary")]'):
                h1.remove(child)
            return self.totext(h1)
        tdiv = root.xpath('//h1[contains(@class, "parseasinTitle")]')[0]
        actual_title = tdiv.xpath('descendant::*[@id="btAsinTitle"]')
        if actual_title:
            title = self.tostring(actual_title[0], encoding=six_unicode, method="text").strip()
        else:
            title = self.tostring(tdiv, encoding=six_unicode, method="text").strip()
        ans = re.sub(r"[(\[].*[)\]]", "", title).strip()
        if not ans:
            ans = title.rpartition("[")[0].strip()
        return ans

    def parse_authors(self, root):
        matches = cssselect("#byline .author .contributorNameID")(root)
        if not matches:
            matches = cssselect("#byline .author a.a-link-normal")(root)
        if matches:
            authors = [self.totext(x) for x in matches]
            return [a for a in authors if a]

        x = (
            '//h1[contains(@class, "parseasinTitle")]/following-sibling::span/*[(name()="a" and @href) or '
            '(name()="span" and @class="contributorNameTrigger")]'
        )
        aname = root.xpath(x)
        if not aname:
            aname = root.xpath(
                """
            //h1[contains(@class, "parseasinTitle")]/following-sibling::*[(name()="a" and @href) or (name()="span" and
            @class="contributorNameTrigger")]
                    """
            )
        for x in aname:
            x.tail = ""
        authors = [self.tostring(x, encoding=six_unicode, method="text").strip() for x in aname]
        authors = [a for a in authors if a]
        return authors

    def parse_rating(self, root):
        for x in root.xpath('//div[@id="cpsims-feature" or @id="purchase-sims-feature" or @id="rhf"]'):
            # Remove the similar books section as it can cause sppurious
            # ratings matches
            x.getparent().remove(x)

        rating_paths = (
            '//div[@data-feature-name="averageCustomerReviews"]',
            '//div[@class="jumpBar"]/descendant::span[contains(@class,"asinReviewsSummary")]',
            '//div[@class="buying"]/descendant::span[contains(@class,"asinReviewsSummary")]',
            '//span[@class="crAvgStars"]/descendant::span[contains(@class,"asinReviewsSummary")]',
        )
        ratings = None
        for p in rating_paths:
            ratings = root.xpath(p)
            if ratings:
                break
        if ratings:
            for elem in ratings[0].xpath("descendant::*[@title]"):
                t = elem.get("title").strip()
                m = self.ratings_pat.match(t)
                if m is not None:
                    return float(m.group(1)) / float(m.group(3)) * 5

    def _render_comments(self, desc):
        from LiuXin.library.comments import sanitize_comments_html

        for c in desc.xpath("descendant::noscript"):
            c.getparent().remove(c)
        for c in desc.xpath(
            'descendant::*[@class="seeAll" or' ' @class="emptyClear" or @id="collapsePS" or' ' @id="expandPS"]'
        ):
            c.getparent().remove(c)

        for a in desc.xpath("descendant::a[@href]"):
            del a.attrib["href"]
            a.tag = "span"
        desc = self.tostring(desc, method="html", encoding=six_unicode).strip()

        # Encoding bug in Amazon data U+fffd (replacement char)
        # in some examples it is present in place of '
        desc = desc.replace("\ufffd", "'")
        # remove all attributes from tags
        desc = re.sub(r"<([a-zA-Z0-9]+)\s[^>]+>", r"<\1>", desc)
        # Collapse whitespace
        # desc = re.sub('\n+', '\n', desc)
        # desc = re.sub(' +', ' ', desc)
        # Remove the notice about text referring to out of print editions
        desc = re.sub(r"(?s)<em>--This text ref.*?</em>", "", desc)
        # Remove comments
        desc = re.sub(r"(?s)<!--.*?-->", "", desc)
        return sanitize_comments_html(desc)

    def parse_comments(self, root):
        ans = ""
        ns = cssselect("#bookDescription_feature_div noscript")(root)
        if ns:
            ns = ns[0]
            if len(ns) == 0 and ns.text:
                import html5lib

                # liuxin_html5lib parsed noscript as CDATA
                ns = html5lib.parseFragment(
                    "<div>%s</div>" % ns.text,
                    treebuilder="lxml",
                    namespaceHTMLElements=False,
                )[0]
            else:
                ns.tag = "div"
            ans = self._render_comments(ns)
        else:
            desc = root.xpath('//div[@id="ps-content"]/div[@class="content"]')
            if desc:
                ans = self._render_comments(desc[0])

        desc = root.xpath('//div[@id="productDescription"]/*[@class="content"]')
        if desc:
            ans += self._render_comments(desc[0])
        return ans

    def parse_series(self, root):
        ans = (None, None)
        desc = root.xpath('//div[@id="ps-content"]/div[@class="buying"]')
        if desc:
            raw = self.tostring(desc[0], method="text", encoding=six_unicode)
            raw = re.sub(r"\s+", " ", raw)
            match = self.series_pat.search(raw)
            if match is not None:
                s, i = match.group("series"), float(match.group("index"))
                if s:
                    ans = (s, i)
        return ans

    def parse_tags(self, root):
        ans = []
        exclude_tokens = {"kindle", "a-z"}
        exclude = {
            "special features",
            "by authors",
            "authors & illustrators",
            "books",
            "new; used & rental textbooks",
        }
        seen = set()
        for li in root.xpath(self.tags_xpath):
            for i, a in enumerate(li.iterdescendants("a")):
                if i > 0:
                    # we ignore the first category since it is almost always too broad
                    raw = (a.text or "").strip().replace(",", ";")
                    lraw = icu_lower(raw)
                    tokens = frozenset(lraw.split())
                    if raw and lraw not in exclude and not tokens.intersection(exclude_tokens) and lraw not in seen:
                        ans.append(raw)
                        seen.add(lraw)
        return ans

    def parse_cover(self, root, raw=b""):
        """
        Look for the image URL in javascript, using the first image in the image gallery as the cover
        :param root:
        :param raw:
        :return:
        """
        # imgpat = re.compile(r"""'imageGalleryData'\s*:\s*(\[\s*{.+])""")
        img_pat = re.compile(r"""'imageGalleryData'\s*:\s*(\[\s*\{.+])""")
        for script in root.xpath("//script"):
            m = img_pat.search(script.text or "")
            if m is not None:
                import json

                try:
                    return json.loads(m.group(1))[0]["mainUrl"]
                except Exception as e:
                    err_str = "Problem trying to parse cover from root"
                    self.log.log_exception(err_str, e, "DEBUG", ("root", root))
                    continue

        def clean_img_src(image_src):
            parts = image_src.split("/")
            if len(parts) > 3:
                bn = parts[-1]
                sparts = bn.split("_")
                if len(sparts) > 2:
                    bn = re.sub(r"\.\.jpg$", ".jpg", (sparts[0] + sparts[-1]))
                    return ("/".join(parts[:-1])) + "/" + bn

        img_pat_2 = re.compile(r'var imageSrc = "([^"]+)"')
        for script in root.xpath("//script"):
            m = img_pat_2.search(script.text or "")
            if m is not None:
                src = m.group(1)
                url = clean_img_src(src)
                if url:
                    return url

        imgs = root.xpath(
            '//img[(@id="prodImage" or @id="original-main-image" or @id="main-image" or @id="main-image-nonjs") and @src]'
        )
        if not imgs:
            imgs = root.xpath('//div[@class="main-image-inner-wrapper"]/img[@src]')
            if not imgs:
                imgs = root.xpath('//div[@id="main-image-container"]//img[@src]')
        for img in imgs:
            src = img.get("src")
            if "data:" in src:
                continue
            if "loading-" in src:
                js_img = re.search(rb'"largeImage":"(http://[^"]+)",', raw)
                if js_img:
                    src = js_img.group(1).decode("utf-8")
            if "/no-image-avail" not in src and "loading-" not in src and "/no-img-sm" not in src:
                self.log("Found image: %s" % src)
                url = clean_img_src(src)
                if url:
                    return url

    def parse_new_details(self, root, mi, non_hero):
        table = non_hero.xpath("descendant::table")[0]
        for tr in table.xpath("descendant::tr"):
            cells = tr.xpath("descendant::td")
            if len(cells) == 2:
                name = self.totext(cells[0])
                val = self.totext(cells[1])
                if not val:
                    continue
                if name in self.language_names:
                    ans = self.lang_map.get(val, None)
                    if not ans:
                        ans = canonicalize_lang(val)
                    if ans:
                        mi.language = ans
                elif name in self.publisher_names:
                    pub = val.partition(";")[0].partition("(")[0].strip()
                    if pub:
                        mi.publisher = pub
                    date = val.rpartition("(")[-1].replace(")", "").strip()
                    try:
                        from LiuXin.utils.date import parse_only_date

                        date = self.delocalize_datestr(date)
                        mi.pubdate = parse_only_date(date, assume_utc=True)
                    except Exception as e:
                        err_msg = "Failed to parse pubdate: %s" % val
                        self.log.log_exception(err_msg, e, "ERROR")
                elif name in {"ISBN", "ISBN-10", "ISBN-13"}:
                    ans = check_isbn(val)
                    if ans:
                        self.isbn = mi.isbn = ans

    @staticmethod
    def parse_isbn(pd):
        items = pd.xpath('descendant::*[starts-with(text(), "ISBN")]')
        if not items:
            items = pd.xpath('descendant::b[contains(text(), "ISBN:")]')
        for x in reversed(items):
            if x.tail:
                ans = check_isbn(x.tail.strip())
                if ans:
                    return ans

    def parse_publisher(self, pd):
        for x in reversed(pd.xpath(self.publisher_xpath)):
            if x.tail:
                ans = x.tail.partition(";")[0]
                return ans.partition("(")[0].strip()

    def parse_pubdate(self, pd):
        for x in reversed(pd.xpath(self.publisher_xpath)):
            if x.tail:
                from LiuXin.utils.date import parse_only_date

                ans = x.tail
                date = ans.rpartition("(")[-1].replace(")", "").strip()
                date = self.delocalize_datestr(date)
                return parse_only_date(date, assume_utc=True)

    def parse_language(self, pd):
        for x in reversed(pd.xpath(self.language_xpath)):
            if x.tail:
                raw = x.tail.strip().partition(",")[0].strip()
                ans = self.lang_map.get(raw, None)
                if ans:
                    return ans
                ans = canonicalize_lang(ans)
                if ans:
                    return ans


# }}}


if __name__ == "__main__":  # tests {{{
    # To run these test use: calibre-debug src/calibre/ebooks/metadata/sources/amazon.py
    from LiuXin.metadata.web_sources.test import (
        test_identify_plugin,
        isbn_test,
        title_test,
        authors_test,
        comments_test,
    )

    com_tests = [  # {{{
        (  # + in title and uses id="main-image" for cover
            {"title": "C++ Concurrency in Action"},
            [
                title_test("C++ Concurrency in Action: Practical Multithreading", exact=True),
            ],
        ),
        (  # noscript description
            {"identifiers": {"amazon": "0756407117"}},
            [
                title_test("Throne of the Crescent Moon"),
                comments_test("Makhslood"),
                comments_test("Dhamsawaat"),
            ],
        ),
        (  # Different comments markup, using Book Description section
            {"identifiers": {"amazon": "0982514506"}},
            [
                title_test(
                    "Griffin's Destiny: Book Three: The Griffin's Daughter Trilogy",
                    exact=True,
                ),
                comments_test("Jelena"),
                comments_test("Ashinji"),
            ],
        ),
        (  # # in title
            {"title": "Expert C# 2008 Business Objects", "authors": ["Lhotka"]},
            [
                title_test("Expert C# 2008 Business Objects"),
                authors_test(["Rockford Lhotka"]),
            ],
        ),
        (  # Description has links
            {"identifiers": {"isbn": "9780671578275"}},
            [
                title_test("A Civil Campaign: A Comedy of Biology and Manners", exact=True),
                authors_test(["Lois McMaster Bujold"]),
            ],
        ),
        (  # Sophisticated comment formatting
            {"identifiers": {"isbn": "9781416580829"}},
            [
                title_test("Angels & Demons - Movie Tie-In: A Novel", exact=True),
                authors_test(["Dan Brown"]),
            ],
        ),
        (  # No specific problems
            {"identifiers": {"isbn": "0743273567"}},
            [
                title_test("The great gatsby", exact=True),
                authors_test(["F. Scott Fitzgerald"]),
            ],
        ),
        (  # A newer book
            {"identifiers": {"amazon": "B004JHY6OG"}},
            [title_test("The Heroes", exact=False), authors_test(["Joe Abercrombie"])],
        ),
    ]  # }}}

    de_tests = [  # {{{
        (
            {"identifiers": {"isbn": "3548283519"}},
            [
                title_test(
                    "Wer Wind Sät: Der Fünfte Fall Für Bodenstein Und Kirchhoff",
                    exact=False,
                ),
                authors_test(["Nele Neuhaus"]),
            ],
        ),
    ]  # }}}

    it_tests = [  # {{{
        (
            {"identifiers": {"isbn": "8838922195"}},
            [
                title_test("La briscola in cinque", exact=True),
                authors_test(["Marco Malvaldi"]),
            ],
        ),
    ]  # }}}

    fr_tests = [  # {{{
        (
            {"identifiers": {"isbn": "2221116798"}},
            [
                title_test("L'étrange voyage de Monsieur Daldry", exact=True),
                authors_test(["Marc Levy"]),
            ],
        ),
    ]  # }}}

    es_tests = [  # {{{
        (
            {"identifiers": {"isbn": "8483460831"}},
            [
                title_test("Tiempos Interesantes", exact=True),
                authors_test(["Terry Pratchett"]),
            ],
        ),
    ]  # }}}

    jp_tests = [  # {{{
        (  # Adult filtering test
            {"identifiers": {"isbn": "4799500066"}},
            [
                title_test("Ｂｉｔｃｈ Ｔｒａｐ"),
            ],
        ),
        (  # isbn -> title, authors
            {"identifiers": {"isbn": "9784101302720"}},
            [title_test("精霊の守り人", exact=True), authors_test(["上橋 菜穂子"])],
        ),
        (  # title, authors -> isbn (will use Shift_JIS encoding in query.)
            {"title": "考えない練習", "authors": ["小池 龍之介"]},
            [
                isbn_test("9784093881067"),
            ],
        ),
    ]  # }}}

    br_tests = [  # {{{
        (
            {"title": "Guerra dos Tronos"},
            [
                title_test("A Guerra dos Tronos - As Crônicas de Gelo e Fogo", exact=True),
                authors_test(["George R. R. Martin"]),
            ],
        ),
    ]  # }}}

    def do_test(domain, start=0, stop=None):
        tests = globals().get(domain + "_tests")
        if stop is None:
            stop = len(tests)
        tests = tests[start:stop]
        test_identify_plugin(
            Amazon.name,
            tests,
            modify_plugin=lambda p: setattr(p, "testing_domain", domain),
        )

    do_test("com")

    # do_test('de')

# }}}
