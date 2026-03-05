# Searches google books using the google books api
# Project name: LiuXin
# project id: liuxin-1254
# project api key name: LiuXin Test Key
# project api key: AIzaSyDiuIq9ak-xBtnvx6q2rpDd1aSoQ-uNPzQ

from __future__ import unicode_literals, print_function

import requests
import pprint
from copy import deepcopy

from LiuXin.utils.localization import _

from LiuXin.metadata.web_sources.base import Source
from LiuXin.metadata import check_isbn
from LiuXin.metadata.metadata import MetaData

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode


class GoogleBooks(Source):
    """
    Source for Google Books.
    Searches Google Books - parses the search page for the google books pages.
    """

    name = "Google"
    description = _("Downloads metadata and covers from Google Books")

    capabilities = frozenset(["identify"])
    touched_fields = frozenset(
        [
            "title",
            "authors",
            "tags",
            "pubdate",
            "comments",
            "publisher",
            "identifier:isbn",
            "rating",
            "identifier:google",
            "languages",
        ]
    )

    supports_gzip_transfer_encoding = True
    cached_cover_url_is_reliable = False

    GOOGLE_COVER = "http://books.google.com/books?id=%s&printsec=frontcover&img=1"

    DUMMY_IMAGE_MD5 = frozenset(["0de4383ebad0adad5eeb8975cd796657"])

    GOOGLE_BOOKS_API_ENTRY = "https://www.googleapis.com/books/v1/volumes"
    GOOGLE_API_KEY = "AIzaSyDiuIq9ak-xBtnvx6q2rpDd1aSoQ-uNPzQ"

    def __init__(self):
        """
        Startup the module, innitializing the connection to the google books api for later use.
        :return:
        """
        Source.__init__(self)

    def limited_results(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        result_count=10,
    ):
        """
        Returns a limited subset of the results - limited by
        :param log:
        :param result_queue:
        :param abort:
        :param title:
        :param authors:
        :param identifiers:
        :param timeout:
        :param result_count:
        :return:
        """
        query = self.create_query(log=log, title=title, authors=authors, identifiers=identifiers)

        params = {
            "q": query,
            "key": self.GOOGLE_API_KEY,
            "maxResults": six_unicode(result_count),
        }
        r = requests.get(url=self.GOOGLE_BOOKS_API_ENTRY, params=params)
        r_json = r.json()

        self.parse_json(log, r_json, result_queue)

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
        Tries to search google books to find a record for the specified book.
        :param log:
        :param result_queue:
        :param abort:
        :param title:
        :param authors:
        :param identifiers:
        :param timeout:
        :return:
        """
        query = self.create_query(log=log, title=title, authors=authors, identifiers=identifiers)

        params = {"q": query, "key": self.GOOGLE_API_KEY, "maxResults": "5"}
        r = requests.get(url=self.GOOGLE_BOOKS_API_ENTRY, params=params)
        r_json = r.json()

        self.parse_json(log, r_json, result_queue)

    def create_query(self, log, title=None, authors=None, identifiers=None):
        """
        Create a query for the google books API - this is a string seperated by '+', with search paramters like intitle
        inauthor e.t.c. THis string will then be used as the query parameter when actually searching the google books
        api.
        :param log:
        :param title:
        :param authors:
        :param identifiers:
        :return:
        """
        if identifiers is None:
            identifiers = {}
        q = ""

        # If one of the recognized unique ids is found, use that and discard the other information.
        isbn = check_isbn(identifiers.get("isbn", None))
        if isbn is not None:

            q += "isbn:{0}".format(isbn)

        elif title or authors:

            def build_term(prefix, parts):
                return "+".join("in" + prefix + ":" + x for x in parts)

            title_tokens = list(self.get_title_tokens(title))
            if title_tokens:
                q += build_term("title", title_tokens)
            author_tokens = self.get_author_tokens(authors, only_first_author=True)
            if author_tokens:
                q += ("+" if q else "") + build_term("author", author_tokens)

        if isinstance(q, unicode):
            q = q.encode("utf-8")
        if q.endswith("+"):
            q = q[:-1]
        return q

    def parse_json(self, log, json_return, result_queue):
        """
        Parse a json return from the google books api.
        :param json_return:
        :return:
        """
        # Iterate through the returned books, parsing the MetaData in to a MetaData object and putting that in the
        # result_queue
        # Everything in the metadata dictionary should be accounted for - if it isn't then complain and log that it
        # wasn't
        for json_metadata in json_return["items"]:

            json_md = deepcopy(json_metadata)
            md = MetaData()

            # print(pprint.pformat(json_md))

            # Remove the chunks of the json metadata which are simply not considered
            self.__safe_remove(log, json_md, "accessInfo")
            self.__safe_remove(log, json_md, "kind")
            self.__safe_remove(log, json_md, "saleInfo")
            self.__safe_remove(log, json_md, "searchInfo")
            self.__safe_remove(log, json_md, "selfLink")

            # Parse google identifiers
            if "etag" in json_md:
                md.set_identifier(typ="google_etag", val=json_md["etag"])
                self.__safe_remove(log, json_md, "etag")
            if "id" in json_md:
                md.set_identifier(typ="google_id", val=json_md["id"])
                self.__safe_remove(log, json_md, "id")

            # Parsing the volumeInfo - the bulk of the Metadata
            md_dict = deepcopy(json_md["volumeInfo"])

            # Remove the volumeInfo - check to see if the json_md is empty - if it isn;t log it
            self.__safe_remove(log, json_md, "volumeInfo")
            if json_md:
                info_str = "json_md still has content after removing all the known fields."
                log.log_variables(info_str, "INFO", ("json_md", json_md))

            # Removing the unhelpful metadata fields
            self.__safe_remove(log, md_dict, "allowAnonLogging")
            self.__safe_remove(log, md_dict, "canonicalVolumeLink")
            self.__safe_remove(log, md_dict, "contentVersion")
            self.__safe_remove(log, md_dict, "infoLink")
            self.__safe_remove(log, md_dict, "imageLinks")
            self.__safe_remove(log, md_dict, "maturityRating")
            self.__safe_remove(log, md_dict, "previewLink")
            self.__safe_remove(log, md_dict, "printType")
            self.__safe_remove(log, md_dict, "readingModes")
            self.__safe_remove(log, md_dict, "ratingsCount")

            # Parse the authors into the metadata object
            if "authors" in md_dict:
                authors_dict = {"authors": md_dict["authors"]}
                md.add_creators(authors_dict)
                self.__safe_remove(log, md_dict, "authors")

            if "averageRating" in md_dict:
                rating = md_dict["averageRating"]
                md.ratings = ("google", rating)
                self.__safe_remove(log, md_dict, "averageRating")

            # Parse categories - adding to tags
            if "categories" in md_dict:
                md_categories = md_dict["categories"]
                for tag in md_categories:
                    md.tag = tag
                self.__safe_remove(log, md_dict, "categories")

            if "description" in md_dict:
                md.synopses = md_dict["description"]
                self.__safe_remove(log, md_dict, "description")

            # Parse the industry identifiers - which contains the ISBN and other information
            if "industryIdentifiers" in md_dict:

                industry_ids = md_dict["industryIdentifiers"]
                identifiers = dict()

                # If an 'other' section of identifiers exists, removes it and tries to process it
                if "other" in industry_ids:
                    other_ids = industry_ids["other"]

                    for id_string in other_ids:
                        id_tokens = id_string.split(":")
                        if len(id_tokens) == 2:
                            id_type = six_unicode(id_tokens[0]).lower().strip()
                            id_val = id_tokens[1]
                            if id_type in identifiers:
                                identifiers[id_type].add(id_val)
                            else:
                                identifiers[id_type] = set()
                                identifiers[id_type].add(id_val)
                            continue
                        else:
                            info_str = "Unable to parse given id string"
                            log.log_variables(info_str, "INFO", ("id_string", id_string))

                    self.__safe_remove(log, industry_ids, "other")

                for id_dict in industry_ids:
                    id_type = id_dict["type"].lower().strip()
                    if id_type in identifiers:
                        identifiers[id_type].add(id_dict["identifier"])
                    else:
                        identifiers[id_type] = set()
                        identifiers[id_type].add(id_dict["identifier"])
                md.add_identifiers(identifiers)

                self.__safe_remove(log, md_dict, "industryIdentifiers")

            if "language" in md_dict:
                language = md_dict["language"]
                md.language = language
                self.__safe_remove(log, md_dict, "language")

            if "pageCount" in md_dict:
                page_count = md_dict["pageCount"]
                md.page_count = page_count
                self.__safe_remove(log, md_dict, "pageCount")

            if "publishedDate" in md_dict:
                pub_date = md_dict["publishedDate"]
                md.pubdate = pub_date
                self.__safe_remove(log, md_dict, "publishedDate")

            if "publisher" in md_dict:
                publisher = md_dict["publisher"]
                md.publisher = publisher
                self.__safe_remove(log, md_dict, "publisher")

            if "subtitle" in md_dict:
                subtitle = md_dict["subtitle"]
                md.tag = subtitle
                self.__safe_remove(log, md_dict, "subtitle")

            if "title" in md_dict:
                title = md_dict["title"]
                md.title = title
                self.__safe_remove(log, md_dict, "title")

            # If the md_dict still has stuff in it, log the fact and the dictionary for later improvement worl
            if md_dict:
                err_str = "md_dict was not fully emptied by removing the known categories."
                log.log_variables(err_str, "INFO", ("md_dict", md_dict))

            result_queue.put(md)

    def __safe_remove(self, log, json_md, key):
        """
        Safely remove a key from a json_md object - logging the error if something goes wrong.
        :param log:
        :param json_md:
        :param key:
        :return:
        """
        try:
            del json_md[key]
        except KeyError:
            pass
        except Exception as e:
            wrn_str = "Couldn't delete target key - an unexpected error occured."
            log.log_exception(wrn_str, e, "DEBUG", ("json_md", pprint.pformat(json_md)), ("key", key))
        return json_md
