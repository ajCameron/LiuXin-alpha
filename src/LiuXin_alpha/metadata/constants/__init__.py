
"""
Constants used for metadata standardization e.t.c.
"""

import re

from collections import OrderedDict

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper


__author__ = "Cameron"

__md_version__ = (0, 0, 1)

# Constants used by the metadata subsystem to do its thing

# Recognized ratings types
RATING_TYPES = frozenset(
    [
        "amazon (us)",
        "amazon (uk)",
        "user",
        "calibre",
        "goodreads",
        "librarything",
        "lovereading",
    ]
)

# Patterns used by the isbn extractor to try and extract ISBNs from strings.
ISBN_PATTENRS = {r"[0-9-/_\\xX\s]+"}

# Used to try and cleanly remove ISBN strings from a string which is being searched for metadata
ISBN_DROP_PATTENRS = [r"(\s*ISBN\s*.*)", r"[0-9-/_\\xX\s]+"]


DICT_OF_SETS_METADATA_FIELDS = ["identifiers", "internal_identifiers", "creators"]


CREATOR_ROLE_REKEY_SCHEME = {
    frozenset(["author", "authors", "aut", "writer", "writers"]): "authors",
    frozenset(["editors", "editor", "ed"]): "editors",
    frozenset(["producers", "producer"]): "producers",
    frozenset(["directors", "director"]): "directors",
    frozenset(["composer", "composers"]): "composers",
    frozenset(["artists", "artist"]): "artists",
    frozenset(["cover_artist", "cover_artists"]): "cover_artists",
    frozenset(["illustrator", "illustrators"]): "illustrators",
    frozenset(["colorists", "colourists", "colorist", "colourist"]): "colorists",
    frozenset(["trl", "translator"]): "translators",
    frozenset(["bkp", "book_producer"]): "book_producer",
}

# What types of creator are recognized by MetaData (and the databases)
CREATOR_CATEGORIES = CREATOR_ROLE_REKEY_SCHEME.values()

CREATOR_TYPE_CAT_DIR = dict((plural_singular_mapper(cc), cc) for cc in CREATOR_CATEGORIES)

# Todo: This should now be stored in the databases
CREATOR_TYPES = set([c for c in CREATOR_CATEGORIES])

# Keyed with the short version (an OPF MARC code) valued with the full name
# The MARC codes for the creator role are the keyes and the values are the full names - which serve as the allowed
# creator_x_type values
# List taken from - http://www.idpf.org/epub/20/spec/OPF_2.0.1_draft.htm#Section2.2.6 - see there for an explanation of
# some of the more obscure roles
# For a full list see the MARC code list for Relators here - http://id.loc.gov/vocabulary/relators.html
# I'm not going to copy them all out, so add more as needed

# Todo: The values for the marc roles and the CREATOR_CATEGORIES should, really, be the same
marc_creator_roles_dict = {
    "adp": "adapter",
    "ann": "annotator",
    "art": "artist",
    "aut": "author",
    "aqt": "quotations_author",
    "aft": "afterword_author",
    "aui": "introduction_author",
    "ant": "bibliographic_antecedant",
    "bkp": "book_producer",
    "clb": "collaborator",
    "clr": "colorist",
    "cmm": "commentator",
    "drt": "director",
    "dsr": "designer",
    "edt": "editor",
    "ill": "illustrator",
    "lyr": "lyricist",
    "mdc": "metadata_contact",
    "mus": "musician",
    "nrt": "narrator",
    "oth": "other",
    "pht": "photographer",
    "prt": "printer",
    "red": "redactor",
    "rev": "reviewer",
    "spn": "sponsor",
    "ths": "thesis_advisor",
    "trc": "transcriber",
    "trl": "translator",
}

creator_marc_roles_dict = dict((y, x) for x, y in marc_creator_roles_dict.items())


# Keyed with a set of all the things which should be mapped to the given marc code
CREATOR_ROLE_MARC_REKEY_DICT = {
    frozenset(["author", "authors", "aut"]): "aut",
    frozenset(["editors", "editor", "ed"]): "edt",
    frozenset(["producers", "producer", "film_producer"]): "fmp",
    frozenset(["directors", "director"]): "drt",
    frozenset(["composer", "composers"]): "cmp",
    frozenset(["artists", "artist"]): "art",
    frozenset(["cover_artist", "cover_artists", "cover artiste", "cover designer"]): "cov",
    frozenset(["illustrator", "illustrators"]): "ill",
    frozenset(["colorists", "colourists", "colorist", "colourist"]): "clr",
    frozenset(["trl", "translator", "translators"]): "trl",
    frozenset(["bkp", "book_producer", "book_producers"]): "bkp",
}


def creator_to_marc(role):
    """
    Translate a human readable form of the person's role in the book into a MARC code.
    :param role:
    :return:
    """
    role = role.lower()
    for rekey_set in CREATOR_ROLE_MARC_REKEY_DICT:
        if role in rekey_set:
            return CREATOR_ROLE_MARC_REKEY_DICT[rekey_set]

    err_str = "role not found in CREATOR_ROLE_MARC_REKEY_DICT"
    err_str = default_log.log_variables(err_str, "ERROR", ("role", role))
    raise KeyError(err_str)


EXTERNAL_EBOOK_REKEY_SCHEME = {
    frozenset(["isbn", "isbn_10", "isbn10", "isbn_13", "isbn13"]): "isbn",
    frozenset(["douban"]): "douban",
    frozenset(["google"]): "google",
    frozenset(
        [
            "amazon",
            "amzon",
            "amzn",
            "asin",
            "asin-504",
            "asin_504",
            "mobi-asin",
            "mobi_asin",
        ]
    ): "amazon",
    frozenset(["ff"]): "ff",
    frozenset(["goodreads"]): "goodreads",
    frozenset(["oclc", "onlinecomputerlibrarycentre"]): "oclc",
    frozenset(["lccn", "librarycongresscataolguenumber"]): "lccn",
    frozenset(["issn", "internationalstandardserialnumber"]): "issn",
    frozenset(["doi"]): "doi",
    frozenset(
        [
            "urn",
            "uniform_resource_name",
            "uniformresourcename",
            "uri",
            "uniform_resource_identifier",
            "uniformresourceidentifier",
        ]
    ): "uri",
}
EXTERNAL_EBOOK_ID_SCHEMA: frozenset[str] = frozenset(EXTERNAL_EBOOK_REKEY_SCHEME.values())

INTERNAL_EBOOK_REKEY_SCHEME: dict[frozenset[str], str] = {frozenset(["uuid", "calibre"]): "uuid"}
INTERNAL_EBOOK_ID_SCHEMA = frozenset(INTERNAL_EBOOK_REKEY_SCHEME.values())

# Todo: Test that this is, at least, minimally true - contains the min values - in tests
ALL_ID_TYPES = frozenset(
    [_ for _ in EXTERNAL_EBOOK_REKEY_SCHEME.values()] + [_ for _ in INTERNAL_EBOOK_REKEY_SCHEME.values()]
)

# If an entry is None it's assumed to be a unicode field, which should only take a single value
# Note - other creator types apart from author are recognized - the full list is below

# Todo: Update metadata explanations
# Todo: languages and languages_available should probably both be of the same type
# Some values, such as all the types of identifier and creaotr roles, are added in at run time
METADATA_NULL_VALUES = {
    "application_id": "",
    "comments": OrderedDict(),
    "cover_data": OrderedDict(),
    "creator_sort": "",
    "creators": OrderedDict(),
    "custom_field_keys": [],
    "custom_fields": {},
    "device_collections": [],
    "doc_type": None,
    "genre": OrderedDict(),
    "filename": [],
    "filepath": [],
    "files": OrderedDict(),
    "imprint": OrderedDict(),
    "internal_identifiers": OrderedDict(),
    "language": "und",
    "languages": [],
    "languages_available": OrderedDict(),
    "last_modified": None,
    "metadata_date": None,
    "metadata_language": "und",
    "notes": OrderedDict(),
    "program_str": "LiuXin",
    "pubdate": None,
    "publisher": OrderedDict(),
    "publication_tye": "ebook",
    "ratings": OrderedDict(),
    "rights": None,
    "series": OrderedDict(),
    "series_index": OrderedDict(),
    "subject": OrderedDict(),
    "synopses": OrderedDict(),
    "tags": OrderedDict(),
    "timestamp": None,
    "title": "",
    "title_sort": None,
    "user_metadata": {},
    "wordcount": None,
}

# Add in all the creator categories, and the two types of identifier categories
for category in CREATOR_CATEGORIES:
    METADATA_NULL_VALUES[category] = OrderedDict()

# Add in all the external ids
for id_type in EXTERNAL_EBOOK_ID_SCHEMA:
    METADATA_NULL_VALUES[id_type] = OrderedDict()

# Add in the internal_ids
for id_type in INTERNAL_EBOOK_ID_SCHEMA:
    METADATA_NULL_VALUES[id_type] = OrderedDict()


# Todo: Update all of these
METADATA_EXPLANATIONS = {
    "comments": "comments on the file",
    "cover_data": "Takes a tuple of the form image type, followed by raw data (if that type "
    "is a data format) or the path if that type is a path a list so that you "
    "can add more than one cover.",
    "creators": "the name of whoever created this work",
    "creators_id_map": "map of the creator name(s) to creator entries in the LiuXin databases",
    "creator_type": "author, director, composer e.t.c - more flexibility is good",
    "creator_sort": "The sort key for this entry in the creators table. If the user has set a "
    "custom one. I'd like it a lot if the user didn't do this thing. But no "
    "accounting for taste",
    "custom_field_keys": "Any custom fields that the user has requested to be included and it would"
    " be nice if this where possible but, if wishes where fishes, I'd feed"
    " Africa.",
    "custom_fields": "Data for those custom fields, keyed by the custom field keys.",
    "device_collections": "devices the file is customized. [] means all.",
    "doc_type": "ebook, movie, T.V., e.t.c",
    "genre": "What it says on the tin. Often confusingly mixed with subject.",
    "identifiers": "Identifiers for the title. A dict keyed by the name of the identifier "
    "value is a set of the identifiers corresponding to the title because it "
    "doesnt really matter to us if it's the hardcover or the paperback (unless"
    " it does) includes Douban, Google. ISBN, Amazon, FF, Goodreads",
    "internal_identifiers": "LiuXin's custom hash, as well as MD5 and any uuid pulled from the file. A "
    "dict keyed off the name of the identifier - as for the identifiers "
    "structure.",
    "language": "the default language for the document (for the author, title e.t.c)",
    "languages": "The languages in the document.",
    "languages_available": "The languages available in the document - mostly just the same as language."
    " Might be more complicated in the case where a TV show or a movie has subs"
    " and dubs.",
    "pubdate": "The publication date of the document",
    "publisher": "The publisher(s) of the document",
    "publisher_id_map": "A map between the publisher name and the  corresponding entry in the " "LiuXin databases.",
    "producer": "if it matters - I guess would be the editor of an anthology",
    "ratings": "Keyed off the name of the ratings body. Valued by the rating.",
    "rights": "Who owns the rights to a work",
    "series": "Which series the work belongs to",
    "series_id_map": "A map between the series and the LiuXin databases series.",
    "series_index": "The position of the title in the various series. "
    "Keyed with the name of the series and valued with the position of the "
    "title in that series.",
    "subject": "What it says on the tin.",
    "synopsis": "An HTML formatted synopsis of the title.",
    "tags": "A set of tags for the title",
    "tags_id_map": "A mapping between the given tags and the tag_ids in the LiuXin databases",
    "timestamp": "When the work was added to the databases",
    "title": "The title of the work",
    "title_id_map": "A mapping between the title of the work and a title_id in the LiuXin" " databases",
    "user_metadata": "Keyed off the metadata name.",
    "wordcount": "The wordcount of the document (freely avalible in some formats).",
}

# Todo: Check the explanations are all they should be

# METADATA_NULL_VALUES explanation
# comments - comments on the file
# cover_data - takes a tuple of the form image type, followed by raw data (if that type is a data format)
#            - or the path if that type is a path
#            - a list so that you can add more than one cover
# creators - the name of whoever created this work
# creator_type - author, director, composer e.t.c - more flexibility is good
# creator_sort - the sort key for this entry in the creators table. If the user has set a custom one.
#              - I'd like it a lot if the user didn't do this thing. But no accounting for taste
# custom_field_keys - any custom fields that the user has requested to be included
#                   - and it would be nice if this where possible
#                   - but, if wishes where fishes, I'd feed Africa
# custom_fields - data for those custom fields, keyed by the custom field keys
# device_collections - devices the file is customized. [] means all.
# doc_type - ebook, movie, T.V., e.t.c
# genre - what it says on the tin. Often confusingly mixed with subject.
#       - quite a lot of mappings are going to have to be employed
# identifiers - does what it says on the tin. Identifiers for the title.
#             - Keyed by the name of the identifier - value is a set of the identifiers corresponding to the title
#             - because it doesnt;t really matter to us if it's the hardcover or the paperback (unless it does)
#             - includes Douban, Google. ISBN, Amazon, FF, Goodreads
# internal_identifiers - LiuXin's custom hash, as well as MD5 and any uuid pulled from the file
# language - the default language of the document
# languages_available - the languages available in the document - mostly just the same as language
# languages_in_doc - the languages in the document
# pubdate - the production date of the document
# publisher - the publisher of the document
# publisher_id_map - a mapping between the publisher name and their actual id in the publishers table
# producer - if it matters - I guess would be the editor of an anthology
# ratings - keyed by the name of the ratings body - should probably be more complex
#         - but there is a limted extent to which I care about ratings
# rights - who owns the rights to a work.
# series - which series the work belongs to
# series_sort - if the user has set a custom series sort.
# series_index - the position of the title in the series
# subject - what it says on the tin
# synopsis - likewise
# tags - likewise
# timestamp - a timestamp for when the life was produced/added
# title - the title of the document
# title_sort - If the user has set custom sorts for the title.
# user_metadata - any user metadata found in an OPF file


# Todo: Move to the actual standardization plugins
def canonicalize_id_name(candidate_id):
    """
    Takes a ID name - makes reasonable guesses as to what the original id name could have been. Returns it.
    :param candidate_id: The name of the id to standardize
    """
    candidate_id = candidate_id.strip().lower()
    candidate_id = re.sub(r"\s+", "_", candidate_id)
    for rekey_set in EXTERNAL_EBOOK_REKEY_SCHEME:
        if candidate_id in rekey_set:
            return EXTERNAL_EBOOK_REKEY_SCHEME[rekey_set]

    for rekey_set in INTERNAL_EBOOK_ID_SCHEMA:
        if candidate_id in rekey_set:
            return INTERNAL_EBOOK_ID_SCHEMA[rekey_set]

    return candidate_id


ALLOWED_KEYS = {
    "creators": CREATOR_CATEGORIES,
    "identifiers": EXTERNAL_EBOOK_ID_SCHEMA,
    "internal_identifiers": INTERNAL_EBOOK_ID_SCHEMA,
}

assert set(ALLOWED_KEYS.keys()) == set(DICT_OF_SETS_METADATA_FIELDS)

# Creator autogenerated drop strings
# Some programs add themselves to the creators of a work - look out for these auto-generated additions and remove them
CREATOR_DROP_REGEX_SET = {
    r"HTML Tidy for Mac OS X \(vers [a-zA-Z0-9\s]+\), see www\.w3\.org",
    r"n//a",
    r"n/a",
}

# Producer seems to be automatically set by many of the programs which produce PDFs.
# This is a list of regex patterns which should match the name of many of these programs
# These can be discounted as giving useless information
PRODUCER_DROP_REGEX_SET = {r".*LaTeX.*", r".*Acrobat.*"}

INFO_DICT_KEY_DROP_SET = {
    r"the process that creates this pdf constitutes a trade secret of codemantra, llc and "
    r"is protected by the copyright laws of the united states"
}

INFO_DICT_VALUE_DROP_SET = {r"HTML Tidy for Mac OS X (vers 1st December 2004), see www.w3.org"}
