
"""
Tools to standardize metadata fields.

For comparison, for searching, for neatness and for sanity being able to bring metadata objects
into a standard form is very useful.
This module provides tools to do that.
"""

from __future__ import unicode_literals, print_function

# Ultimately the exact forms these functions take should be settable by user input

# Names come in many forms - this is annoying.
# These functions provide standardization functions to bring names and titles into universal forms.
# Author should have the following form First_name Initial. Last_name
# Thus it should be [Arthur C. Clarke] and [George R. R. Martin]
# But, for example, [Raven St Pierre] should still become [Raven St. Pierre]

from copy import deepcopy
import re

from LiuXin_alpha.constants import VERBOSE_DEBUG, preferred_encoding

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.metadata.ebook_metadata_tools import check_isbn, format_isbn

from LiuXin_alpha.utils.text import isbytestring
from LiuXin_alpha.utils.text import drop_bracketed_text
from LiuXin_alpha.utils.text.icu import lower as icu_lower
from LiuXin_alpha.utils.libraries.iso639.iso639_tools import canonicalize_lang
from LiuXin_alpha.utils.libraries.titlecase import titlecase

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__author__ = "Cameron"

# Todo: This should be merged with metadata_standardize in metadata


# 0) Replace all white space with single spaces
# 1) Insert [. ] after every capital followed by another capital
# 1.5) Insert a [ ] after every full stop
# 2) Isolated lower case letters should be capitalized and followed with a .as
# 3) Isolated capital letters should be followed with a full stop
# 4) The first letter of the string should always become a capital
# 5) Lower case letters should never be immediately followed by an upper case (except in the case of Mc/Mac
# Todo: Hilariously unicode/multi-language unsafe
def standardize_creator_name(input_string):
    """
    Takes a string - does it's level best to mangle it into standard form.
    :param input_string:
    :return:
    """
    input_string = deepcopy(input_string)
    input_string_tokenized = input_string.split(",")
    if len(input_string_tokenized) == 1:
        working_string = input_string_tokenized[0]
    elif len(input_string_tokenized) == 2:
        working_string = input_string_tokenized[1] + " " + input_string_tokenized[0]
    elif len(input_string_tokenized) > 2:
        working_string = ",".join(input_string_tokenized)
    else:
        if VERBOSE_DEBUG:
            err_str = "standardize_name has failed. Input could not be parsed.\n"
            err_str += "input_string: " + repr(input_string) + "\n"
            raise InputIntegrityError(err_str)
        else:
            raise InputIntegrityError

    # 0) Replace all white space with single spaces
    working_string = re.sub(r"\s+", r" ", working_string)

    # 1) Insert [. ] after every capital followed by another capital
    double_caps_re = r"[a-zA-Z0-9. ]*[A-Z][A-Z][a-zA-Z0-9. ]*"
    double_caps_pat = re.compile(double_caps_re)
    while double_caps_pat.match(working_string) is not None:
        working_string = re.sub(r"(?P<I>[A-Z])(?P<II>[A-Z])", r"\g<I>. \g<II>", working_string)

    # 1.5) Insert a [ ] after every full stop
    working_string = re.sub(r"(?P<I>\.)(?P<II>[^\.\s])", r"\g<I> \g<II>", working_string)

    # 2) Isolated lower case letters should be capitalized and followed with a .as
    isolated_lower_re = r"([a-zA-Z0-9.\s]*\s)([a-z])(\s[a-zA-Z0-9.\s]*)"
    isolated_lower_pat = re.compile(isolated_lower_re)
    while isolated_lower_pat.match(working_string) is not None:
        match = isolated_lower_pat.match(working_string)
        working_string = match.group(1) + match.group(2).upper() + match.group(3)

    # 3) Isolated capital letters should be followed with a full stop
    isolated_capital_regex = r"([a-zA-Z0-9. ]*\s)([A-Z])\s([a-zA-Z0-9. ]*)"
    isolated_capital_pat = re.compile(isolated_capital_regex)
    while isolated_capital_pat.match(working_string) is not None:
        match = isolated_capital_pat.match(working_string)
        working_string = match.group(1) + match.group(2).upper() + ". " + match.group(3)

    # 4) The first letter of the string should always be a capital.
    first_letter_regex = r"([a-zA-Z])([a-zA-Z0-9.\s]*)"
    first_letter_pat = re.compile(first_letter_regex)
    working_string_match = first_letter_pat.match(working_string)
    if working_string_match is not None:
        working_string = working_string_match.group(1).upper() + working_string_match.group(2)

    # 5) Unless in the case of Mc/Mac a capital should always be preceded by a space, unless it's Mc/Mac
    # Crude - puts a space in front of every capital
    pre_capital_insert_regex = r"([a-zA-Z0-9.\s]*[a-z])([A-Z])([a-zA-Z0-9.\s]*)"
    pre_capital_insert_pat = re.compile(pre_capital_insert_regex)
    while pre_capital_insert_pat.match(working_string) is not None:
        match = pre_capital_insert_pat.match(working_string)
        working_string = match.group(1) + " " + match.group(2) + match.group(3)

    # The first letter of any word should be a capital
    working_string_tokens = working_string.split()
    new_tokens = []
    for token in working_string_tokens:
        if len(token) == 0:
            current_token = ""
        elif len(token) == 1:
            current_token = token[0].upper() + "."
        else:
            current_token = token[0].upper() + token[1:]
        new_tokens.append(current_token)
    working_string = " ".join(new_tokens)

    # combine any instance of u'Mac' or u'Mc' into the next word.
    post_mc_space_regex = r"([a-zA-Z0-9.\s]*)(Mc|Mac) ([A-Z][a-zA-Z0-9.\s]*)"
    post_mc_space_pat = re.compile(post_mc_space_regex)
    while post_mc_space_pat.match(working_string) is not None:
        match = post_mc_space_pat.match(working_string)
        working_string = match.group(1) + match.group(2) + match.group(3)

    # making sure any white space is reduced to a single space
    working_string = re.sub(r"\s+", r" ", working_string).strip()

    return working_string


# Attempts to bring titles into a standard form.
# 0) Strip drop characters. Drop bracketed text
# 1) Makes the first separator : and any subsequent ones -. Inserts white space around them.
# 2) Normalize whitespace and bring it into title case
# Todo: Make sure this is reflected in the metadata_from_string method
BRACKETS = ("<>", "{}", "()", "[]")
SEPARATORS = ("_", "-", ":", ";", "|")
DROP_CHARACTERS = (".", ",", '"', "'")


def standardize_title(target_string):
    """
    Takes a title - tries to bring it into a standard form.
    If the title is None then returns an empty string.
    :param target_string:
    :return:
    """
    if target_string is None:
        return ""

    target_string = deepcopy(target_string)

    # 0) Strip drop characters
    new_target_string = ""
    for char in target_string:
        if char not in DROP_CHARACTERS:
            new_target_string += char
        else:
            new_target_string += " "
    target_string = new_target_string
    target_string = drop_bracketed_text(target_string)

    # 0.5) Ensure white space around every separator
    sep_re = r"\{}"
    sep_re_space = r" \{} "
    for sep in SEPARATORS:
        current_sep_re = sep_re.format(sep)
        current_sep_re_space = sep_re_space.format(sep)
        target_string = re.sub(current_sep_re, current_sep_re_space, target_string)

    # 1) Makes the first separator : and any subsequent ones -. Inserts white space around them.
    first_sep = True
    new_target_string = ""
    for char in target_string:
        if char in SEPARATORS:
            if first_sep:
                new_target_string += " : "
                first_sep = False
            else:
                new_target_string += " - "
        else:
            new_target_string += char
    target_string = new_target_string

    # 2) Normalize whitespace and bring into title case
    target_string = re.sub(r"\s+", " ", target_string)
    target_string.strip()
    return titlecase(target_string)


# The algorithm for generating the hash is as follows
# 1) Aggressively strip down the title - drop all 'little words (and, of, the )
# 1.1) Drop all punctuation
# 1.2) Convert any existing separators to u'_' and drop everything after the second separator
# 1.3) convert to lower case and drop all spaces
# 2) take the last name of the first author - stick it in front of the title followed by an _
LITTLE_WORDS = ("on", "the", "a", "at", "of", "and")
ALL_DROP_CHARACTERS = (
    ".",
    ",",
    '"',
    "'",
    "?",
    "!",
    "$",
    "%",
    "^",
    "&",
    "*",
    "#",
    ":",
    ";",
    "-",
)


def gen_title_author_phash(author_string, title_string):
    """
    Takes an author string and a title string. From them produces a title_author phash which can be used to search the
    titles table for existing title_creator combinations.
    :param author_string: The name of the first author associated with a title
    :param title_string: The title of the work
    :return title_author_phash: Something which should hopefully be usefully unique given any author-title pair
    """
    author_string = deepcopy(author_string).strip()
    title_string = deepcopy(title_string).strip()

    if "&" in author_string:
        author_tokens = author_string.split("&")
        author_string = author_tokens[0]

    author_string = standardize_creator_name(author_string)
    author_name_tokens = author_string.split(" ")
    author_surname = author_name_tokens[-1].lower()

    title_string = make_title_search_term(title_string)

    title_author_phash = author_surname + "_" + title_string
    return title_author_phash


def make_title_search_term(title_string):
    """
    Makes a simplified form of the title (a title hash, if you will) for easier searching.
    :param title_string:
    :return:
    """
    return make_simpler_search_term(title_string)


def make_simpler_search_term(search_string):
    """
    Makes a simplified form of the title (a title hash, if you will) for easier searching.
    :param search_string:
    :return:
    """
    # Based on how the title string is normalized in the standardize_title method this should drop everything after the
    # second separator
    search_string = deepcopy(search_string)
    search_string_tokens = search_string.split("-")
    search_string = search_string_tokens[0]

    # Dropping all characters in the ALL_DROP_CHARACTERS list
    new_title_string = ""
    for char in search_string:
        if char not in ALL_DROP_CHARACTERS:
            new_title_string += char
    search_string = new_title_string

    # Dropping all words on the little words list
    search_string = search_string.lower()
    search_string = re.sub(r"\s+", " ", search_string)
    search_string_tokens = search_string.split()
    new_search_string_tokens = []
    for token in search_string_tokens:
        if token not in LITTLE_WORDS:
            new_search_string_tokens.append(token)
    search_string_tokens = new_search_string_tokens
    search_string = "_".join(search_string_tokens)
    return search_string


# ----------------------------------------------------------------------------------------------------------------------
#
# -- STANDARDIZATION METHODS FOR OTHER FIELDS START HERE
#
# ----------------------------------------------------------------------------------------------------------------------

# Genre standardization is a bit simple - it runs through a bunch of lookup tables and tries to convert anything which
# reasonably could be an abbreviation into the right form.

# Todo: Put the Regexes used here somewhere they can be more easily gotten to
#
# NOTE:
#   Historically this module carried a tiny GENRE_SHORTENED_MAPPING and a
#   match()-based standardize_genre(). We now delegate to
#   LiuXin_alpha.metadata.standardize_genre which:
#     - normalizes unicode (accent-insensitive)
#     - compiles regexes once
#     - contains a substantially larger mapping
from LiuXin_alpha.metadata import standardize_genre as _genre_std


# Public alias retained for backwards compatibility
GENRE_SHORTENED_MAPPING = _genre_std.GENRE_SHORTENED_MAPPING
_COMPILED_GENRE_SHORTENED_MAPPING = _genre_std.compile_genre_mapping(GENRE_SHORTENED_MAPPING)


def standardize_genre(genre_string):
    """Normalize a genre-ish string into a canonical shelf label.

    Backwards compatible wrapper around :func:`LiuXin_alpha.metadata.standardize_genre.standardize_genre`.
    """
    if genre_string is None:
        return ""
    default = titlecase(deepcopy(genre_string))
    out = _genre_std.standardize_genre(genre_string, _COMPILED_GENRE_SHORTENED_MAPPING, default=default)
    return out if out is not None else default


def classify_fiction_genre(genre_string, *, multi_leaf=False, default_branch=None, default_leaf=None):
    """Classify a genre-ish string into (branch, leaf) for fiction.

    Returns a FictionGenreClassification (dataclass) from
    :func:`LiuXin_alpha.metadata.standardize_genre.classify_fiction_genre`.
    """
    return _genre_std.classify_fiction_genre(
        genre_string,
        multi_leaf=multi_leaf,
        default_branch=default_branch,
        default_leaf=default_leaf,
    )


def standardize_language(language_string):
    """
    Tries to bring the language name into an iso639 recognized form.
    :param language_string:
    :return:
    """
    language_string = six_unicode(deepcopy(language_string)).lower()
    candidate_language = canonicalize_lang(language_string)
    if candidate_language is None:
        return titlecase(language_string)
    else:
        return candidate_language


def make_tag_search_term(tag_string):
    """
    Two tags are considered to be the same if they are the same up to the placement of spaces and capitalization.
    This method takes a tag in the form of a string and produces something which can be used to search the tags table.
    :param tag_string:
    :return:
    """
    tag_string = deepcopy(tag_string)
    tag_string = re.sub(r"\s+", "", tag_string)
    tag_string = tag_string.lower()
    return tag_string


def standardize_tag(tag_string):
    """
    Tries to bring a tag into standard form (currently just applies titlecase). Mostly here for symmetry.
    :param tag_string:
    :return:
    """
    return titlecase(tag_string)


# Todo: Extend this to as many forms of identifier as can be found
def standardize_identifier(identifier_string):
    """
    Does it's best to bring any given identifier into a standard form.
    :param identifier_string:
    :return:
    """
    identifier_string = deepcopy(identifier_string)
    isbn_string = standardize_isbn(identifier_string)
    if isbn_string:
        return isbn_string
    else:
        return identifier_string


def standardize_isbn(isbn_string):
    """
    Brings an identifier into standard form.
    :param isbn_string:
    :return:
    """
    if not check_isbn(isbn_string):
        return False
    else:
        return format_isbn(isbn_string)


def standardize_publisher(publisher_string):
    """
    Brings a publisher string into standard form.
    :param publisher_string:
    :return:
    """
    if publisher_string is None:
        return ""

    return titlecase(publisher_string)


def standardize_series(series_string):
    """
    Brings a series string into standard form.
    If the series string is None, then return the empty string.
    :param series_string:
    :return:
    """
    if series_string is None:
        return ""

    return titlecase(series_string)


def make_series_phash(creator_string, series_string):
    """
    Takes a creator string, and a series string - and uses them to generate a phash which can be used to search the
    series table for the particular series.
    (attempt to get round the problem that series and names might be written inconsistently.)
    :param creator_string: The given name of the creator of the series
    :param series_string: The given name of the series
    :return :
    """
    creator_string = deepcopy(creator_string)
    series_string = deepcopy(series_string)

    creator = standardize_creator_name(creator_string)
    creator_tokens = creator.split()
    try:
        creator_surname_token = creator_tokens[-1].lower()
    except IndexError:
        creator_surname_token = ""
    simpler_series_string = make_simpler_search_term(series_string).lower()

    series_phash = creator_surname_token + "_" + simpler_series_string
    return series_phash


def make_creator_phash(creator_string):
    """
    Make a creator hash string out of a creator string.
    :param creator_string:
    :return:
    """
    # 1) remove all the whitespace
    creator_string = re.sub(r"\s+", r"", creator_string)

    # 2) lowercase
    return icu_lower(creator_string)


# Todo: Make sure that this is used everywhere it should be
def cleanup_tags(tags):
    """
    Render an iterable of tags safe/sane for inclusion in the database.
    :param tags:
    :return:
    """
    tags = [x.strip().replace(",", ";") for x in tags if x.strip()]
    tags = [x.decode(preferred_encoding, "replace") if isbytestring(x) else x for x in tags]
    tags = [" ".join(x.split()) for x in tags]
    ans, seen = [], set([])
    for tag in tags:
        if tag.lower() not in seen:
            seen.add(tag.lower())
            ans.append(tag)
    return ans
