
"""
Normalize display fields and derive lossy comparison keys using legacy creator/title heuristics and genre adapters.

Name formatting uses ASCII-oriented rules, comparison keys can collide, and language
lookup may return canonical names rather than codes. Genre classification delegates
to the ordered modern genre mappings.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_standardize_coverage.py
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
    Apply legacy name-order, initial-spacing, capitalization, and Mc/Mac joining heuristics.

    Reverse a single comma-separated surname/given-name pair; preserve multi-comma
    ordering. Normalize whitespace, separate adjacent ASCII capitals, add periods to
    initial-like tokens, and capitalize token starts. The rules are heuristic and are
    not a general multilingual name parser.

    Example:
        >>> standardize_creator_name('Clarke, Arthur C')
        'Arthur C. Clarke'


    :param input_string: Creator-name string; comma order and ASCII-oriented
        initial/capitalization heuristics are applied.
    :return: Normalized creator string; an empty string stays empty.
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

    # 4) The first ASCII letter of the string should always become a capital,
    # while preserving punctuation such as hyphens and apostrophes in names.
    working_string = re.sub(
        r"([a-zA-Z])",
        lambda match: match.group(1).upper(),
        working_string,
        count=1,
    )

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
    Replace configured punctuation with spaces, drop bracketed text, and normalize separators before title-casing.

    The first underscore, hyphen, colon, semicolon, or vertical bar becomes a colon;
    later separators become hyphens. Collapse whitespace before applying titlecase.

    Example:
        >>> standardize_title('the_book-part')
        'The : Book - Part'


    :param target_string: Title string, or None for an empty result.
    :return: Normalized title, or empty string for None.
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
    for sep in SEPARATORS:
        target_string = re.sub(re.escape(sep), f" {sep} ", target_string)

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
    target_string = re.sub(r"\s+", " ", target_string).strip()
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
    Combine the first author’s standardized final name token with a simplified title key.

    Split author text at the first ampersand, then join the lowercased surname token and
    title key with an underscore. This lossy key is not guaranteed unique.

    Example:
        >>> gen_title_author_phash('Ada Lovelace', 'The Book')
        'lovelace_book'


    :param author_string: Author string; only the part before the first ampersand
        contributes.
    :param title_string: Title string used to derive a lossy search key.
    :return: Surname/title search key.
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
    Delegate title-key generation to make_simpler_search_term.

    Example:
        >>> make_title_search_term('The Left Hand of Darkness')
        'left_hand_darkness'


    :param title_string: Title string used to derive a lossy search key.
    :return: Lossy underscore-separated search key.
    """
    return make_simpler_search_term(title_string)


def make_simpler_search_term(search_string):
    """
    Keep text before the first hyphen, remove configured punctuation, lowercase, and omit six common words.

    Join remaining whitespace-delimited tokens with underscores. The removed words are
    on, the, a, at, of, and and; other punctuation can remain.

    Example:
        >>> make_simpler_search_term('The Book of Stars - Volume Two')
        'book_stars'


    :param search_string: String to reduce to a search key.
    :return: Lossy search key; distinct original strings can collide.
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
    """
    Return an empty string for None, otherwise match the compiled genre mapping with titlecase as fallback.

    The mapping is compiled at module import, so later raw mapping edits do not refresh
    it automatically.

    Example:
        >>> standardize_genre('space opera')
        'Space Opera'


    :param genre_string: Genre-label string to standardize.
    :return: First matching canonical label or title-cased original input.
    """
    if genre_string is None:
        return ""
    default = titlecase(deepcopy(genre_string))
    out = _genre_std.standardize_genre(genre_string, _COMPILED_GENRE_SHORTENED_MAPPING, default=default)
    return out if out is not None else default


def classify_fiction_genre(genre_string, *, multi_leaf=False, default_branch=None, default_leaf=None):
    """
    Forward branch/leaf classification options to the genre classifier.

    Example:
        >>> classify_fiction_genre('space opera').leaf
        'Space Opera'


    :param genre_string: Genre-label string to standardize.
    :param multi_leaf: Whether to collect and prune all matching leaves within the
        chosen branch.
    :param default_branch: Fallback branch when no branch expression matches.
    :param default_leaf: Fallback leaf when no matching leaf is selected.
    :return: FictionGenreClassification carrying the selected branch, representative
        leaf, leaf set, and normalized text.
    """
    return _genre_std.classify_fiction_genre(
        genre_string,
        multi_leaf=multi_leaf,
        default_branch=default_branch,
        default_leaf=default_leaf,
    )


def standardize_language(language_string):
    """
    Stringify and lowercase the input, then try the bundled language canonicalizer.

    Example:
        >>> standardize_language('zho')
        'Chinese'


    :param language_string: Value stringified and lowercased before language lookup.
    :return: Canonicalizer result when known, otherwise title-cased input text; the
        result need not be an ISO code.
    """
    language_string = six_unicode(deepcopy(language_string)).lower()
    candidate_language = canonicalize_lang(language_string)
    if candidate_language is None:
        return titlecase(language_string)
    else:
        return candidate_language


def make_tag_search_term(tag_string):
    """
    Remove all whitespace and lowercase a tag without other punctuation or Unicode normalization.

    Example:
        >>> make_tag_search_term(' Space  Opera ')
        'spaceopera'


    :param tag_string: Tag string to normalize.
    :return: Compact case-insensitive comparison string.
    """
    tag_string = deepcopy(tag_string)
    tag_string = re.sub(r"\s+", "", tag_string)
    tag_string = tag_string.lower()
    return tag_string


def standardize_tag(tag_string):
    """
    Apply titlecase to the supplied tag without additional cleanup.

    Example:
        >>> standardize_tag('space opera')
        'Space Opera'


    :param tag_string: Tag string to normalize.
    :return: Title-cased tag.
    """
    return titlecase(tag_string)


# Todo: Extend this to as many forms of identifier as can be found
def standardize_identifier(identifier_string):
    """
    Try fixed-position ISBN normalization and otherwise return a copy of the original identifier.

    Example:
        >>> standardize_identifier('abc-123')
        'abc-123'


    :param identifier_string: Identifier value passed to the ISBN helpers.
    :return: Formatted ISBN or unchanged copied input; does not generally strip
        arbitrary identifiers.
    """
    identifier_string = deepcopy(identifier_string)
    isbn_string = standardize_isbn(identifier_string)
    if isbn_string:
        return isbn_string
    else:
        return identifier_string


def standardize_isbn(isbn_string):
    """
    Validate the candidate and use fixed-position ISBN formatting when valid.

    Example:
        >>> standardize_isbn('not an isbn') is False
        True


    :param isbn_string: ISBN candidate passed to the checksum helpers.
    :return: Formatted ISBN string, or False when validation fails.
    """
    if not check_isbn(isbn_string):
        return False
    else:
        return format_isbn(isbn_string)


def standardize_publisher(publisher_string):
    """
    Title-case a publisher string, treating None as empty.

    Example:
        >>> standardize_publisher(None)
        ''


    :param publisher_string: Publisher string, or None for an empty result.
    :return: Title-cased publisher or empty string.
    """
    if publisher_string is None:
        return ""

    return titlecase(publisher_string)


def standardize_series(series_string):
    """
    Title-case a series string, treating None as empty.

    Example:
        >>> standardize_series('earthsea cycle')
        'Earthsea Cycle'


    :param series_string: Series string to normalize.
    :return: Title-cased series or empty string.
    """
    if series_string is None:
        return ""

    return titlecase(series_string)


def make_series_phash(creator_string, series_string):
    """
    Combine the standardized creator’s final token with a simplified series key.

    Use an empty surname when creator tokenization is empty. Join the two components
    with an underscore; the result is a lossy comparison key.

    Example:
        >>> make_series_phash('', 'The Wheel of Time')
        '_wheel_time'


    :param creator_string: Creator-name string contributing to a search key.
    :param series_string: Series string to normalize.
    :return: Surname/series key, potentially beginning with an underscore.
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
    Remove whitespace from a creator string and lowercase it through the ICU helper.

    Example:
        >>> make_creator_phash(' Ada Lovelace ')
        'adalovelace'


    :param creator_string: Creator-name string contributing to a search key.
    :return: Compact name key without punctuation removal or uniqueness guarantees.
    """
    # 1) remove all the whitespace
    creator_string = re.sub(r"\s+", r"", creator_string)

    # 2) lowercase
    return icu_lower(creator_string)


# Todo: Make sure that this is used everywhere it should be
def cleanup_tags(tags):
    """
    Decode or stringify tags, trim and collapse whitespace, replace commas with semicolons, and deduplicate by lowercase form.

    Preserve the first surviving spelling and input order. Ignore None and empty values;
    decode bytes/bytearray with the preferred encoding and replacement errors. The input
    iterable is consumed without mutation.

    Example:
        >>> cleanup_tags([' Space  Opera ', 'space opera', None, 'a,b'])
        ['Space Opera', 'a;b']


    :param tags: Iterable of tags; None elements are skipped, bytes are decoded, and
        other values are stringified.
    :return: New ordered list of cleaned tags.
    """
    normalized_tags = []
    for tag in tags:
        if tag is None:
            continue
        if isinstance(tag, (bytes, bytearray)):
            tag = bytes(tag).decode(preferred_encoding, "replace")
        elif not isbytestring(tag):
            tag = six_unicode(tag)
        tag = tag.strip()
        if tag:
            normalized_tags.append(tag.replace(",", ";"))
    tags = normalized_tags
    tags = [" ".join(x.split()) for x in tags]
    ans, seen = [], set([])
    for tag in tags:
        if tag.lower() not in seen:
            seen.add(tag.lower())
            ans.append(tag)
    return ans
