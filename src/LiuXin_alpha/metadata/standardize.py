
"""
Provide legacy author splitting, creator/identifier aliases, display-field cleanup, and lossy metadata search keys.

The author split pattern is compiled from preferences at import, with a warned
fallback after supported configuration errors. Genre matching uses this module’s
small prefix-matching table; tag normalization lowercases after removing one leading
BOM.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_standardize_coverage.py
"""

from __future__ import unicode_literals, print_function

import re

from copy import deepcopy

from LiuXin_alpha.constants import VERBOSE_DEBUG, preferred_encoding

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.metadata.ebook_metadata_tools import check_isbn
from LiuXin_alpha.metadata.ebook_metadata_tools import format_isbn

from LiuXin_alpha.utils.text import isbytestring
from LiuXin_alpha.utils.text import remove_bracketed_text as drop_bracketed_text
from LiuXin_alpha.utils.text.icu import lower as icu_lower
from LiuXin_alpha.utils.libraries.iso639.iso639_tools import canonicalize_lang
from LiuXin_alpha.utils.libraries.titlecase import titlecase

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


from LiuXin_alpha.metadata.constants import CREATOR_CATEGORIES
from LiuXin_alpha.metadata.constants import CREATOR_ROLE_REKEY_SCHEME
from LiuXin_alpha.metadata.constants import EXTERNAL_EBOOK_REKEY_SCHEME
from LiuXin_alpha.metadata.constants import INTERNAL_EBOOK_REKEY_SCHEME
from LiuXin_alpha.utils.libraries.iso639.iso639_tools import canonicalize_lang
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.logging import LiuXin_warning_print
from LiuXin_alpha.preferences import preferences as tweaks


try:
    _author_pat = re.compile(tweaks["authors_split_regex"])
except (TypeError, re.error, KeyError) as e:
    LiuXin_warning_print(
        "Author split regexp:",
        "is invalid or not present, using default",
        str(e)
    )
    _author_pat = re.compile(r"(?i),?\s+(and|with)\s+")


# imported from calibre
# TODO: MAKE THIS BETTER (though including backwards compatibility for the calibre methods that rely on it)
# Till they can be re-written too
# TODO: Find a calibre tweaks file. Read it.
def string_to_authors(raw: str) -> list[str]:
    """
    Protect doubled ampersands, apply the import-time author separator pattern, and return nonempty title-cased author segments.

    Use U+FFFF internally for escaped ampersands; an existing occurrence is also
    converted to an ampersand. Later preference changes require reloading to rebuild the
    split pattern.

    Example:
        >>> string_to_authors('ada lovelace & grace hopper')
        ['Ada Lovelace', 'Grace Hopper']


    :param raw: Author string containing configured separators and ampersand escapes.
    :return: List of author strings, or empty list for falsy input.
    """

    if not raw:
        return []
    raw = raw.replace("&&", "\uffff")
    raw = _author_pat.sub("&", raw)
    authors = [a.strip().replace("\uffff", "&") for a in raw.split("&")]
    return [titlecase(a) for a in authors if a]



def standardize_creator_category(creator_type, logging=False):
    """
    Lowercase and strip a creator category, then check canonical categories before ordered alias groups.

    Example:
        >>> standardize_creator_category('AUTHOR')
        'authors'


    :param creator_type: Category string to normalize.
    :param logging: Whether to log the normalized unmatched value and category mappings.
    :return: Canonical role string, or None when unknown; optional failure logging can
        itself raise.
    """
    # Todo: Make this method DRYer - deal with duplicaiton in metadata constants
    creator_type = creator_type.lower().strip()

    if creator_type in CREATOR_CATEGORIES:
        return deepcopy(creator_type.lower())

    for rekey_set in CREATOR_ROLE_REKEY_SCHEME:
        if creator_type in rekey_set:
            return CREATOR_ROLE_REKEY_SCHEME[rekey_set]

    if logging:
        info_str = "Attempt to standardize creator category failed"
        default_log.log_variables(
            info_str,
            "INFO",
            ("creator_type", creator_type),
            ("CREATOR_CATEGORIES", CREATOR_CATEGORIES),
            ("CREATOR_ROLE_REKEY_SCHEME", CREATOR_ROLE_REKEY_SCHEME),
        )
    return None


def standardize_id_name(id_name, logging=False):
    """
    Lowercase and strip an identifier scheme, searching external aliases before internal aliases.

    Example:
        >>> standardize_id_name('ISBN10')
        'isbn'


    :param id_name: Identifier scheme string to match.
    :param logging: Whether to log an unmatched scheme and alias data.
    :return: First matching canonical scheme or None; optionally logs unmatched inputs.
    """
    id_name_key = id_name.lower().strip()

    for constant_map in EXTERNAL_EBOOK_REKEY_SCHEME:
        if id_name_key in constant_map:
            return EXTERNAL_EBOOK_REKEY_SCHEME[constant_map]

    for constant_map in INTERNAL_EBOOK_REKEY_SCHEME:
        if id_name_key in constant_map:
            return INTERNAL_EBOOK_REKEY_SCHEME[constant_map]

    # If the identifier cannot be transformed to one of the known types, log if for update of the whole project
    if logging:
        err_str = (
            "Unable to bring an identifier into standard form - add to " "LiuXin.constants:EXTERNAL_EBOOK_REKEY_SCHEME?"
        )
        default_log.log_variables(
            err_str,
            "INFO",
            ("id_name", id_name),
            ("id_name_key", id_name_key),
            ("EXTERNAL_EBOOK_REKEY_SCHEME", EXTERNAL_EBOOK_REKEY_SCHEME),
        )
    return None


def standardize_internal_id_name(internal_id_name, logging=False):
    """
    Lowercase and strip a scheme, then search only internal identifier aliases.

    Example:
        >>> standardize_internal_id_name('calibre')
        'uuid'


    :param internal_id_name: Internal scheme candidate string.
    :param logging: Whether to log an unmatched internal scheme.
    :return: Canonical internal scheme or None; optionally logs unmatched inputs.
    """
    id_name_key = internal_id_name.lower().strip()

    for constant_map in INTERNAL_EBOOK_REKEY_SCHEME:
        if id_name_key in constant_map:
            return INTERNAL_EBOOK_REKEY_SCHEME[constant_map]

    # If the identifier cannot be transformed to one of the known types, log if for update of the whole project
    if logging:
        err_str = (
            "Unable to bring an identifier into standard form - add to " "LiuXin.constants:EXTERNAL_EBOOK_REKEY_SCHEME?"
        )
        default_log.log_variables(
            err_str,
            "INFO",
            ("internal_id_name", internal_id_name),
            ("id_name_key", id_name_key),
            ("INTERNAL_EBOOK_REKEY_SCHEME", INTERNAL_EBOOK_REKEY_SCHEME),
        )
    return None


def standardize_lang(lang):
    """
    Delegate language lookup directly to the bundled canonicalizer without local preprocessing.

    Example:
        >>> standardize_lang('zh')
        'Chinese'


    :param lang: Language value accepted by canonicalize_lang.
    :return: Canonical language result or None for an unrecognized input.
    """
    return canonicalize_lang(lang)


def standardize_rating_type(rating_name: str) -> str:
    """
    Lowercase a rating-type string without checking a recognized-type registry.

    Example:
        >>> standardize_rating_type('Amazon (US)')
        'amazon (us)'


    :param rating_name: Rating label string.
    :return: Lowercased input; whitespace and punctuation remain.
    """
    return rating_name.lower()


def standardize_identifier(identifier: str) -> str:
    """
    Stringify an identifier and strip its surrounding whitespace without checksum validation.

    Example:
        >>> standardize_identifier(12345)
        '12345'


    :param identifier: Any value accepted by str.
    :return: Stripped string representation.
    """
    return str(identifier).strip()


def standardize_tag(tag_str: str) -> str:
    """
    Strip outer whitespace, remove one leading U+FEFF character, then strip again and lowercase.

    Example:
        >>> standardize_tag(chr(0xFEFF) + ' Tag ')
        'tag'


    :param tag_str: Tag string to clean.
    :return: Lowercased tag; internal whitespace is retained.
    """
    tag_str = tag_str.strip()
    if tag_str.startswith("\ufeff"):
        tag_str = tag_str[1:]
    return tag_str.strip().lower()



# Ultimately the exact forms these functions take should be settable by user input

# Names come in many forms - this is annoying.
# These functions provide standardization functions to bring names and titles into universal forms.
# Author should have the following form First_name Initial. Last_name
# Thus it should be [Arthur C. Clarke] and [George R. R. Martin]
# But, for example, [Raven St Pierre] should still become [Raven St. Pierre]

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
GENRE_SHORTENED_MAPPING = {
    "Science Fiction": (r"science ?fiction", r"sci ?fi", "s ?f"),
    "Fantasy": (r"fant?a?s?y?",),
    "High Fantasy": (r"h. ?fan", r"high ?fantasy"),
    "Military Science Fiction": (
        r"military science fiction",
        r"mil.? ?s ? f",
        r"military ?sf",
    ),
    "Realistic Fiction": (r"rf", r"realistic ?fiction"),
    "Urban Fantasy": (r"urban ?fantasy", r"uf"),
}


def standardize_genre(genre_string):
    """
    Try the small ordered genre regex table against the start of the copied input, compiling patterns on each call.

    Example:
        >>> standardize_genre('high fantasy')
        'High Fantasy'


    :param genre_string: Genre-label string to standardize.
    :return: First matching canonical label, otherwise title-cased input; there is no
        explicit None handling.
    """
    genre_string = deepcopy(genre_string)

    for genre in GENRE_SHORTENED_MAPPING:
        regex_tuple = GENRE_SHORTENED_MAPPING[genre]
        for regex in regex_tuple:
            if re.compile(regex, re.I).match(genre_string) is not None:
                return genre

    # If not matches are found, fall back on title casing the given genre string and returning that
    return titlecase(genre_string)


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


# Todo: Extend this to as many forms of identifier as can be found
def standardize_identifier_value(identifier_string):
    """
    Try fixed-position ISBN formatting and otherwise preserve a copy of the original identifier value.

    Example:
        >>> standardize_identifier_value('0-261-10357-1')
        '02-6110-357-1'


    :param identifier_string: Identifier candidate passed to the ISBN helpers.
    :return: Formatted ISBN or unchanged copied input.
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
