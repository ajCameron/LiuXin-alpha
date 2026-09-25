
"""
Provide timestamp conversion, author/title sort helpers, identifier checks, and legacy name/title heuristics.

Sorting reads preference tweaks and caches article patterns. Name recognition loads
the configured name lists; score_title currently returns zero after preprocessing.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_utils_coverage.py
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional


def to_epoch_ms(
    value: Any,
    *,
    assume_tz=timezone.utc,
    now: Optional[datetime] = None,
    clamp_range: bool = False,
) -> int:
    """
    Convert date/time objects, finite numbers, or supported text into rounded integer Unix milliseconds.

    Guess numeric units from absolute magnitude: at least 1e17 means nanoseconds, 1e14
    microseconds, 1e11 milliseconds, otherwise seconds. Numbers pass through float, so
    large integers may lose precision. Decode bytes as UTF-8 with replacement fallback;
    try numeric text, Date wrappers, ISO text, then fixed formats with day-first slash
    dates before month-first. Reject None/bool with TypeError and nonfinite or
    unrecognized values with ValueError.

    Example:
        >>> to_epoch_ms('1970-01-01T00:00:01Z')
        1000
        >>> to_epoch_ms('/Date(1609459200000)/')
        1609459200000


    :param value: Datetime/date, int/float, bytes/bytearray, or supported timestamp
        string.
    :param assume_tz: Timezone attached to naive datetime/date inputs; defaults to UTC.
    :param now: Reference datetime for clamping; defaults to current UTC time even when
        clamping is disabled.
    :param clamp_range: Whether to clamp to 73000 days before or after now.
    :return: Rounded epoch milliseconds, optionally clipped to the configured time
        window.
    """
    if value is None:
        raise TypeError("None is not a timestamp")
    if isinstance(value, bool):
        raise TypeError("bool is not a timestamp")

    if now is None:
        now = datetime.now(timezone.utc)

    def dt_to_ms(dt: datetime) -> int:
        """
        Attach the enclosing assumed timezone to naive datetimes, convert to UTC, and round milliseconds.

        Example:
            >>> to_epoch_ms(datetime(1970, 1, 1, tzinfo=timezone.utc))
            0


        :param dt: Datetime being converted; aware inputs retain their represented instant.
        :return: Integer milliseconds before optional clamping.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=assume_tz)
        dt_utc = dt.astimezone(timezone.utc)
        return int(round(dt_utc.timestamp() * 1000))

    # datetime / date
    if isinstance(value, datetime):
        out = dt_to_ms(value)
        return _clamp_ms(out, now, clamp_range)
    if isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, tzinfo=assume_tz)
        out = dt_to_ms(dt)
        return _clamp_ms(out, now, clamp_range)

    # numbers
    if isinstance(value, (int, float)):
        v = float(value)
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("NaN/inf is not a timestamp")

        av = abs(v)
        if av >= 1e17:          # nanoseconds
            ms = v / 1e6
        elif av >= 1e14:        # microseconds
            ms = v / 1e3
        elif av >= 1e11:        # milliseconds
            ms = v
        else:                   # seconds
            ms = v * 1e3

        out = int(round(ms))
        return _clamp_ms(out, now, clamp_range)

    # bytes -> str
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8", "strict")
        except Exception:
            value = value.decode("utf-8", "replace")

    # strings
    if isinstance(value, str):
        s = value.strip().strip("\ufeff\u200b\u200e\u200f")
        if not s:
            raise ValueError("empty string is not a timestamp")

        # numeric string (int/float)
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", s):
            num = float(s) if "." in s else int(s)
            return to_epoch_ms(num, assume_tz=assume_tz, now=now, clamp_range=clamp_range)

        # extract a long integer (>= 9 digits) from wrappers like "/Date(1609459200000)/"
        m = re.search(r"[-+]?\d{9,}", s)
        if m and (s.startswith(("/Date(", "Date(")) or "Date(" in s):
            try:
                return to_epoch_ms(int(m.group(0)), assume_tz=assume_tz, now=now, clamp_range=clamp_range)
            except Exception:
                pass

        # ISO-ish: support trailing Z
        iso = s
        if iso.endswith(("Z", "z")):
            iso = iso[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(iso)
            out = dt_to_ms(dt)
            return _clamp_ms(out, now, clamp_range)
        except Exception:
            pass

        # common fallback formats
        fmts = [
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                out = dt_to_ms(dt)
                return _clamp_ms(out, now, clamp_range)
            except Exception:
                continue

        raise ValueError(f"Unrecognised timestamp string: {value!r}")

    raise TypeError(f"Unsupported timestamp type: {type(value).__name__}")


def _clamp_ms(ms: int, now: datetime, clamp_range: bool) -> int:
    """
    Return milliseconds unchanged unless clamping is enabled, then constrain them to now plus or minus 73000 days.

    Example:
        >>> _clamp_ms(42, datetime(2020, 1, 1, tzinfo=timezone.utc), False)
        42


    :param ms: Epoch millisecond value to constrain.
    :param now: Reference datetime used directly for both boundaries.
    :param clamp_range: Falsy to return immediately without computing boundaries.
    :return: Input value or nearest rounded boundary; datetime arithmetic and timestamp
        errors propagate.
    """
    if not clamp_range:
        return ms
    # clamp to +/- 200 years around 'now' to defuse wildly-wrong unit guesses
    lo = int(round((now - timedelta(days=365 * 200)).timestamp() * 1000))
    hi = int(round((now + timedelta(days=365 * 200)).timestamp() * 1000))
    return lo if ms < lo else hi if ms > hi else ms


import re
from copy import deepcopy

from typing import Optional

from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.text import remove_bracketed_text
from LiuXin_alpha.utils.plugins.name_loader import load_names

from LiuXin_alpha.constants import name_prefixes
from LiuXin_alpha.constants import name_suffixes

from LiuXin_alpha.utils.text.icu import lower as icu_lower

from LiuXin_alpha.metadata.utils import string_to_authors




def authors_to_string(authors):
    """
    Join truthy author strings with spaced ampersands after doubling each embedded ampersand.

    Example:
        >>> authors_to_string(['Ada & Bob', '', 'Grace'])
        'Ada && Bob & Grace'


    :param authors: Optional iterable of author strings; falsy elements are omitted.
    :return: Joined string, or empty string for None.
    """
    if authors is not None:
        return " & ".join([a.replace("&", "&&") for a in authors if a])
    else:
        return ""


def author_to_author_sort(author, method=None):
    """
    Build a surname-first author sort string according to the requested method and preference tweaks.

    Strip bracketed text for tokenization, remove configured prefixes/suffixes, rotate
    the final name token, and append suffixes. Falsy input becomes empty; short names,
    copy mode, matching copywords, exhausted name tokens, and comma-mode names already
    containing commas return the original author.

    Example:
        >>> author_to_author_sort('Ada Lovelace', method='copy')
        'Ada Lovelace'


    :param author: Author string to tokenize and reorder.
    :param method: Optional sorting method; None reads author_sort_copy_method. copy
        preserves, nocomma omits the inserted comma, and comma preserves
        already-comma-separated names.
    :return: Derived sort string or unchanged original author.
    """
    if not author:
        return ""
    sauthor = remove_bracketed_text(author).strip()
    tokens = sauthor.split()
    if len(tokens) < 2:
        return author
    if method is None:
        method = tweaks["author_sort_copy_method"]

    ltoks = frozenset(x.lower() for x in tokens)
    copy_words = frozenset(x.lower() for x in tweaks["author_name_copywords"])
    if ltoks.intersection(copy_words):
        method = "copy"

    if method == "copy":
        return author

    prefixes = set([y.lower() for y in tweaks["author_name_prefixes"]])
    prefixes |= set([y + "." for y in prefixes])
    while True:
        if not tokens:
            return author
        tok = tokens[0].lower()
        if tok in prefixes:
            tokens = tokens[1:]
        else:
            break

    suffixes = set([y.lower() for y in tweaks["author_name_suffixes"]])
    suffixes |= set([y + "." for y in suffixes])

    suffix = ""
    while True:
        if not tokens:
            return author
        last = tokens[-1].lower()
        if last in suffixes:
            suffix = tokens[-1] + " " + suffix
            tokens = tokens[:-1]
        else:
            break
    suffix = suffix.strip()

    if method == "comma" and "," in "".join(tokens):
        return author

    atokens = tokens[-1:] + tokens[:-1]
    num_toks = len(atokens)
    if suffix:
        atokens.append(suffix)

    if method != "nocomma" and num_toks > 1:
        atokens[0] += ","

    return " ".join(atokens)


def authors_to_sort_string(authors):
    """
    Apply preference-driven author sorting to each supplied name and join results with spaced ampersands.

    Example:
        >>> authors_to_sort_string(['Plato'])
        'Plato'


    :param authors: Iterable of author names; None is not accepted.
    :return: Combined sort string; embedded ampersands are not separately escaped.
    """
    return " & ".join(map(author_to_author_sort, authors))


# imported from calibre
# Todo: Remove spaces, "-" e.t.c - common ways of breakup up an isbn10
def check_isbn10(isbn: str) -> Optional[str]:
    """
    Check the first ten characters using the ISBN-10 checksum and uppercase X convention.

    No cleanup or exact-length check occurs; a valid prefix can return an input with
    trailing characters. All exceptions raised within the checksum block are suppressed.

    Example:
        >>> check_isbn10('0261103571')
        '0261103571'


    :param isbn: Indexable digit string with an optional uppercase X check character.
    :return: Original input on a matching checksum, otherwise None.
    """
    try:
        digits = [_ for _ in map(int, isbn[:9])]
        products = [(i + 1) * digits[i] for i in range(9)]
        check = sum(products) % 11
        if (check == 10 and isbn[9] == "X") or check == int(isbn[9]):
            return isbn
    except:
        pass
    return None


# imported from calibre
def check_isbn13(isbn):
    """
    Check the first thirteen characters using alternating ISBN-13 checksum weights.

    No prefix or exact-length validation occurs; suffix characters are retained when the
    checked prefix succeeds. Exceptions inside the checksum block are suppressed.

    Example:
        >>> check_isbn13('9780261103573')
        '9780261103573'


    :param isbn: Indexable string whose first thirteen characters form the candidate.
    :return: Original input on a matching checksum, otherwise None.
    """
    try:
        digits = list(map(int, isbn[:12]))
        products = [(1 if i % 2 == 0 else 3) * digits[i] for i in range(12)]
        check = 10 - (sum(products) % 10)
        if check == 10:
            check = 0
        if str(check) == isbn[12]:
            return isbn
    except:
        pass
    return None


# imported from calibre
def check_isbn(isbn):
    """
    Uppercase and remove characters except digits/X, reject repeated-digit values, then validate ten or thirteen characters.

    Example:
        >>> check_isbn('978-0-261-10357-3')
        '9780261103573'


    :param isbn: Candidate string; falsy values return None.
    :return: Cleaned ISBN on success, otherwise None; truthy unsupported input types can
        raise before validation.
    """
    if not isbn:
        return None
    isbn = re.sub(r"[^0-9X]", "", isbn.upper())
    all_same = re.match(r"(\d)\1{9,12}$", isbn)
    if all_same is not None:
        return None
    if len(isbn) == 10:
        return check_isbn10(isbn)
    if len(isbn) == 13:
        return check_isbn13(isbn)
    return None


# imported from calibre
def check_issn(issn):
    """
    Clean a candidate to digits/X and validate the first eight characters with the ISSN checksum.

    No exact-length check occurs; trailing cleaned characters can survive a successful
    prefix check. Ordinary checksum errors become None; cleanup errors propagate.

    Example:
        >>> check_issn('0000-0000')
        '00000000'


    :param issn: Candidate string; falsy values return None.
    :return: Cleaned candidate when the checksum succeeds, otherwise None.
    """
    if not issn:
        return None
    issn = re.sub(r"[^0-9X]", "", issn.upper())
    try:
        digits = map(int, issn[:7])
        products = [(8 - i) * d for i, d in enumerate(digits)]
        check = 11 - sum(products) % 11
        if (check == 10 and issn[7] == "X") or check == int(issn[7]) or (check == 11 and issn[7] == "0"):
            return issn
    except Exception:
        pass
    return None


# imported from calibre
def format_isbn(isbn):
    """
    Validate an ISBN and insert separators at fixed positions for its cleaned length.

    These positions are fixed slices, not registration-group-aware hyphenation.

    Example:
        >>> format_isbn('0261103571')
        '02-6110-357-1'


    :param isbn: ISBN candidate passed to check_isbn.
    :return: Hyphenated valid ISBN, or the original input when validation fails.
    """
    cisbn = check_isbn(isbn)
    if not cisbn:
        return isbn
    i = cisbn
    if len(i) == 10:
        return "-".join((i[:2], i[2:6], i[6:9], i[9]))
    return "-".join((i[:3], i[3:5], i[5:9], i[9:12], i[12]))


# imported from calibre
def check_doi(doi):
    """
    Search anywhere for 10. followed by exactly four digits, a slash, and non-whitespace characters.

    Example:
        >>> check_doi('See 10.1234/example')
        '10.1234/example'


    :param doi: Candidate string; falsy input returns None.
    :return: First matching substring, including any non-whitespace trailing
        punctuation, or None.
    """
    if not doi:
        return None
    doi_check = re.search(r"10\.\d{4}/\S+", doi)
    if doi_check is not None:
        return doi_check.group()
    return None


_title_pats = {}


def get_title_sort_pat(lang=None):
    """
    Cache a leading-article regular expression under the original language argument.

    Resolve an absent language from preferences or locale, canonicalize it, then use
    configured articles with English and built-in fallbacks. Invalid expressions fall
    back to A/The/An. Existing cached entries do not reflect later preference changes.

    Example:
        >>> bool(get_title_sort_pat('eng').match('The Book'))
        True


    :param lang: Optional language selector used as the cache key and localization
        input.
    :return: Compiled case-insensitive pattern; this call may populate the module cache.
    """
    ans = _title_pats.get(lang, None)
    if ans is not None:
        return ans
    q = lang
    from LiuXin_alpha.utils.localization import canonicalize_lang, get_lang

    if lang is None:
        q = tweaks["default_language_for_title_sort"]
        if q is None:
            q = get_lang()
    q = canonicalize_lang(q) if q else q
    data = tweaks["per_language_title_sort_articles"]
    try:
        ans = data.get(q, None)
    except AttributeError:
        ans = None  # invalid tweak value
    try:
        ans = frozenset(ans) if ans else frozenset(data["eng"])
    except:
        ans = frozenset((r"A\s+", r"The\s+", r"An\s+"))
    ans = "|".join(ans)
    ans = "^(%s)" % ans
    try:
        ans = re.compile(ans, re.IGNORECASE)
    except:
        ans = re.compile(r"^(A|The|An)\s+", re.IGNORECASE)
    _title_pats[lang] = ans
    return ans


# Todo: This seems to be duplication - saw this somewhere else in the code base
_ignore_starts = "'\"" + "".join(chr(x) for x in [_ for _ in range(0x2018, 0x201E)] + [0x2032, 0x2033])


def title_sort(title, order=None, lang=None):
    """
    Strip a title and, unless strictly alphabetic, remove a leading quote and move a matching article to the end.

    Article patterns come from the language cache; a matching article is followed by a
    comma in the resulting sort string.

    Example:
        >>> title_sort(' The Book ', order='strictly_alphabetic')
        'The Book'


    :param title: Title string to normalize for sorting.
    :param order: Optional mode; None reads title_series_sorting, and
        strictly_alphabetic only strips outer whitespace.
    :param lang: Language selector passed to the article-pattern helper.
    :return: Stripped title sort string.
    """
    if order is None:
        order = tweaks["title_series_sorting"]
    title = title.strip()
    if order == "strictly_alphabetic":
        return title
    if title and title[0] in _ignore_starts:
        title = title[1:]
    match = get_title_sort_pat(lang).search(title)
    if match:
        try:
            prep = match.group(1)
        except IndexError:
            pass
        else:
            title = title[len(prep) :] + ", " + prep
            if title[0] in _ignore_starts:
                title = title[1:]
    return title.strip()


def check_name(candidate_name):
    """
    Classify cleaned multi-character tokens against first-name, last-name, and prefix/suffix lists.

    Reject unknown tokens and prefix/suffix-only candidates. A surname is not required,
    and a candidate with no retained tokens succeeds. First-name membership takes
    precedence over last-name membership. Name-list loading errors propagate.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/test_utils_coverage.py


    :param candidate_name: String split into whitespace tokens, lowercased and stripped
        of non-word characters; tokens shorter than two characters are ignored.
    :return: Boolean heuristic result; this is not proof that the input names a person.
    """
    candidate_name = deepcopy(candidate_name)

    # Separating the individual names and formatting them ready for checking
    candidate_name_split = candidate_name.split()
    candidate_name_split = [icu_lower(name.strip()) for name in candidate_name_split]

    # Dropping any special characters
    candidate_name_split = [re.sub(r"\W+", "", item) for item in candidate_name_split]
    candidate_name_split = [item for item in candidate_name_split if item is not None]

    # Filters the list for any empty strings, or strings with only one character
    # These might be initials.
    len_filter = lambda x: False if len(x) == 0 or len(x) == 1 else True
    candidate_name_split = [item for item in candidate_name_split if len_filter(item)]

    # Assembles the data it needs to actually preform the test and transforming it into a consistent format
    first_names, last_names = load_names(lower_case=True)
    prefix_suffix_set = set(name_prefixes.keys()).union(set(name_suffixes.keys()))
    prefix_suffix_set = set([icu_lower(re.sub(r"\W+", "", item)) for item in prefix_suffix_set])

    # A name is allowed any amount of prefixes and suffixes
    # It must be composed of a combination of valid names and suffixes
    # There must be at least one last name
    token_type_count = {
        "first_names": 0,
        "last_names": 0,
        "pre-suffixes": 0,
        "other": 0,
    }
    for token in candidate_name_split:
        if token in first_names:
            token_type_count["first_names"] += 1
        elif token in last_names:
            token_type_count["last_names"] += 1
        elif token in prefix_suffix_set:
            token_type_count["pre-suffixes"] += 1
        else:
            token_type_count["other"] += 1

    ttc = token_type_count

    # Analyses the count dictionary
    if ttc["other"] > 0:
        return False
    elif ttc["pre-suffixes"] > 0 and (ttc["first_names"] == 0) and (ttc["last_names"] == 0):
        return False
    else:
        return True


def score_title(title_string):
    """
    Preprocess title tokens and return the current constant score of zero.

    The cleaned token list is currently unused, so no ranking or name recognition is
    performed.

    Example:
        >>> score_title('Some Title')
        0


    :param title_string: String split, lowercased, and stripped of non-word characters.
    :return: Zero after successful token preprocessing.
    """
    # Separating the individual names and formatting them ready for checking
    title_string_split = title_string.split()
    title_string_split = [icu_lower(token.strip()) for token in title_string_split]

    # Dropping any special characters
    title_string_split = [re.sub(r"\W+", "", item) for item in title_string_split]
    title_string_split = [item for item in title_string_split if item is not None]

    # Filters the list for any empty strings, or strings with only one character
    # These might be initials.
    len_filter = lambda x: False if len(x) == 0 or len(x) == 1 else True
    title_string_split = [item for item in title_string_split if len_filter(item)]

    # If the title string contains words which aren't used as names then assume it's a title
    # Todo: Add support for dictionaries to check to see if this word is known

    return 0
