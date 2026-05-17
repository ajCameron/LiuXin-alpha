
"""
Tools for metadata handling.

Very generic. Probably needs a better name.
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
    Convert "timestamp-like" inputs into an integer epoch milliseconds.

    Accepted inputs:
      - datetime / date
      - int / float: epoch seconds, milliseconds, microseconds, nanoseconds (auto-guessed)
      - str / bytes: numeric, ISO-8601-ish, and a few common datetime formats
      - also supports extracting a long integer from strings like "/Date(1609459200000)/"

    Heuristic for numeric magnitude (abs):
      - >= 1e17  -> nanoseconds
      - >= 1e14  -> microseconds
      - >= 1e11  -> milliseconds
      - else     -> seconds
    """
    if value is None:
        raise TypeError("None is not a timestamp")
    if isinstance(value, bool):
        raise TypeError("bool is not a timestamp")

    if now is None:
        now = datetime.now(timezone.utc)

    def dt_to_ms(dt: datetime) -> int:
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
    if authors is not None:
        return " & ".join([a.replace("&", "&&") for a in authors if a])
    else:
        return ""


def author_to_author_sort(author, method=None):
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
    return " & ".join(map(author_to_author_sort, authors))


# imported from calibre
# Todo: Remove spaces, "-" e.t.c - common ways of breakup up an isbn10
def check_isbn10(isbn: str) -> Optional[str]:
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
    cisbn = check_isbn(isbn)
    if not cisbn:
        return isbn
    i = cisbn
    if len(i) == 10:
        return "-".join((i[:2], i[2:6], i[6:9], i[9]))
    return "-".join((i[:3], i[3:5], i[5:9], i[9:12], i[12]))


# imported from calibre
def check_doi(doi):
    "Check if something that looks like a DOI is present anywhere in the string"
    if not doi:
        return None
    doi_check = re.search(r"10\.\d{4}/\S+", doi)
    if doi_check is not None:
        return doi_check.group()
    return None


_title_pats = {}


def get_title_sort_pat(lang=None):
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
    Uses the cv file in calibre names to try and test to see if the candidate name is, in fact, a name.
    Only works for English at the moment - a False doesn't indicate that it is certainly false.
    Just that it doesn't appear in the given (English) name lists.
    :param candidate_name:
    :return True/False:
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
    Unlike the names case it is very hard to be sure if something is a title or not.
    Thus the return is an integer score.
    Currently only 0 and 1.
    :param title_string:
    :return:
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
