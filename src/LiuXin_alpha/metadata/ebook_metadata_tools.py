
"""
Tools for metadata handling.

Very generic. Probably needs a better name.
"""


import re
from copy import deepcopy

from typing import Optional

from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.text import remove_bracketed_text
from LiuXin_alpha.utils.plugins.name_loader import load_names

from LiuXin_alpha.constants import name_prefixes
from LiuXin_alpha.constants import name_suffixes

from LiuXin_alpha.utils.text.icu import lower as icu_lower




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
        digits = map(int, isbn[:12])
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
        if (check == 10 and issn[7] == "X") or check == int(issn[7]):
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
    from LiuXin.utils.localization import canonicalize_lang, get_lang

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
