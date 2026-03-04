"""
Infer metadata from a filename/string.

This module is intentionally heuristic. It should fail soft, return a metadata
container, and preserve useful leftovers as comments.
"""

from __future__ import annotations

import os
import re
from collections import OrderedDict
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from typing import Any

from LiuXin_alpha.metadata.constants import ISBN_PATTENRS
from LiuXin_alpha.metadata.ebook_metadata_tools import check_name
from LiuXin_alpha.metadata.metadata import MetaData
from LiuXin_alpha.metadata.utils import check_isbn, string_to_authors
from LiuXin_alpha.utils.date import parse_only_date
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.python_tools import drop_characters_from_string as drop_strip

VALID_FOR = ["FILENAME"]
PRIORITY_FOR = ["NONE"]
RUN_COST = ["LOW"]

# Legacy exports retained for compatibility.
ISBN_DROP_PATTERNS = [r"(\s*ISBN.*)", r"[0-9-/_\\xX\s]+"]
possible_separators = {"-", "/", "_", "\\", ".", " ", "&", ",", "|", "~", ":", ";", "—", "–"}
regex_special_characters = {
    ".",
    "\\",
    "+",
    "*",
    "?",
    "[",
    "^",
    "]",
    "$",
    "(",
    ")",
    "{",
    "}",
    "=",
    "!",
    "<",
    ">",
    "|",
    ":",
    "-",
}
possible_parenthesis = {r"(": r")", r"[": r"]", r"{": r"}", r"<": r">"}
token_trial_regex = {r"(?P<title>.+) by (?P<authors>.+)"}

_SPLIT_MARKER = "(SPLIT)"

_LABELLED_ISBN_RE = re.compile(r"(?i)\bISBN(?:-1[03])?\s*[:=]?\s*([0-9Xx][0-9Xx\-\s]{8,30})")
_GENERIC_ISBN_RE = re.compile(r"(?<!\d)(?:97[89][\dXx\-\s]{10,20}|[\dXx][\dXx\-\s]{9,20})(?!\d)")

_BRACKETED_DATE_PATTERNS = (
    re.compile(r"[\(\[\{<]\s*(?P<date>[12]\d{3}[-_/\.][01]?\d[-_/\.][0-3]?\d)\s*[\)\]\}>]"),
    re.compile(r"[\(\[\{<]\s*(?P<date>[12]\d{3})\s*[\)\]\}>]"),
)
_PREFIX_DATE_RE = re.compile(
    r"(?i)\b(?:published|pub|date|year)\s*[:=_-]?\s*(?P<date>[12]\d{3}(?:[-_/\.][01]?\d(?:[-_/\.][0-3]?\d)?)?)"
)

_FORCE_PATTERNS = (
    r"(?i)^(?P<title>.+?)\s+by\s+(?P<authors>.+)$",
    r"^(?P<title>.+?)\s*[-–—]\s*(?P<authors>[^-–—]+)$",
    r"^(?P<authors>[^-–—]+)\s*[-–—]\s*(?P<title>.+)$",
)

_COMMENT_HINTS = {"unabridged", "abridged", "annotated", "illustrated", "revised", "edition", "vol", "volume"}
_TITLE_HINT_WORDS = {"the", "a", "an", "of", "for", "to", "in", "on", "at", "from", "into", "with"}


def _coerce_text(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    if isinstance(raw, os.PathLike):
        return os.fspath(raw)
    return str(raw)


def _normalize_whitespace(raw: str) -> str:
    return re.sub(r"\s+", " ", raw or "").strip()


def _normalize_title_text(raw: str) -> str:
    text = _coerce_text(raw)
    text = text.replace("_", " ")
    return _normalize_whitespace(text)


def _is_bracketed(token: str) -> bool:
    token = (token or "").strip()
    if len(token) < 2:
        return False
    return token[0] in possible_parenthesis and token[-1] == possible_parenthesis[token[0]]


def _strip_outer_brackets(token: str) -> str:
    token = (token or "").strip()
    if _is_bracketed(token):
        return token[1:-1].strip()
    return token


def _path_to_parse_text(target: Any, *, full_path_regex: bool = False) -> tuple[str, str]:
    raw = _coerce_text(target)
    no_ext = os.path.splitext(raw)[0]
    if full_path_regex:
        parse_text = no_ext
    else:
        parse_text = os.path.basename(no_ext)
    fallback_title = _normalize_title_text(os.path.splitext(os.path.basename(raw))[0]) or _("Unknown")
    return parse_text, fallback_title


def _split_tags(raw: str) -> list[str]:
    parts = re.split(r"\s*(?:,|;|\||/)\s*", raw)
    out: list[str] = []
    for part in parts:
        p = _normalize_whitespace(part)
        if p:
            out.append(p)
    return out


def _split_authors(raw: str) -> list[str]:
    raw = _normalize_whitespace(raw)
    if not raw:
        return []

    raw = re.sub(r"(?i)^by\s+", "", raw).strip()
    first_pass = string_to_authors(raw)

    out: list[str] = []
    for author in first_pass:
        author = _normalize_whitespace(author)
        if not author:
            continue
        for piece in re.split(r"\s*(?:;|\||/)\s*", author):
            piece = _normalize_whitespace(piece)
            if piece:
                out.append(piece)

    # Stable dedupe.
    seen = set()
    deduped: list[str] = []
    for author in out:
        key = author.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(author)
    return deduped


def _author_score(candidate: str) -> int:
    text = _normalize_whitespace(candidate)
    if not text:
        return 0

    score = 0
    if any(sep in text for sep in ("&", ";", "|", "/")):
        score += 3

    try:
        if check_name(text):
            score += 3
    except Exception:
        pass

    words = re.findall(r"[\w\u00C0-\u024F\u0370-\u03FF\u0400-\u04FF\u4E00-\u9FFF']+", text)
    if words and not any(w.isdigit() for w in words):
        score += 1
    if 1 <= len(words) <= 5:
        score += 1
    if any(ch.isdigit() for ch in text):
        score -= 2

    low = text.lower()
    if any(hint in low for hint in _COMMENT_HINTS):
        score -= 2

    return score


def _title_score(candidate: str) -> int:
    text = _normalize_whitespace(candidate)
    if not text:
        return 0

    score = 0
    words = [w.lower() for w in re.findall(r"[\w\u00C0-\u024F\u0370-\u03FF\u0400-\u04FF\u4E00-\u9FFF']+", text)]
    if len(words) >= 3:
        score += 2
    elif len(words) == 2:
        score += 1

    if any(word in _TITLE_HINT_WORDS for word in words):
        score += 1
    if ":" in text or text.endswith("!") or text.endswith("?"):
        score += 1

    try:
        if check_name(text):
            score -= 2
    except Exception:
        pass

    if any(sep in text for sep in ("&", ";", " and ", " with ", "|")):
        score -= 1
    return score


def _set_if_non_empty(mi: MetaData, field: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str) and not _normalize_whitespace(value):
        return
    setattr(mi, field, value)


def _append_comment(mi: MetaData, value: str) -> None:
    value = _normalize_whitespace(value)
    if not value:
        return
    mi.comments = value


def _known_series_names(mi: MetaData) -> list[str]:
    raw = getattr(mi, "series", None)
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [str(k) for k in raw.keys()]
    if isinstance(raw, str):
        text = _normalize_whitespace(raw)
        return [text] if text else []
    try:
        return [str(x) for x in raw]
    except Exception:
        return []


def _parse_date_value(raw: str | None):
    raw = _normalize_whitespace(raw or "")
    if not raw:
        return None

    raw = raw.replace("_", "-").replace("/", "-").replace(".", "-")
    if re.fullmatch(r"[12]\d{3}", raw):
        raw = f"{raw}-06-02"
    try:
        return parse_only_date(raw)
    except Exception:
        # Last resort: strict YYYY-MM-DD parser.
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d")
        except Exception:
            return None
        return dt.date()


def _apply_regex_groups_to_metadata(mi: MetaData, groups: dict[str, str]) -> None:
    # Normalize known aliases to one metadata surface.
    title = groups.get("title")
    authors = groups.get("authors") or groups.get("author")
    series = groups.get("series")
    series_index = groups.get("series_index")
    isbn = groups.get("isbn")
    publisher = groups.get("publisher")
    comments = groups.get("comments") or groups.get("comment")
    tags = groups.get("tags") or groups.get("tag")
    language = groups.get("language") or groups.get("lang")
    published = groups.get("published") or groups.get("pubdate") or groups.get("date") or groups.get("year")

    if title:
        _set_if_non_empty(mi, "title", _normalize_title_text(title))
    if authors:
        parsed_authors = _split_authors(authors)
        if parsed_authors:
            _set_if_non_empty(mi, "authors", parsed_authors)
    if series:
        _set_if_non_empty(mi, "series", _normalize_whitespace(series))
    if series_index:
        try:
            index_value = float(str(series_index).strip().replace(",", "."))
            if series:
                _set_if_non_empty(mi, "series_index", (_normalize_whitespace(series), index_value))
            else:
                names = _known_series_names(mi)
                if len(names) == 1:
                    _set_if_non_empty(mi, "series_index", (names[0], index_value))
        except Exception:
            pass
    if isbn:
        checked = check_isbn(re.sub(r"[^0-9Xx]", "", str(isbn)))
        if checked:
            _set_if_non_empty(mi, "isbn", checked)
    if publisher:
        _set_if_non_empty(mi, "publisher", _normalize_whitespace(publisher))
    if comments:
        _append_comment(mi, comments)
    if tags:
        parsed_tags = _split_tags(tags)
        if parsed_tags:
            _set_if_non_empty(mi, "tags", parsed_tags)
    if language:
        _set_if_non_empty(mi, "language", _normalize_whitespace(language))
    if published:
        date_value = _parse_date_value(published)
        if date_value is not None:
            _set_if_non_empty(mi, "pubdate", date_value)


def _compile_patterns(force_regex: Any) -> list[re.Pattern[str]]:
    if not force_regex:
        return []

    if force_regex is True:
        raw_patterns: Iterable[Any] = _FORCE_PATTERNS
    elif isinstance(force_regex, (str, bytes)):
        raw_patterns = [force_regex]
    elif isinstance(force_regex, re.Pattern):
        return [force_regex]
    elif isinstance(force_regex, Iterable):
        raw_patterns = force_regex
    else:
        return []

    compiled: list[re.Pattern[str]] = []
    for pattern in raw_patterns:
        if isinstance(pattern, re.Pattern):
            compiled.append(pattern)
            continue
        pat_text = _coerce_text(pattern)
        if not pat_text:
            continue
        try:
            compiled.append(re.compile(pat_text, re.IGNORECASE | re.UNICODE))
        except re.error:
            continue
    return compiled


def _consume_parenthesized_tokens(tokens: list[str]) -> tuple[list[str], list[str]]:
    parenthesized = [t for t in tokens if _is_bracketed(t)]
    plain = [t for t in tokens if not _is_bracketed(t)]
    return parenthesized, plain


def _extract_series_from_parenthesized_tokens(tokens: list[str]) -> tuple[tuple[str, float] | None, list[str]]:
    kept: list[str] = []
    for token_idx, token in enumerate(tokens):
        core = _strip_outer_brackets(token)
        m = re.match(r"(?i)^(.+?)\s*(?:#|book\s*)\s*(\d+(?:\.\d+)?)$", core)
        if m is None:
            m = re.match(r"(?i)^(.+?)\s*[\(\[]\s*(\d+(?:\.\d+)?)\s*[\)\]]$", core)
        if m is None:
            kept.append(token)
            continue
        series = _normalize_whitespace(m.group(1))
        if not series:
            kept.append(token)
            continue
        try:
            series_index = float(m.group(2))
        except Exception:
            kept.append(token)
            continue
        return (series, series_index), kept + tokens[token_idx + 1 :]
    return None, kept


def _parse_title_and_authors_heuristic(raw: str) -> tuple[str | None, list[str], list[str]]:
    text = _normalize_whitespace(raw)
    if not text:
        return None, [], []

    # No obvious separator -> keep full text as title.
    if re.search(r"(?i)\s+by\s+", text) is None and re.search(r"\s+[-–—_/\\]\s+", text) is None:
        return text, [], []

    # Strong signal: "title by author".
    by_match = re.match(r"(?is)^(?P<title>.+?)\s+by\s+(?P<authors>.+)$", text)
    if by_match:
        title = _normalize_whitespace(by_match.group("title"))
        authors = _split_authors(by_match.group("authors"))
        if authors:
            return title or None, authors, []

    # Try split around common separators and score both sides as author candidates.
    splitters = [r"\s+[-–—]\s+", r"\s+_\s+", r"\s+/\s+", r"\s+\\\s+"]
    for splitter in splitters:
        parts = [p for p in re.split(splitter, text) if _normalize_whitespace(p)]
        if len(parts) < 2:
            continue

        left = _normalize_whitespace(" - ".join(parts[:-1]))
        right = _normalize_whitespace(parts[-1])
        left_author = _author_score(left)
        right_author = _author_score(right)
        left_words = re.findall(r"[\w\u00C0-\u024F\u0370-\u03FF\u0400-\u04FF\u4E00-\u9FFF']+", left)
        right_words = re.findall(r"[\w\u00C0-\u024F\u0370-\u03FF\u0400-\u04FF\u4E00-\u9FFF']+", right)

        # Common compact patterns:
        #   "Author Name - OneWordTitle"
        #   "OneWordTitle - Author Name"
        if len(left_words) == 2 and len(right_words) == 1:
            return right or None, _split_authors(left), []
        if len(left_words) == 1 and len(right_words) == 2:
            return left or None, _split_authors(right), []

        # Option A: left is title, right is author.
        option_a = _title_score(left) + right_author
        # Option B: right is title, left is author.
        option_b = _title_score(right) + left_author

        if option_b >= option_a + 2 and left_author >= 2:
            return right or None, _split_authors(left), []
        if option_a >= option_b and right_author >= 2:
            return left or None, _split_authors(right), []
        if option_b > option_a and left_author >= 2:
            return right or None, _split_authors(left), []

    return text, [], []


def _extract_tags_and_comments_from_parenthesized(tokens: list[str]) -> tuple[list[str], list[str]]:
    tags: list[str] = []
    comments: list[str] = []

    for token in tokens:
        core = _strip_outer_brackets(token)
        lower = core.lower()

        if lower.startswith("tags:"):
            tags.extend(_split_tags(core.partition(":")[2]))
            continue

        if core.startswith("#"):
            tag_parts = [p.strip() for p in re.split(r"\s+", core) if p.strip()]
            for part in tag_parts:
                part = part.lstrip("#").strip()
                if part:
                    tags.append(part)
            continue

        comments.append(core)

    return tags, comments


def get_metadata(target_string, force_regex=False, full_path_regex=False):
    """
    Parse metadata from a filename/path-like string.

    :param target_string: filename/path-like or arbitrary string
    :param force_regex: bool/regex/string/list of regexes used as explicit parser
    :param full_path_regex: if True, regex parsing uses the full path (minus extension)
    """
    parse_text, fallback_title = _path_to_parse_text(target_string, full_path_regex=full_path_regex)
    working_text = deepcopy(parse_text)

    mi = MetaData()

    for pattern in _compile_patterns(force_regex):
        match = pattern.search(working_text)
        if match is None:
            continue
        groups = {k: v for k, v in match.groupdict().items() if v is not None}
        if groups:
            _apply_regex_groups_to_metadata(mi, groups)
        # keep parsing with heuristics for missing fields, but remove exact match
        span = match.span(0)
        if span[0] != span[1]:
            working_text = (working_text[: span[0]] + f" {_SPLIT_MARKER} " + working_text[span[1] :]).strip()
        break

    if mi.is_null("isbn"):
        isbns = get_isbn_from_string(working_text)
        if isbns:
            mi.isbn = isbns[0]
    working_text = drop_isbn_from_string(working_text, replacement=_SPLIT_MARKER)

    date_value, working_text = pop_date(working_text, replacement=_SPLIT_MARKER)
    if date_value is not None and mi.is_null("pubdate"):
        mi.pubdate = date_value

    tokens = tokenize(working_text)
    parenthesized_tokens, _plain_tokens = _consume_parenthesized_tokens(tokens)

    if mi.is_null("series"):
        series_data, parenthesized_tokens = _extract_series_from_parenthesized_tokens(parenthesized_tokens)
        if series_data is not None:
            series, idx = series_data
            mi.series = series
            mi.series_index = (series, idx)

    p_tags, p_comments = _extract_tags_and_comments_from_parenthesized(parenthesized_tokens)
    if p_tags and mi.is_null("tags"):
        mi.tags = p_tags

    plain_text = re.sub(r"[\(\[\{<][^()\[\]{}<>]*[\)\]\}>]", " ", working_text)
    plain_text = _normalize_whitespace(plain_text.replace(_SPLIT_MARKER, " "))

    if mi.is_null("title") or mi.is_null("authors"):
        title, authors, comments = _parse_title_and_authors_heuristic(plain_text)
        if title and mi.is_null("title"):
            mi.title = _normalize_title_text(title)
        if authors and mi.is_null("authors"):
            mi.authors = authors
        for comment in comments:
            _append_comment(mi, comment)

    for comment in p_comments:
        _append_comment(mi, comment)

    if mi.is_null("title"):
        mi.title = _normalize_title_text(fallback_title)

    if mi.is_null("authors"):
        mi.authors = _("Unknown")

    return mi


# Attempts to bring any string, no matter how weird the layout, into a regular arrangement of tokens for processing

def tokenize(target_string):
    """
    Convert a string into coarse metadata tokens.

    Parenthesized groups are preserved as individual tokens.
    """
    target_string = _coerce_text(target_string)
    if not target_string:
        return []

    target_string = target_string.replace(_SPLIT_MARKER, f" {_SPLIT_MARKER} ")
    tokens = split_out_parenthesized_text([target_string])

    out: list[str] = []
    for token in tokens:
        if _is_bracketed(token):
            core = _normalize_whitespace(token)
            if core:
                out.append(core)
            continue

        chunks = re.split(r"(?:\s+|[\-_/\\.,&|~:;])+", token)
        for chunk in chunks:
            chunk = _normalize_whitespace(chunk)
            if chunk:
                out.append(chunk)

    # Stable dedupe of accidental repeated split markers.
    cleaned: list[str] = []
    prev = None
    for token in out:
        if token == _SPLIT_MARKER and prev == _SPLIT_MARKER:
            continue
        cleaned.append(token)
        prev = token
    return cleaned


def split_out_parenthesized_text(string_index):
    """
    Split a list of strings around recognized bracket pairs.
    """
    string_index = [str(s) for s in (string_index or [])]

    parenthesis_regex_set = set()
    base_regex = r"{}([^{}]*){}"
    for parenthesis in possible_parenthesis:
        if parenthesis in regex_special_characters:
            l_sub_string = "\\" + parenthesis
            r_sub_string = "\\" + possible_parenthesis[parenthesis]
            parenthesis_regex_set.add(base_regex.format(l_sub_string, r_sub_string, r_sub_string))
        else:
            l_sub_string = parenthesis
            r_sub_string = possible_parenthesis[parenthesis]
            parenthesis_regex_set.add(base_regex.format(l_sub_string, r_sub_string, r_sub_string))

    for regex in parenthesis_regex_set:
        return_index = []
        for string in string_index:
            return_index += extract_by_parenthesis_regex(string, regex)
        string_index = return_index

    return [item.strip() for item in string_index if item is not None and item.strip() != ""]


def extract_by_parenthesis_regex(target_string, regex):
    """
    Split a string around one parenthesized regex group.
    """
    target_string = _coerce_text(target_string)
    regex = _coerce_text(regex)

    return_index = []
    regex_results = re.findall(regex, target_string, re.I)
    if len(regex_results) == 0:
        return [target_string]

    split_string = re.split(regex, target_string)
    for string in split_string:
        if string in regex_results:
            return_index.append("(" + string + ")")
        else:
            return_index.append(string)
    return return_index


def test_for_parenthesis(target_string):
    """
    Return True if any recognized opening parenthesis appears in the string.
    """
    target_string = _coerce_text(target_string)
    for character in target_string:
        if character in possible_parenthesis.keys():
            return True
    return False


def get_separator_count(target_string, separators=possible_separators):
    """
    Count separator frequency and return an OrderedDict sorted by count (desc).
    """
    target_string = _coerce_text(target_string)
    separators = list(separators)

    separator_count = []
    for separator in separators:
        separator_count.append(target_string.count(separator))
    sep_count_pairs = zip(separators, separator_count)
    sep_count_pairs = sorted(sep_count_pairs, key=lambda count: count[1], reverse=True)
    return OrderedDict([pair for pair in sep_count_pairs])


# (SPLIT) is used as a hard delimiter marker that something significant was removed

def pop_date(target_string, replacement=_SPLIT_MARKER):
    """
    Extract a publication-like date from a string and replace it with a split marker.

    Returns `(date_or_none, new_string)`.
    """
    target_string = _coerce_text(target_string)

    # 1) Bracketed forms: (2020), [2020-10-03], etc.
    for pat in _BRACKETED_DATE_PATTERNS:
        match = pat.search(target_string)
        if match is None:
            continue
        raw_date = match.group("date")
        parsed = _parse_date_value(raw_date)
        if parsed is None:
            continue
        new_string = target_string[: match.start()] + replacement + target_string[match.end() :]
        return parsed, new_string

    # 2) Prefixed forms: published:2020
    match = _PREFIX_DATE_RE.search(target_string)
    if match is not None:
        parsed = _parse_date_value(match.group("date"))
        if parsed is not None:
            new_string = target_string[: match.start()] + replacement + target_string[match.end() :]
            return parsed, new_string

    return None, target_string


# Todo - can't find ISBNs embedded in larger blocks of numbers. Might be a good feature. Or not.
def get_isbn_from_string(target_string):
    """
    Extract valid ISBNs from a string and return them as a sorted list.
    """
    target_string = _coerce_text(target_string)
    candidate_set = set()

    for regex in ISBN_PATTENRS:
        try:
            for candidate in re.findall(regex, target_string):
                candidate_set.add(candidate)
        except re.error:
            continue

    for match in _LABELLED_ISBN_RE.finditer(target_string):
        candidate_set.add(match.group(1))

    for match in _GENERIC_ISBN_RE.finditer(target_string):
        candidate_set.add(match.group(0))

    drop_set = {"-", "/", "_", "\\", " ", "\t", "\n", "\r"}

    cleaned = set()
    for candidate in candidate_set:
        raw = drop_strip(str(candidate), drop_set)
        raw = re.sub(r"[^0-9Xx]", "", raw)
        checked = check_isbn(raw)
        if checked is not None:
            cleaned.add(checked)

    return sorted(cleaned)


def drop_isbn_from_string(target_string, replacement=_SPLIT_MARKER):
    """
    Remove valid ISBN occurrences from a string.
    """
    target_string = _coerce_text(target_string)

    # First remove explicit labelled segments so ISBN text doesn't leak into comments.
    target_string = _LABELLED_ISBN_RE.sub(replacement, target_string)

    # Then remove any generic candidates that validate as ISBN.
    for raw in list(_GENERIC_ISBN_RE.findall(target_string)):
        cleaned = re.sub(r"[^0-9Xx]", "", raw)
        if check_isbn(cleaned) is None:
            continue
        target_string = target_string.replace(raw, replacement)

    # Finally remove leftover bare ISBN labels.
    target_string = re.sub(r"(?i)\bISBN(?:-1[03])?\s*[:=]?", "", target_string)

    # De-noise repeated replacement markers.
    target_string = re.sub(r"(?:\s*\(SPLIT\)\s*){2,}", f" {replacement} ", target_string)
    return _normalize_whitespace(target_string)


__all__ = [
    "VALID_FOR",
    "PRIORITY_FOR",
    "RUN_COST",
    "get_metadata",
    "tokenize",
    "split_out_parenthesized_text",
    "extract_by_parenthesis_regex",
    "test_for_parenthesis",
    "get_separator_count",
    "pop_date",
    "get_isbn_from_string",
    "drop_isbn_from_string",
]
