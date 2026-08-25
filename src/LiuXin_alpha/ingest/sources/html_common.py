from __future__ import annotations

import string

from urllib.parse import parse_qsl, quote, unquote, urlparse, urlunparse

_HTML_PAGE_EXTENSIONS = {"", "htm", "html", "xhtm", "xhtml", "php", "asp", "aspx", "jsp", "jspx", "cgi"}


def normalize_http_url(url: str) -> str | None:
    text = str(url or "").strip()
    if not text:
        return None
    try:
        text.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return None
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in text):
        return None
    try:
        parsed = urlparse(text)
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if not hostname:
        return None
    try:
        ascii_hostname = hostname.encode("idna").decode("ascii").lower()
    except UnicodeError:
        return None
    authority_host = (
        f"[{ascii_hostname}]" if ":" in ascii_hostname else ascii_hostname
    )
    authority = (
        authority_host if port is None else f"{authority_host}:{port}"
    )
    if "\\" in parsed.path:
        return None
    if not _valid_percent_escapes(parsed.path) or not _valid_percent_escapes(
        parsed.query
    ):
        return None
    decoded_path = unquote(parsed.path)
    if "\\" in decoded_path or any(
        ord(character) < 32 or ord(character) == 127
        for character in decoded_path
    ):
        return None
    decoded_segments = decoded_path.split("/")
    if any(segment in {".", ".."} for segment in decoded_segments):
        return None
    if any(
        segment == ""
        for segment in decoded_segments[1:-1]
    ):
        return None
    if _contains_sensitive_query(parsed.query):
        return None
    encoded_path = quote(
        parsed.path,
        safe="/%:@!$&'()*+,;=-._~",
    )
    encoded_query = quote(
        parsed.query,
        safe="%:@!$&'()*+,;=/?-._~",
    )
    normalized = parsed._replace(
        scheme=scheme,
        netloc=authority,
        path=encoded_path,
        query=encoded_query,
        fragment="",
    )
    return urlunparse(normalized)


def _valid_percent_escapes(value: str) -> bool:
    hexadecimal = frozenset(string.hexdigits)
    position = 0
    while True:
        position = value.find("%", position)
        if position < 0:
            return True
        escape = value[position + 1 : position + 3]
        if len(escape) != 2 or any(char not in hexadecimal for char in escape):
            return False
        position += 3


def _contains_sensitive_query(query: str) -> bool:
    sensitive = {
        "access_token",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "credential",
        "key",
        "password",
        "secret",
        "sig",
        "signature",
        "token",
    }
    for name, _value in parse_qsl(query, keep_blank_values=True):
        normalized = name.strip().lower().replace("-", "_")
        if (
            normalized in sensitive
            or normalized.startswith("x_amz_")
            or normalized.startswith("x_goog_")
            or normalized.startswith("x_ms_")
        ):
            return True
    return False


def is_within_root_scope(root_url: str, candidate_url: str, *, span_hosts: bool, no_parent: bool) -> bool:
    normalized_root = normalize_http_url(root_url)
    normalized_candidate = normalize_http_url(candidate_url)
    if normalized_root is None or normalized_candidate is None:
        return False
    root = urlparse(normalized_root)
    candidate = urlparse(normalized_candidate)
    if candidate.scheme.lower() not in {"http", "https"}:
        return False
    if root.scheme and candidate.scheme.lower() != root.scheme.lower():
        return False
    if root.netloc and candidate.netloc.lower() != root.netloc.lower():
        return bool(span_hosts)
    if not no_parent:
        return True
    if not root.path:
        return True
    root_path = root.path.rstrip("/")
    if not root_path:
        return True
    return candidate.path.startswith(root_path + "/") or candidate.path == root_path


def looks_like_file_url(candidate_url: str) -> bool:
    parsed = urlparse(candidate_url)
    path = parsed.path or ""
    if not path:
        return False
    if path.endswith("/"):
        return False
    leaf = path.rsplit("/", 1)[-1]
    if "." not in leaf:
        return False
    return True


def looks_like_html_page_url(candidate_url: str) -> bool:
    parsed = urlparse(candidate_url)
    path = parsed.path or ""
    if not path or path.endswith("/"):
        return True
    leaf = path.rsplit("/", 1)[-1]
    if not leaf:
        return True
    if "." not in leaf:
        return True
    ext = leaf.rsplit(".", 1)[-1].lower().strip()
    return ext in _HTML_PAGE_EXTENSIONS
