"""
Normalize crawler URLs and apply syntactic root-scope and filename heuristics.

These helpers reject selected malformed or credential-bearing URLs, not every
secret or unsafe network destination. They do not resolve DNS, restrict private
addresses, validate response bytes, or authorize requests. Scope comparisons
operate on normalized URL text rather than a server's decoded routing rules.
"""

from __future__ import annotations

import string

from urllib.parse import parse_qsl, quote, unquote, urlparse, urlunparse

_HTML_PAGE_EXTENSIONS = {"", "htm", "html", "xhtm", "xhtml", "php", "asp", "aspx", "jsp", "jspx", "cgi"}


def normalize_http_url(url: str) -> str | None:
    """
    Normalize accepted HTTP(S) URL text, returning None for the checked invalid forms.

    Strip outer whitespace; reject embedded raw whitespace/controls, invalid
    Unicode, userinfo, malformed host/port, bad path/query escapes, decoded path
    controls/backslashes, dot segments, interior empty segments, and recognized
    sensitive query names. Lowercase scheme/IDNA host, quote raw Unicode path/query
    text, retain existing percent escapes, and remove fragments.

    Checks are component-specific: percent-encoded query controls, unknown secret
    names, and the separate urlparse params field are not comprehensively vetted.
    Path decoding is single-pass with unquote's replacement behavior; equivalent
    escaped spellings/default ports are not collapsed. No network lookup occurs.

    Example:
        >>> normalize_http_url('HTTPS://Bücher.example/books/café.epub#chapter')
        'https://xn--bcher-kva.example/books/caf%C3%A9.epub'
        >>> normalize_http_url('https://reader:secret@example.test/book') is None
        True


    :param url: Candidate coerced to text, with falsey values treated as empty.
    :return: Accepted normalized URL, or None after a handled validation rejection.
    """
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
    """
    Require every percent sign to begin exactly two available hexadecimal digits.

    This checks escape syntax only, not UTF-8 decoding or the meaning of bytes.

    Example:
        >>> _valid_percent_escapes('book%20one'), _valid_percent_escapes('book%GG')
        (True, False)


    :param value: Path/query component scanned without decoding or normalization.
    :return: Whether all encountered percent triplets have valid hexadecimal syntax.
    """
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
    """
    Recognize selected credential/signature parameter names in a query component.

    Decode query names, strip/lowercase them, and replace hyphens with underscores.
    Values are ignored; unknown names, paths, fragments, and userinfo are outside
    this predicate's scope.

    Example:
        >>> _contains_sensitive_query('X-Amz-Signature=abc')
        True


    :param query: Query text without the leading question mark.
    :return: True for an explicit sensitive name or supported cloud-signing prefix.
    """
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
    """
    Compare normalized schemes, authorities, and optionally root-path prefixes.

    Invalid URLs fail closed and schemes must match. A different authority
    returns span_hosts immediately, bypassing the root-path restriction when
    enabled. On the same authority, no_parent requires equality or a slash-boundary
    descendant unless the root path is empty or '/'. Query/params do not define
    scope. Percent-escape spelling and explicit default ports remain significant.

    Example:
        >>> is_within_root_scope('https://example.test/books/', 'https://example.test/books/a.epub', span_hosts=False, no_parent=True)
        True


    :param root_url: Root URL normalized before comparison.
    :param candidate_url: Candidate normalized independently before comparison.
    :param span_hosts: Accept differing authorities when schemes match.
    :param no_parent: Restrict same-authority candidates to the normalized root path.
    :return: Whether these textual scope rules accept the candidate.
    """
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
    """
    Treat a nonempty, non-slash-terminated path leaf containing a dot as file-like.

    HTML pages and unknown suffixes can qualify; query-provided filenames do not.
    No URL normalization, MIME check, fetch, or file existence check occurs.

    Example:
        >>> looks_like_file_url('https://example.test/index.html')
        True


    :param candidate_url: URL text parsed solely to inspect its path leaf.
    :return: Whether the path satisfies the dotted-leaf heuristic.
    """
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
    """
    Recognize directory/extensionless paths or a configured HTML/dynamic-page suffix.

    A URL can be both page-like and file-like. This heuristic inspects only
    parsed path text, without fetching or validating the scheme/scope.

    Example:
        >>> looks_like_html_page_url('https://example.test/catalog.php?format=epub')
        True


    :param candidate_url: Candidate whose final path suffix is lowercased and stripped.
    :return: Whether the path is a plausible page to descend into.
    """
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
