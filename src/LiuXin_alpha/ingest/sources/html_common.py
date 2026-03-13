from __future__ import annotations

from urllib.parse import urlparse, urlunparse

_HTML_PAGE_EXTENSIONS = {"", "htm", "html", "xhtm", "xhtml", "php", "asp", "aspx", "jsp", "jspx", "cgi"}


def normalize_http_url(url: str) -> str | None:
    text = str(url or "").strip()
    if not text:
        return None
    parsed = urlparse(text)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    normalized = parsed._replace(fragment="")
    return urlunparse(normalized)


def is_within_root_scope(root_url: str, candidate_url: str, *, span_hosts: bool, no_parent: bool) -> bool:
    root = urlparse(root_url)
    candidate = urlparse(candidate_url)
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
