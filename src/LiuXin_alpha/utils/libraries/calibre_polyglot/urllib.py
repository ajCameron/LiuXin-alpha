#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2018, Kovid Goyal <kovid at kovidgoyal.net>


from urllib.request import (
    build_opener,
    getproxies,
    install_opener,
    HTTPBasicAuthHandler,
    HTTPCookieProcessor,
    HTTPDigestAuthHandler,
    url2pathname,
    urlopen,
    Request,
)  # noqa
from urllib.parse import (
    parse_qs,
    quote,
    unquote as uq,
    quote_plus,
    urldefrag,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
    urlsplit,
    urlunsplit,
)
from urllib.error import HTTPError, URLError  # noqa

__all__ = [
    "build_opener",
    "getproxies",
    "install_opener",
    "HTTPBasicAuthHandler",
    "HTTPCookieProcessor",
    "HTTPDigestAuthHandler",
    "url2pathname",
    "urlopen",
    "Request",
    "parse_qs",
    "quote",
    "uq",
    "quote_plus",
    "urldefrag",
    "urlencode",
    "urljoin",
    "urlparse",
    "urlunparse",
    "urlsplit",
    "urlunsplit",
    "HTTPError",
    "URLError",
    "unquote",
    "unquote_plus",
]


def unquote(x, encoding="utf-8", errors="replace"):
    binary = isinstance(x, bytes)
    if binary:
        x = x.decode(encoding, errors)
    ans = uq(x, encoding, errors)
    if binary:
        ans = ans.encode(encoding, errors)
    return ans


def unquote_plus(x, encoding="utf-8", errors="replace"):
    q, repl = (b"+", b" ") if isinstance(x, bytes) else ("+", " ")
    x = x.replace(q, repl)
    return unquote(x, encoding=encoding, errors=errors)
