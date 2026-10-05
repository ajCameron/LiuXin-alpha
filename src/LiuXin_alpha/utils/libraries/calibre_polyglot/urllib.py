#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2018, Kovid Goyal <kovid at kovidgoyal.net>


"""
Expose normalized URL parsing, quoting and request helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise urllib through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
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
    """
    Perform the unquote utility operation under explicit compatibility rules.

    Example:
        Exercise unquote through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    binary = isinstance(x, bytes)
    if binary:
        x = x.decode(encoding, errors)
    ans = uq(x, encoding, errors)
    if binary:
        ans = ans.encode(encoding, errors)
    return ans


def unquote_plus(x, encoding="utf-8", errors="replace"):
    """
    Perform the unquote plus utility operation under explicit compatibility rules.

    Example:
        Exercise unquote plus through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    q, repl = (b"+", b" ") if isinstance(x, bytes) else ("+", " ")
    x = x.replace(q, repl)
    return unquote(x, encoding=encoding, errors=errors)
