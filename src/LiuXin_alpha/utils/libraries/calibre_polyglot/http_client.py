#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Eli Schwartz <eschwartz@archlinux.org>

"""
Expose HTTP client compatibility aliases.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise http client through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from http.client import (
    responses,
    HTTPConnection,
    HTTPSConnection,
    BAD_REQUEST,
    FOUND,
    FORBIDDEN,
    HTTP_VERSION_NOT_SUPPORTED,
    INTERNAL_SERVER_ERROR,
    METHOD_NOT_ALLOWED,
    MOVED_PERMANENTLY,
    NOT_FOUND,
    NOT_IMPLEMENTED,
    NOT_MODIFIED,
    OK,
    PARTIAL_CONTENT,
    PRECONDITION_FAILED,
    REQUEST_ENTITY_TOO_LARGE,
    REQUEST_URI_TOO_LONG,
    REQUESTED_RANGE_NOT_SATISFIABLE,
    REQUEST_TIMEOUT,
    SEE_OTHER,
    SERVICE_UNAVAILABLE,
    UNAUTHORIZED,
    PRECONDITION_REQUIRED,
    UNPROCESSABLE_ENTITY,
)

__all__ = [
    "responses",
    "HTTPConnection",
    "HTTPSConnection",
    "BAD_REQUEST",
    "FOUND",
    "FORBIDDEN",
    "HTTP_VERSION_NOT_SUPPORTED",
    "INTERNAL_SERVER_ERROR",
    "METHOD_NOT_ALLOWED",
    "MOVED_PERMANENTLY",
    "NOT_FOUND",
    "NOT_IMPLEMENTED",
    "NOT_MODIFIED",
    "OK",
    "PARTIAL_CONTENT",
    "PRECONDITION_FAILED",
    "REQUEST_ENTITY_TOO_LARGE",
    "REQUEST_URI_TOO_LONG",
    "REQUESTED_RANGE_NOT_SATISFIABLE",
    "REQUEST_TIMEOUT",
    "SEE_OTHER",
    "SERVICE_UNAVAILABLE",
    "UNAUTHORIZED",
    "PRECONDITION_REQUIRED",
    "UNPROCESSABLE_ENTITY",
]
