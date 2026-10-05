#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2019, Kovid Goyal <kovid at kovidgoyal.net>


"""
Convert text and byte values across Calibre polyglot boundaries.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise binary through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
from base64 import standard_b64decode, standard_b64encode
from binascii import hexlify, unhexlify

from LiuXin_alpha.utils.libraries.calibre_polyglot.builtins import unicode_type


def as_base64_bytes(x, enc="utf-8"):
    """
    Perform the as base64 bytes utility operation under explicit compatibility rules.

    Example:
        Exercise as base64 bytes through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode(enc)
    return standard_b64encode(x)


def as_base64_unicode(x, enc="utf-8"):
    """
    Perform the as base64 unicode utility operation under explicit compatibility rules.

    Example:
        Exercise as base64 unicode through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode(enc)
    return standard_b64encode(x).decode("ascii")


def from_base64_unicode(x, enc="utf-8"):
    """
    Perform the from base64 unicode utility operation under explicit compatibility rules.

    Example:
        Exercise from base64 unicode through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode("ascii")
    return standard_b64decode(x).decode(enc)


def from_base64_bytes(x):
    """
    Perform the from base64 bytes utility operation under explicit compatibility rules.

    Example:
        Exercise from base64 bytes through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode("ascii")
    return standard_b64decode(x)


def as_hex_bytes(x, enc="utf-8"):
    """
    Perform the as hex bytes utility operation under explicit compatibility rules.

    Example:
        Exercise as hex bytes through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode(enc)
    return hexlify(x)


def as_hex_unicode(x, enc="utf-8"):
    """
    Perform the as hex unicode utility operation under explicit compatibility rules.

    Example:
        Exercise as hex unicode through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode(enc)
    return hexlify(x).decode("ascii")


def from_hex_unicode(x, enc="utf-8"):
    """
    Perform the from hex unicode utility operation under explicit compatibility rules.

    Example:
        Exercise from hex unicode through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param enc: Value supplied for enc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode("ascii")
    return unhexlify(x).decode(enc)


def from_hex_bytes(x):
    """
    Perform the from hex bytes utility operation under explicit compatibility rules.

    Example:
        Exercise from hex bytes through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        x = x.encode("ascii")
    return unhexlify(x)
