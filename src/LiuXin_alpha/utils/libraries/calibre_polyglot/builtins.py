#!/usr/bin/env python
# vim:fileencoding=utf-8
# License: GPL v3 Copyright: 2018, Kovid Goyal <kovid at kovidgoyal.net>

"""
Expose cross-version built-in aliases used by Calibre compatibility code.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise builtins through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
import os
import sys

is_py3 = sys.version_info.major >= 3
native_string_type = str
iterkeys = iter


def hasenv(x):
    """
    Perform the hasenv utility operation under explicit compatibility rules.

    Example:
        Exercise hasenv through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return getenv(x) is not None


def as_bytes(x, encoding="utf-8"):
    """
    Perform the as bytes utility operation under explicit compatibility rules.

    Example:
        Exercise as bytes through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, unicode_type):
        return x.encode(encoding)
    if isinstance(x, bytes):
        return x
    if isinstance(x, bytearray):
        return bytes(x)
    if isinstance(x, memoryview):
        return x.tobytes()
    ans = unicode_type(x)
    if isinstance(ans, unicode_type):
        ans = ans.encode(encoding)
    return ans


def as_unicode(x, encoding="utf-8", errors="strict"):
    """
    Perform the as unicode utility operation under explicit compatibility rules.

    Example:
        Exercise as unicode through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, bytes):
        return x.decode(encoding, errors)
    return unicode_type(x)


def only_unicode_recursive(x, encoding="utf-8", errors="strict"):
    # Convert any bytestrings in sets/lists/tuples/dicts to unicode
    """
    Perform the only unicode recursive utility operation under explicit compatibility rules.

    Example:
        Exercise only unicode recursive through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, bytes):
        return x.decode(encoding, errors)
    if isinstance(x, unicode_type):
        return x
    if isinstance(x, (set, list, tuple, frozenset)):
        return type(x)(only_unicode_recursive(i, encoding, errors) for i in x)
    if isinstance(x, dict):
        return {
            only_unicode_recursive(k, encoding, errors): only_unicode_recursive(v, encoding, errors)
            for k, v in iteritems(x)
        }
    return x


def reraise(tp, value, tb=None):
    """
    Perform the reraise utility operation under explicit compatibility rules.

    Example:
        Exercise reraise through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param tp: Value supplied for tp under the utility contract.
    :param value: Value normalized, stored, formatted or returned.
    :param tb: Value supplied for tb under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        if value is None:
            value = tp()
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value
    finally:
        value = None
        tb = None


import builtins

zip = builtins.zip
map = builtins.map
filter = builtins.filter
range = builtins.range

codepoint_to_chr = chr
unicode_type = str
string_or_bytes = str, bytes
string_or_unicode = str
long_type = int
raw_input = input
getcwd = os.getcwd
getenv = os.getenv


def error_message(exc):
    """
    Perform the error message utility operation under explicit compatibility rules.

    Example:
        Exercise error message through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param exc: Value supplied for exc under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    args = getattr(exc, "args", None)
    if args and isinstance(args[0], unicode_type):
        return args[0]
    return unicode_type(exc)


def iteritems(d):
    """
    Perform the iteritems utility operation under explicit compatibility rules.

    Example:
        Exercise iteritems through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param d: Value supplied for d under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return iter(d.items())


def itervalues(d):
    """
    Perform the itervalues utility operation under explicit compatibility rules.

    Example:
        Exercise itervalues through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param d: Value supplied for d under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return iter(d.values())


def environ_item(x):
    """
    Perform the environ item utility operation under explicit compatibility rules.

    Example:
        Exercise environ item through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, bytes):
        x = x.decode("utf-8")
    return x


def exec_path(path, ctx=None):
    """
    Perform the exec path utility operation under explicit compatibility rules.

    Example:
        Exercise exec path through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param ctx: Value supplied for ctx under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ctx = ctx or {}
    with open(path, "rb") as f:
        code = f.read()
    code = compile(code, f.name, "exec")
    exec(code, ctx)


def cmp(a, b):
    """
    Perform the cmp utility operation under explicit compatibility rules.

    Example:
        Exercise cmp through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param a: Value supplied for a under the utility contract.
    :param b: Value supplied for b under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (a > b) - (a < b)


def int_to_byte(x):
    """
    Perform the int to byte utility operation under explicit compatibility rules.

    Example:
        Exercise int to byte through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return bytes((x,))


def reload(module):
    """
    Perform the reload utility operation under explicit compatibility rules.

    Example:
        Exercise reload through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param module: Value supplied for module under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import importlib

    return importlib.reload(module)


def print_to_binary_file(fileobj, encoding="utf-8"):
    """
    Perform the print to binary file utility operation under explicit compatibility rules.

    Example:
        Exercise print to binary file through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param fileobj: Value supplied for fileobj under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def print(*a, **kw):
        """
        Perform the print utility operation under explicit compatibility rules.

        Example:
            Exercise print to binary file.print through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param a: Value supplied for a under the utility contract.
        :param kw: Value supplied for kw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f = kw.get("file", fileobj)
        if a:
            sep = as_bytes(kw.get("sep", " "), encoding)
            for x in a:
                x = as_bytes(x, encoding)
                f.write(x)
                if x is not a[-1]:
                    f.write(sep)
        f.write(as_bytes(kw.get("end", "\n")))

    return print
