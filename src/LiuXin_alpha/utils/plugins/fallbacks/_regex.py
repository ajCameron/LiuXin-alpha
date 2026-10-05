# -*- coding: utf-8 -*-
"""
Provide regex utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  regex through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Iterator, List, Match, Optional, Pattern, Sequence, Tuple, Union

error = re.error

def compile(pattern: Union[str, bytes], flags: int = 0) -> Pattern[Any]:
    """
    Perform the compile utility operation under explicit compatibility rules.

    Example:
        Exercise compile through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return re.compile(pattern, flags)

def match(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0) -> Optional[Match[Any]]:
    """
    Perform the match utility operation under explicit compatibility rules.

    Example:
        Exercise match through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if hasattr(pattern, "match"):
        return pattern.match(string)  # type: ignore[return-value]
    return re.match(pattern, string, flags)

def search(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0) -> Optional[Match[Any]]:
    """
    Perform the search utility operation under explicit compatibility rules.

    Example:
        Exercise search through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if hasattr(pattern, "search"):
        return pattern.search(string)  # type: ignore[return-value]
    return re.search(pattern, string, flags)

def sub(pattern: Union[str, bytes, Pattern[Any]], repl, string: Union[str, bytes], count: int = 0, flags: int = 0):
    """
    Perform the sub utility operation under explicit compatibility rules.

    Example:
        Exercise sub through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :param repl: Value supplied for repl under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param count: Value supplied for count under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if hasattr(pattern, "sub"):
        return pattern.sub(repl, string, count=count)  # type: ignore[return-value]
    return re.sub(pattern, repl, string, count=count, flags=flags)

def split(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], maxsplit: int = 0, flags: int = 0):
    """
    Perform the split utility operation under explicit compatibility rules.

    Example:
        Exercise split through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param maxsplit: Value supplied for maxsplit under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if hasattr(pattern, "split"):
        return pattern.split(string, maxsplit=maxsplit)  # type: ignore[return-value]
    return re.split(pattern, string, maxsplit=maxsplit, flags=flags)

def findall(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0):
    """
    Perform the findall utility operation under explicit compatibility rules.

    Example:
        Exercise findall through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if hasattr(pattern, "findall"):
        return pattern.findall(string)  # type: ignore[return-value]
    return re.findall(pattern, string, flags)

def finditer(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0):
    """
    Perform the finditer utility operation under explicit compatibility rules.

    Example:
        Exercise finditer through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :param string: Value supplied for string under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if hasattr(pattern, "finditer"):
        return pattern.finditer(string)  # type: ignore[return-value]
    return re.finditer(pattern, string, flags)

def escape(pattern: Union[str, bytes]) -> Union[str, bytes]:
    """
    Perform the escape utility operation under explicit compatibility rules.

    Example:
        Exercise escape through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pattern: Value supplied for pattern under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return re.escape(pattern)

# Compatibility: expose flags used by regex-like modules
IGNORECASE = re.IGNORECASE
MULTILINE = re.MULTILINE
DOTALL = re.DOTALL
VERBOSE = re.VERBOSE
ASCII = re.ASCII
