# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``_regex`` extension.

This is a very small compatibility shim around Python's stdlib ``re``. It does *not*
support the full feature set of calibre's/regex's engine (captures lists, fuzzy
matching, etc.), but it allows code that only needs basic regex operations to run.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Iterator, List, Match, Optional, Pattern, Sequence, Tuple, Union

error = re.error

def compile(pattern: Union[str, bytes], flags: int = 0) -> Pattern[Any]:
    return re.compile(pattern, flags)

def match(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0) -> Optional[Match[Any]]:
    if hasattr(pattern, "match"):
        return pattern.match(string)  # type: ignore[return-value]
    return re.match(pattern, string, flags)

def search(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0) -> Optional[Match[Any]]:
    if hasattr(pattern, "search"):
        return pattern.search(string)  # type: ignore[return-value]
    return re.search(pattern, string, flags)

def sub(pattern: Union[str, bytes, Pattern[Any]], repl, string: Union[str, bytes], count: int = 0, flags: int = 0):
    if hasattr(pattern, "sub"):
        return pattern.sub(repl, string, count=count)  # type: ignore[return-value]
    return re.sub(pattern, repl, string, count=count, flags=flags)

def split(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], maxsplit: int = 0, flags: int = 0):
    if hasattr(pattern, "split"):
        return pattern.split(string, maxsplit=maxsplit)  # type: ignore[return-value]
    return re.split(pattern, string, maxsplit=maxsplit, flags=flags)

def findall(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0):
    if hasattr(pattern, "findall"):
        return pattern.findall(string)  # type: ignore[return-value]
    return re.findall(pattern, string, flags)

def finditer(pattern: Union[str, bytes, Pattern[Any]], string: Union[str, bytes], flags: int = 0):
    if hasattr(pattern, "finditer"):
        return pattern.finditer(string)  # type: ignore[return-value]
    return re.finditer(pattern, string, flags)

def escape(pattern: Union[str, bytes]) -> Union[str, bytes]:
    return re.escape(pattern)

# Compatibility: expose flags used by regex-like modules
IGNORECASE = re.IGNORECASE
MULTILINE = re.MULTILINE
DOTALL = re.DOTALL
VERBOSE = re.VERBOSE
ASCII = re.ASCII
