# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``hunspell`` extension.

The compiled module exposes a type named ``Dictionary`` (backed by Hunspell) and an
exception ``HunspellError``.

This fallback implements a minimal dictionary that supports:
    - recognized(word) -> bool
    - suggest(word) -> list[str]
    - add(word) -> None
    - remove(word) -> None

Constructor signature matches the extension: Dictionary(dic_bytes, aff_bytes)
(where dic_bytes/aff_bytes are file *contents*).
"""

from __future__ import annotations

import difflib
import re
from typing import Iterable, List, Optional, Set


class HunspellError(Exception):
    pass


def _detect_encoding(aff_text: bytes) -> str:
    # Hunspell .aff often includes: "SET UTF-8"
    try:
        s = aff_text.decode("ascii", "ignore")
    except Exception:
        return "utf-8"
    for line in s.splitlines():
        line = line.strip()
        if line.upper().startswith("SET "):
            enc = line.split(None, 1)[1].strip()
            if enc:
                return enc
    return "utf-8"


def _decode_words(dic_bytes: bytes, encoding: str) -> List[str]:
    try:
        text = dic_bytes.decode(encoding, "ignore")
    except Exception:
        text = dic_bytes.decode("utf-8", "ignore")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    # First line can be a count.
    if re.fullmatch(r"\d+", lines[0]):
        lines = lines[1:]
    words: List[str] = []
    for ln in lines:
        # Word can have flags after "/" or morphological info after whitespace.
        w = ln.split()[0]
        if "/" in w:
            w = w.split("/", 1)[0]
        if w:
            words.append(w)
    return words


class Dictionary:
    def __init__(self, dic: bytes, aff: bytes):
        if not isinstance(dic, (bytes, bytearray, memoryview)):
            raise TypeError("Dictionary expects dic bytes")
        if not isinstance(aff, (bytes, bytearray, memoryview)):
            raise TypeError("Dictionary expects aff bytes")

        dic_b = bytes(dic)
        aff_b = bytes(aff)

        self.encoding = _detect_encoding(aff_b)
        self._words: Set[str] = set(_decode_words(dic_b, self.encoding))

    def recognized(self, word: str) -> bool:
        if not isinstance(word, str):
            word = str(word)
        return word in self._words or word.lower() in (w.lower() for w in self._words)

    def suggest(self, word: str) -> List[str]:
        if not isinstance(word, str):
            word = str(word)
        # difflib can be slow on huge dictionaries; keep it modest.
        pool = list(self._words)
        return difflib.get_close_matches(word, pool, n=10, cutoff=0.7)

    def add(self, word: str) -> None:
        if not isinstance(word, str):
            word = str(word)
        if word:
            self._words.add(word)

    def remove(self, word: str) -> None:
        if not isinstance(word, str):
            word = str(word)
        self._words.discard(word)
