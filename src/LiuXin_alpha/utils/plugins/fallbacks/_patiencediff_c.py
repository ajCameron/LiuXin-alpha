# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``_patiencediff_c`` extension.

This module provides:
- PatienceSequenceMatcher (API-compatible subset of the compiled type)
- unique_lcs_c(a, b) -> list[tuple[int,int]]
- recurse_matches_c(a, b, alo, blo, ahi, bhi, answer, maxrecursion) -> None

The true patience diff algorithm is more nuanced; this fallback uses difflib's
SequenceMatcher to provide functional (but potentially different) results.
"""

from __future__ import annotations

import difflib
from typing import Any, Iterable, List, Sequence, Tuple


class PatienceSequenceMatcher:
    def __init__(self, a: Sequence[Any] = (), b: Sequence[Any] = ()):
        self._sm = difflib.SequenceMatcher(a=a, b=b)

    def set_seqs(self, a: Sequence[Any], b: Sequence[Any]) -> None:
        self._sm.set_seqs(a, b)

    def set_seq1(self, a: Sequence[Any]) -> None:
        self._sm.set_seq1(a)

    def set_seq2(self, b: Sequence[Any]) -> None:
        self._sm.set_seq2(b)

    def get_matching_blocks(self):
        return self._sm.get_matching_blocks()

    def get_opcodes(self):
        return self._sm.get_opcodes()

    def get_grouped_opcodes(self, n: int = 3):
        return self._sm.get_grouped_opcodes(n)


def unique_lcs_c(a: Sequence[Any], b: Sequence[Any]) -> List[Tuple[int, int]]:
    sm = difflib.SequenceMatcher(a=a, b=b)
    pairs: List[Tuple[int, int]] = []
    for i, j, n in sm.get_matching_blocks():
        for k in range(n):
            pairs.append((i + k, j + k))
    return pairs


def recurse_matches_c(a: Sequence[Any], b: Sequence[Any], alo: int, blo: int, ahi: int, bhi: int, answer: List[Tuple[int, int, int]], maxrecursion: int) -> None:
    # Compute matching blocks for the requested slices and append to answer as (i, j, n)
    a_slice = a[alo:ahi]
    b_slice = b[blo:bhi]
    sm = difflib.SequenceMatcher(a=a_slice, b=b_slice)
    for i, j, n in sm.get_matching_blocks():
        if n:
            answer.append((alo + i, blo + j, n))
