# -*- coding: utf-8 -*-
"""
Provide patiencediff c utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  patiencediff c through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import difflib
from typing import Any, Iterable, List, Sequence, Tuple


class PatienceSequenceMatcher:
    """
    Provide the PatienceSequenceMatcher utility contract with explicit state and cleanup behavior.

    Example:
        Exercise PatienceSequenceMatcher through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    def __init__(self, a: Sequence[Any] = (), b: Sequence[Any] = ()):
        """
        Initialize and validate the PatienceSequenceMatcher state.

        Example:
            Exercise PatienceSequenceMatcher.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param a: Value supplied for a under the utility contract.
        :param b: Value supplied for b under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._sm = difflib.SequenceMatcher(a=a, b=b)

    def set_seqs(self, a: Sequence[Any], b: Sequence[Any]) -> None:
        """
        Set seqs under the documented compatibility and safety rules.

        Example:
            Exercise PatienceSequenceMatcher.set seqs through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param a: Value supplied for a under the utility contract.
        :param b: Value supplied for b under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._sm.set_seqs(a, b)

    def set_seq1(self, a: Sequence[Any]) -> None:
        """
        Set seq1 under the documented compatibility and safety rules.

        Example:
            Exercise PatienceSequenceMatcher.set seq1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param a: Value supplied for a under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._sm.set_seq1(a)

    def set_seq2(self, b: Sequence[Any]) -> None:
        """
        Set seq2 under the documented compatibility and safety rules.

        Example:
            Exercise PatienceSequenceMatcher.set seq2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param b: Value supplied for b under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._sm.set_seq2(b)

    def get_matching_blocks(self):
        """
        Return matching blocks under the documented compatibility and safety rules.

        Example:
            Exercise PatienceSequenceMatcher.get matching blocks through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._sm.get_matching_blocks()

    def get_opcodes(self):
        """
        Return opcodes under the documented compatibility and safety rules.

        Example:
            Exercise PatienceSequenceMatcher.get opcodes through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._sm.get_opcodes()

    def get_grouped_opcodes(self, n: int = 3):
        """
        Return grouped opcodes under the documented compatibility and safety rules.

        Example:
            Exercise PatienceSequenceMatcher.get grouped opcodes through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param n: Value supplied for n under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._sm.get_grouped_opcodes(n)


def unique_lcs_c(a: Sequence[Any], b: Sequence[Any]) -> List[Tuple[int, int]]:
    """
    Perform the unique lcs c utility operation under explicit compatibility rules.

    Example:
        Exercise unique lcs c through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param a: Value supplied for a under the utility contract.
    :param b: Value supplied for b under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    sm = difflib.SequenceMatcher(a=a, b=b)
    pairs: List[Tuple[int, int]] = []
    for i, j, n in sm.get_matching_blocks():
        for k in range(n):
            pairs.append((i + k, j + k))
    return pairs


def recurse_matches_c(a: Sequence[Any], b: Sequence[Any], alo: int, blo: int, ahi: int, bhi: int, answer: List[Tuple[int, int, int]], maxrecursion: int) -> None:
    # Compute matching blocks for the requested slices and append to answer as (i, j, n)
    """
    Perform the recurse matches c utility operation under explicit compatibility rules.

    Example:
        Exercise recurse matches c through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param a: Value supplied for a under the utility contract.
    :param b: Value supplied for b under the utility contract.
    :param alo: Value supplied for alo under the utility contract.
    :param blo: Value supplied for blo under the utility contract.
    :param ahi: Value supplied for ahi under the utility contract.
    :param bhi: Value supplied for bhi under the utility contract.
    :param answer: Value supplied for answer under the utility contract.
    :param maxrecursion: Value supplied for maxrecursion under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    a_slice = a[alo:ahi]
    b_slice = b[blo:bhi]
    sm = difflib.SequenceMatcher(a=a_slice, b=b_slice)
    for i, j, n in sm.get_matching_blocks():
        if n:
            answer.append((alo + i, blo + j, n))
