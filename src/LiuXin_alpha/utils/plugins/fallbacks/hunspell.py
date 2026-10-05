# -*- coding: utf-8 -*-
"""
Provide hunspell utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise hunspell through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import difflib
import re
from typing import Iterable, List, Optional, Set


class HunspellError(Exception):
    """
    Report the HunspellError Calibre compatibility failure.

    Example:
        Exercise HunspellError through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    pass


def _detect_encoding(aff_text: bytes) -> str:
    # Hunspell .aff often includes: "SET UTF-8"
    """
    Perform the detect encoding utility operation under explicit compatibility rules.

    Example:
        Exercise  detect encoding through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param aff_text: Value supplied for aff text under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the decode words utility operation under explicit compatibility rules.

    Example:
        Exercise  decode words through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param dic_bytes: Value supplied for dic bytes under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Provide the Dictionary utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Dictionary through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    def __init__(self, dic: bytes, aff: bytes):
        """
        Initialize and validate the Dictionary state.

        Example:
            Exercise Dictionary.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param dic: Value supplied for dic under the utility contract.
        :param aff: Value supplied for aff under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if not isinstance(dic, (bytes, bytearray, memoryview)):
            raise TypeError("Dictionary expects dic bytes")
        if not isinstance(aff, (bytes, bytearray, memoryview)):
            raise TypeError("Dictionary expects aff bytes")

        dic_b = bytes(dic)
        aff_b = bytes(aff)

        self.encoding = _detect_encoding(aff_b)
        self._words: Set[str] = set(_decode_words(dic_b, self.encoding))

    def recognized(self, word: str) -> bool:
        """
        Perform the recognized utility operation under explicit compatibility rules.

        Example:
            Exercise Dictionary.recognized through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param word: Value supplied for word under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(word, str):
            word = str(word)
        return word in self._words or word.lower() in (w.lower() for w in self._words)

    def suggest(self, word: str) -> List[str]:
        """
        Perform the suggest utility operation under explicit compatibility rules.

        Example:
            Exercise Dictionary.suggest through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param word: Value supplied for word under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(word, str):
            word = str(word)
        # difflib can be slow on huge dictionaries; keep it modest.
        pool = list(self._words)
        return difflib.get_close_matches(word, pool, n=10, cutoff=0.7)

    def add(self, word: str) -> None:
        """
        Perform the add utility operation under explicit compatibility rules.

        Example:
            Exercise Dictionary.add through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param word: Value supplied for word under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(word, str):
            word = str(word)
        if word:
            self._words.add(word)

    def remove(self, word: str) -> None:
        """
        Perform the remove utility operation under explicit compatibility rules.

        Example:
            Exercise Dictionary.remove through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param word: Value supplied for word under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(word, str):
            word = str(word)
        self._words.discard(word)
