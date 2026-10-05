"""
Implement trie lookup in pure Python for HTML5 entity and prefix matching.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise py through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

try:
    text_type = unicode
except NameError:
    text_type = str

from bisect import bisect_left

from LiuXin_alpha.utils.libraries.liuxin_html5lib.trie._base import Trie as ABCTrie


class Trie(ABCTrie):
    """
    Provide the Trie utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Trie through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, data):
        """
        Initialize and validate the Trie state.

        Example:
            Exercise Trie.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if not all(isinstance(x, text_type) for x in data.keys()):
            raise TypeError("All keys must be strings")

        self._data = data
        self._keys = sorted(data.keys())
        self._cachestr = ""
        self._cachepoints = (0, len(data))

    def __contains__(self, key):
        """
        Perform the contains utility operation under explicit compatibility rules.

        Example:
            Exercise Trie.  contains   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return key in self._data

    def __len__(self):
        """
        Perform the len utility operation under explicit compatibility rules.

        Example:
            Exercise Trie.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self._data)

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise Trie.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(self._data)

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise Trie.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._data[key]

    def keys(self, prefix=None):
        """
        Perform the keys utility operation under explicit compatibility rules.

        Example:
            Exercise Trie.keys through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if prefix is None or prefix == "" or not self._keys:
            return set(self._keys)

        if prefix.startswith(self._cachestr):
            lo, hi = self._cachepoints
            start = i = bisect_left(self._keys, prefix, lo, hi)
        else:
            start = i = bisect_left(self._keys, prefix)

        keys = set()
        if start == len(self._keys):
            return keys

        while self._keys[i].startswith(prefix):
            keys.add(self._keys[i])
            i += 1

        self._cachestr = prefix
        self._cachepoints = (start, i)

        return keys

    def has_keys_with_prefix(self, prefix):
        """
        Return or update whether has keys with prefix holds for the compatibility value.

        Example:
            Exercise Trie.has keys with prefix through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: True when the documented condition holds; otherwise False.
        """
        if prefix in self._data:
            return True

        if prefix.startswith(self._cachestr):
            lo, hi = self._cachepoints
            i = bisect_left(self._keys, prefix, lo, hi)
        else:
            i = bisect_left(self._keys, prefix)

        if i == len(self._keys):
            return False

        return self._keys[i].startswith(prefix)
