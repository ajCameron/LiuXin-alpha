"""
Expose trie lookup through the optional datrie backend.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise datrie through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from datrie import Trie as DATrie

try:
    text_type = unicode
except NameError:
    text_type = str

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
        chars = set()
        for key in data.keys():
            if not isinstance(key, text_type):
                raise TypeError("All keys must be strings")
            for char in key:
                chars.add(char)

        self._data = DATrie("".join(chars))
        for key, value in data.items():
            self._data[key] = value

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


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()

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
        return self._data.keys(prefix)

    def has_keys_with_prefix(self, prefix):
        """
        Return or update whether has keys with prefix holds for the compatibility value.

        Example:
            Exercise Trie.has keys with prefix through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: True when the documented condition holds; otherwise False.
        """
        return self._data.has_keys_with_prefix(prefix)

    def longest_prefix(self, prefix):
        """
        Perform the longest prefix utility operation under explicit compatibility rules.

        Example:
            Exercise Trie.longest prefix through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._data.longest_prefix(prefix)

    def longest_prefix_item(self, prefix):
        """
        Perform the longest prefix item utility operation under explicit compatibility rules.

        Example:
            Exercise Trie.longest prefix item through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._data.longest_prefix_item(prefix)
