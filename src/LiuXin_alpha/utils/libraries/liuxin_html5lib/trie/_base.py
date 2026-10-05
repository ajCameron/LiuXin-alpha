"""
Provide base utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  base through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from collections.abc import Mapping


class Trie(Mapping):
    """
    Abstract base class for tries

    Example:
        Exercise Trie through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

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
        keys = super(self).keys()

        if prefix is None:
            return set(keys)

        # Python 2.6: no set comprehensions
        return set([x for x in keys if x.startswith(prefix)])

    def has_keys_with_prefix(self, prefix):
        """
        Return or update whether has keys with prefix holds for the compatibility value.

        Example:
            Exercise Trie.has keys with prefix through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param prefix: Text prepended to the formatted or selected result.
        :return: True when the documented condition holds; otherwise False.
        """
        for key in self.keys():
            if key.startswith(prefix):
                return True

        return False

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
        if prefix in self:
            return prefix

        for i in range(1, len(prefix) + 1):
            if prefix[:-i] in self:
                return prefix[:-i]

        raise KeyError(prefix)

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
        lprefix = self.longest_prefix(prefix)
        return (lprefix, self[lprefix])
