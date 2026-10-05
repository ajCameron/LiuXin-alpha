
"""
Select language-aware singular and plural word forms.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pluralizers through a consuming regression::

        python -m pytest -q tests/utils/language_tools/test_pluralizers.py
"""

from copy import deepcopy

from LiuXin_alpha.utils.libraries.inflector import Inflector


def singular_plural_mapper(word):
    """
    Takes a word. Works out its plural form. Returns it as unicode. In lower case - will later refine so it returns the case it was sent. Currently a wrapper for Inflector-2.0.11. Need to add an English/Spanish dictionary, so it can automatically detect the language it's being fed.

    Example:
        Exercise singular plural mapper through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param word: Value supplied for word under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    test = Inflector()
    word_local = deepcopy(word)
    word_local = word_local

    return test.pluralize(word_local)


def plural_singular_mapper(word):
    """
    Takes a word. Works out it's singular form. Returns it as unicode in lower case.

    Example:
        Exercise plural singular mapper through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param word: Value supplied for word under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    test = Inflector()
    word_local = deepcopy(word)
    word_local = word_local

    return test.singularize(word_local)
