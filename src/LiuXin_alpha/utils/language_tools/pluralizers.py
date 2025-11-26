
"""
Tools use to map singular words to plurals and visa-versa.
"""

from copy import deepcopy

from LiuXin_alpha.utils.libraries.inflector import Inflector


def singular_plural_mapper(word):
    """
    Takes a word. Works out its plural form. Returns it as unicode.
    In lower case - will later refine so it returns the case it was sent.
    Currently a wrapper for Inflector-2.0.11.
    Need to add an English/Spanish dictionary,
    so it can automatically detect the language it's being fed.
    """

    test = Inflector()
    word_local = deepcopy(word)
    word_local = word_local

    return test.pluralize(word_local)


def plural_singular_mapper(word):
    """
    Takes a word. Works out it's singular form.
    Returns it as unicode in lower case.
    """

    test = Inflector()
    word_local = deepcopy(word)
    word_local = word_local

    return test.singularize(word_local)
