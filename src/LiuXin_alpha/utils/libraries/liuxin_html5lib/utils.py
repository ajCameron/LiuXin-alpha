"""
Provide shared HTML5 parser, serializer and tree utility helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise utils through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from types import ModuleType

try:
    import xml.etree.cElementTree as default_etree
except ImportError:
    import xml.etree.ElementTree as default_etree


__all__ = [
    "default_etree",
    "MethodDispatcher",
    "isSurrogatePair",
    "surrogatePairToCodepoint",
    "moduleFactoryFactory",
]


class MethodDispatcher(dict):
    """
    Dict with 2 special properties:

    Example:
        Exercise MethodDispatcher through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    def __init__(self, items=()):
        # Using _dictEntries instead of directly assigning to self is about
        # twice as fast. Please do careful performance testing before changing
        # anything here.
        """
        Initialize and validate the MethodDispatcher state.

        Example:
            Exercise MethodDispatcher.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param items: Value supplied for items under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        _dictEntries = []
        for name, value in items:
            if type(name) in (list, tuple, frozenset, set):
                for item in name:
                    _dictEntries.append((item, value))
            else:
                _dictEntries.append((name, value))
        dict.__init__(self, _dictEntries)
        self.default = None

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise MethodDispatcher.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return dict.get(self, key, self.default)


# Some utility functions to dal with weirdness around UCS2 vs UCS4
# python builds


def isSurrogatePair(data):
    """
    Perform the isSurrogatePair utility operation under explicit compatibility rules.

    Example:
        Exercise isSurrogatePair through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (
        len(data) == 2
        and ord(data[0]) >= 0xD800
        and ord(data[0]) <= 0xDBFF
        and ord(data[1]) >= 0xDC00
        and ord(data[1]) <= 0xDFFF
    )


def surrogatePairToCodepoint(data):
    """
    Perform the surrogatePairToCodepoint utility operation under explicit compatibility rules.

    Example:
        Exercise surrogatePairToCodepoint through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    char_val = 0x10000 + (ord(data[0]) - 0xD800) * 0x400 + (ord(data[1]) - 0xDC00)
    return char_val


# Module Factory Factory (no, this isn't Java, I know)
# Here to stop this being duplicated all over the place.


def moduleFactoryFactory(factory):
    """
    Perform the moduleFactoryFactory utility operation under explicit compatibility rules.

    Example:
        Exercise moduleFactoryFactory through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param factory: Value supplied for factory under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    moduleCache = {}

    def moduleFactory(baseModule, *args, **kwargs):
        """
        Perform the moduleFactory utility operation under explicit compatibility rules.

        Example:
            Exercise moduleFactoryFactory.moduleFactory through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param baseModule: Value supplied for baseModule under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(ModuleType.__name__, type("")):
            name = "_%s_factory" % baseModule.__name__
        else:
            name = b"_%s_factory" % baseModule.__name__

        if name in moduleCache:
            return moduleCache[name]
        else:
            mod = ModuleType(name)
            objs = factory(baseModule, *args, **kwargs)
            mod.__dict__.update(objs)
            moduleCache[name] = mod
            return mod

    return moduleFactory
