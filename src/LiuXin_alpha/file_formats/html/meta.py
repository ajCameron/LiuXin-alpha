"""
Extract and normalize metadata declarations from HTML documents.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise meta through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

__license__ = "GPL 3"
__copyright__ = "2010, Fabian Grassl <fg@jusmeum.de>"
__docformat__ = "restructuredtext en"


class EasyMeta(object):
    """
    Provide the easymeta contract for validated ebook processing.

    Example:
        Exercise EasyMeta through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self: _typing.Self, meta: _typing.Any) -> None:
        """
        Initialize and validate the easymeta state.

        Example:
            Exercise EasyMeta.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param meta: Value supplied for meta under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.meta = meta

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:

        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise EasyMeta.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        from LiuXin_alpha.file_formats.oeb.base import namespace, barename, DC11_NS

        meta = self.meta
        for item_name in meta.items:
            for item in meta[item_name]:
                if namespace(item.term) == DC11_NS:
                    yield {"name": barename(item.term), "value": item.value}

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise EasyMeta.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return sum(1 for _ in self)

    def titles(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the titles operation under explicit file-format and conversion rules.

        Example:
            Exercise EasyMeta.titles through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for item in self.meta["title"]:
            yield item.value

    def creators(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the creators operation under explicit file-format and conversion rules.

        Example:
            Exercise EasyMeta.creators through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for item in self.meta["creator"]:
            yield item.value
