#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Resolve book fields for Calibre-compatible template formatting.

SafeFormat supplies TemplateFormatter with display values from a bound metadata
book, including custom columns and identifier aliases.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/book/test_formatter_and_render.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function

from LiuXin_alpha.metadata.book import TOP_LEVEL_IDENTIFIERS, ALL_METADATA_FIELDS

from LiuXin_alpha.utils.calibre_compat.utils.formatter import TemplateFormatter
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class SafeFormat(TemplateFormatter):
    """
    Format templates using the bound book's field descriptors and display values.

    Set book through the inherited formatting API or explicitly before get_value.
    Unknown fields raise ValueError in get_value; the inherited safe_format controls
    error rendering.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py
    """
    def __init__(self):
        """
        Initialize the inherited TemplateFormatter state.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_formatter_and_render.py


        :return: None.
        """
        TemplateFormatter.__init__(self)

    def get_value(self, orig_key, args, kwargs):
        """
        Resolve a case-insensitive template field against the bound book.

        Try known metadata names, registry aliases, then an original-name attribute. Unknown
        names raise ValueError. Empty keys, missing numeric custom values, and None/empty
        formatted values become an empty string; series indices are omitted.

        Example:
            >>> from LiuXin_alpha.metadata.book.base import calibreMetadata
            >>> formatter = SafeFormat()
            >>> formatter.book = calibreMetadata('Example')
            >>> formatter.get_value('TITLE', (), {})
            'Example'


        :param orig_key: Template field name to lower-case and resolve.
        :param args: Positional formatting arguments required by the base interface; unused
            here.
        :param kwargs: Keyword formatting arguments required by the base interface; unused
            here.
        :return: Formatted field value or empty string.
        """
        if not orig_key:
            return ""
        key = orig_key = orig_key.lower()
        if key != "title_sort" and key not in TOP_LEVEL_IDENTIFIERS and key not in ALL_METADATA_FIELDS:
            from LiuXin_alpha.metadata.book.base import field_metadata

            key = field_metadata.search_term_to_field_key(key)
            if key is None or (self.book and key not in self.book.all_field_keys()):
                if hasattr(self.book, orig_key):
                    key = orig_key
                else:
                    raise ValueError(_("Value: unknown field ") + orig_key)
        try:
            b = self.book.get_user_metadata(key, False)
        except:
            b = None
        if b and b["datatype"] in {"int", "float"} and self.book.get(key, None) is None:
            v = ""
        else:
            v = self.book.format_field(key, series_with_index=False)[1]
        if v is None:
            return ""
        if v == "":
            return ""
        return v
