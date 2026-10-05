#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Provide legacy metadata rendering entry points that delegate to surfaces.renderers.calibre_metadata.

These wrappers preserve the old import path and forward all arguments. Rendering
behavior and output types are owned by the surfaces implementation.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/book/test_formatter_and_render.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"

default_sort = (
    "title",
    "title_sort",
    "authors",
    "author_sort",
    "series",
    "rating",
    "pubdate",
    "tags",
    "publisher",
    "identifiers",
)


def field_sort(mi, name):
    """
    Delegate construction of the field display-order key.

    The renderer prioritizes its default field sequence, then orders other fields by
    their descriptor title.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py


    :param mi: Calibre-compatible metadata object passed to the renderer.
    :param name: Field lookup name.
    :return: Renderer sort-key tuple.
    """
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import field_sort as renderer

    return renderer(mi, name)


def displayable_field_keys(mi):
    """
    Delegate iteration over field keys eligible for human-facing display.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py


    :param mi: Calibre-compatible metadata object passed to the renderer.
    :return: Renderer iterator of displayable field names.
    """
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import (
        displayable_field_keys as renderer,
    )

    return renderer(mi)


def get_field_list(mi):
    """
    Delegate construction of ordered field/display-flag pairs.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py


    :param mi: Calibre-compatible metadata object passed to the renderer.
    :return: Renderer iterator of (field, True) pairs.
    """
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import get_field_list as renderer

    return renderer(mi)


def search_href(search_term, value):
    """
    Delegate construction of a hex-encoded catalogue search URI.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py


    :param search_term: Catalogue field or search term.
    :param value: Exact-match search value.
    :return: XML-escaped search href string.
    """
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import search_href as renderer

    return renderer(search_term, value)


def mi_to_html(
    mi,
    field_list=None,
    default_author_link=None,
    use_roman_numbers=True,
    rating_font="Liberation Serif",
):
    """
    Render a detailed metadata table and separate comment fields through the surfaces renderer.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_formatter_and_render.py


    :param mi: Calibre-compatible metadata object passed to the renderer.
    :param field_list: Optional iterable of (field, display) pairs; None uses the
        renderer default list.
    :param default_author_link: Optional author-link template or search-calibre selector
        used when no explicit link exists.
    :param use_roman_numbers: Whether the renderer formats series indices as Roman
        numerals.
    :param rating_font: Font family used for rating-star output.
    :return: Pair (HTML table string, comment-field fragments).
    """
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import mi_to_html as renderer

    return renderer(
        mi,
        field_list=field_list,
        default_author_link=default_author_link,
        use_roman_numbers=use_roman_numbers,
        rating_font=rating_font,
    )
