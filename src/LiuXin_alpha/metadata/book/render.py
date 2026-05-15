#!/usr/bin/env python
# vim:fileencoding=utf-8

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
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import field_sort as renderer

    return renderer(mi, name)


def displayable_field_keys(mi):
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import (
        displayable_field_keys as renderer,
    )

    return renderer(mi)


def get_field_list(mi):
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import get_field_list as renderer

    return renderer(mi)


def search_href(search_term, value):
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import search_href as renderer

    return renderer(search_term, value)


def mi_to_html(
    mi,
    field_list=None,
    default_author_link=None,
    use_roman_numbers=True,
    rating_font="Liberation Serif",
):
    from LiuXin_alpha.surfaces.renderers.calibre_metadata import mi_to_html as renderer

    return renderer(
        mi,
        field_list=field_list,
        default_author_link=default_author_link,
        use_roman_numbers=use_roman_numbers,
        rating_font=rating_font,
    )
