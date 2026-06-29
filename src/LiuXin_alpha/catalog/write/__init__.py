#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Write convenience methods for items linked to the titles table.

These are intended to make reasoning about and writing to the database easier by providing methods which abstract away
much of the complications of setting up and preforming writes to the database.

"""

from __future__ import unicode_literals, division, absolute_import, print_function, annotations

from typing import Union

from LiuXin_alpha.databases.db_types import ONE_MANY, MANY_ONE, MANY_MANY
from LiuXin_alpha.catalog.write.base_writer import BaseWriter
from LiuXin_alpha.catalog.write.generic_writers.many_to_many_writer import ManyToManyWriter
from LiuXin_alpha.catalog.write.generic_writers.many_to_one_writer import ManyToOneWriter
from LiuXin_alpha.catalog.write.generic_writers.one_to_many_writer import OneToManyWriter
from LiuXin_alpha.catalog.write.generic_writers.one_to_one_writer import OneToOneWriter
from LiuXin_alpha.catalog.write.author_sort_writer import AuthorSortWriter
from LiuXin_alpha.catalog.write.base_writer import BaseWriter
from LiuXin_alpha.catalog.write.covers_writer import CoversWrite
from LiuXin_alpha.catalog.write.custom_columns_writers import CustomSeriesIndexWriter
from LiuXin_alpha.catalog.write.generic_writers.one_to_one_writer import OneToOneWriter
from LiuXin_alpha.catalog.write.identifiers_writer import IdentifiersWrite
from LiuXin_alpha.catalog.write.languages_writer import LanguagesWriter
from LiuXin_alpha.catalog.catalog_macros import (
    library_set_cover,
    library_set_publisher,
    library_set_comment,
    library_unset_series,
    library_set_series,
    library_set_series_index,
    library_set_last_modified,
    library_set_publisher,
    library_set_series)
from LiuXin_alpha.catalog.write.title_writer import TitleWriter
from LiuXin_alpha.catalog.write.utils import DummyWriter
from LiuXin_alpha.catalog.write.uuid_writer import UUIDWriter

# Py2/Py3 compatibility layer


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


# Todo: I _think_ the concept of a field is a good idea. Probably. Likewise table.
#       So we need to actually sodding implement it
# Todo: Actually might also want to be able to call this by name?
def get_writer(field) -> Union["BaseWriter", "DummyWriter"]:
    """
    Return a writer object suitable for the table.

    :param field:
    :return:
    """
    if field.metadata["datatype"] == "composite" or field.name in {
        "id",
        "size",
        "path",
        "formats",
        "news",
    }:
        return DummyWriter(field)

    elif field.name == "identifiers" or field.table.name == "identifiers":
        return IdentifiersWrite(field)

    elif field.name == "languages":
        return LanguagesWriter(field)

    elif field.name == "cover":
        return CoversWrite(field)

    elif field.name == "uuid":
        return UUIDWriter(field)

    elif field.name[0] == "#" and field.name.endswith("_index"):
        return CustomSeriesIndexWriter(field)

    elif field.name == "title":
        return TitleWriter(field)

    elif field.name == "author_sort":
        return AuthorSortWriter(field)

    # Todo: Likewise for one_one, many_one, one_many
    elif field.table.table_type == MANY_ONE:
        return ManyToOneWriter(field)

    # Todo: Remove the is_many_many and is_many entirely - table type does the same thing and is less badly named
    elif field.name == "publisher" or field.is_many_many or field.table.table_type == MANY_MANY:
        return ManyToManyWriter(field)

    # Todo: This probably doesn't work, at least not the way you expect
    elif field.is_many or field.table.table_type == ONE_MANY:
        return OneToManyWriter(field)

    else:
        return OneToOneWriter(field)


# Todo: When you say many_one, do you actually mean one_many - which would make a lot more sense in the context


