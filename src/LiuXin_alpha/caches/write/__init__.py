#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Export legacy writer classes and dispatch fields to specialized implementations.

Writer construction uses field name, datatype and legacy table/shape flags.
The package retains canonical classes from their implementation modules;
this factory is separate from modern Cache/Catalog write coordination.
"""

from __future__ import unicode_literals, division, absolute_import, print_function, annotations

from typing import Union

from LiuXin_alpha.databases.db_types import ONE_MANY, MANY_ONE, MANY_MANY
from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.caches.write.generic_writers.many_to_many_writer import ManyToManyWriter
from LiuXin_alpha.caches.write.generic_writers.many_to_one_writer import ManyToOneWriter
from LiuXin_alpha.caches.write.generic_writers.one_to_many_writer import OneToManyWriter
from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import OneToOneWriter
from LiuXin_alpha.caches.write.author_sort_writer import AuthorSortWriter
from LiuXin_alpha.caches.write.covers_writer import CoversWrite
from LiuXin_alpha.caches.write.custom_columns_writers import CustomSeriesIndexWriter
from LiuXin_alpha.caches.write.identifiers_writer import IdentifiersWrite
from LiuXin_alpha.caches.write.languages_writer import LanguagesWriter
from LiuXin_alpha.caches.write.title_writer import TitleWriter
from LiuXin_alpha.caches.write.utils import DummyWriter
from LiuXin_alpha.caches.write.uuid_writer import UUIDWriter

# Py2/Py3 compatibility layer


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


# Todo: I _think_ the concept of a field is a good idea. Probably. Likewise table.
#       So we need to actually sodding implement it
# Todo: Actually might also want to be able to call this by name?
def get_writer(field) -> Union["BaseWriter", "DummyWriter"]:
    """
    Select and construct a legacy writer using ordered field-specific rules.

    First choose DummyWriter for composite datatype or protected fields
    id/size/path/formats/news. Next check identifiers by field or table name,
    then languages, cover, uuid, custom #..._index, title and author_sort.
    After those special cases select MANY_ONE by table type, then MANY_MANY
    for publisher/is_many_many/type, then ONE_MANY for is_many/type; otherwise
    use OneToOneWriter. Earlier matches win.

    The factory adds no shape validation: missing metadata/attributes propagate,
    and an empty name can fail at name[0] before generic dispatch. Constructors
    select adapters but this function does not perform database writes.

    Example:
        A title field uses TitleWriter even when generic relation flags could
        otherwise select another writer.


    :param field: Legacy field providing name, metadata, table and relation-shape attributes.
    :return: New specialized BaseWriter or DummyWriter instance bound to the supplied field.
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

