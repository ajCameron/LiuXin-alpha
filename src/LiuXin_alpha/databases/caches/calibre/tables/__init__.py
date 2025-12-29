#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Data model to represent tables from the database.

Classes to cache data from the database for active manipulation
Stores the relevant contents of those tables for faster access.
Provides methods to update the cached copy and the database backend.
"""

from __future__ import unicode_literals, division, absolute_import, print_function


from typing import Union

from LiuXin.databases.caches.calibre.tables.many_many_tables import (
    CalibreManyToManyTable,
    CalibreTypedManyToManyTable,
    CalibreAuthorsTable,
    CalibreFormatsTable,
    CalibrePriorityManyToManyTable,
)
from LiuXin.databases.caches.calibre.tables.one_many_tables import (
    CalibreIdentifiersTable,
)
from LiuXin.databases.caches.calibre.tables.one_one_tables import CalibreCoversTable
from LiuXin.databases.caches.calibre.tables.many_one_tables import CalibreRatingTable
from LiuXin.databases.caches.calibre.tables.many_one_tables import CalibreManyToOneTable
from LiuXin.databases.caches.calibre.tables.many_one_tables import (
    CalibrePriorityManyToOneTable,
)
from LiuXin.databases.caches.calibre.tables.one_one_tables import (
    CalibrePathTable,
    CalibreSizeTable,
    CalibreUUIDTable,
    CalibreCompositeTable,
)
from LiuXin.databases.caches.calibre.tables.one_one_tables import CalibreOneToOneTable
from LiuXin.databases.caches.calibre.tables.one_many_tables import (
    CalibreOneToManyTable,
    CalibrePriorityOneToManyTable,
)
from LiuXin.databases.caches.calibre.tables.many_one_tables import (
    CalibreCustomColumnsManyOneTable,
)

from LiuXin.databases.caches.calibre.tables.one_one_tables import (
    CalibreCustomColumnsOneToOneTable,
)

from LiuXin.databases.caches.calibre.tables.one_one_tables import (
    CalibreCustomColumnsOneToOneTableFloatInt,
)
from LiuXin.databases.caches.calibre.tables.one_one_tables import (
    CalibreCustomColumnsOneToOneTableDatetime,
)
from LiuXin.databases.caches.calibre.tables.one_one_tables import (
    CalibreCustomColumnsOneToOneTableBool,
)

from LiuXin.databases.db_types import MetadataDict


# Types of table

# - Generic table types
# -- one-to-one

# -- one-to-many
# -- priority one-to-many
# -- typed one-to-many
# -- priority & typed one-to-many

# -- many-to-one
# -- priority many-to-one
# -- typed - many-to-one
# -- priority & typed many-to-one

# -- many-to-many
# -- priority many-to-many
# -- typed many-to-many
# -- priority & typed many-to-many

# - Specialized table types
# -- PathTable
# -- SizeTable
# -- UUIDTable
# -- CompositeTable
# -- RatingTable
# -- AuthorsTable
# -- CoversTable
# -- FormatsTable
# -- IdentifiersTable


__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


# Todo: Check is actually used
def calibre_create_table(
    name: str, metadata: MetadataDict, fsm
) -> Union[CalibreOneToOneTable, CalibreOneToManyTable, CalibreManyToOneTable, CalibreManyToManyTable]:
    """
    Create a calibre style table from the given name and metadata.

    Fields are empty - with no data loaded - that has to happen elsewhere.
    :param name: Name of the table.
    :param metadata: Metadata from field_metadata
    :param fsm: The folder store manager for this instance of the library
    :return:
    """
    # - one_to_one tables
    if name in (
        "title",
        "sort",
        "author_sort",
        "series_index",
        "timestamp",
        "pubdate",
        "uuid",
        "path",
        "last_modified",
        "notes",
        "cover",
    ):
        # Deal with any metadata oddities
        if name == "comments":
            metadata["table"], metadata["column"] = "comments", "comment"
        if not metadata["table"]:
            metadata["table"], metadata["column"] = "books", ("has_cover" if name == "cover" else name)
        if not metadata["column"]:
            metadata["column"] = name

        # The path table needs access to the fsm to know the locations of objects and deal with them
        if name == "path":
            calibre_path_table = CalibrePathTable(name, metadata)
            calibre_path_table.fsm = fsm
            return calibre_path_table
        elif name == "uuid":
            return CalibreUUIDTable(name, metadata)
        elif name == "cover":
            # This is a virtual one_to_one table
            calibre_cover_table = CalibreCoversTable(name, metadata)
            calibre_cover_table.fsm = fsm
            return calibre_cover_table
        else:
            return CalibreOneToOneTable(name, metadata)

    # - many_to_one tables
    if name in ("subjects", "synopses", "genre"):
        return CalibreManyToOneTable(name, metadata)

    # - one_many tables
    if name in ("comments",):
        cls = {
            "comments": CalibrePriorityOneToManyTable,
        }.get(name, CalibreOneToManyTable)
        return cls(name, metadata)

    # - many_many tables
    if name in (
        "authors",
        "tags",
        "formats",
        "identifiers",
        "languages",
        "rating",
        "series",
        "publisher",
    ):
        cls = {
            "authors": CalibreAuthorsTable,
            "formats": CalibreFormatsTable,
            "identifiers": CalibreIdentifiersTable,
            "rating": CalibreRatingTable,
            "languages": CalibreTypedManyToManyTable,
            "series": CalibrePriorityManyToManyTable,
            "publisher": CalibrePriorityManyToManyTable,
        }.get(name, CalibreManyToManyTable)

        calibre_table = cls(name, metadata)
        if name == "formats":
            calibre_table.fsm = fsm
        return calibre_table

    # - Other virtual tables
    if name in ("size",):
        return CalibreSizeTable("size", metadata)

    raise NotImplementedError(
        "This position should never be reached - requested table could not be created - {}" "".format(name)
    )


# Todo: Needs to be written
# Todo: needs a rename to custom_column_table - otherwise might cause confusion with
def calibre_create_custom_table(
    name: str, metadata: MetadataDict, fsm
) -> Union[CalibreCustomColumnsOneToOneTable, CalibreCustomColumnsManyOneTable]:
    """
    Create tables to hold custom values.

    :param name:
    :param metadata:
    :param link_table:
    :return:
    """
    # Read the metadata to determine the type of table which has to be created and returned
    cc_datatype = metadata["datatype"]
    cc_is_multiple = metadata["is_multiple"]

    if not cc_is_multiple:

        # in the non-multiple case of rating, each title can have one and only one rating - thus a ManyToOne table
        # In the non-multiple case of int, each title can have only one integer - thus a OneToOne table
        # In the non-multiple case of text, each text object is linked to one and only tag analog
        # In the non-multiple case, can only have one comment - thus a OneToOne table

        cls = {
            "rating": CalibreCustomColumnsManyOneTable,
            "int": CalibreCustomColumnsOneToOneTableFloatInt,
            "float": CalibreCustomColumnsOneToOneTableFloatInt,
            "text": CalibreCustomColumnsManyOneTable,
            "comments": CalibreCustomColumnsOneToOneTable,
            "series": CalibreCustomColumnsManyOneTable,
            "enumeration": CalibreCustomColumnsManyOneTable,
            "datetime": CalibreCustomColumnsOneToOneTableDatetime,
            "bool": CalibreCustomColumnsOneToOneTableBool,
        }.get(cc_datatype, None)

        if cls is None:
            raise NotImplementedError("cc_datatype: {}".format(cc_datatype))

        calibre_table = cls(name, metadata, custom=True)

        return calibre_table

    raise NotImplementedError("end of method - cc_datatype: {}".format(cc_datatype))
