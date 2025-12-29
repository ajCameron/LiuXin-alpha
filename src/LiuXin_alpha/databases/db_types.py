"""
Custom types which are used in the db.

May be superseded by a global typing module in utils later.
"""

from enum import Enum

from typing import Optional, Any, Literal
from typing_extensions import TypedDict, NotRequired

TriStateBool = Optional[bool]

# Keyed with the book_id and valued with a list of the languages for that book - or None
LangMap = dict[int, Optional[list[str]]]

TableID = int

# Fields are a mapping between two tables - each of these tables has IDs
# - The "main" table the field is in
SrcTableID = TableID
# - The "secondary" table that the main table is linked to
DstTableID = TableID

# Some of the specific tables MUST return, for some of their functions, an id in a specific table
# (e.g. The "covers" tables).
# These classes represent ids in these tables
CoverID = TableID


# e.g. for the "Tags" field - which is a ManyToManyField
# - The "main" table for the field should be "titles"
# - The "secondary" table for the field should be "tags"

# In calibre - eerything is centered around the books view - so the "main" table is always


class CreatorDataDict(TypedDict):
    """
    Creator data - data about creators.
    """

    name: str
    sort: str
    link: str


# When you are specifying which format you want - either to add to a book or to get from a book - you have two
# options.
#  - Specific format - something like "EPUB_1" or something like that - a format and it's priority
SpecificFormat = str
# - Generic format - something like "EPUB" or something like that
#                    Depending on context, will return either the highest priority format of that type or, in some way
#                    all formats of that type.
GenericFormat = str


# Used to inform interfaces how to display imformation in a table
MetadataDisplayDict = dict[Any, Any]


class MetadataDict(TypedDict):
    """
    Creator data - data about creators.
    """

    table: Optional[str]
    column: NotRequired[Optional[str]]  # Not needed for virtual tables
    link_column: NotRequired[str]
    datatype: str
    is_multiple: NotRequired[dict[Any, Any]]  # Not needed for virtual tables
    kind: NotRequired[str]
    name: NotRequired[str]
    search_terms: NotRequired[list[str, ...]]
    is_custom: NotRequired[bool]
    is_category: NotRequired[bool]
    is_csp: NotRequired[bool]
    display: NotRequired[MetadataDisplayDict]
    val_unique: NotRequired[bool]

    # Used in composite columns
    contains_html: NotRequired[bool]
    make_category: NotRequired[bool]
    composite_sort: NotRequired[bool]
    use_decorations: NotRequired[bool]


DataTypes = Literal["json", "text"]


class DataTypesEnum(Enum):
    """
    Valid enums for the database.
    """

    JSON: str = "json"
    TEXT: str = "text"


TableTypes = Literal[0, 1, 2, 3]


class TableTypesEnum(Enum):
    """
    Valid and recognized table types.
    """

    ONE_ONE: int = 0
    MANY_ONE: int = 1
    MANY_MANY: int = 2
    ONE_MANY: int = 3


MainTableName = str


InterLinkTableName = str


IntraLinkTableName = str


HelperTableName = str


TableColumnName = str


UUIDStr = str


IdentifiersStr = str


ValidLinkAttributes = Literal["index", "datestamp"]


RatingInt = Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


# Todo: Translated string - string which can be translated - stores original value as well
