"""
Custom types which are used in the db.

May be superseded by a global typing module in utils later.
"""

from enum import Enum, StrEnum

from typing import Final, Optional, Any, Literal, Union

try:
    from typing_extensions import TypedDict, NotRequired
except ImportError:
    from typing import TypedDict, NotRequired

TriStateBool = Optional[bool]

# Keyed with the book_id and valued with a list of the languages for that book - or None
LangMap = dict[int, Optional[list[str]]]

# Table classification types
# - Main
MainTableID = int
MainTableName = str
MainTableColumnName = str

# - Interlink
InterlinkTableID = int
InterLinkTableName = str
InterlinkTableColumnName = str

# - Intralink
IntraLinkTableID = int
IntraLinkTableName = str
IntraLinkTableColumnName = str

# - Helper
HelperTableID = int
HelperTableName = str
HelperTableColumnName = str


TableColumnName = str


# Fields are a mapping between two tables - each of these tables has IDs
# - The "main" table the field is in
SrcTableID = MainTableID
# - The "secondary" table that the main table is linked to
DstTableID = MainTableID



# Some of the specific tables MUST return, for some of their functions, an id in a specific table
# (e.g. The "covers" tables).
# These classes represent ids in these tables
CoverID = MainTableID



AgentID = int

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


ONE_ONE = TableTypesEnum.ONE_ONE.value
MANY_ONE = TableTypesEnum.MANY_ONE.value
MANY_MANY = TableTypesEnum.MANY_MANY.value
ONE_MANY = TableTypesEnum.ONE_MANY.value


UUIDStr = str


IdentifiersStr = str


IdentifierEntityTypeStr = Literal["work", "expression", "manifestation", "item"]


class IdentifierEntityType(StrEnum):
    """Supported curated-identifier attachment targets in the FRBR graph."""

    WORK = "work"
    EXPRESSION = "expression"
    MANIFESTATION = "manifestation"
    ITEM = "item"


IdentifierSchemeStr = Literal[
    "isbn_10",
    "isbn_13",
    "asin",
    "uuid",
    "calibre_uuid",
]


class IdentifierScheme(StrEnum):
    """Canonical identifier scheme names for the FRBR identifier tables."""

    ISBN_10 = "isbn_10"
    ISBN_13 = "isbn_13"
    ASIN = "asin"
    UUID = "uuid"
    CALIBRE_UUID = "calibre_uuid"


ALL_IDENTIFIER_ENTITY_TYPES: Final[tuple[str, ...]] = tuple(
    entity_type.value for entity_type in IdentifierEntityType
)

ALL_IDENTIFIER_SCHEMES: Final[tuple[str, ...]] = tuple(
    scheme.value for scheme in IdentifierScheme
)


WORK_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
})

EXPRESSION_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
})

MANIFESTATION_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.ISBN_10,
    IdentifierScheme.ISBN_13,
    IdentifierScheme.ASIN,
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
})

ITEM_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset({
    IdentifierScheme.UUID,
    IdentifierScheme.CALIBRE_UUID,
})

# Item-observed identifiers are intentionally broader than curated item identifiers.
# A specific copy may physically carry manifestation-level identifiers such as ISBNs/ASINs.
OBSERVED_ITEM_IDENTIFIER_SCHEMES: Final[frozenset[IdentifierScheme]] = frozenset(
    IdentifierScheme
)

ENTITY_IDENTIFIER_SCHEMES_BY_TYPE: Final[dict[IdentifierEntityType, frozenset[IdentifierScheme]]] = {
    IdentifierEntityType.WORK: WORK_IDENTIFIER_SCHEMES,
    IdentifierEntityType.EXPRESSION: EXPRESSION_IDENTIFIER_SCHEMES,
    IdentifierEntityType.MANIFESTATION: MANIFESTATION_IDENTIFIER_SCHEMES,
    IdentifierEntityType.ITEM: ITEM_IDENTIFIER_SCHEMES,
}


ValidLinkAttributes = Literal["index", "datestamp", "sequence_number", "is_required"]


RatingInt = Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


# Todo: Translated string - string which can be translated - stores original value as well

InterlinkExtraTypes = Union[
    Literal["priority"],
    Literal["primary"],
    Literal["type"],
    Literal["origin"],
    Literal["policy"],
    Literal["data"],
    Literal["index"],
    Literal["sequence_number"],
    Literal["is_required"]]


MarcRelatorRoleStr = Literal[
    "abr",
    "act",
    "adp",
    "ann",
    "arr",
    "art",
    "auc",
    "aui",
    "aus",
    "aut",
    "bnd",
    "com",
    "cmp",
    "cre",
    "ctb",
    "ctr",
    "cur",
    "dpt",
    "drt",
    "edt",
    "fmo",
    "ill",
    "ins",
    "itr",
    "ive",
    "ivr",
    "lyr",
    "nrt",
    "own",
    "pbl",
    "prf",
    "prt",
    "red",
    "res",
    "rev",
    "spk",
    "ths",
    "trl",
]


class MarcRelatorRole(StrEnum):
    """Small curated MARC relator code set for agent links."""

    ABRIDGER = "abr"
    ACTOR = "act"
    ADAPTER = "adp"
    ANNOTATOR = "ann"
    ARRANGER = "arr"
    ARTIST = "art"
    AUTHOR_OF_DIALOG = "auc"
    AUTHOR_OF_INTRODUCTION = "aui"
    AUTHOR_OF_SCREENPLAY = "aus"
    AUTHOR = "aut"
    BINDER = "bnd"
    COMPILER = "com"
    COMPOSER = "cmp"
    CREATOR = "cre"
    CONTRIBUTOR = "ctb"
    CONTRACTOR = "ctr"
    CURATOR = "cur"
    DEGREE_SUPERVISOR = "dpt"
    DIRECTOR = "drt"
    EDITOR = "edt"
    FORMER_OWNER = "fmo"
    ILLUSTRATOR = "ill"
    INSCRIBER = "ins"
    INSTRUMENTALIST = "itr"
    INTERVIEWEE = "ive"
    INTERVIEWER = "ivr"
    LYRICIST = "lyr"
    NARRATOR = "nrt"
    OWNER = "own"
    PUBLISHER = "pbl"
    PERFORMER = "prf"
    PRINTER = "prt"
    REDACTOR = "red"
    RESEARCHER = "res"
    REVIEWER = "rev"
    SPEAKER = "spk"
    THESIS_ADVISOR = "ths"
    TRANSLATOR = "trl"


ALL_MARC_RELATOR_ROLES: Final[tuple[str, ...]] = tuple(
    role.value for role in MarcRelatorRole
)

WORK_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.ABRIDGER,
    MarcRelatorRole.ADAPTER,
    MarcRelatorRole.ARRANGER,
    MarcRelatorRole.ARTIST,
    MarcRelatorRole.AUTHOR,
    MarcRelatorRole.AUTHOR_OF_DIALOG,
    MarcRelatorRole.AUTHOR_OF_INTRODUCTION,
    MarcRelatorRole.AUTHOR_OF_SCREENPLAY,
    MarcRelatorRole.COMPOSER,
    MarcRelatorRole.CREATOR,
    MarcRelatorRole.CONTRIBUTOR,
    MarcRelatorRole.DIRECTOR,
    MarcRelatorRole.EDITOR,
    MarcRelatorRole.ILLUSTRATOR,
    MarcRelatorRole.LYRICIST,
})

EXPRESSION_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.ABRIDGER,
    MarcRelatorRole.ADAPTER,
    MarcRelatorRole.ANNOTATOR,
    MarcRelatorRole.ARRANGER,
    MarcRelatorRole.AUTHOR,
    MarcRelatorRole.COMPOSER,
    MarcRelatorRole.CONTRIBUTOR,
    MarcRelatorRole.EDITOR,
    MarcRelatorRole.ILLUSTRATOR,
    MarcRelatorRole.NARRATOR,
    MarcRelatorRole.PERFORMER,
    MarcRelatorRole.REDACTOR,
    MarcRelatorRole.TRANSLATOR,
})

MANIFESTATION_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.CONTRIBUTOR,
    MarcRelatorRole.EDITOR,
    MarcRelatorRole.PUBLISHER,
    MarcRelatorRole.PRINTER,
})

ITEM_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset({
    MarcRelatorRole.ANNOTATOR,
    MarcRelatorRole.BINDER,
    MarcRelatorRole.FORMER_OWNER,
    MarcRelatorRole.INSCRIBER,
    MarcRelatorRole.OWNER,
})

ENTITY_MARC_RELATOR_ROLES_BY_TYPE: Final[
    dict[IdentifierEntityType, frozenset[MarcRelatorRole]]
] = {
    IdentifierEntityType.WORK: WORK_MARC_RELATOR_ROLES,
    IdentifierEntityType.EXPRESSION: EXPRESSION_MARC_RELATOR_ROLES,
    IdentifierEntityType.MANIFESTATION: MANIFESTATION_MARC_RELATOR_ROLES,
    IdentifierEntityType.ITEM: ITEM_MARC_RELATOR_ROLES,
}
