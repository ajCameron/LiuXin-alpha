"""Define persistence-free MARC relator vocabulary for Agent relationships.

The enum names the supported role codes; the frozen per-WEMI sets define where
those roles may be attached. Database schema code may consume these values but
does not own or redefine their bibliographic meaning.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final, Literal

from LiuXin_alpha.metadata.identifiers import IdentifierEntityType

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
    """Name the curated MARC relator codes used for Agent credits.

    WEMI eligibility comes from the separate role collections; the enum itself
    does not validate a relationship.

    Example:
        >>> MarcRelatorRole.AUTHOR == "aut"
        True
    """

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
WORK_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset(
    {
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
    }
)
EXPRESSION_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset(
    {
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
    }
)
MANIFESTATION_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset(
    {
        MarcRelatorRole.CONTRIBUTOR,
        MarcRelatorRole.EDITOR,
        MarcRelatorRole.PUBLISHER,
        MarcRelatorRole.PRINTER,
    }
)
ITEM_MARC_RELATOR_ROLES: Final[frozenset[MarcRelatorRole]] = frozenset(
    {
        MarcRelatorRole.ANNOTATOR,
        MarcRelatorRole.BINDER,
        MarcRelatorRole.FORMER_OWNER,
        MarcRelatorRole.INSCRIBER,
        MarcRelatorRole.OWNER,
    }
)
ENTITY_MARC_RELATOR_ROLES_BY_TYPE: Final[
    dict[IdentifierEntityType, frozenset[MarcRelatorRole]]
] = {
    IdentifierEntityType.WORK: WORK_MARC_RELATOR_ROLES,
    IdentifierEntityType.EXPRESSION: EXPRESSION_MARC_RELATOR_ROLES,
    IdentifierEntityType.MANIFESTATION: MANIFESTATION_MARC_RELATOR_ROLES,
    IdentifierEntityType.ITEM: ITEM_MARC_RELATOR_ROLES,
}
