"""
Repository implementations for catalog entities.


"""

from .agents import AgentRepository
from .entities import (
    AnnotationRepository,
    CommentRepository,
    GenreRepository,
    LabelRepository,
    LanguageRepository,
    RatingRepository,
    SeriesRepository,
    SubjectRepository,
    SynopsisRepository,
    TagRepository,
)
from .exact import ExactEntityRepository
from .expressions import ExpressionRepository
from .identifiers import IdentifierRepository
from .items import ItemRepository
from .item_identifiers import ItemIdentifierRepository
from .manifestations import ManifestationRepository
from .notes import NoteRepository
from .titles import TitleRepository
from .works import WorkRepository

__all__ = [
    "AgentRepository",
    "AnnotationRepository",
    "CommentRepository",
    "ExactEntityRepository",
    "ExpressionRepository",
    "GenreRepository",
    "IdentifierRepository",
    "ItemRepository",
    "ItemIdentifierRepository",
    "LabelRepository",
    "LanguageRepository",
    "ManifestationRepository",
    "NoteRepository",
    "RatingRepository",
    "SeriesRepository",
    "SubjectRepository",
    "SynopsisRepository",
    "TagRepository",
    "TitleRepository",
    "WorkRepository",
]
