"""
Re-export the concrete Catalog entity repository implementations.

The public names include all nineteen entity repositories plus ExactEntityRepository,
which supplies shared configured matching/persistence behavior. Imports bind class
objects, not database-backed instances; Catalog constructs and binds the repository
group separately. Specialized operations live with their concrete owners, and this
package adds no database lifetime management, queries, or transaction wrapper.
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
