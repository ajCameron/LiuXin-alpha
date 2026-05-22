"""Repository API contracts for catalog entities."""

from .agents import AgentRepositoryAPI
from .base import BaseRepositoryAPI
from .expressions import ExpressionRepositoryAPI
from .identifiers import IdentifierRepositoryAPI
from .items import ItemRepositoryAPI
from .manifestations import ManifestationRepositoryAPI
from .notes import NoteRepositoryAPI
from .titles import TitleRepositoryAPI
from .works import WorkRepositoryAPI


class CatalogRepositoriesAPI(
    WorkRepositoryAPI,
    ExpressionRepositoryAPI,
    ManifestationRepositoryAPI,
    ItemRepositoryAPI,
    AgentRepositoryAPI,
    IdentifierRepositoryAPI,
    TitleRepositoryAPI,
    NoteRepositoryAPI,
):
    """Marker protocol group for repository API imports.

    The implementation facade does not need to inherit from this directly; it just
    needs to expose matching attributes.
    """


__all__ = [
    "AgentRepositoryAPI",
    "BaseRepositoryAPI",
    "CatalogRepositoriesAPI",
    "ExpressionRepositoryAPI",
    "IdentifierRepositoryAPI",
    "ItemRepositoryAPI",
    "ManifestationRepositoryAPI",
    "NoteRepositoryAPI",
    "TitleRepositoryAPI",
    "WorkRepositoryAPI",
]
