"""
Export canonical read-side contracts for work, expression, manifestation, item and agent metadata.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise   init   with the owning regression module::

        python -m pytest -q tests/catalog/retrieval/test_metadata_hydrator_api.py
"""

from LiuXin_alpha.catalog.api.retrieval.hydration.wemi_sources.agents_sources import (
    AgentProfileGetterAPI,
)
from LiuXin_alpha.catalog.api.retrieval.hydration.wemi_sources.expression_sources import (
    ExpressionMetadataGetterAPI,
)
from LiuXin_alpha.catalog.api.retrieval.hydration.wemi_sources.items_sources import (
    ItemMetadataGetterAPI,
)
from LiuXin_alpha.catalog.api.retrieval.hydration.wemi_sources.manifestation_sources import (
    ManifestationMetadataGetterAPI,
)
from LiuXin_alpha.catalog.api.retrieval.hydration.wemi_sources.work_sources import (
    WorkMetadataGetterAPI,
)

__all__ = [
    "WorkMetadataGetterAPI",
    "ExpressionMetadataGetterAPI",
    "ManifestationMetadataGetterAPI",
    "ItemMetadataGetterAPI",
    "AgentProfileGetterAPI",
]
