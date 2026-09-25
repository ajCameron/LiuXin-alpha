"""
Re-export abstract metadata contracts and concatenate their published export lists.

Concrete containers live in metadata.containers; the __all__ assembly preserves the
imported list ordering and any duplicate names.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_metadata_top_level_facade.py
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api import *  # noqa: F403
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api import __all__ as calibre_metadata_api_all
from LiuXin_alpha.metadata.api.containers_api import *  # noqa: F403
from LiuXin_alpha.metadata.api.containers_api import __all__ as containers_api_all
from LiuXin_alpha.metadata.api.from_database_api import *  # noqa: F403
from LiuXin_alpha.metadata.api.from_database_api import __all__ as from_database_api_all
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api import *  # noqa: F403
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api import __all__ as liuxin_metadata_api_all
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_wemi_metadata_api import *  # noqa: F403
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_wemi_metadata_api import __all__ as liuxin_wemi_metadata_api_all

__all__ = [
    *containers_api_all,
    *from_database_api_all,
    *calibre_metadata_api_all,
    *liuxin_metadata_api_all,
    *liuxin_wemi_metadata_api_all,
]
