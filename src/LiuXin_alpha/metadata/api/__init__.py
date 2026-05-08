"""Public metadata API surface.

Only abstract metadata contracts are exported here. Import concrete containers
from ``LiuXin_alpha.metadata.containers``.
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
from LiuXin_alpha.metadata.api.containers_api.liuxin_wemi_metadata_api import *  # noqa: F403
from LiuXin_alpha.metadata.api.containers_api.liuxin_wemi_metadata_api import __all__ as liuxin_wemi_metadata_api_all

__all__ = [
    *containers_api_all,
    *from_database_api_all,
    *calibre_metadata_api_all,
    *liuxin_metadata_api_all,
    *liuxin_wemi_metadata_api_all,
]
