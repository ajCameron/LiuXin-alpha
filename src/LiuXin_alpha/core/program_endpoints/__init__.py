"""Owned endpoint-provider families for :mod:`LiuXin_alpha.core.program_api`."""

from __future__ import annotations

from LiuXin_alpha.core.program_endpoints.backup_maintenance import (
    install_commands as install_backup_maintenance_commands,
)
from LiuXin_alpha.core.program_endpoints.backup_maintenance import (
    install_queries as install_backup_maintenance_queries,
)
from LiuXin_alpha.core.program_endpoints.catalog_search import (
    install_commands as install_catalog_search_commands,
)
from LiuXin_alpha.core.program_endpoints.catalog_search import (
    install_queries as install_catalog_search_queries,
)
from LiuXin_alpha.core.program_endpoints.content_workflows import (
    install_commands as install_content_workflows_commands,
)
from LiuXin_alpha.core.program_endpoints.content_workflows import (
    install_queries as install_content_workflows_queries,
)
from LiuXin_alpha.core.program_endpoints.database_schema import (
    install_commands as install_database_schema_commands,
)
from LiuXin_alpha.core.program_endpoints.database_schema import (
    install_queries as install_database_schema_queries,
)
from LiuXin_alpha.core.program_endpoints.storage import (
    install_commands as install_storage_commands,
)
from LiuXin_alpha.core.program_endpoints.storage import (
    install_queries as install_storage_queries,
)
from LiuXin_alpha.core.program_endpoints.system_jobs import (
    install_commands as install_system_jobs_commands,
)
from LiuXin_alpha.core.program_endpoints.system_jobs import (
    install_queries as install_system_jobs_queries,
)

_QUERY_PROVIDERS = (
    install_system_jobs_queries,
    install_database_schema_queries,
    install_catalog_search_queries,
    install_storage_queries,
    install_content_workflows_queries,
    install_backup_maintenance_queries,
)
_COMMAND_PROVIDERS = (
    install_system_jobs_commands,
    install_database_schema_commands,
    install_catalog_search_commands,
    install_storage_commands,
    install_content_workflows_commands,
    install_backup_maintenance_commands,
)


def install_program_endpoints(api: object, runtime: object) -> None:
    """Install every provider while preserving query-before-command ordering."""

    for provider in _QUERY_PROVIDERS:
        provider(api, runtime)
    for provider in _COMMAND_PROVIDERS:
        provider(api, runtime)


__all__ = ["install_program_endpoints"]
