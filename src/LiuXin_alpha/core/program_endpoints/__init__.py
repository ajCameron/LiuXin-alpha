"""
Install the named whole-program Core endpoint families in a fixed provider order.

Providers describe routes and bind handlers from the supplied facade; execution
belongs to program_services. Every query family is installed before any command
family. Registration is incremental, without rollback if a later provider fails.
Importing this package loads providers but does not register them on a runtime.
"""

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
from LiuXin_alpha.core.program_endpoints.common import ProgramEndpointRegistrar
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
from LiuXin_alpha.core.program_endpoints.handlers import ProgramEndpointHandlers
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


def install_program_endpoints(
    api: ProgramEndpointHandlers, runtime: ProgramEndpointRegistrar
) -> None:
    """
    Register all query families, then all command families, using the supplied handler facade.

    Within each phase, order is system/jobs, database/schema, catalog/search,
    storage, content workflows, then backup/maintenance. Errors propagate and
    leave earlier registrations intact; duplicate-name policy belongs to runtime.

    Example:
        >>> install_program_endpoints(api, runtime)  # doctest: +SKIP


    :param api: Facade supplying the named program handler callables used by providers.
    :param runtime: Registrar receiving handler bindings and transport-facing descriptions.
    :return: None after every provider has returned; no handler operation is executed here.
    """

    for provider in _QUERY_PROVIDERS:
        provider(api, runtime)
    for provider in _COMMAND_PROVIDERS:
        provider(api, runtime)


__all__ = ["install_program_endpoints"]
