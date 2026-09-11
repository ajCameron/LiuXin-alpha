"""
Preserve SquashFS command aliases and the historical complete-CLI entry point.

Parser construction imports ``squashfs_commands`` directly. This facade may
call the application, but command implementations never import back through it.
Exports bind the implementation objects directly, including historical private
helpers and the PostgreSQL parser attribute outside __all__. Importing this
facade does not construct the full application grammar or publish an archive.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.cli.postgres import (
    build_postgres_parser as build_postgres_parser,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    _build_provenance_payload as _build_provenance_payload,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    _collect_file_ids as _collect_file_ids,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    _file_payload as _file_payload,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    _print_publish_report as _print_publish_report,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    _run_job as _run_job,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    cmd_provenance as cmd_provenance,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    cmd_publish_from_ids as cmd_publish_from_ids,
)
from LiuXin_alpha.surfaces.cli.squashfs_commands import (
    cmd_publish_store as cmd_publish_store,
)
from LiuXin_alpha.surfaces.cli.squashfs_parsers import (
    build_squashfs_parser as build_squashfs_parser,
)


def main(argv: list[str] | None = None) -> int:
    """
    Lazily dispatch the complete installed CLI through the historical SquashFS seam.

    Forward the same argument object without adding a squashfs command prefix.
    Application exit codes and uncaught parser/runtime exceptions remain intact.

    Example:
        >>> main(['squashfs', 'provenance', '--database', 'library.sqlite', '--file-id', '1'])  # doctest: +SKIP


    :param argv: Complete command tokens, or None for the application's process arguments.
    :return: Integer status returned by the application dispatcher.
    """
    from LiuXin_alpha.surfaces.cli.app import main as application_main

    return application_main(argv)


__all__ = [
    "main",
    "build_squashfs_parser",
    "cmd_publish_store",
    "cmd_publish_from_ids",
    "cmd_provenance",
]
