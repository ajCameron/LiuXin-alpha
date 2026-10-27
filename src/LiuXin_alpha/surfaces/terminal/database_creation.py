"""
Collect interactive database-creation choices separately from applying them through Core.

The wizard inspects paths and asks for approval without creating directories or
databases itself. The application step creates parents and opens/closes a creation
session; backend validation, replacement, and backup behavior belong to Core.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.surfaces.core import SurfaceCoreSession
from LiuXin_alpha.surfaces.terminal.presentation import (
    ask_text as _ask_text,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    ask_yes_no as _ask_yes_no,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    render_ascii_table as _render_ascii_table,
)


@dataclass
class DatabaseCreationWizardConfig:
    """
    Hold a database target/backend and the approved backup/storage-bootstrap options.

    ``database_path`` is a filesystem target, ``db_type`` selects the backend,
    and explicit replacement requires ``replace_existing`` plus ``backup_path``.
    ``backup_existing`` remains a compatibility field, not replacement permission. Storage options
    request manager integration, strict bootstrap errors, and store startup checks
    on addition. This mutable record does not validate those values or create state.

    Example:
        >>> config = DatabaseCreationWizardConfig(Path("library.sqlite"), "SQLite", True, False, False, False)
        >>> (config.db_type, config.backup_existing)
        ('SQLite', True)
    """

    database_path: Path
    db_type: str
    backup_existing: bool
    enable_storage_manager: bool
    strict_storage_manager_bootstrap: bool
    storage_startup_on_add: bool
    replace_existing: bool = False
    backup_path: Path | None = None


def run_database_creation_wizard(
    *,
    default_database_path: str,
    default_db_type: str = "SQLite",
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
) -> DatabaseCreationWizardConfig | None:
    """
    Prompt for a target/backend and creation policies, returning approved choices or cancellation.

    Expand home-directory syntax and inspect whether the target/parent exists.
    Missing-parent approval is collected but not executed here. Existing targets
    require replacement approval and a backup destination. Storage choices and a printed
    summary precede final confirmation. Backend names and path suitability are not
    validated; EOF uses each prompt's default, including final approval by default.

    Example:
        >>> config = run_database_creation_wizard(  # doctest: +SKIP
        ...     default_database_path="library.sqlite"
        ... )


    :param default_database_path: Initial filesystem target displayed after home expansion.
    :param default_db_type: Initial backend name retained when its response is blank.
    :param input_stream: Text stream supplying wizard responses; default is bound at definition time.
    :param output_stream: Text stream receiving flushed prompts, summary, and cancellation messages.
    :return: Collected configuration, or ``None`` when parent creation, recreation, or final approval is declined.
    """
    output_stream.write("Database creation wizard\n")
    output_stream.write("------------------------\n")
    output_stream.flush()

    db_path_raw = _ask_text(
        "Target database path",
        default=str(Path(default_database_path).expanduser()),
        input_stream=input_stream,
        output_stream=output_stream,
    )
    db_path = Path(db_path_raw).expanduser()
    db_type = (
        _ask_text(
            "Database backend type",
            default=default_db_type,
            input_stream=input_stream,
            output_stream=output_stream,
        ).strip()
        or default_db_type
    )

    parent = db_path.parent
    if not parent.exists():
        make_parent = _ask_yes_no(
            f"Create parent directory {parent}?",
            default=True,
            input_stream=input_stream,
            output_stream=output_stream,
        )
        if not make_parent:
            output_stream.write(
                "Wizard canceled: parent directory creation declined.\n"
            )
            output_stream.flush()
            return None

    backup_existing = False
    replace_existing = False
    backup_path = None
    if db_path.exists():
        recreate = _ask_yes_no(
            "Database file already exists. Replace it after stopping all database users?",
            default=False,
            input_stream=input_stream,
            output_stream=output_stream,
        )
        if not recreate:
            output_stream.write("Wizard canceled: existing database kept unchanged.\n")
            output_stream.flush()
            return None
        replace_existing = True
        backup_existing = True
        backup_path = Path(
            _ask_text(
                "Required recovery backup path",
                default=str(db_path) + ".backup",
                input_stream=input_stream,
                output_stream=output_stream,
            )
        ).expanduser()

    enable_storage_manager = _ask_yes_no(
        "Enable storage manager integration?",
        default=True,
        input_stream=input_stream,
        output_stream=output_stream,
    )
    strict_storage_manager_bootstrap = _ask_yes_no(
        "Fail on storage manager bootstrap errors?",
        default=False,
        input_stream=input_stream,
        output_stream=output_stream,
    )
    storage_startup_on_add = _ask_yes_no(
        "Run store startup checks while adding stores?",
        default=False,
        input_stream=input_stream,
        output_stream=output_stream,
    )

    output_stream.write("\nCreation summary\n")
    output_stream.write(
        _render_ascii_table(
            ["field", "value"],
            [
                ["database_path", db_path],
                ["db_type", db_type],
                ["backup_existing", backup_existing],
                ["replace_existing", replace_existing],
                ["backup_path", backup_path],
                ["enable_storage_manager", enable_storage_manager],
                ["strict_storage_manager_bootstrap", strict_storage_manager_bootstrap],
                ["storage_startup_on_add", storage_startup_on_add],
            ],
            max_cell_width=120,
        )
    )
    output_stream.write("\n")
    output_stream.flush()

    proceed = _ask_yes_no(
        "Proceed with creation?",
        default=True,
        input_stream=input_stream,
        output_stream=output_stream,
    )
    if not proceed:
        output_stream.write("Wizard canceled.\n")
        output_stream.flush()
        return None

    return DatabaseCreationWizardConfig(
        database_path=db_path,
        db_type=db_type,
        backup_existing=bool(backup_existing),
        enable_storage_manager=bool(enable_storage_manager),
        strict_storage_manager_bootstrap=bool(strict_storage_manager_bootstrap),
        storage_startup_on_add=bool(storage_startup_on_add),
        replace_existing=replace_existing,
        backup_path=backup_path,
    )


def create_database_from_wizard(config: DatabaseCreationWizardConfig) -> Path:
    """
    Create parent directories and open/close a Core database-creation session with the collected options.

    The target is home-expanded but not resolved to an absolute path. Replacement
    requires an explicit flag and recovery path before opening Core. Filesystem,
    creation, bootstrap, or close errors propagate; created parents are not rolled back.

    Example:
        >>> created_path = create_database_from_wizard(config)  # doctest: +SKIP


    :param config: Target, backend, and backup/storage policies approved by the caller.
    :return: Expanded database path after the creation session closes normally.
    """
    db_path = config.database_path.expanduser()
    if config.replace_existing:
        if config.backup_path is None:
            raise ValueError("Explicit replacement requires a recovery backup path.")
        LibraryServices.replace_database(
            database_path=db_path,
            db_type=config.db_type,
            backup_path=config.backup_path,
        )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SurfaceCoreSession.open(
        database_path=db_path,
        db_type=config.db_type,
        create=not config.replace_existing,
        backup=bool(config.backup_existing),
        enable_storage_manager=bool(config.enable_storage_manager),
        strict_storage_manager_bootstrap=bool(config.strict_storage_manager_bootstrap),
        storage_startup_on_add=bool(config.storage_startup_on_add),
    ):
        pass
    return db_path


__all__ = [
    "DatabaseCreationWizardConfig",
    "run_database_creation_wizard",
    "create_database_from_wizard",
]
