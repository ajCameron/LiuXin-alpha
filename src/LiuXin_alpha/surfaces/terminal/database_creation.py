"""Collect database creation choices and apply them through a Core session."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

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
    """Configuration collected from the interactive database creation wizard."""

    database_path: Path
    db_type: str
    backup_existing: bool
    enable_storage_manager: bool
    strict_storage_manager_bootstrap: bool
    storage_startup_on_add: bool


def run_database_creation_wizard(
    *,
    default_database_path: str,
    default_db_type: str = "SQLite",
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
) -> DatabaseCreationWizardConfig | None:
    """Interactively collect configuration for creating a new database."""
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
    if db_path.exists():
        recreate = _ask_yes_no(
            "Database file already exists. Recreate it?",
            default=False,
            input_stream=input_stream,
            output_stream=output_stream,
        )
        if not recreate:
            output_stream.write("Wizard canceled: existing database kept unchanged.\n")
            output_stream.flush()
            return None
        backup_existing = _ask_yes_no(
            "Backup existing database before recreate?",
            default=True,
            input_stream=input_stream,
            output_stream=output_stream,
        )

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
    )


def create_database_from_wizard(config: DatabaseCreationWizardConfig) -> Path:
    """Create a database using wizard-provided configuration."""
    db_path = config.database_path.expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SurfaceCoreSession.open(
        database_path=db_path,
        db_type=config.db_type,
        create=True,
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
