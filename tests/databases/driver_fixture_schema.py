"""Format metadata for intentionally minimal SQL-driver unit fixtures.

These are not catalogue provisioning helpers: Database integration tests generate
the complete native schema. Driver-only fixtures isolate SQL grammar, codecs and
transaction behavior while satisfying the driver's format-admission contract.
"""

from pathlib import Path

from LiuXin_alpha.databases.schema_revision import CURRENT_SCHEMA_REVISION


def revision_statements() -> tuple[str, str]:
    """Reuse the actual format-marker DDL without duplicating its column contract."""
    root = Path(__file__).resolve().parents[2]
    ddl = (
        root
        / "src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/schema_revision.sql"
    ).read_text()
    return ddl.replace(
        "`", '"'
    ), f"INSERT INTO schema_revision VALUES (1, {CURRENT_SCHEMA_REVISION})"
