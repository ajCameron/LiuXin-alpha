"""Audit and explicitly resolve legacy preference-key duplicates.

No row-order or timestamp heuristic chooses a winner. The caller supplies each
retained row ID after reviewing the complete values in the preferences table.
DBPrefs owns its cache; these helpers own only database migration policy.
"""

from collections.abc import Mapping
from typing import TYPE_CHECKING

from LiuXin_alpha.errors import DatabaseIntegrityError

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


def audit_preference_keys(database: "DatabaseAPI") -> dict[str | None, tuple[int, ...]]:
    """Return duplicate or null keys and their row IDs without choosing a value."""
    rows = database.macros.get_rows("preferences", order_by=("preference_id",))
    groups: dict[str | None, list[int]] = {}
    for row in rows:
        groups.setdefault(row["preference_key"], []).append(row["preference_id"])
    return {
        key: tuple(ids) for key, ids in groups.items() if key is None or len(ids) > 1
    }


def migrate_preference_keys(
    database: "DatabaseAPI", *, keep: Mapping[str, int] | None = None
) -> None:
    """Resolve exactly the audited conflicts and install key uniqueness atomically.

    Acquire an outer writer reservation and, on PostgreSQL, a physical table
    lock. Null keys require explicit catalogue repair before this operation.
    Existing DBPrefs caches should be reloaded after the migration commits.
    """
    chosen = dict(keep or {})
    with database.macros.transaction(
        serialized=True, require_outermost=True
    ) as connection:
        if getattr(database.driver, "schema", None):
            connection.execute(
                "LOCK TABLE preferences IN SHARE ROW EXCLUSIVE MODE"
            ).close()
        conflicts = audit_preference_keys(database)
        if None in conflicts or set(chosen) != set(conflicts):
            raise DatabaseIntegrityError(
                f"Explicit preference conflict resolution required: {conflicts!r}"
            )
        for key, ids in conflicts.items():
            if key is None or chosen[key] not in ids:
                raise DatabaseIntegrityError(
                    f"Chosen preference row is not a candidate for {key!r}"
                )
        for key, row_id in chosen.items():
            connection.execute(
                "DELETE FROM preferences WHERE preference_key=? AND preference_id<>?",
                (key, row_id),
            ).close()
        # An incompatible existing index cannot make later upserts silently work;
        # PostgreSQL and SQLite both require an actual matching unique key.
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_preferences_key_unique ON preferences(preference_key)"
        ).close()
        # Compile the actual upsert conflict target without inserting anything.
        # This also refuses a pre-existing same-name nonunique/wrong-column index.
        connection.execute(
            "INSERT INTO preferences (preference_key, preference_value) "
            "SELECT NULL, NULL WHERE 0=1 ON CONFLICT (preference_key) DO NOTHING"
        ).close()
