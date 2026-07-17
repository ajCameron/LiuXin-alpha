"""PostgreSQL database driver plugin."""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.checker import (
    format_postgres_self_test,
    run_postgres_self_test,
)

__all__ = [
    "format_postgres_self_test",
    "run_postgres_self_test",
]
