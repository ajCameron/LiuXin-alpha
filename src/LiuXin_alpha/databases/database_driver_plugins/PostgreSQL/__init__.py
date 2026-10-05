"""
PostgreSQL plugin namespace exposing readiness checks and their report formatter.

The concrete driver, connection configuration and schema builder live in separate
modules; importing this package does not establish a database connection.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.checker import (
    format_postgres_self_test,
    run_postgres_self_test,
)

__all__ = [
    "format_postgres_self_test",
    "run_postgres_self_test",
]
