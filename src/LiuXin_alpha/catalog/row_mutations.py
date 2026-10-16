"""Adapt Row-shaped requests and receipts to the Catalog mutation owners."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from functools import wraps
from typing import Any

from LiuXin_alpha.catalog.relationships import CatalogRelationships
from LiuXin_alpha.catalog.repositories.group import CatalogRepositories
from LiuXin_alpha.databases.row import Row


def atomic_write[**P, T](operation: Callable[P, T]) -> Callable[P, T]:
    """Enclose a compatibility workflow in the handle's available transaction."""

    @wraps(operation)
    def write(*args: P.args, **kwargs: P.kwargs) -> T:
        host = args[0]
        transaction = getattr(getattr(host.db, "macros", None), "transaction", None)
        with transaction() if callable(transaction) else nullcontext():
            return operation(*args, **kwargs)

    return write


def repository_for(host: Any, table: str) -> Any:
    """Select the semantic table owner without backend-dependent fallback rules."""
    repositories = getattr(host, "repositories", None)
    if repositories is None:
        repositories = CatalogRepositories.from_database(host.db)
        host.repositories = repositories
    return repositories.for_table(table)


def create_row(
    host: Any,
    row_dict: dict[str, Any],
    *,
    table: str | None = None,
    read_only: bool = False,
    reload_from_db: bool = True,
) -> Row:
    """Create through the semantic owner and preserve the Row return contract."""
    table = table or host.db.driver_wrapper.identify_table_from_row_dict(row_dict)
    repository = repository_for(host, table)
    if repository is None:
        return Row.from_idless_row_dict(
            host.db,
            row_dict,
            table=table,
            read_only=read_only,
            reload_from_db=reload_from_db,
        )
    values = dict(row_dict)
    if values.get(repository.id_column) is None:
        values.pop(repository.id_column, None)
    row_id = repository.create(values)
    return Row(host.db, dict(repository.require(row_id)), read_only=read_only)


def persist_row(host: Any, row: Row) -> None:
    """Persist a compatibility draft through its owner, retaining object identity."""
    repository = repository_for(host, str(row.table))
    if repository is None:
        row.sync()
        return
    values = dict(row.row_dict)
    values.pop(repository.id_column, None)
    if row.row_id is None:
        row_id = repository.create(values)
    else:
        row_id = int(row.row_id)
        repository.update(row_id, values)
    row.row_dict = dict(repository.require(row_id))
    row.refresh_db_properties()


def ensure_value(host: Any, table: str, column: str, value: Any) -> Row | None:
    """Find by the database's identity policy, creating through its owner if absent."""
    repository = repository_for(host, table)
    if repository is None:
        return None
    with host.db.macros.transaction():
        row_id = host.db.macros.find_table_value(table, column, value)
        if row_id is None:
            row_id = repository.create({column: value})
        return Row(host.db, dict(repository.require(row_id)))


def link_rows(
    host: Any,
    primary_row: Any,
    secondary_row: Any,
    priority: Any = "highest",
    type: Any = None,
    **extra: Any,
) -> Row:
    """Retain legacy Row receipts while using Catalog relationship validation."""
    owner = repository_for(host, str(primary_row.table)) or repository_for(
        host, str(secondary_row.table)
    )
    if owner is None:
        return host.db.interlink_rows(
            primary_row=primary_row,
            secondary_row=secondary_row,
            priority=priority,
            type=type,
            **extra,
        )
    with host.db.macros.transaction():
        result = CatalogRelationships(host.db).link(
            primary_row.table,
            primary_row.row_id,
            secondary_row.table,
            secondary_row.row_id,
            link_type=type,
            priority=priority,
            extra=extra,
            legacy_priority=True,
        )
        spec = host.db.driver_wrapper.get_link_spec(
            primary_row.table, secondary_row.table
        )
        id_column = host.db.driver_wrapper.get_id_column(spec.link_table)
        return Row(
            host.db,
            dict(
                host.db.macros.get_row(
                    spec.link_table,
                    result.extra[id_column],
                    id_column=id_column,
                )
            ),
        )


def unlink_rows(host: Any, primary_row: Any, secondary_row: Any) -> None:
    """Remove an endpoint pair through the same relationship owner."""
    owner = repository_for(host, str(primary_row.table)) or repository_for(
        host, str(secondary_row.table)
    )
    if owner is None:
        host.db.unlink_interlink(primary_row, secondary_row)
        return
    CatalogRelationships(host.db).unlink(
        primary_row.table,
        primary_row.row_id,
        secondary_row.table,
        secondary_row.row_id,
    )


def intralink_rows(
    host: Any, primary_row: Any, secondary_row: Any, link_type: Any = None
) -> Row:
    """Route modern self-relations through the shared relationship owner."""
    owner = repository_for(host, str(primary_row.table))
    if owner is None:
        return host.db.intralink_rows(
            primary_row=primary_row, secondary_row=secondary_row, link_type=link_type
        )
    if primary_row.table != secondary_row.table:
        raise ValueError("self-relations require endpoints from the same table")
    with host.db.macros.transaction():
        result = CatalogRelationships(host.db).link(
            primary_row.table,
            primary_row.row_id,
            secondary_row.table,
            secondary_row.row_id,
            link_type=link_type,
        )
        spec = host.db.driver_wrapper.get_intralink_spec(primary_row.table)
        id_column = host.db.driver_wrapper.get_id_column(spec.link_table)
        return Row(
            host.db,
            dict(
                host.db.macros.get_row(
                    spec.link_table,
                    result.extra[id_column],
                    id_column=id_column,
                )
            ),
        )
