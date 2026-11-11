"""Stage physical relationship batches while retaining row identities.

Called only inside the portable macro transaction after link-spec validation.
Catalog still decides the desired relationships; this module executes them.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec

if TYPE_CHECKING:
    from .portable_macros_mixin import SQLPortableMacrosMixin
from contextlib import contextmanager
from dataclasses import replace
from itertools import groupby

from LiuXin_alpha.databases.db_types.macro_types import (
    LINK_TYPE_UNSET,
    LinkValue,
    LinkRow,
)
from LiuXin_alpha.databases.transactions import transaction_active, transaction_scope
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@contextmanager
def _stage(
    macros: SQLPortableMacrosMixin,
    conn: SQLConnection,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    *,
    key_columns: Sequence[str] | None = None,
) -> Iterator[str]:
    """Borrow a savepoint for a temporary table with the stored column types."""
    name = "liuxin_links_" + uuid.uuid4().hex
    target = _q(name)
    names = ", ".join(_q(column) for column in columns)
    with transaction_scope(conn, owns_transaction=not transaction_active(conn)):
        conn.execute(
            f"CREATE TEMP TABLE {target} AS SELECT {names} FROM {macros._macro_table_sql(table)} WHERE 1=0"
        )
        size = 500 // len(columns)
        if not size:
            raise InputIntegrityError("Link staging exceeds 500 columns")
        for start in range(0, len(rows), size):
            chunk = rows[start : start + size]
            placeholders = ", ".join(
                "(" + ", ".join("?" for _ in columns) + ")" for _ in chunk
            )
            conn.execute(
                f"INSERT INTO {target} ({names}) VALUES {placeholders}",
                tuple(row[column] for row in chunk for column in columns),
            )
        indexed = ", ".join(_q(column) for column in (key_columns or columns))
        conn.execute(f"CREATE INDEX {_q(name + '_key')} ON {target} ({indexed})")
        yield target
        conn.execute(f"DROP TABLE {target}")


def _match(columns: Iterable[str], left: str, right: str) -> str:
    # Type-as-identity includes NULL. Endpoints on this path are integer IDs.
    return " AND ".join(
        f"({left}.{_q(column)} = {right}.{_q(column)} OR "
        f"({left}.{_q(column)} IS NULL AND {right}.{_q(column)} IS NULL))"
        for column in columns
    )


def upsert_links_batch(
    macros: SQLPortableMacrosMixin,
    conn: SQLConnection,
    spec: StorageLinkSpec,
    owner: int,
    prepared: Sequence[LinkValue],
) -> tuple[LinkRow, ...]:
    """Upsert one owner's prepared links with two reads and staged insert sets.

    Existing updates execute in input order, retaining immediate priority
    constraint behavior. Consecutive new rows with matching payload shapes
    share INSERT SELECT. Missing optional fields retain defaults/stored values.
    """
    if not prepared:
        return ()
    requested = {macros._link_identity(spec, link) for link in prepared}
    existing: dict[tuple[Any, ...], LinkRow] = {}
    for row in macros.get_link_rows(spec, owner):
        identity = macros._link_identity(spec, row)
        if identity in requested and identity in existing:
            raise DatabaseIntegrityError(
                f"Link identity matched multiple rows in {spec.link_table!r}."
            )
        existing[identity] = row
    target = macros._macro_table_sql(spec.link_table)
    identity_columns: tuple[str, ...] = (spec.primary_link_col, spec.secondary_link_col)
    if spec.type_part_of_identity:
        if spec.type_link_col is None:
            raise InputIntegrityError(
                "Typed link identity requires its physical type column"
            )
        identity_columns += (spec.type_link_col,)
    operations: list[tuple[bool, tuple[str, ...], LinkValue, dict[str, Any]]] = []
    for link in prepared:
        found = macros._link_identity(spec, link) in existing
        values: dict[str, Any] = {}
        if not found:
            values.update(
                {
                    spec.primary_link_col: owner,
                    spec.secondary_link_col: link.secondary_id,
                }
            )
        if spec.type_link_col is not None and (
            not found or not spec.type_part_of_identity
        ):
            values[spec.type_link_col] = link.link_type
        if spec.priority_link_col is not None and link.priority is not None:
            values[spec.priority_link_col] = link.priority
        values.update(link.extra)
        operations.append((found, tuple(sorted(values)), link, values))
    for (found, columns, null_type), group in groupby(
        operations,
        key=lambda op: (
            op[0],
            op[1],
            spec.type_part_of_identity and op[2].link_type is None,
        ),
    ):
        operations_group = list(group)
        if not found:
            with _stage(
                macros,
                conn,
                spec.link_table,
                columns,
                [op[3] for op in operations_group],
                key_columns=identity_columns,
            ) as staged:
                names = ", ".join(_q(column) for column in columns)
                conn.execute(
                    f"INSERT INTO {target} ({names}) SELECT {names} FROM {staged} WHERE 1=1 ON CONFLICT DO NOTHING"
                )
        elif columns:
            conditions = [
                f"{_q(spec.primary_link_col)} = ?",
                f"{_q(spec.secondary_link_col)} = ?",
            ]
            if spec.type_part_of_identity:
                if spec.type_link_col is None:
                    raise InputIntegrityError(
                        "Typed link identity requires its physical type column"
                    )
                conditions.append(
                    f"{_q(spec.type_link_col)} IS NULL"
                    if null_type
                    else f"{_q(spec.type_link_col)} = ?"
                )
            sql = (
                f"UPDATE {target} SET "
                + ", ".join(f"{_q(column)} = ?" for column in columns)
                + " WHERE "
                + " AND ".join(conditions)
            )
            bindings = [
                tuple(values[column] for column in columns)
                + (owner, link.secondary_id)
                + (
                    (link.link_type,)
                    if spec.type_part_of_identity and not null_type
                    else ()
                )
                for _, _, link, values in operations_group
            ]
            for start in range(0, len(bindings), 500):
                conn.executemany(sql, bindings[start : start + 500])
    actual: dict[tuple[Any, ...], LinkRow] = {}
    for row in macros.get_link_rows(spec, owner):
        identity = macros._link_identity(spec, row)
        if identity in requested and identity in actual:
            raise DatabaseIntegrityError(
                f"Link identity matched multiple rows in {spec.link_table!r}."
            )
        actual[identity] = row
    if not requested <= actual.keys():
        raise DatabaseIntegrityError(
            f"Could not upsert links in {spec.link_table!r}; another uniqueness rule rejected a link."
        )
    return tuple(actual[macros._link_identity(spec, link)] for link in prepared)


def replace_links_batch(
    macros: SQLPortableMacrosMixin,
    conn: SQLConnection,
    spec: StorageLinkSpec,
    replacements: Mapping[int, Iterable[LinkValue]],
    link_type: object,
    allowed_types: tuple[str, ...] | None,
) -> dict[int, tuple[LinkRow, ...]]:
    """Replace many integer owners with bounded reads and staged set operations."""
    if link_type is not LINK_TYPE_UNSET:
        if not spec.type_part_of_identity:
            raise InputIntegrityError(
                "Type-scoped replacement requires the type column to be part of the link identity."
            )
        macros._validate_link_type_value(
            spec, link_type, live_allowed_types=allowed_types
        )
    desired: dict[int, tuple[LinkValue, ...]] = {}
    for owner, links in replacements.items():
        prepared = tuple(
            macros._prepare_link_value(
                spec, link, scoped_type=link_type, live_allowed_types=allowed_types
            )
            for link in links
        )
        identities = [macros._link_identity(spec, link) for link in prepared]
        if len(set(identities)) != len(identities):
            raise InputIntegrityError(
                "replace_links received duplicate logical link identities."
            )
        if spec.ordered:
            prepared = tuple(
                link
                if link.priority is not None
                else replace(link, priority=len(prepared) - index)
                for index, link in enumerate(prepared)
            )
            priorities = [
                (link.link_type if spec.type_part_of_identity else None, link.priority)
                for link in prepared
            ]
            if len(set(priorities)) != len(priorities):
                raise InputIntegrityError(
                    "replace_links received duplicate priorities in one ordering scope."
                )
        desired[owner] = prepared
    if not desired:
        return {}

    # Read all types for temporary-priority allocation, even for scoped replacement.
    existing = macros.get_link_rows_bulk(spec, desired)
    desired_keys = {
        (owner, *macros._link_identity(spec, link))
        for owner, links in desired.items()
        for link in links
    }
    surviving: list[LinkRow] = []
    seen = set()
    for owner, stored_links in existing.items():
        for stored_link in stored_links:
            identity = (owner, *macros._link_identity(spec, stored_link))
            if identity in seen:
                raise DatabaseIntegrityError(
                    f"Link identity matched multiple rows in {spec.link_table!r}."
                )
            seen.add(identity)
            if identity in desired_keys:
                surviving.append(stored_link)

    key_columns: tuple[str, ...] = (spec.primary_link_col, spec.secondary_link_col)
    if spec.type_part_of_identity:
        if spec.type_link_col is None:
            raise InputIntegrityError(
                "Typed link identity requires its physical type column"
            )
        key_columns += (spec.type_link_col,)

    def payload(
        owner: int, link: LinkValue | LinkRow, *, keys_only: bool = False
    ) -> dict[str, Any]:
        values: dict[str, Any] = {
            spec.primary_link_col: owner,
            spec.secondary_link_col: link.secondary_id,
        }
        if spec.type_link_col is not None and (
            not keys_only or spec.type_part_of_identity
        ):
            values[spec.type_link_col] = link.link_type
        if not keys_only:
            if spec.priority_link_col is not None and link.priority is not None:
                values[spec.priority_link_col] = link.priority
            values.update(link.extra)
        return values

    target = macros._macro_table_sql(spec.link_table)
    keys = [
        payload(owner, link, keys_only=True)
        for owner, links in desired.items()
        for link in links
    ]
    owners = [{spec.primary_link_col: owner} for owner in desired]
    with _stage(macros, conn, spec.link_table, key_columns, keys) as wanted:
        with _stage(
            macros, conn, spec.link_table, (spec.primary_link_col,), owners
        ) as scope:
            condition = f"EXISTS (SELECT 1 FROM {scope} AS owners WHERE owners.{_q(spec.primary_link_col)} = {target}.{_q(spec.primary_link_col)})"
            bindings: tuple[object, ...] = ()
            if link_type is not LINK_TYPE_UNSET:
                if spec.type_link_col is None:
                    raise InputIntegrityError(
                        "A link type filter requires its physical type column"
                    )
                column = _q(spec.type_link_col)
                condition += (
                    f" AND {column} IS NULL"
                    if link_type is None
                    else f" AND {column} = ?"
                )
                if link_type is not None:
                    bindings = (link_type,)
            conn.execute(
                f"DELETE FROM {target} WHERE {condition} AND NOT EXISTS (SELECT 1 FROM {wanted} AS desired WHERE {_match(key_columns, target, 'desired')})",
                bindings,
            )

    if spec.priority_link_col is not None and surviving:
        priority_values = [row.priority for links in existing.values() for row in links]
        priority_values.extend(
            link.priority for links in desired.values() for link in links
        )
        numeric = [
            value
            for value in priority_values
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        high = math.ceil(max([0, *numeric])) + len(surviving) + 1
        staged = [
            {
                **payload(row.primary_id, row, keys_only=True),
                spec.priority_link_col: high - index,
            }
            for index, row in enumerate(surviving)
        ]
        with _stage(
            macros,
            conn,
            spec.link_table,
            (*key_columns, spec.priority_link_col),
            staged,
            key_columns=key_columns,
        ) as temporary:
            match = _match(key_columns, target, "staged")
            conn.execute(
                f"UPDATE {target} SET {_q(spec.priority_link_col)} = (SELECT staged.{_q(spec.priority_link_col)} FROM {temporary} AS staged WHERE {match}) WHERE EXISTS (SELECT 1 FROM {temporary} AS staged WHERE {match})"
            )

    values = [
        payload(owner, link) for owner, links in desired.items() for link in links
    ]
    for columns, group in groupby(values, key=lambda row: tuple(sorted(row))):
        rows = list(group)
        with _stage(
            macros, conn, spec.link_table, columns, rows, key_columns=key_columns
        ) as temporary:
            match = _match(key_columns, target, "staged")
            updates = tuple(column for column in columns if column not in key_columns)
            if updates:
                assignments = ", ".join(
                    f"{_q(column)} = (SELECT staged.{_q(column)} FROM {temporary} AS staged WHERE {match})"
                    for column in updates
                )
                conn.execute(
                    f"UPDATE {target} SET {assignments} WHERE EXISTS (SELECT 1 FROM {temporary} AS staged WHERE {match})"
                )
            names = ", ".join(_q(column) for column in columns)
            selected = ", ".join(f"staged.{_q(column)}" for column in columns)
            conn.execute(
                f"INSERT INTO {target} ({names}) SELECT {selected} FROM {temporary} AS staged WHERE NOT EXISTS (SELECT 1 FROM {target} WHERE {match}) ON CONFLICT DO NOTHING"
            )

    result = macros.get_link_rows_bulk(spec, desired, link_type=link_type)
    actual_keys = [
        (owner, *macros._link_identity(spec, row))
        for owner, links in result.items()
        for row in links
    ]
    if len(actual_keys) != len(set(actual_keys)) or set(actual_keys) != desired_keys:
        raise DatabaseIntegrityError(
            f"Could not replace links in {spec.link_table!r}; another uniqueness rule rejected a link."
        )
    return result
