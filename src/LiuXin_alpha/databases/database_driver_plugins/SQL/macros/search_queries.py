"""Compile validated physical row predicates into bound SQL and EXISTS paths."""

from __future__ import annotations

import re

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET
from LiuXin_alpha.databases.db_types.query_types import (
    BooleanMatch,
    ForeignMatch,
    IdentifierMatch,
    LinkedMatch,
    MissingSearchReference,
    TextMatch,
    ValueMatch,
)
from LiuXin_alpha.databases.search_functions import (
    IDENTIFIER_KEY_FUNCTION,
    TEXT_KEY_FUNCTION,
    normalized_identifier_key,
)
from LiuXin_alpha.errors import InputIntegrityError


def _q(name):
    return '"' + name.replace('"', '""') + '"'


class PredicateCompiler:
    """Compile one query, retaining guards for dangling traversed references."""

    def __init__(self, macros):
        self.macros = macros
        self.counter = 0

    def alias(self):
        self.counter += 1
        return f"r{self.counter}"

    def columns(self, table, names):
        return self.macros._validate_columns(table, names)

    def table(self, table):
        return self.macros._macro_table_sql(self.macros._physical_table(table))

    def compile(self, table, alias, predicate):
        """Return SQL, bindings and (table, guard SQL, bindings) integrity checks."""
        if isinstance(predicate, TextMatch):
            self.columns(table, predicate.columns)
            clauses, values = [], []
            for column in predicate.columns:
                expression = f"{TEXT_KEY_FUNCTION}({alias}.{_q(column)})"
                if predicate.exact:
                    clauses.append(f"({expression} IS NOT NULL AND {expression} = ?)")
                else:
                    clauses.append(
                        f"({expression} IS NOT NULL AND instr({expression}, ?) > 0)"
                    )
                values.append(predicate.term)
            return (
                "(" + " OR ".join(clauses) + ")" if clauses else "0=1",
                tuple(values),
                [],
            )
        if isinstance(predicate, IdentifierMatch):
            self.columns(table, (predicate.scheme_column, predicate.value_column))
            expression = (
                f"{IDENTIFIER_KEY_FUNCTION}({alias}.{_q(predicate.scheme_column)}, "
                f"{alias}.{_q(predicate.value_column)})"
            )
            key = normalized_identifier_key(
                predicate.normalized_scheme, predicate.normalized_value
            )
            return f"({expression} IS NOT NULL AND {expression} = ?)", (key,), []
        if isinstance(predicate, ValueMatch):
            self.columns(table, (predicate.column,))
            column = f"{alias}.{_q(predicate.column)}"
            return (
                (f"{column} IS NULL", (), [])
                if predicate.value is None
                else (
                    f"({column} IS NOT NULL AND {column} = ?)",
                    (predicate.value,),
                    [],
                )
            )
        if isinstance(predicate, BooleanMatch):
            if predicate.operator not in {"and", "or", "not"}:
                raise InputIntegrityError("Unknown predicate operator")
            if predicate.operator == "not" and len(predicate.operands) != 1:
                raise InputIntegrityError("NOT requires one operand")
            parts = [
                self.compile(table, alias, operand) for operand in predicate.operands
            ]
            if predicate.operator == "not":
                sql = f"NOT ({parts[0][0]})"
            else:
                sql = (
                    "("
                    + f" {predicate.operator.upper()} ".join(p[0] for p in parts)
                    + ")"
                    if parts
                    else "1=1"
                    if predicate.operator == "and"
                    else "0=1"
                )
            return (
                sql,
                tuple(value for p in parts for value in p[1]),
                [guard for p in parts for guard in p[2]],
            )
        if isinstance(predicate, LinkedMatch):
            spec = self.macros._validate_link_spec(predicate.link_spec)
            if spec.primary_table != table:
                raise InputIntegrityError(
                    "Relationship predicate has the wrong source table"
                )
            links, target = self.alias(), self.alias()
            target_sql, links_sql = (
                self.table(spec.secondary_table),
                self.table(spec.link_table),
            )
            self.columns(table, (spec.primary_id_col,))
            self.columns(spec.secondary_table, (spec.secondary_id_col,))
            base = f"{links}.{_q(spec.primary_link_col)} = {alias}.{_q(spec.primary_id_col)}"
            bindings = ()
            if predicate.link_type is not LINK_TYPE_UNSET:
                if not spec.typed or spec.type_link_col is None:
                    raise InputIntegrityError(
                        "Cannot filter an untyped predicate by type"
                    )
                if predicate.link_type is None:
                    base += f" AND {links}.{_q(spec.type_link_col)} IS NULL"
                else:
                    base += f" AND {links}.{_q(spec.type_link_col)} = ?"
                    bindings = (predicate.link_type,)
            join = f"{links}.{_q(spec.secondary_link_col)} = {target}.{_q(spec.secondary_id_col)}"
            source = f"{links_sql} AS {links} JOIN {target_sql} AS {target} ON {join}"
            child, values, checks = self.compile(
                spec.secondary_table, target, predicate.predicate
            )
            guards = [
                (
                    spec.secondary_table,
                    f"EXISTS (SELECT 1 FROM {links_sql} AS {links} LEFT JOIN {target_sql} AS {target} ON {join} WHERE {base} AND {target}.{_q(spec.secondary_id_col)} IS NULL)",
                    bindings,
                )
            ]
            guards.extend(
                (
                    name,
                    f"EXISTS (SELECT 1 FROM {source} WHERE {base} AND ({sql}))",
                    (*bindings, *args),
                )
                for name, sql, args in checks
            )
            return (
                f"EXISTS (SELECT 1 FROM {source} WHERE {base} AND ({child}))",
                (*bindings, *values),
                guards,
            )
        if isinstance(predicate, ForeignMatch):
            self.columns(table, (predicate.local_column,))
            self.columns(predicate.table, (predicate.remote_column,))
            target, target_sql = self.alias(), self.table(predicate.table)
            local = f"{alias}.{_q(predicate.local_column)}"
            join = f"{target}.{_q(predicate.remote_column)} = {local}"
            source = f"{target_sql} AS {target}"
            child, values, checks = self.compile(
                predicate.table, target, predicate.predicate
            )
            guards = [
                (
                    name,
                    f"EXISTS (SELECT 1 FROM {source} WHERE {join} AND ({sql}))",
                    args,
                )
                for name, sql, args in checks
            ]
            if predicate.require_target:
                guards.insert(
                    0,
                    (
                        predicate.table,
                        f"({local} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {source} WHERE {join}))",
                        (),
                    ),
                )
            return (
                f"EXISTS (SELECT 1 FROM {source} WHERE {join} AND ({child}))",
                values,
                guards,
            )
        raise InputIntegrityError("Unsupported physical row predicate")


def find_rows(macros, table, predicate, *, limit, offset):
    """Run a physical predicate and SQL pagination on the active connection."""
    compiler = PredicateCompiler(macros)
    table_sql = compiler.table(table)
    for name, value in (("limit", limit), ("offset", offset)):
        if name == "limit" and value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InputIntegrityError(f"{name} must be a nonnegative integer")
    sql, values, guards = compiler.compile(table, "root", predicate)
    if limit == 0:
        return ()
    conn = macros._macro_connection()
    # The SQL path must not silently turn dangling graph rows into missing hits.
    # Guard each physical path once; no endpoint payloads are materialized.
    unique = {}
    for name, guard, args in guards:
        aliases = {}
        canonical = re.sub(
            r"\br\d+(?=\.|\s|$)",
            lambda m, aliases=aliases: aliases.setdefault(m[0], f"a{len(aliases)}"),
            guard,
        )
        unique.setdefault((name, canonical, args), (name, guard, args))
    if unique:
        checks, parameters = [], []
        for name, guard, args in unique.values():
            checks.append(
                f"SELECT ? WHERE EXISTS (SELECT 1 FROM {table_sql} AS root WHERE {guard})"
            )
            parameters.extend((name, *args))
        missing = next(
            iter(
                conn.execute(" UNION ALL ".join(checks) + " LIMIT 1", tuple(parameters))
            ),
            None,
        )
        if missing is not None:
            raise MissingSearchReference(missing[0])
    columns = macros._column_names(table)
    identity = macros._row_id_column(table, None)
    query = (
        f"SELECT {', '.join('root.' + _q(c) for c in columns)} FROM {table_sql} AS root "
        f"WHERE {sql} ORDER BY root.{_q(identity)} LIMIT ? OFFSET ?"
    )
    return macros._mapping_rows(
        columns, conn.execute(query, (*values, -1 if limit is None else limit, offset))
    )
