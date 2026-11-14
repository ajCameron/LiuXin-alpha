"""Bounded custom-value inventory, usage and owner filtering."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.custom_columns.native_repository import names, stem
from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    FieldRef,
    OwnerRef,
    Page,
    Predicate,
)
from LiuXin_alpha.databases.custom_columns.native_values import adapt, encode, identity
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.databases.schema_readers import rows

if TYPE_CHECKING:
    from LiuXin_alpha.databases.custom_columns.native_api import CustomFieldHost


class NativeQueries:
    """Query stored values with bounded pages and attachment-aware owner identities."""

    def find_owners(
        self: CustomFieldHost,
        attachment: str,
        predicates: Iterable[Predicate] = (),
        *,
        limit: int = 100,
        after_id: int = 0,
    ) -> Page:
        """Return ascending owner IDs matching every supplied stored-field predicate.

        Equality/membership use canonical identities; substring/prefix checks are literal
        and case-sensitive. Collection all is non-vacuous: empty values do not match it.
        Page limits are 1..1000 with an exclusive owner-ID cursor. Hold an outer read()
        context when several pages must use one snapshot. Composites require Catalog."""
        if not 1 <= limit <= 1000 or after_id < 0:
            raise CustomFieldError("Page size must be 1..1000 and cursor nonnegative")
        with self.read() as conn:
            owner_id = self.repository.owner_column(conn, attachment)
            clauses = [f"o.{q(owner_id)}>?"]
            parameters: list[Any] = [after_id]
            for predicate in predicates:
                definition = self.repository.resolve(conn, predicate.field)
                if (
                    definition.attachment != attachment
                    or definition.datatype == "composite"
                ):
                    raise CustomFieldError(
                        "Queries require stored fields on the selected attachment"
                    )
                self.repository.require_native(conn, definition)
                table, link = names(definition)
                storage = link if definition.normalized else table
                ss, base = stem(storage), stem(table)
                source = f"{q(storage)} e"
                value = f"e.{q(ss + '_value')}"
                key = f"e.{q(ss + '_key')}"
                if definition.normalized:
                    source += f" JOIN {q(table)} v ON v.{q(base + '_id')}=e.{q(ss + '_value')}"
                    value, key = f"v.{q(base + '_value')}", f"v.{q(base + '_key')}"
                owner = f"e.{q(ss + '_book')}=o.{q(owner_id)}"
                op = predicate.operator
                if op == "unset":
                    clauses.append(f"NOT EXISTS (SELECT 1 FROM {source} WHERE {owner})")
                    continue
                if op == "eq":
                    comparison = f"{key}=?"
                    parameters.append(
                        identity(adapt(predicate.value, definition), definition)
                    )
                elif op == "in":
                    values = list(predicate.value)
                    if not values:
                        comparison = "1=0"
                    else:
                        comparison = f"{key} IN ({','.join('?' for _ in values)})"
                        parameters.extend(
                            identity(adapt(v, definition), definition) for v in values
                        )
                elif op in {"contains", "prefix"}:
                    if definition.datatype not in {
                        "text",
                        "comments",
                        "series",
                        "enumeration",
                    }:
                        raise CustomFieldError("Text predicates require a text field")
                    if not isinstance(predicate.value, str):
                        raise CustomFieldError(
                            "Text predicates require string operands"
                        )
                    function = "strpos" if self.repository.postgres else "instr"
                    comparison = (
                        f"{function}({value}, ?) {'>0' if op == 'contains' else '=1'}"
                    )
                    parameters.append(predicate.value)
                elif op == "range":
                    if definition.datatype not in {
                        "int",
                        "float",
                        "rating",
                        "datetime",
                    }:
                        raise CustomFieldError(
                            "Range predicates require numeric or datetime fields"
                        )
                    lo, hi = predicate.value
                    comparison = f"{value} BETWEEN ? AND ?"
                    parameters.extend(
                        (encode(adapt(lo, definition)), encode(adapt(hi, definition)))
                    )
                else:
                    raise CustomFieldError(f"Unknown predicate {op}")
                if predicate.quantifier == "all":
                    clauses.append(
                        f"EXISTS (SELECT 1 FROM {source} WHERE {owner}) AND NOT EXISTS (SELECT 1 FROM {source} WHERE {owner} AND NOT ({comparison}))"
                    )
                elif predicate.quantifier == "any":
                    clauses.append(
                        f"EXISTS (SELECT 1 FROM {source} WHERE {owner} AND ({comparison}))"
                    )
                else:
                    raise CustomFieldError("Quantifier must be any or all")
            found = rows(
                conn,
                f"SELECT o.{q(owner_id)} FROM {q(attachment)} o WHERE {' AND '.join(clauses)} ORDER BY o.{q(owner_id)} LIMIT ?",
                (*parameters, limit + 1),
            )
            return Page(
                tuple(OwnerRef(attachment, int(r[0])) for r in found[:limit]),
                int(found[limit - 1][0]) if len(found) > limit else None,
            )

    def inventory(
        self: CustomFieldHost,
        field: FieldRef,
        *,
        limit: int = 100,
        after_id: int = 0,
        unused: bool = False,
    ) -> Page:
        """Page distinct values with their distinct-owner counts, including retired fields.

        Normalized fields use stable vocabulary IDs and can restrict results to unused
        values. Inline fields group by canonical identity and use the smallest current
        entry ID as the page key; unused=True is empty for those fields. A cursor is
        exclusive and remains snapshot-relative when values change between pages."""
        if not 1 <= limit <= 1000 or after_id < 0:
            raise CustomFieldError("Page size must be 1..1000 and cursor nonnegative")
        with self.read() as conn:
            d = self.repository.resolve(conn, field, retired=True)
            self.repository.require_native(conn, d)
            table, link = names(d)
            ts, ls = stem(table), stem(link)
            if d.normalized:
                sql = f"SELECT v.{q(ts + '_id')}, v.{q(ts + '_value')}, COUNT(DISTINCT e.{q(ls + '_book')}) FROM {q(table)} v LEFT JOIN {q(link)} e ON e.{q(ls + '_value')}=v.{q(ts + '_id')} GROUP BY v.{q(ts + '_id')},v.{q(ts + '_value')} HAVING v.{q(ts + '_id')}>?"
                if unused:
                    sql += f" AND COUNT(e.{q(ls + '_id')})=0"
                sql += f" ORDER BY v.{q(ts + '_id')} LIMIT ?"
            else:
                if unused:
                    return Page((), None)
                sql = f"SELECT MIN({q(ts + '_id')}), MIN({q(ts + '_value')}), COUNT(DISTINCT {q(ts + '_book')}) FROM {q(table)} GROUP BY {q(ts + '_key')} HAVING MIN({q(ts + '_id')})>? ORDER BY MIN({q(ts + '_id')}) LIMIT ?"
            found = rows(conn, sql, (after_id, limit + 1))
            from LiuXin_alpha.databases.custom_columns.native_values import decode

            return Page(
                tuple(
                    {"id": r[0], "value": decode(r[1], d), "owners": r[2]}
                    for r in found[:limit]
                ),
                int(found[limit - 1][0]) if len(found) > limit else None,
            )

    def usage_count(self: CustomFieldHost, field: FieldRef, value: Any) -> int:
        """Count distinct owners using a canonical value, including a retired field's data."""
        with self.read() as conn:
            definition = self.repository.resolve(conn, field, retired=True)
            self.repository.require_native(conn, definition)
            table, link = names(definition)
            ts, ls = stem(table), stem(link)
            source = f"{q(table)} v"
            key = f"v.{q(ts + '_key')}"
            owner = f"v.{q(ts + '_book')}"
            if definition.normalized:
                source += f" JOIN {q(link)} e ON e.{q(ls + '_value')}=v.{q(ts + '_id')}"
                owner = f"e.{q(ls + '_book')}"
            return int(
                rows(
                    conn,
                    f"SELECT COUNT(DISTINCT {owner}) FROM {source} WHERE {key}=?",
                    (identity(adapt(value, definition), definition),),
                )[0][0]
            )

    def is_shared(self: CustomFieldHost, field: FieldRef, value: Any) -> bool:
        """Report whether a canonical value is assigned to more than one owner."""
        return self.usage_count(field, value) > 1

    def next_series_position(
        self: CustomFieldHost, field: FieldRef, series: str
    ) -> float:
        """Return the greatest position for a Series identity plus one, or one if absent.

        This reads a suggestion, not a reservation; group dependent writes in a native
        write context when the selection must remain stable until assignment."""
        with self.read() as conn:
            d = self.repository.resolve(conn, field)
            if d.datatype != "series":
                raise CustomFieldError("Expected a Series field")
            table, link = names(d)
            ts, ls = stem(table), stem(link)
            found = rows(
                conn,
                f"SELECT MAX(e.{q(ls + '_extra')}) FROM {q(link)} e JOIN {q(table)} v ON v.{q(ts + '_id')}=e.{q(ls + '_value')} WHERE v.{q(ts + '_key')}=?",
                (identity(series, d),),
            )
            return float(found[0][0] or 0) + 1.0
