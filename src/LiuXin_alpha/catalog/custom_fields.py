"""Catalog-owned Calibre templates over explicitly selected FRBR owners and paths."""

from __future__ import annotations

from typing import Any

from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    FieldRef,
    OwnerRef,
    SeriesValue,
    ValueEntry,
)
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.databases.schema_readers import rows
from LiuXin_alpha.utils.calibre_compat.utils.formatter import TemplateFormatter


class CompositeEvaluationError(CustomFieldError):
    """A computed field cannot be resolved in the requested owner context."""


class CustomFieldEvaluator:
    """Evaluate Calibre composites in a snapshot for explicit owners and paths."""

    def __init__(self, service: CustomFieldService):
        self.service = service

    def get_value(self, owner: OwnerRef, field: FieldRef) -> Any:
        with self.service.read():
            return self._evaluate(owner, field, {}, set())

    def get_owner_values(self, owner: OwnerRef) -> dict[int, Any]:
        with self.service.read():
            memo: dict[tuple[OwnerRef, int], Any] = {}
            return {
                d.id: self._evaluate(owner, d.id, memo, set())
                for d in self.service.list_fields(attachment=owner.table)
            }

    def _evaluate(
        self,
        owner: OwnerRef,
        field: FieldRef,
        memo: dict[tuple[OwnerRef, int], Any],
        stack: set[tuple[OwnerRef, int]],
    ) -> Any:
        definition = self.service.get_field(field)
        if definition.attachment != owner.table:
            raise CompositeEvaluationError(
                "A field needs an owner from its attachment table"
            )
        if definition.datatype != "composite":
            return self.service.get_value(owner, definition.id)
        key = (owner, definition.id)
        if key in stack:
            raise CompositeEvaluationError(
                f"Composite cycle at {definition.key} for {owner}"
            )
        if key in memo:
            return memo[key]
        stack.add(key)
        evaluator = self

        class Formatter(TemplateFormatter):
            def get_value(self, key, args, kwargs):
                value = evaluator._resolve(
                    owner, str(key), definition.display.get("paths", {}), memo, stack
                )
                return evaluator._text(value)

            def get_field(self, field_name, args, kwargs):
                return self.get_value(field_name, args, kwargs), field_name

        formatter = Formatter()
        proxy = _MetadataProxy(
            lambda name: self._resolve(
                owner, name, definition.display.get("paths", {}), memo, stack
            )
        )
        try:
            result = formatter.unsafe_format(
                definition.display["composite_template"], {}, proxy
            )
            if definition.mode != "scalar":
                separator = definition.display.get("separator", ",")
                if not isinstance(separator, str) or not separator:
                    raise CompositeEvaluationError(
                        "A multiple composite needs a nonempty separator"
                    )
                values = [
                    part.strip() for part in result.split(separator) if part.strip()
                ]
                result = (
                    tuple(dict.fromkeys(values))
                    if definition.mode == "set"
                    else tuple(values)
                )
            memo[key] = result
            return result
        except Exception as error:
            raise CompositeEvaluationError(
                f"{definition.key} on {owner.table}:{owner.id}: {error}"
            ) from error
        finally:
            stack.remove(key)

    @staticmethod
    def _text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (ValueEntry, SeriesValue)):
            return str(value.value)
        if isinstance(value, (tuple, list)):
            return ", ".join(CustomFieldEvaluator._text(item) for item in value)
        return str(value)

    def _resolve(
        self,
        owner: OwnerRef,
        name: str,
        paths: dict[str, Any],
        memo: dict[tuple[OwnerRef, int], Any],
        stack: set[tuple[OwnerRef, int]],
    ) -> Any:
        if not name:
            return ""
        if name in paths:
            return self._path(owner, paths[name], memo, stack)
        if name.startswith("#"):
            label = name[1:]
            if "." in label:
                attachment, label = label.split(".", 1)
                if attachment != owner.table:
                    raise CompositeEvaluationError(
                        "Cross-attachment fields require an explicit path"
                    )
            return self._evaluate(owner, (owner.table, label), memo, stack)
        with self.service.read() as conn:
            repo = self.service.repository
            repo.require_owner(conn, owner)
            columns = repo.columns(conn, owner.table)
            owner_id = repo.owner_column(conn, owner.table)
            column = (
                name if name in columns else owner_id.removesuffix("_id") + "_" + name
            )
            if column not in columns:
                raise CompositeEvaluationError(
                    f"Unknown field {name!r} on {owner.table}"
                )
            return rows(
                conn,
                f"SELECT {q(column)} FROM {q(owner.table)} WHERE {q(owner_id)}=?",
                (owner.id,),
            )[0][0]

    def _path(
        self,
        owner: OwnerRef,
        path: dict[str, Any],
        memo: dict[tuple[OwnerRef, int], Any],
        stack: set[tuple[OwnerRef, int]],
    ) -> Any:
        owners = [owner]
        for target in path.get("tables", []):
            linked = set()
            for current in owners:
                spec = self.service.db.driver_wrapper.get_link_spec(
                    current.table, target
                )
                if spec is None:
                    raise CompositeEvaluationError(
                        f"No relationship {current.table} -> {target}"
                    )
                for result in self.service.db.macros.get_linked_rows(spec, current.id):
                    if result.row is not None:
                        linked.add(
                            OwnerRef(target, int(result.row[spec.secondary_id_col]))
                        )
            owners = sorted(linked)
        values = [
            self._resolve(current, path["field"], {}, memo, stack) for current in owners
        ]
        aggregate = path.get("aggregate")
        if aggregate is None:
            if len(values) > 1:
                raise CompositeEvaluationError(
                    "Several related owners require an explicit aggregation"
                )
            return values[0] if values else None
        if aggregate == "join":
            return str(path.get("separator", ", ")).join(
                self._text(value) for value in values
            )
        if aggregate == "list":
            return tuple(values)
        if aggregate == "count":
            return len(values)
        if aggregate == "sum":
            return sum(values)
        if aggregate == "min":
            return min(values) if values else None
        if aggregate == "max":
            return max(values) if values else None
        raise CompositeEvaluationError(f"Unknown aggregation {aggregate!r}")


class _MetadataProxy:
    def __init__(self, resolve):
        self.resolve = resolve

    def get(self, key, default=None):
        value = self.resolve(key)
        return default if value is None else value

    def __getattr__(self, key):
        return self.resolve(key)

    def format_field(self, key, **kwargs):
        return key, CustomFieldEvaluator._text(self.resolve(key))

    def get_user_metadata(self, key, *args):
        return None
