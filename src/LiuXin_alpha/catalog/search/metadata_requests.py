"""Resolve public metadata search requests using Catalog's WEMI semantics.

Text filters share the existing search locations and normalized literal matching.
ID/year filters select a particular entity on a monotonic WEMI path. Sort keys
refer only to scalar attributes of the requested result level, with no implicit
choice of an author, child, translation or preferred Calibre projection.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Literal, cast

from LiuXin_alpha.caches.api import CacheQuery, CacheSort
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataFilterOperator,
    MetadataSearchField,
    MetadataSearchFilter,
    MetadataSearchRequest,
    MetadataSearchSort,
    MetadataSearchValidationError,
    MetadataSortField,
)
from LiuXin_alpha.catalog.search.database_search import (
    _FIELDS,
    _LEVELS,
    _LOCATIONS,
    _text,
)
from LiuXin_alpha.catalog.search.physical_plan import PhysicalSearchPlan
from LiuXin_alpha.databases.db_types.query_types import (
    BooleanMatch,
    ComparisonMatch,
    RowPredicate,
)
from LiuXin_alpha.utils.search_query_parser import ParseException, Parser

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import WemiLevel
    from LiuXin_alpha.catalog.search.database_search import CatalogSearch

_NUMERIC_OPERATORS: tuple[MetadataFilterOperator, ...] = (
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "in",
    "is_null",
)
_YEARS = {
    "work": "work_original_year",
    "expression": "expression_year",
    "manifestation": "manifestation_pub_year",
}
_SORTS: dict[str, tuple[tuple[str, str, Literal["text", "integer"]], ...]] = {
    "work": (
        ("title", "work_title", "text"),
        ("sort_title", "work_sort_title", "text"),
        ("creator_sort", "work_creator_sort", "text"),
        ("year", "work_original_year", "integer"),
    ),
    "expression": (
        ("label", "expression_label", "text"),
        ("title", "expression_title_override", "text"),
        ("year", "expression_year", "integer"),
    ),
    "manifestation": (
        ("subtitle", "manifestation_subtitle", "text"),
        ("edition_statement", "manifestation_edition_statement", "text"),
        ("year", "manifestation_pub_year", "integer"),
    ),
    "item": (
        ("inventory_code", "item_inventory_code", "text"),
        ("location", "item_location", "text"),
        ("type", "item_type", "text"),
        ("condition", "item_condition", "text"),
    ),
}


def _level(level: str) -> None:
    if not isinstance(level, str) or level not in _LEVELS:
        raise MetadataSearchValidationError("level", f"Unknown WEMI level: {level!r}")


def fields(level: WemiLevel) -> tuple[MetadataSearchField, ...]:
    """Describe canonical text locations and explicit numeric WEMI constraints."""
    _level(level)
    result = []
    for scope in (None, *_LEVELS):
        for name in ("all", *_FIELDS):
            key = (f"{scope}.{name}" if name != "all" else scope) if scope else name
            assert key is not None
            result.append(
                MetadataSearchField(
                    key,
                    "text",
                    ("contains", "eq"),
                    tuple(
                        alias
                        for alias, target in _LOCATIONS.items()
                        if target == (scope, name) and alias != key
                    ),
                    f"Normalized text on reachable {scope or 'WEMI'} entities; monotonic paths only.",
                )
            )
    for scope in _LEVELS:
        for name in ("id", "year") if scope in _YEARS else ("id",):
            result.append(
                MetadataSearchField(
                    f"{scope}.{name}",
                    "integer",
                    _NUMERIC_OPERATORS,
                    (name,) if scope == level else (),
                    f"{name} on a reachable {scope}; filters match independently along WEMI paths.",
                )
            )
    return tuple(result)


def sort_fields(level: WemiLevel) -> tuple[MetadataSortField, ...]:
    """Describe single-valued result attributes; nulls last and ID ties ascending."""
    _level(level)
    return (MetadataSortField("id", "integer", f"Own {level} identity."),) + tuple(
        MetadataSortField(name, kind, f"Own {level} {name}; no inherited fallback.")
        for name, _, kind in _SORTS[level]
    )


def _numeric_value(term: MetadataSearchFilter, parameter: str) -> None:
    value, operator = term.value, term.operator
    if operator == "is_null":
        valid = isinstance(value, bool)
    elif operator == "in":
        valid = isinstance(value, tuple) and all(type(v) is int for v in value)
    else:
        valid = type(value) is int or (value is None and operator in {"eq", "ne"})
    if not valid:
        raise MetadataSearchValidationError(
            parameter, f"Invalid integer operand for {operator}"
        )


def validate(search: CatalogSearch, request: MetadataSearchRequest) -> None:
    """Validate all user input without reading schema, cache rows or database rows."""
    if not isinstance(request, MetadataSearchRequest):
        raise MetadataSearchValidationError("request", "Expected MetadataSearchRequest")
    _level(request.level)
    if not isinstance(request.query, str):
        raise MetadataSearchValidationError("query", "Expected text")
    for name, value in (("limit", request.limit), ("offset", request.offset)):
        if name == "limit" and value is None:
            continue
        if type(value) is not int or value < 0:
            raise MetadataSearchValidationError(name, "Expected a nonnegative integer")
    available = {
        name: field
        for field in fields(request.level)
        for name in (field.name, *field.aliases)
    }
    for index, term in enumerate(request.filters):
        parameter = f"filters[{index}]"
        if not isinstance(term, MetadataSearchFilter):
            raise MetadataSearchValidationError(
                parameter, "Expected MetadataSearchFilter"
            )
        if not isinstance(term.field, str) or term.field not in available:
            raise MetadataSearchValidationError(
                parameter + ".field", f"Unknown field: {term.field!r}"
            )
        field = available[term.field]
        if term.operator not in field.operators:
            raise MetadataSearchValidationError(
                parameter + ".operator", f"Unsupported operator for {term.field}"
            )
        if field.value_type == "integer":
            _numeric_value(term, parameter + ".value")
        elif not isinstance(term.value, str) or not _text(term.value):
            raise MetadataSearchValidationError(
                parameter + ".value", "Expected nonempty text"
            )
    ordering = {field.name for field in sort_fields(request.level)}
    seen = set()
    for index, component in enumerate(request.sort):
        parameter = f"sort[{index}]"
        if not isinstance(component, MetadataSearchSort):
            raise MetadataSearchValidationError(
                parameter, "Expected MetadataSearchSort"
            )
        if not isinstance(component.field, str) or component.field not in ordering:
            raise MetadataSearchValidationError(
                parameter + ".field", f"Unknown sort key: {component.field!r}"
            )
        if component.field in seen:
            raise MetadataSearchValidationError(
                parameter + ".field", "Duplicate sort key"
            )
        seen.add(component.field)
        if not isinstance(component.ascending, bool):
            raise MetadataSearchValidationError(
                parameter + ".ascending", "Expected bool"
            )
    try:
        if request.query.strip():
            search._validate_terms(Parser().parse(request.query, _LOCATIONS))
    except (ParseException, RecursionError) as error:
        raise MetadataSearchValidationError("query", str(error)) from error


def compile_request(
    search: CatalogSearch, request: MetadataSearchRequest
) -> CacheQuery:
    """Resolve validated semantic terms to the existing physical query vocabulary."""
    validate(search, request)
    plan = cast(CacheQuery, search.compile(request.query, level=request.level))
    physical = PhysicalSearchPlan(search.db)
    predicates = [] if plan.where is None else [plan.where]
    for term in request.filters:
        if term.field in _LOCATIONS:
            target, name = _LOCATIONS[term.field]
            predicate = BooleanMatch(
                "or",
                tuple(
                    cast(
                        RowPredicate,
                        physical.path(
                            request.level,
                            destination,
                            physical.field(
                                destination,
                                name,
                                _text(term.value),
                                term.operator == "eq",
                            ),
                        ),
                    )
                    for destination in ((target,) if target else _LEVELS)
                ),
            )
            predicates.append(predicate)
        else:
            scope, _, name = term.field.rpartition(".")
            scope = scope or request.level
            column = f"{scope}_id" if name == "id" else _YEARS[scope]
            predicates.append(
                cast(
                    RowPredicate,
                    physical.path(
                        request.level,
                        scope,
                        ComparisonMatch(column, term.operator, term.value),
                    ),
                )
            )
    ordering = {name: column for name, column, _ in _SORTS[request.level]}
    ordering["id"] = f"{request.level}_id"
    return replace(
        plan,
        where=BooleanMatch("and", tuple(predicates)),
        sort=tuple(
            CacheSort(ordering[part.field], part.ascending) for part in request.sort
        ),
        limit=request.limit,
        offset=request.offset,
    )


__all__ = ["compile_request", "fields", "sort_fields", "validate"]
