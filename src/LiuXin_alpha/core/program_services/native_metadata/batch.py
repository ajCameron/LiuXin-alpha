"""Translate bounded declarative edits into MetadataAPI's atomic batch callback.

Only the installed native metadata write routes are eligible. Result references
address earlier results by key and an optional mapping/array path; they cannot
invoke methods, query live objects or submit arbitrary Core commands.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt
from LiuXin_alpha.core.errors import CoreDispatchError, core_error_details
from LiuXin_alpha.core.payloads import invalid_payload, validate_payload
from LiuXin_alpha.core.wire import to_wire

from .common import (
    MAX_BATCH,
    Payload,
    Route,
    copy_input,
    mapping,
    revision,
    route,
    sequence,
)


@dataclass(frozen=True)
class Step:
    """A preflighted operation whose result key cannot change during execution."""

    key: str
    route: Route
    payload: Payload


def reference(value: Mapping[str, Any], available: set[str]) -> tuple[str, list[Any]]:
    """Validate the reserved result-reference form and reject forward references."""
    key = value.get("$ref")
    if (
        set(value) - {"$ref", "path"}
        or not isinstance(key, str)
        or key not in available
    ):
        invalid_payload(
            "$ref", "must name one earlier operation key, with optional path only."
        )
    path = sequence(value.get("path", ()), "path", 48)
    if any(
        not isinstance(part, str) and not (type(part) is int and part >= 0)
        for part in path
    ):
        invalid_payload(
            "path", "must contain string keys or nonnegative array indexes."
        )
    return key, path


def check_references(value: Any, available: set[str]) -> None:
    """Walk detached bounded input, rejecting invalid references before mutation."""
    if isinstance(value, Mapping):
        if "$ref" in value:
            reference(value, available)
        else:
            for child in value.values():
                check_references(child, available)
    elif isinstance(value, list):
        for child in value:
            check_references(child, available)


def resolve(value: Any, results: dict[str, Any]) -> Any:
    """Substitute detached prior results, limiting traversal to JSON containers."""
    if isinstance(value, Mapping):
        if "$ref" in value:
            key, path = reference(value, set(results))
            selected = results[key]
            for part in path:
                if (
                    isinstance(selected, dict)
                    and isinstance(part, str)
                    and part in selected
                ):
                    selected = selected[part]
                elif (
                    isinstance(selected, list)
                    and type(part) is int
                    and 0 <= part < len(selected)
                ):
                    selected = selected[part]
                else:
                    invalid_payload("path", "does not exist in the referenced result.")
            return copy_input(selected)
        return {key: resolve(child, results) for key, child in value.items()}
    if isinstance(value, list):
        return [resolve(child, results) for child in value]
    return value


def prepare(payload: Payload, allowed: dict[str, Route]) -> list[Step]:
    """Check operation names, keys, field shapes and references before opening a batch."""
    steps = []
    available: set[str] = set()
    for index, value in enumerate(
        sequence(payload["operations"], "operations", MAX_BATCH)
    ):
        raw = mapping(value, "operations")
        if set(raw) - {"operation", "payload", "key"}:
            invalid_payload("operations", "accepts only operation, payload and key.")
        name, key = raw.get("operation"), raw.get("key", str(index))
        if not isinstance(name, str) or name not in allowed:
            invalid_payload(
                "operation", "must select an eligible native metadata write."
            )
        if not isinstance(key, str) or not key or key in available:
            invalid_payload("key", "must be nonempty and unique within the batch.")
        selected = allowed[name]
        arguments = mapping(raw.get("payload"), "payload")
        if set(arguments) - {field.name for field in selected.fields}:
            invalid_payload("payload", "contains unknown operation fields.")
        check_references(arguments, available)
        for field in selected.fields:
            item = arguments.get(field.name)
            if not (isinstance(item, Mapping) and "$ref" in item):
                validate_payload(arguments, (field,))
        steps.append(Step(key, selected, arguments))
        available.add(key)
    return steps


def batch_route(routes: tuple[Route, ...]) -> Route:
    """Bind a private immutable-by-construction allowlist of metadata mutation routes."""
    allowed = {item.name: item for item in routes if item.write}

    def execute(api: MetadataAPIInterface, payload: Payload) -> Any:
        steps = prepare(payload, allowed)
        tokens = tuple(
            revision(value)
            for value in sequence(payload.get("expected", ()), "expected", MAX_BATCH)
        )

        def apply(bound: MetadataAPIInterface) -> list[dict[str, Any]]:
            values: dict[str, Any] = {}
            output = []
            for index, step in enumerate(steps):
                try:
                    receipt = step.route.execute(bound, resolve(step.payload, values))
                    if not isinstance(receipt, MetadataWriteReceipt):
                        raise TypeError(
                            "Native batch operation did not return a metadata receipt"
                        )
                    # Project before commit, so serialization failures roll back.
                    result = to_wire(receipt.result)
                except Exception as error:
                    code, details = core_error_details(error)
                    raise CoreDispatchError(
                        str(error),
                        code=code,
                        details={
                            **details,
                            "operation_index": index,
                            "operation": step.route.name,
                            "key": step.key,
                        },
                    ) from error
                values[step.key] = result
                output.append(
                    {
                        "key": step.key,
                        "operation": step.route.name,
                        "result": result,
                        "changed": receipt.changed,
                    }
                )
            return output

        return api.batch(apply, expected=tokens)

    return route(
        "metadata.batch",
        True,
        "Apply up to 100 native metadata writes atomically and reconcile once.",
        execute,
        "operations!:array",
        "expected:array",
    )
