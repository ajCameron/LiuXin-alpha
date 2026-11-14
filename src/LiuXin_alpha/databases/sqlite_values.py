"""Explicit SQLite container conversion without process-wide registrations.

Bindings on LiuXin connections encode containers as JSON. Raw SQL returns that
JSON; schema-aware reads or decode_sqlite_value restore the declared type.
"""

import json
from collections.abc import Mapping
from typing import Any


def encode_sqlite_value(value: Any) -> Any:
    """Encode a supported container, preserving scalar SQLite values unchanged."""
    if isinstance(value, set):
        if any(not isinstance(item, str) for item in value):
            raise TypeError("PYSET values must be strings")
        return json.dumps(sorted(value), ensure_ascii=False)
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def encode_sqlite_parameters(parameters: Any) -> Any:
    """Adapt one binding collection locally, preserving named or indexed bindings."""
    if isinstance(parameters, Mapping):
        return {key: encode_sqlite_value(value) for key, value in parameters.items()}
    return tuple(
        encode_sqlite_value(parameters[index]) for index in range(len(parameters))
    )


def decode_sqlite_value(value: Any, declared_type: str) -> Any:
    """Decode JSON for an explicit PYSET/PYLIST/PYDICT declaration; reject bad shapes."""
    kind = declared_type.upper().split("(", 1)[0].strip()
    if value is None or kind not in {"PYSET", "PYLIST", "PYDICT"}:
        return value
    decoded = json.loads(value)
    if kind == "PYDICT":
        if not isinstance(decoded, dict):
            raise ValueError("PYDICT requires a JSON object")
        return decoded
    if not isinstance(decoded, list):
        raise ValueError(f"{kind} requires a JSON array")
    if kind == "PYSET":
        if any(not isinstance(item, str) for item in decoded):
            raise ValueError("PYSET requires string elements")
        return set(decoded)
    return decoded
