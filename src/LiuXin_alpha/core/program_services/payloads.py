"""Envelope validation and wire conversion for dynamic subsystem boundaries.

Legacy metadata adapters are tried in a defined order before iterable or
attribute fallbacks; handlers share that translation instead of inventing it.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.utils.jobs import JobRequest

if TYPE_CHECKING:
    from LiuXin_alpha.core.runtime import CoreRuntime


def _payload(envelope: Any) -> dict[str, Any]:
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_text(payload: Mapping[str, Any], name: str) -> str:
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError(f"`{name}` is required.")
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    if name not in payload or isinstance(payload[name], bool):
        raise CoreDispatchError(f"`{name}` must be an integer.")
    try:
        return int(payload[name])
    except Exception as exc:
        raise CoreDispatchError(f"`{name}` must be an integer.") from exc


def _optional_int(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: int | None = None,
    minimum: int | None = None,
) -> int | None:
    value = payload.get(name, default)
    if value is None:
        return None
    if isinstance(value, bool):
        raise CoreDispatchError(f"`{name}` must be an integer or null.")
    try:
        converted = int(value)
    except Exception as exc:
        raise CoreDispatchError(f"`{name}` must be an integer or null.") from exc
    if minimum is not None and converted < minimum:
        raise CoreDispatchError(f"`{name}` must be >= {minimum}.")
    return converted


def _mapping(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    value = payload.get(name, default)
    if not isinstance(value, Mapping):
        raise CoreDispatchError(f"`{name}` must be an object.")
    return dict(value)


def _text_list(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: Iterable[str] = (),
) -> list[str]:
    raw = payload.get(name, default)
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, Iterable) or isinstance(raw, Mapping):
        raise CoreDispatchError(f"`{name}` must be an array of strings.")
    values: list[str] = []
    for item in raw:
        token = str(item).strip()
        if token and token not in values:
            values.append(token)
    return values


def _callable(target: Any, name: str, *, area: str) -> Any:
    method = getattr(target, name, None)
    if not callable(method):
        raise CoreDispatchError(
            f"{area} does not support `{name}`.",
            code="capability_unavailable",
            details={"area": area, "operation": name},
        )
    return method


def _database_callable(
    runtime: CoreRuntime,
    name: str,
    *,
    area: str,
) -> Any:
    method = getattr(runtime.database, name, None)
    if callable(method):
        return method
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    return _callable(wrapper, name, area=area)


def _metadata_projection(value: Any) -> tuple[bool, Any]:
    """Try legacy metadata adapters in their established precedence order."""
    for method_name in ("to_mapping", "to_dict", "as_dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            converted = method()
            if isinstance(converted, Mapping):
                return True, converted
    to_calibre = getattr(value, "to_calibre", None)
    if callable(to_calibre):
        converted = to_calibre()
        if converted is not value:
            return True, converted
    all_fields = getattr(value, "all_non_none_fields", None)
    if callable(all_fields):
        converted = all_fields()
        if isinstance(converted, Mapping):
            return True, converted
    return False, None


def plain(value: Any) -> Any:
    """Recursively remove subsystem objects from a Core wire result."""
    if value is None or isinstance(value, (str, bytes, bool, int, float)):
        return value
    if isinstance(value, Mapping):
        return {str(key): plain(item) for key, item in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: plain(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    converted, projection = _metadata_projection(value)
    if converted:
        return plain(projection)
    row_dict = getattr(value, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return plain(row_dict)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [plain(item) for item in value]
    if isinstance(value, Iterable):
        return [plain(item) for item in value]
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, Mapping):
        return {
            str(key): plain(item)
            for key, item in attributes.items()
            if not str(key).startswith("_") and not callable(item)
        }
    return value


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value).casefold()


def _database_path(runtime: CoreRuntime) -> str:
    metadata = getattr(runtime.database, "metadata", None)
    if isinstance(metadata, Mapping):
        value = metadata.get("database_path")
        if value is not None and str(value).strip():
            return str(value)
    raise CoreDispatchError(
        "This workflow requires a path-backed database.",
        code="database_path_unavailable",
    )


def _database_type(runtime: CoreRuntime) -> str:
    value = getattr(runtime.database, "type", None)
    return str(value or "SQLite")


def _agent_role(value: Any) -> str:
    token = str(value or "author").strip().lower()
    aliases = {
        "author": "aut",
        "editor": "edt",
        "translator": "trl",
        "illustrator": "ill",
    }
    return aliases.get(token, token)


def _job_submit(
    runtime: CoreRuntime,
    payload: Mapping[str, Any],
    *,
    function_name: str,
    kwargs: Mapping[str, Any],
    default_label: str,
) -> dict[str, Any]:
    timeout_raw = payload.get("job_timeout_s")
    timeout = -1.0 if timeout_raw is None else float(timeout_raw)
    backend = payload.get("job_backend")
    no_output = bool(payload.get("job_no_output", False))
    label = str(payload.get("label", "")).strip() or default_label
    job_id = runtime.job_manager.submit(
        JobRequest(
            module_name="LiuXin_alpha.core.workflow_jobs",
            function_name=function_name,
            kwargs=dict(kwargs),
        ),
        timeout=timeout,
        no_output=no_output,
        backend=backend,
        label=label,
    )
    return {
        "job_id": job_id,
        "label": label,
        "backend": "" if backend is None else str(backend),
        "timeout_s": None if timeout_raw is None else float(timeout_raw),
        "no_output": no_output,
    }
