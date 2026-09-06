"""Core-owned preferences operations and wire translation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import _payload, _required_text

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def _preference_store(runtime: CoreRuntime, scope: str) -> Any:
    token = str(scope or "library").strip().lower()
    if token in {"library", "database", "db"}:
        return runtime.services.library_preferences
    if token in {"application", "process", "global"}:
        return runtime.services.preferences
    raise CoreDispatchError("`scope` must be `library` or `application`.")


def preferences_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    scope = str(payload.get("scope") or "library").strip().lower()
    store = _preference_store(runtime, scope)
    if not isinstance(store, Mapping):
        items = getattr(store, "items", None)
        if not callable(items):
            raise CoreDispatchError("Preference store is not mapping-like.")
        values = {
            str(key): value
            for key, value in cast(
                Iterable[tuple[Any, Any]],
                items(),
            )
        }
    else:
        values = dict(store)
    return {
        "scope": scope,
        "values": values,
    }


def preferences_get(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    key = _required_text(payload, "key")
    scope = str(payload.get("scope") or "library").strip().lower()
    store = _preference_store(runtime, scope)
    getter = getattr(store, "get", None)
    value = (
        getter(key, payload.get("default"))
        if callable(getter)
        else payload.get("default")
    )
    contains = False
    try:
        contains = key in store
    except Exception:
        contains = value is not payload.get("default")
    return {
        "scope": scope,
        "key": key,
        "exists": contains,
        "value": value,
    }


def preferences_set(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    key = _required_text(payload, "key")
    if "value" not in payload:
        raise CoreDispatchError("`value` is required.")
    scope = str(payload.get("scope") or "library").strip().lower()
    store = _preference_store(runtime, scope)
    setter = getattr(store, "set", None)
    if callable(setter):
        setter(key, payload["value"])
    else:
        store[key] = payload["value"]
    return {
        "scope": scope,
        "key": key,
        "value": payload["value"],
        "updated": True,
    }


def preferences_delete(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    key = _required_text(payload, "key")
    scope = str(payload.get("scope") or "library").strip().lower()
    store = _preference_store(runtime, scope)
    existed = False
    try:
        existed = key in store
    except Exception:
        pass
    if existed:
        del store[key]
    return {"scope": scope, "key": key, "deleted": existed}
