"""
Read and mutate library or application preferences through Core service bindings.

Handlers copy request mappings, select a store by scope, and return receipts for
the runtime's outer wire conversion. They do not normalize preference values or
add persistence, transaction, or rollback guarantees beyond the selected store.
Scope aliases remain aliases in receipts. A whitespace-only handler scope becomes
an empty receipt token but selects library preferences through the helper's fallback.
"""

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
    """
    Resolve library/database/db or application/process/global to the corresponding service preference store.

    Falsey scope values select library; otherwise text is stripped and lowercased.
    A directly supplied whitespace-only string is invalid. Resolving a service
    property may initialize its store and propagate dependency errors.

    Example:
        >>> from types import SimpleNamespace
        >>> runtime = SimpleNamespace(services=SimpleNamespace(library_preferences={"theme": "dark"}))
        >>> _preference_store(runtime, " DB ")["theme"]
        'dark'


    :param runtime: Runtime whose services expose library_preferences and preferences.
    :param scope: Case-insensitive scope name or supported alias; falsey values default to library.
    :return: The selected store itself, not a copy or a uniformly validated mapping adapter.
    :raises CoreDispatchError: If the normalized token names neither supported scope family.
    """
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
    """
    Snapshot preference items from a Mapping or an object with a callable items method.

    Mapping keys are preserved; keys from the items-only fallback are stringified,
    so collisions after conversion overwrite earlier values. Values remain shallow
    references until outer Core serialization. Store iteration errors propagate.

    Example:
        >>> from types import SimpleNamespace
        >>> from LiuXin_alpha.core.queries import CoreQuery
        >>> runtime = SimpleNamespace(services=SimpleNamespace(library_preferences={"theme": "dark"}))
        >>> preferences_list(runtime, CoreQuery("preferences.list"))
        {'scope': 'library', 'values': {'theme': 'dark'}}


    :param runtime: Runtime providing the requested preference store through its services.
    :param query: Query with optional scope, defaulting to library for missing or falsey values.
    :return: Normalized input scope token and a shallow dictionary of the store's current items.
    :raises CoreDispatchError: If payload/scope is invalid or the store has neither Mapping support nor callable items.
    """
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
    """
    Read one key with an optional default and report a separately determined membership flag.

    Without callable get, the supplied default is returned even if membership is
    true. If membership raises Exception, exists falls back to value/default object
    identity, not equality; a stored default object can therefore look absent.
    Getter failures propagate. The key is stringified and stripped, so explicit
    None becomes the literal key "None" rather than a missing-key error.

    Example:
        >>> from types import SimpleNamespace
        >>> from LiuXin_alpha.core.queries import CoreQuery
        >>> runtime = SimpleNamespace(services=SimpleNamespace(library_preferences={}))
        >>> preferences_get(runtime, CoreQuery("preferences.get", {"key": "theme", "default": "light"}))
        {'scope': 'library', 'key': 'theme', 'exists': False, 'value': 'light'}


    :param runtime: Runtime providing the selected preference store.
    :param query: Query containing key, optional scope, and optional default (None when omitted).
    :return: Scope, normalized key, membership estimate, and value or caller default.
    :raises CoreDispatchError: If payload/scope is invalid or key is missing or blank after text conversion.
    """
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
    """
    Set one preference using callable store.set when available, otherwise item assignment.

    Value must be present but may be None. A setter's return value is ignored:
    updated means the call returned without raising, not that data changed or was
    durably flushed. No readback or compensation is performed on store failures.

    Example:
        >>> from types import SimpleNamespace
        >>> from LiuXin_alpha.core.commands import CoreCommand
        >>> store = {}
        >>> runtime = SimpleNamespace(services=SimpleNamespace(library_preferences=store))
        >>> preferences_set(runtime, CoreCommand("preferences.set", {"key": "theme", "value": None}))["updated"]
        True
        >>> store
        {'theme': None}


    :param runtime: Runtime providing the selected writable preference store.
    :param command: Command with text-normalized key, required value, and optional scope defaulting to library.
    :return: Scope, key, original supplied value, and updated=True after the write call returns.
    :raises CoreDispatchError: If payload/scope is invalid, key is absent/blank, or value is absent.
    """
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
    """
    Delete an existing preference by item deletion, treating failed membership checks as absence.

    Membership Exception is suppressed and prevents deletion. Actual deletion
    failures propagate, including a key disappearing after the membership check.
    There is no delete-method fallback or readback of the store afterward.

    Example:
        >>> from types import SimpleNamespace
        >>> from LiuXin_alpha.core.commands import CoreCommand
        >>> store = {"theme": "dark"}
        >>> runtime = SimpleNamespace(services=SimpleNamespace(library_preferences=store))
        >>> command = CoreCommand("preferences.delete", {"key": "theme"})
        >>> preferences_delete(runtime, command)["deleted"], preferences_delete(runtime, command)["deleted"]
        (True, False)


    :param runtime: Runtime providing the selected preference store with membership and deletion support.
    :param command: Command with text-normalized key and optional scope defaulting to library.
    :return: Scope, key, and deleted flag reflecting the successful pre-deletion membership check.
    :raises CoreDispatchError: If payload/scope is invalid or key is absent/blank after text conversion.
    """
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
