"""
Validate program-service payloads, adapt legacy objects, and submit named Core workflow jobs.

Legacy metadata adapters are tried in a defined order before iterable or
attribute fallbacks. plain prepares results for the outer wire encoder but is
not itself a JSON validator or cycle/size guard: bytes and unsupported opaque
objects can remain. Primitive validation uses Core's shared strict helpers;
text arrays require string elements. These helpers do not provide authorization.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.payloads import boolean, invalid_payload, number_value
from LiuXin_alpha.core.payloads import optional_int as _optional_int
from LiuXin_alpha.core.payloads import required_int as _required_int
from LiuXin_alpha.core.payloads import required_text as _required_text

if TYPE_CHECKING:
    from LiuXin_alpha.core.runtime import CoreRuntime


def _payload(envelope: Any) -> dict[str, Any]:
    """
    Shallow-copy an envelope's Mapping payload, treating missing/None payload as empty.

    Example:
        >>> from types import SimpleNamespace
        >>> _payload(SimpleNamespace(payload={"limit": 3}))
        {'limit': 3}


    :param envelope: Object whose optional payload attribute contains request data.
    :return: New dictionary preserving keys and nested value identities.
    :raises CoreDispatchError: If a present non-None payload is not a Mapping.
    """
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _mapping(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Copy a Mapping field, optionally using a Mapping default when the key is absent.

    Explicit None never selects the default. Nested values and key types are retained.

    Example:
        >>> _mapping({}, "options", default={})
        {}


    :param payload: Request mapping containing the nested object.
    :param name: Nested Mapping field key.
    :param default: Missing-key fallback; None makes absence an error.
    :return: Shallow dictionary copy of the selected Mapping.
    :raises CoreDispatchError: If the selected value is not a Mapping.
    """
    value = payload.get(name, default)
    if not isinstance(value, Mapping):
        raise CoreDispatchError(f"`{name}` must be an object.")
    return dict(value)


def _text_list(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: Sequence[str] = (),
) -> list[str]:
    """
    Strip and deduplicate an array of strings, dropping blanks in first-occurrence order.

    Scalars, bytes, mappings, generators and non-string elements are rejected.
    No case normalization is applied.

    Example:
        >>> _text_list({"tags": [" a ", "", "a", "B"]}, "tags")
        ['a', 'B']


    :param payload: Request mapping supplying a string-array field.
    :param name: Field key used for lookup and errors.
    :param default: Iterable consumed when the key is absent; empty by default.
    :return: Ordered list of unique nonempty stripped strings.
    :raises CoreDispatchError: If the selected value is not a list/tuple of strings.
    """
    raw = payload.get(name, default)
    if not isinstance(raw, (list, tuple)):
        invalid_payload(name, "must be an array of strings.")
    values: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            invalid_payload(name, "must contain only strings.")
        token = item.strip()
        if token and token not in values:
            values.append(token)
    return values


def _callable(target: Any, name: str, *, area: str) -> Any:
    """
    Resolve a named callable or raise a structured capability-unavailable error without invoking it.

    Attribute access may execute descriptors; their exceptions are not suppressed.

    Example:
        >>> _callable("book", "upper", area="text")()
        'BOOK'


    :param target: Object whose attribute supplies the requested operation; None has no such capability.
    :param name: Exact method/attribute name to resolve.
    :param area: Human-readable subsystem label retained in the error details.
    :return: Callable attribute, with no signature or operation-success check.
    :raises CoreDispatchError: If the attribute is absent or non-callable, using capability_unavailable.
    """
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
    """
    Prefer a database-level callable, using its driver wrapper only when that callable is unavailable.

    This selects a method; failures when it is later invoked do not trigger fallback.

    Example:
        >>> method = _database_callable(runtime, "vacuum", area="database")  # doctest: +SKIP


    :param runtime: Runtime exposing database and its optional driver_wrapper.
    :param name: Exact operation name to resolve in database-first order.
    :param area: Capability error's subsystem label if neither object supports the name.
    :return: First supported callable; attribute-access failures propagate.
    """
    method = getattr(runtime.database, name, None)
    if callable(method):
        return method
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    return _callable(wrapper, name, area=area)


def _metadata_projection(value: Any) -> tuple[bool, Any]:
    """
    Try to_mapping, to_dict, as_dict, to_calibre, then all_non_none_fields in that order.

    Mapping adapters count only Mapping results. to_calibre accepts any result
    other than the input object itself, including None. Exceptions abort projection
    rather than trying the next adapter; accepted values are not recursively converted here.

    Example:
        >>> from types import SimpleNamespace
        >>> _metadata_projection(SimpleNamespace(to_dict=lambda: {"title": "Book"}))
        (True, {'title': 'Book'})


    :param value: Object inspected for supported zero-argument metadata adapters.
    :return: (True, raw projection) on the first accepted result, otherwise (False, None).
    """
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
    """
    Recursively project known subsystem containers while leaving primitive and unsupported opaque values intact.

    Precedence is primitive, Mapping, dataclass instance, metadata adapter, row_dict,
    Sequence, other Iterable, then public non-callable __dict__ attributes. Mappings
    stringify keys, so collisions can overwrite values; iterable order is retained
    without imposing order on sets. Dataclass fields include private names, unlike
    the attribute fallback. Bytes and opaque values remain for the outer wire layer.
    Adapter/attribute errors propagate; cycles and infinite iterables are not guarded.

    Example:
        >>> from types import SimpleNamespace
        >>> plain(SimpleNamespace(title="Book", _private="hidden", tags=("a", "b")))
        {'title': 'Book', 'tags': ['a', 'b']}


    :param value: Arbitrary handler result to prepare for subsequent Core wire encoding.
    :return: Recursively projected containers or original primitive/opaque value; not guaranteed JSON-safe.
    """
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
    """
    Casefold scalars and recursively join Mapping values or non-string Sequences for text search.

    Mapping keys are omitted, None becomes empty text, and bytes/sets/generators
    are stringified rather than traversed. There is no recursion or output-size cap.

    Example:
        >>> _flatten_text({"title": "Straße", "year": 2026})
        'strasse 2026'


    :param value: Scalar or nested row data to render as searchable text.
    :return: Casefolded string with spaces joining container values.
    """
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value).casefold()


def _database_path(runtime: CoreRuntime) -> str:
    """
    Return the captured absolute file path or PostgreSQL selector for a worker.

    The Core graph captures connection options at construction. Borrowed test or
    standalone adapters without CoreServices resolve their own metadata here.

    Example:
        >>> path = _database_path(runtime)  # doctest: +SKIP


    :param runtime: Runtime exposing database.metadata with a database_path value.
    :return: Captured database path or server selector.
    :raises CoreDispatchError: If path metadata is unavailable, using database_path_unavailable.
    """
    return _database_connection(runtime)["database_path"]


def _database_connection(runtime: CoreRuntime) -> dict[str, Any]:
    """Copy the Core graph's captured connection options for a worker submission."""
    from LiuXin_alpha.core.connections import capture_database_connection

    services = getattr(runtime, "services", None)
    connection = (
        services.database_connection
        if services is not None
        else capture_database_connection(runtime.database)
    )
    if connection is None:
        raise CoreDispatchError(
            "This workflow requires a reopenable database connection.",
            code="database_path_unavailable",
        )
    return connection


def _database_type(runtime: CoreRuntime) -> str:
    """
    Stringify the database's type attribute, using SQLite for any missing or falsey value.

    Example:
        >>> from types import SimpleNamespace
        >>> _database_type(SimpleNamespace(database=object()))
        'SQLite'


    :param runtime: Runtime whose database optionally advertises a backend type.
    :return: Backend label without stripping, validation, or alias normalization.
    """
    connection = getattr(
        getattr(runtime, "services", None), "database_connection", None
    )
    return (
        connection["db_type"]
        if connection is not None
        else str(getattr(runtime.database, "type", None) or "SQLite")
    )


def _agent_role(value: Any) -> str:
    """
    Normalize four common Agent role names to relator codes, defaulting falsey inputs to author.

    Text is stripped/lowercased; whitespace-only truthy text becomes empty rather
    than author. Unknown tokens are preserved after normalization.

    Example:
        >>> _agent_role(" Translator "), _agent_role(None)
        ('trl', 'aut')


    :param value: Role name/code or falsey value selecting the author default.
    :return: aut, edt, trl, or ill for known names; otherwise the normalized token.
    """
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
    """
    Submit a named Core workflow JobRequest and report the requested submission settings.

    Missing/None job_timeout_s sends -1.0 to the manager but reports None; supplied
    values must be finite numbers. Backend passes through, no_output must be a
    boolean, and a stripped blank label uses default_label while
    explicit None becomes "None". Receipt construction follows submission and may
    itself fail; the manager owns validation, scheduling, and effective backend choice.

    Example:
        >>> receipt = _job_submit(runtime, {}, function_name="run_conversion_job", kwargs=options, default_label="convert")  # doctest: +SKIP


    :param runtime: Runtime whose job_manager accepts JobRequest and execution options.
    :param payload: Optional job_timeout_s, job_backend, job_no_output, and label fields.
    :param function_name: Worker name within LiuXin_alpha.core.workflow_jobs; not resolved or allowlisted here.
    :param kwargs: Independently copied worker options; database jobs receive the captured Core connection. This is not a serialization proof.
    :param default_label: Label used when the stringified/stripped requested label is empty.
    :return: job_id, label, requested backend text, timeout_s, and no_output; not a completion result.
    """
    timeout_raw = payload.get("job_timeout_s")
    timeout = (
        -1.0 if timeout_raw is None else number_value(timeout_raw, "job_timeout_s")
    )
    backend = payload.get("job_backend")
    no_output = boolean(payload, "job_no_output", False)
    label = str(payload.get("label", "")).strip() or default_label
    worker_kwargs = deepcopy(dict(kwargs))
    if "database_path" in worker_kwargs:
        worker_kwargs.update(_database_connection(runtime))
    if getattr(runtime, "_durable_jobs", None) is None:
        raise CoreDispatchError(
            "Workflow submission requires a configured durable jobs ledger",
            code="capability_unavailable",
        )
    from LiuXin_alpha.core.command_recovery import submit_workflow

    return submit_workflow(
        runtime,
        function_name=function_name,
        kwargs=worker_kwargs,
        label=label,
        timeout=timeout,
        backend=backend,
        no_output=no_output,
    )


__all__ = ["_required_int", "_required_text", "_optional_int"]
