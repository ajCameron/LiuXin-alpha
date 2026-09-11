"""
Validate program-service payloads, adapt legacy objects, and submit named Core workflow jobs.

Legacy metadata adapters are tried in a defined order before iterable or
attribute fallbacks. plain prepares results for the outer wire encoder but is
not itself a JSON validator or cycle/size guard: bytes and unsupported opaque
objects can remain. Validation helpers perform intentionally different coercions
for text, integer, and iterable fields; they do not provide authorization.
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


def _required_text(payload: Mapping[str, Any], name: str) -> str:
    """
    Stringify and strip one required field, rejecting empty text but accepting explicit None as "None".

    Example:
        >>> _required_text({"table": " works "}, "table")
        'works'


    :param payload: Request mapping supplying the field.
    :param name: Exact key used for lookup and the absence/blank error.
    :return: Nonempty stripped string, without case normalization or type restrictions.
    :raises CoreDispatchError: If the key is absent or its stringified value is blank.
    """
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError(f"`{name}` is required.")
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    """
    Require a non-boolean field convertible by int, wrapping all ordinary conversion failures.

    Numeric fractions can truncate; no lower or upper bound is enforced.

    Example:
        >>> _required_int({"row_id": "7"}, "row_id")
        7


    :param payload: Mapping with a required integer-convertible value.
    :param name: Field key also used in the dispatch error message.
    :return: Converted integer, including zero or negative values.
    :raises CoreDispatchError: If the key is absent, its value is bool, or int raises Exception.
    """
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
    """
    Convert an optional integer field, retaining None and applying a lower bound only when supplied.

    Missing keys use default; explicit None bypasses both default and minimum.
    Unlike the application API helper, this helper has no minimum by default.

    Example:
        >>> _optional_int({"offset": -2}, "offset")
        -2


    :param payload: Mapping carrying the optional field.
    :param name: Field key used for lookup and validation messages.
    :param default: Value used only when the key is absent, subject to the same validation.
    :param minimum: Optional inclusive lower bound for non-None converted values.
    :return: None or converted integer; numeric fractions may truncate.
    :raises CoreDispatchError: For bool, ordinary conversion failures, or a violated minimum.
    """
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
    default: Iterable[str] = (),
) -> list[str]:
    """
    Strip and deduplicate stringified iterable values in first-occurrence order, dropping blanks.

    A scalar str becomes one entry, while bytes iterate into integer values.
    Mappings and non-iterables are rejected; generators are consumed. None elements
    become "None", but a None field is invalid. No case normalization is applied.

    Example:
        >>> _text_list({"tags": [" a ", "", "a", "B"]}, "tags")
        ['a', 'B']


    :param payload: Request mapping supplying a text or iterable field.
    :param name: Field key used for lookup and errors.
    :param default: Iterable consumed when the key is absent; empty by default.
    :return: Ordered list of unique nonempty stripped strings.
    :raises CoreDispatchError: If the selected value is a Mapping or is not iterable.
    """
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
    Require nonblank database_path metadata for a worker that reopens a path-backed database.

    The returned text is not stripped or checked for existence, confinement, or
    backend suitability. Metadata must be a Mapping; other representations are rejected.

    Example:
        >>> path = _database_path(runtime)  # doctest: +SKIP


    :param runtime: Runtime exposing database.metadata with a database_path value.
    :return: Stringified nonblank path metadata, preserving surrounding whitespace.
    :raises CoreDispatchError: If path metadata is unavailable, using database_path_unavailable.
    """
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
    """
    Stringify the database's type attribute, using SQLite for any missing or falsey value.

    Example:
        >>> from types import SimpleNamespace
        >>> _database_type(SimpleNamespace(database=object()))
        'SQLite'


    :param runtime: Runtime whose database optionally advertises a backend type.
    :return: Backend label without stripping, validation, or alias normalization.
    """
    value = getattr(runtime.database, "type", None)
    return str(value or "SQLite")


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
    values use float without adapter range/finite validation. Backend passes through,
    no_output is truth-tested, and a stripped blank label uses default_label while
    explicit None becomes "None". Receipt construction follows submission and may
    itself fail; the manager owns validation, scheduling, and effective backend choice.

    Example:
        >>> receipt = _job_submit(runtime, {}, function_name="run_conversion_job", kwargs=options, default_label="convert")  # doctest: +SKIP


    :param runtime: Runtime whose job_manager accepts JobRequest and execution options.
    :param payload: Optional job_timeout_s, job_backend, job_no_output, and label fields.
    :param function_name: Worker name within LiuXin_alpha.core.workflow_jobs; not resolved or allowlisted here.
    :param kwargs: Shallow-copied keyword arguments carried by the job request, not a serialization proof.
    :param default_label: Label used when the stringified/stripped requested label is empty.
    :return: job_id, label, requested backend text, timeout_s, and no_output; not a completion result.
    """
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
