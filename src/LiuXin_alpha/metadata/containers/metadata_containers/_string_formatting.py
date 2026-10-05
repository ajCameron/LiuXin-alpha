"""
Format compact diagnostic strings for metadata values, bundles and collections.

Attribute and method probes tolerate ordinary exceptions, but iteration and value
rendering can still propagate failures. These helpers may invoke lazy accessors;
their output is a diagnostic summary rather than a serialization format.

Example:
    >>> compact_mapping_string(object(), {'name': 'Example'})
    "object(name='Example')"
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any, ClassVar


_DEFAULT_DISPLAY_KEYS = (
    "text",
    "value",
    "name",
    "display_text",
    "credited_as",
    "language_name",
    "language_code",
    "uri",
    "body",
    "note",
)
_SKIP_EXTRA_KEY_SUFFIXES = (
    "_created_timestamp_ep_k",
    "_modified_timestamp_ep_k",
    "_source_created_datestamp_ep_k",
    "_source_modified_datestamp_ep_k",
    "_scratch",
)


def _is_empty(value: Any) -> bool:
    """
    Recognize None, empty strings and empty built-in collection values.

    Zero and False are retained as meaningful values; arbitrary Mapping implementations
    are not treated as empty solely by length.

    Example:
        >>> [_is_empty(x) for x in (None, "", [], 0, False)]
        [True, True, True, False, False]


    :param value: Value to test for diagnostic omission.
    :return: True for a recognized empty value.
    """
    if value is None:
        return True
    if value == "":
        return True
    if isinstance(value, (tuple, list, dict, set, frozenset)) and len(value) == 0:
        return True
    return False


def _safe_getattr(obj: object, name: str, default: Any = None) -> Any:
    """
    Read an attribute, returning a fallback if ordinary attribute access fails.

    Example:
        >>> _safe_getattr(object(), "missing", "fallback")
        'fallback'


    :param obj: Object whose attribute is accessed.
    :param name: Attribute name passed to getattr.
    :param default: Fallback for missing attributes or access errors.
    :return: Attribute value or default when an Exception is raised.
    """
    try:
        return getattr(obj, name)
    except Exception:
        return default


def _safe_call(method: Any, *args: Any, default: Any = None, **kwargs: Any) -> Any:
    """
    Invoke a callable and suppress ordinary call failures.

    Example:
        >>> _safe_call(int, "bad", default=7)
        7


    :param method: Candidate callable to invoke.
    :param args: Positional arguments forwarded to the callable.
    :param default: Fallback returned when the call cannot produce a result.
    :param kwargs: Keyword arguments forwarded to the callable.
    :return: Call result, or default for a non-callable or an Exception.
    """
    if not callable(method):
        return default
    try:
        return method(*args, **kwargs)
    except Exception:
        return default


def _format_value(value: Any, *, max_length: int = 96, max_items: int = 4) -> str:
    """
    Render a bounded diagnostic value, recursively formatting mappings and sequences.

    Enums use their underlying values; strings and bytes use repr. Nested containers
    inherit max_length but use the default item limit.

    Example:
        >>> _format_value({"a": 1, "b": 2}, max_items=1)
        '{a=1, ...}'


    :param value: Value to render.
    :param max_length: Character budget applied to the rendered string; use at least
        three characters.
    :param max_items: Maximum entries shown at the current mapping or sequence level.
    :return: Rendered value, truncated with an ellipsis when it exceeds max_length.
    """
    if isinstance(value, Enum):
        value = value.value

    if isinstance(value, Mapping):
        items = []
        for index, (key, item_value) in enumerate(value.items()):
            if index >= max_items:
                items.append("...")
                break
            items.append(f"{key}={_format_value(item_value, max_length=max_length)}")
        rendered = "{" + ", ".join(items) + "}"
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = []
        for index, item_value in enumerate(value):
            if index >= max_items:
                items.append("...")
                break
            items.append(_format_value(item_value, max_length=max_length))
        rendered = "[" + ", ".join(items) + "]"
    else:
        rendered = repr(value)

    if len(rendered) > max_length:
        return rendered[: max_length - 3] + "..."
    return rendered


def _target_piece(obj: object) -> str | None:
    """
    Build a target-id label when both target_kind and target_id are nonempty.

    Example:
        >>> from types import SimpleNamespace
        >>> _target_piece(SimpleNamespace(target_kind="work", target_id=2))
        'work_id=2'


    :param obj: Object exposing optional target_kind and target_id attributes.
    :return: Target-kind id fragment, or None.
    """
    target_kind = _safe_getattr(obj, "target_kind")
    target_id = _safe_getattr(obj, "target_id")
    if _is_empty(target_kind) or _is_empty(target_id):
        return None
    return f"{target_kind}_id={target_id}"


def _payload_for(obj: object) -> Mapping[str, Any]:
    """
    Prefer a mapping from to_mapping, then try as_write_payload.

    Failed or non-mapping results fall through without propagating ordinary call errors.

    Example:
        >>> _payload_for(object())
        {}


    :param obj: Metadata value with optional serialization methods.
    :return: First mapping returned by a supported method, or an empty dictionary.
    """
    to_mapping = _safe_getattr(obj, "to_mapping")
    payload = _safe_call(to_mapping)
    if isinstance(payload, Mapping):
        return payload

    as_write_payload = _safe_getattr(obj, "as_write_payload")
    payload = _safe_call(as_write_payload)
    if isinstance(payload, Mapping):
        return payload

    return {}


def _container_count(obj: object) -> int | None:
    """
    Try len first, then count a sequence returned by as_write_payload.

    Example:
        >>> _container_count([1, 2])
        2


    :param obj: Object with an optional length or sequence write payload.
    :return: Container length, or None when neither count is available.
    """
    try:
        return len(obj)  # type: ignore[arg-type]
    except Exception:
        pass

    payload = _safe_call(_safe_getattr(obj, "as_write_payload"))
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return len(payload)

    return None


def compact_mapping_string(
    obj: object,
    mapping: Mapping[str, Any],
    *,
    id_keys: Sequence[str] = (),
    display_keys: Sequence[str] = (),
    max_fields: int = 4,
) -> str:
    """
    Summarize a mapping using target ids, requested ids, display text and extra fields.

    Empty values and bookkeeping suffixes are omitted from extra fields. max_fields
    limits the extra-field loop; mandatory id and display pieces can already exceed it.

    Example:
        >>> compact_mapping_string(object(), {"id": 1, "name": "Book"}, id_keys=("id",))
        "object(id=1, name='Book')"


    :param obj: Object supplying the class name and optional target identity.
    :param mapping: Column or payload mapping to summarize in its iteration order.
    :param id_keys: Id keys to include before display text and extra fields.
    :param display_keys: Preferred text keys; an empty sequence uses the module
        defaults.
    :param max_fields: Piece count at which optional extra fields stop being appended.
    :return: Class-named diagnostic string.
    """
    pieces: list[str] = []
    used_keys: set[str] = set()

    target = _target_piece(obj)
    if target is not None:
        pieces.append(target)

    for key in id_keys:
        value = mapping.get(key)
        if not _is_empty(value):
            pieces.append(f"{key}={_format_value(value)}")
            used_keys.add(key)

    for key in display_keys or _DEFAULT_DISPLAY_KEYS:
        value = mapping.get(key)
        if not _is_empty(value):
            pieces.append(f"{key}={_format_value(value)}")
            used_keys.add(key)
            break

    for key, value in mapping.items():
        if len(pieces) >= max_fields:
            break
        if key in used_keys or _is_empty(value):
            continue
        if key.endswith(_SKIP_EXTRA_KEY_SUFFIXES):
            continue
        pieces.append(f"{key}={_format_value(value)}")
        used_keys.add(key)

    if not pieces:
        pieces.append("empty")
    return f"{obj.__class__.__name__}({', '.join(pieces)})"


def compact_container_string(
    obj: object,
    *,
    count_label: str = "items",
    text_methods: Sequence[str] = ("to_text", "full_title"),
    text_attributes: Sequence[str] = ("display_title", "display_name", "display_genre"),
) -> str:
    """
    Summarize a collection with target identity, count and available display text.

    Text probes try attributes, methods, recognized entry iterators and a write payload.
    Calling accessors or iterators can materialize deferred data.

    Example:
        >>> compact_container_string([1, 2], count_label="entries")
        'list(2 entries)'


    :param obj: Collection or metadata container to describe.
    :param count_label: Noun appended to a discovered count.
    :param text_methods: Zero-argument text method names tried after attributes.
    :param text_attributes: Display attribute names tried before methods.
    :return: Class-named diagnostic string, with empty when no pieces are available.
    """
    pieces: list[str] = []

    target = _target_piece(obj)
    if target is not None:
        pieces.append(target)

    count = _container_count(obj)
    if count is not None:
        pieces.append(f"{count} {count_label}")

    for attribute in text_attributes:
        value = _safe_getattr(obj, attribute)
        if not _is_empty(value):
            pieces.append(f"text={_format_value(value)}")
            break
    else:
        for method_name in text_methods:
            value = _safe_call(_safe_getattr(obj, method_name))
            if not _is_empty(value):
                pieces.append(f"text={_format_value(value)}")
                break
        else:
            for iterator_name in (
                "iter_all_titles",
                "iter_all_identifiers",
                "iter_all_subjects",
                "iter_all_labels",
                "iter_all_languages",
                "iter_all_dates",
                "iter_all_ratings",
                "iter_all_entries",
                "iter_all_resources",
                "iter_all_notes",
                "iter_all_credits",
            ):
                iterator = _safe_call(_safe_getattr(obj, iterator_name))
                try:
                    first = next(iter(iterator), None) if iterator is not None else None
                except TypeError:
                    first = None
                if first is None:
                    continue
                for attribute in (
                    "display_text",
                    "text",
                    "value",
                    "credited_as",
                    "language_name",
                    "language_code",
                    "uri",
                    "body",
                ):
                    value = _safe_getattr(first, attribute)
                    if not _is_empty(value):
                        pieces.append(f"text={_format_value(value)}")
                        break
                if len(pieces) > (2 if target is not None else 1):
                    break

        if not any(piece.startswith("text=") for piece in pieces):
            payload = _safe_call(_safe_getattr(obj, "as_write_payload"))
            if (
                isinstance(payload, Sequence)
                and not isinstance(payload, (str, bytes, bytearray))
                and payload
            ):
                first = payload[0]
                if isinstance(first, Mapping):
                    for key in _DEFAULT_DISPLAY_KEYS:
                        value = first.get(key)
                        if not _is_empty(value):
                            pieces.append(f"text={_format_value(value)}")
                            break

    if not pieces:
        pieces.append("empty")
    return f"{obj.__class__.__name__}({', '.join(pieces)})"


def relation_count_summary(
    relation_names: Sequence[str],
    get_links: Any,
) -> str:
    """
    Count nonempty relation buckets while tolerating lookup or length failures.

    Example:
        >>> relation_count_summary(("tags", "notes"), lambda key: [1] if key == "tags" else [])
        'tags:1'


    :param relation_names: Relation names to query in display order.
    :param get_links: Callable accepting a relation name and returning a sized
        collection.
    :return: Comma-separated name:count fragments in the requested order.
    """
    counts: list[str] = []
    for relation in relation_names:
        links = _safe_call(get_links, relation, default=())
        try:
            count = len(links)
        except Exception:
            count = 0
        if count:
            counts.append(f"{relation}:{count}")
    return ", ".join(counts)


def metadata_bundle_string(
    obj: object,
    *,
    identity_name: str,
    relation_names: Sequence[str],
    get_links: Any,
) -> str:
    """
    Summarize a bundle identity and its nonempty relation counts.

    Example:
        >>> metadata_bundle_string(object(), identity_name="work", relation_names=(), get_links=None)
        'object(empty)'


    :param obj: Bundle exposing the named identity attribute.
    :param identity_name: Attribute name of the identity to render.
    :param relation_names: Relation buckets to count in order.
    :param get_links: Callable returning links for one relation name.
    :return: Class-named diagnostic string.
    """
    pieces: list[str] = []
    identity = _safe_getattr(obj, identity_name)
    if identity is not None:
        pieces.append(f"{identity_name}={identity}")

    relation_counts = relation_count_summary(relation_names, get_links)
    if relation_counts:
        pieces.append(f"relations={relation_counts}")

    if not pieces:
        pieces.append("empty")
    return f"{obj.__class__.__name__}({', '.join(pieces)})"


class MetadataValueStringMixin:
    """
    Provide compact string output for values with mapping or write-payload methods.

    Subclasses can choose STRING_ID_KEYS and STRING_DISPLAY_KEYS; no instance state is
    added.

    Example:
        >>> str(MetadataValueStringMixin())
        'MetadataValueStringMixin(empty)'
    """

    __slots__ = ()

    STRING_DISPLAY_KEYS: ClassVar[Sequence[str]] = _DEFAULT_DISPLAY_KEYS
    STRING_ID_KEYS: ClassVar[Sequence[str]] = ()

    def __str__(self) -> str:
        """
        Format the value payload using configured id and display-key priorities.

        Example:
            >>> str(MetadataValueStringMixin())
            'MetadataValueStringMixin(empty)'


        :return: Compact class-named value description.
        """
        payload = _payload_for(self)
        target_kind = _safe_getattr(self, "target_kind")
        target_id_key = f"{target_kind}_id" if not _is_empty(target_kind) else ""
        id_keys = tuple(self.STRING_ID_KEYS)
        if target_id_key:
            id_keys = (target_id_key, *id_keys)
        return compact_mapping_string(
            self,
            payload,
            id_keys=id_keys,
            display_keys=tuple(self.STRING_DISPLAY_KEYS),
        )


class MetadataSequenceStringMixin:
    """
    Provide compact string output for grouped metadata collections.

    Subclasses can set STRING_COUNT_LABEL to name counted entries.

    Example:
        >>> str(MetadataSequenceStringMixin())
        'MetadataSequenceStringMixin(empty)'
    """

    __slots__ = ()

    STRING_COUNT_LABEL: ClassVar[str] = "items"

    def __str__(self) -> str:
        """
        Describe the collection using its configured count label and text probes.

        Example:
            >>> str(MetadataSequenceStringMixin())
            'MetadataSequenceStringMixin(empty)'


        :return: Compact class-named collection description.
        """
        return compact_container_string(
            self,
            count_label=self.STRING_COUNT_LABEL,
        )


__all__ = [
    "MetadataSequenceStringMixin",
    "MetadataValueStringMixin",
    "compact_container_string",
    "compact_mapping_string",
    "metadata_bundle_string",
    "relation_count_summary",
]
