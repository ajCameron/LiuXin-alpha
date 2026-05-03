"""Shared compact string formatting for metadata containers."""

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
    if value is None:
        return True
    if value == "":
        return True
    if isinstance(value, (tuple, list, dict, set, frozenset)) and len(value) == 0:
        return True
    return False


def _safe_getattr(obj: object, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name)
    except Exception:
        return default


def _safe_call(method: Any, *args: Any, default: Any = None, **kwargs: Any) -> Any:
    if not callable(method):
        return default
    try:
        return method(*args, **kwargs)
    except Exception:
        return default


def _format_value(value: Any, *, max_length: int = 96, max_items: int = 4) -> str:
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
    target_kind = _safe_getattr(obj, "target_kind")
    target_id = _safe_getattr(obj, "target_id")
    if _is_empty(target_kind) or _is_empty(target_id):
        return None
    return f"{target_kind}_id={target_id}"


def _payload_for(obj: object) -> Mapping[str, Any]:
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
    """Compact ``str()`` for value objects with mapping/write payloads."""

    __slots__ = ()

    STRING_DISPLAY_KEYS: ClassVar[Sequence[str]] = _DEFAULT_DISPLAY_KEYS
    STRING_ID_KEYS: ClassVar[Sequence[str]] = ()

    def __str__(self) -> str:
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
    """Compact ``str()`` for grouped metadata containers."""

    __slots__ = ()

    STRING_COUNT_LABEL: ClassVar[str] = "items"

    def __str__(self) -> str:
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
