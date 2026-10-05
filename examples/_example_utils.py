"""
Share checkout imports and compact diagnostic JSON rendering across examples.

Call bootstrap_src_path explicitly to make this checkout's src directory importable.
JSON conversion summarizes bytes, long text, and collections; it is a presentation
helper rather than a lossless serializer or a general resource/safety boundary.
Dates and UUIDs become text, and unfamiliar objects can supply to_dict or __dict__.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
import sys

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID


def bootstrap_src_path() -> Path:
    """
    Return this helper's resolved checkout root and expose its src directory when present. Insert
    the exact src string at the front of sys.path only if it is a directory and that string is
    absent. An existing entry is not moved, equivalent path spellings are not deduplicated, and
    already imported modules are not reloaded. No directory is created.

    Example:
        >>> bootstrap_src_path() == Path(__file__).resolve().parents[1]
        True


    :return: Resolved parent of the examples directory, even when it has no src directory.
    """
    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "src"
    if src.is_dir():
        src_text = str(src)
        if src_text not in sys.path:
            sys.path.insert(0, src_text)
    return repo_root


def json_sanitize(
    obj: Any,
    *,
    max_text: int = 800,
    max_items: int = 80,
    _depth: int = 0,
    _max_depth: int = 6,
) -> Any:
    """
    Recursively summarize an object as values suitable for the examples' JSON renderer.

    Preserve None, booleans, integers, and floats, including non-finite floats. Shorten long strings
    with an ellipsis; dates/times use isoformat and UUIDs use their string form. Bytes-like values
    become a type/byte-count/first-32-bytes-hex mapping. Dataclasses expose non-private fields
    before the depth check, without a field-count limit.

    At the depth cutoff, remaining objects become repr text. Otherwise mappings stringify keys and
    keep at most max_items entries plus a truncation marker; key collisions can overwrite values.
    Lists, tuples, sets, and frozensets become lists with a possible final marker. Collection
    conversion materializes the input, and set order is not stabilized. Other objects try to_dict(),
    then vars(), then repr; failures inside either conversion attempt are swallowed, while errors
    outside those guarded attempts can propagate.

    Limits are presentation heuristics, not hard bounds: ellipses can exceed a small max_text, repr
    text is unbounded, and recursive calls reset _max_depth to its default. Dataclass traversal
    precedes that cutoff and does not detect cycles. Inputs are not deliberately mutated, but
    user-defined accessors/conversion methods may have effects.

    Example:
        >>> json_sanitize(b"abc")
        {'__type__': 'bytes', 'size': 3, 'preview_hex': '616263'}
        >>> json_sanitize("abcdef", max_text=5)
        'ab...'
        >>> json_sanitize([1, 2, 3], max_items=2)
        [1, 2, '... (truncated)']
        >>> json_sanitize({1: "first", "1": "second"})
        {'1': 'second'}
        >>> json_sanitize("abcd", max_text=1)
        '...'
        >>> json_sanitize({"nested": [1]}, _max_depth=1)
        {'nested': [1]}


    :param obj: Value to render; custom conversion methods and repr may be invoked.
    :param max_text: String character threshold, default 800; shortened output reserves three characters for an ellipsis.
    :param max_items: Per-mapping or sequence item limit, default 80, excluding an added truncation marker; not validated.
    :param _depth: Current recursive nesting counter, normally left at zero by callers.
    :param _max_depth: Cutoff for this call, default 6; overrides are not forwarded to recursive calls.
    :return: Converted scalar, list, mapping, or repr string; output may be lossy and is not guaranteed strict JSON.
    """
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        if len(obj) > max_text:
            return obj[: max(0, max_text - 3)] + "..."
        return obj
    if isinstance(obj, (_dt.datetime, _dt.date, _dt.time)):
        try:
            return obj.isoformat()
        except Exception:
            return repr(obj)
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        raw = bytes(obj)
        return {
            "__type__": "bytes",
            "size": len(raw),
            "preview_hex": raw[:32].hex(),
        }
    if dataclasses.is_dataclass(obj):
        return {
            field.name: json_sanitize(
                getattr(obj, field.name),
                max_text=max_text,
                max_items=max_items,
                _depth=_depth + 1,
            )
            for field in dataclasses.fields(obj)
            if not field.name.startswith("_")
        }
    if _depth >= _max_depth:
        return repr(obj)
    if isinstance(obj, Mapping):
        out = {}
        for idx, (k, v) in enumerate(obj.items()):
            if idx >= max_items:
                out["__truncated__"] = "dict truncated"
                break
            out[str(k)] = json_sanitize(v, max_text=max_text, max_items=max_items, _depth=_depth + 1)
        return out
    if isinstance(obj, (list, tuple, set, frozenset)):
        items = list(obj)
        rendered = [
            json_sanitize(v, max_text=max_text, max_items=max_items, _depth=_depth + 1)
            for v in items[:max_items]
        ]
        if len(items) > max_items:
            rendered.append("... (truncated)")
        return rendered

    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        try:
            return json_sanitize(obj.to_dict(), max_text=max_text, max_items=max_items, _depth=_depth + 1)
        except Exception:
            pass

    if hasattr(obj, "__dict__"):
        try:
            return json_sanitize(vars(obj), max_text=max_text, max_items=max_items, _depth=_depth + 1)
        except Exception:
            pass

    return repr(obj)


def dump_json(data: Any) -> str:
    """
    Return indented, sorted-key JSON text after applying the shared diagnostic sanitizer. ASCII
    escaping preserves Unicode and surrogateescaped filename text without sending lone surrogates to
    a terminal encoder. Use two-space indentation and no appended newline; callers normally print
    the result. Sanitizer truncation remains lossy, and json.dumps retains its default allowance for
    NaN and infinities. Conversion/encoding failures propagate when the sanitizer does not handle
    them.

    Example:
        >>> json.loads(dump_json({"payload": b"ok"}))["payload"]["size"]
        2
        >>> rendered = dump_json({"name": "café"})
        >>> rendered.isascii(), json.loads(rendered)["name"]
        (True, 'café')


    :param data: Object to sanitize and encode for display, not for lossless persistence.
    :return: ASCII-only JSON-formatted text with sorted keys and two-space indentation.
    """
    # ASCII escaping keeps example output valid even when a POSIX filename was
    # decoded with ``surrogateescape``. JSON readers reconstruct the exact
    # Python string while terminal encoders never see a lone surrogate.
    return json.dumps(json_sanitize(data), ensure_ascii=True, indent=2, sort_keys=True)
