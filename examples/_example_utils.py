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
    # ASCII escaping keeps example output valid even when a POSIX filename was
    # decoded with ``surrogateescape``. JSON readers reconstruct the exact
    # Python string while terminal encoders never see a lone surrogate.
    return json.dumps(json_sanitize(data), ensure_ascii=True, indent=2, sort_keys=True)
