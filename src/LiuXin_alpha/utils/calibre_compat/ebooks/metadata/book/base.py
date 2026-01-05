"""A minimal, calibre-like :class:`Metadata` object for LiuXin.

Many calibre plugins use:
    from calibre.ebooks.metadata.book.base import Metadata

This implementation focuses on API compatibility, not full parity.
"""

from __future__ import annotations

import copy
import datetime as _dt
import re
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Tuple

from . import TOP_LEVEL_IDENTIFIERS
from .. import string_to_authors, authors_to_string

try:
    # Reuse LiuXin's normalization if available; fall back to a conservative cleaner.
    from LiuXin_alpha.metadata.standardize import standardize_id_name as _standardize_id_name  # type: ignore
except Exception:  # pragma: no cover
    _standardize_id_name = None


class FieldMetadata(dict):
    """Very small stand-in for calibre's FieldMetadata."""
    pass


def _now_utc() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.timezone.utc)


# Pragmatic NULL_VALUES for plugin expectations.
NULL_VALUES: Dict[str, Any] = {
    "title": None,
    "title_sort": None,
    "authors": [],
    "author_sort": None,
    "creator_sort": None,
    "tags": [],
    "publisher": None,
    "pubdate": None,       # datetime
    "timestamp": None,     # datetime
    "comments": None,      # str (HTML often)
    "series": None,
    "series_index": None,  # float
    "rating": None,        # int 0-10
    "languages": [],       # list[str]
    "identifiers": {},     # dict[str,str]
    "cover": None,         # path
    "cover_data": None,    # (fmt, bytes)
    "uuid": None,
    "rights": None,
    "publication_type": None,
    "user_metadata": {},   # dict[field_key -> calibre-style dict]
}

STANDARD_METADATA_FIELDS = frozenset(NULL_VALUES.keys())
SIMPLE_GET = frozenset(STANDARD_METADATA_FIELDS - TOP_LEVEL_IDENTIFIERS)
SIMPLE_SET = frozenset(SIMPLE_GET - {"identifiers", "user_metadata"})

field_metadata = FieldMetadata({
    k: {"kind": "field", "name": k, "datatype": "text", "is_custom": False}
    for k in STANDARD_METADATA_FIELDS
    if k not in {"user_metadata"}
})
field_metadata["identifiers"] = {
    "kind": "field",
    "name": "identifiers",
    "datatype": "identifiers",
    "is_custom": False,
}

_TEMPLATE_ATTR_RE = re.compile(r"\{\s*([a-zA-Z0-9_]+)\s*\}")


def _clean_identifier_key(typ: str) -> str:
    typ = (typ or "").strip()
    if not typ:
        return ""
    if _standardize_id_name is not None:
        try:
            norm = _standardize_id_name(typ)
            if norm:
                return str(norm)
        except Exception:
            pass
    return typ.lower().replace(":", "").replace(",", "").strip()


class Metadata:
    """Calibre-compatible metadata container (subset)."""

    def __init__(
        self,
        title: Optional[str],
        authors: Iterable[str] = ("Unknown",),
        other: Optional[object] = None,
        template_cache: Any = None,
        formatter: Any = None,
    ):
        _data = copy.deepcopy(NULL_VALUES)
        _data["user_metadata"] = {}
        object.__setattr__(self, "_data", _data)

        if other is not None:
            self.smart_update(other)
        else:
            if title:
                self.title = title
            if authors is not None:
                self.authors = list(authors)

        if self.timestamp is None:
            self.timestamp = _now_utc()

    def is_null(self, field: str) -> bool:
        d = object.__getattribute__(self, "_data")
        if field in TOP_LEVEL_IDENTIFIERS:
            return not bool(d["identifiers"].get(field))
        if field == "language":
            return not bool(d.get("languages") or [])
        if field not in d:
            return True
        return d[field] == NULL_VALUES.get(field)

    def set_null(self, field: str) -> None:
        d = object.__getattribute__(self, "_data")
        if field in TOP_LEVEL_IDENTIFIERS:
            d["identifiers"].pop(field, None)
            return
        if field == "language":
            d["languages"] = []
            return
        if field in d:
            d[field] = copy.deepcopy(NULL_VALUES.get(field))
            return
        raise AttributeError(f"Metadata object has no field named: {field!r}")

    def __getattribute__(self, field: str) -> Any:
        if field in {"_data", "__dict__", "__class__"}:
            return object.__getattribute__(self, field)

        d = object.__getattribute__(self, "_data")

        if field in SIMPLE_GET:
            return d.get(field, None)

        if field in TOP_LEVEL_IDENTIFIERS:
            return d.get("identifiers", {}).get(field, None)

        if field == "language":
            langs = d.get("languages", [])
            return langs[0] if langs else "und"

        um = d.get("user_metadata", {})
        if field in um:
            return um[field].get("#value#")

        return object.__getattribute__(self, field)

    def __setattr__(self, field: str, val: Any, extra: Any = None) -> None:
        d = object.__getattribute__(self, "_data")

        if field in SIMPLE_SET:
            d[field] = val
            return

        if field in TOP_LEVEL_IDENTIFIERS:
            self.set_identifier(field, val)
            return

        if field == "identifiers":
            self.set_identifiers(val)
            return

        if field == "language":
            if val and str(val).lower() != "und":
                d["languages"] = [str(val)]
            else:
                d["languages"] = []
            return

        if field == "user_metadata":
            self.set_all_user_metadata(val or {})
            return

        if field in d.get("user_metadata", {}):
            d["user_metadata"][field]["#value#"] = val
            d["user_metadata"][field]["#extra#"] = extra
            return

        self.__dict__[field] = val

    def __iter__(self) -> Iterator[str]:
        d = object.__getattribute__(self, "_data")
        return iter(d.get("user_metadata", {}))

    def has_key(self, field: str) -> bool:
        return field in self.all_field_keys()

    def get(self, field: str, default: Any = None) -> Any:
        try:
            return getattr(self, field)
        except AttributeError:
            return default

    def get_extra(self, field: str, default: Any = None) -> Any:
        d = object.__getattribute__(self, "_data")
        um = d.get("user_metadata", {})
        if field in um:
            return um[field].get("#extra#", default)
        raise AttributeError(f"Metadata object has no attribute named: {field!r}")

    def set(self, field: str, val: Any, extra: Any = None) -> None:
        self.__setattr__(field, val, extra)

    def get_identifiers(self) -> Dict[str, str]:
        d = object.__getattribute__(self, "_data")
        return dict(d.get("identifiers", {}) or {})

    def set_identifiers(self, identifiers: Optional[Mapping[str, Any]]) -> None:
        d = object.__getattribute__(self, "_data")
        cleaned: Dict[str, str] = {}
        if identifiers:
            for k, v in dict(identifiers).items():
                ck = _clean_identifier_key(str(k))
                if not ck or v is None:
                    continue
                cleaned[ck] = str(v)
        d["identifiers"] = cleaned

    def set_identifier(self, typ: str, val: Any) -> None:
        d = object.__getattribute__(self, "_data")
        typ = _clean_identifier_key(typ)
        if not typ:
            return
        if val is None or val == "":
            d["identifiers"].pop(typ, None)
        else:
            d["identifiers"][typ] = str(val)

    def has_identifier(self, typ: str) -> bool:
        typ = _clean_identifier_key(typ)
        return bool(object.__getattribute__(self, "_data")["identifiers"].get(typ))

    def standard_field_keys(self) -> frozenset[str]:
        return frozenset(STANDARD_METADATA_FIELDS)

    def custom_field_keys(self) -> Iterator[str]:
        d = object.__getattribute__(self, "_data")
        return iter(d.get("user_metadata", {}))

    def all_field_keys(self) -> frozenset[str]:
        d = object.__getattribute__(self, "_data")
        return frozenset(STANDARD_METADATA_FIELDS.union(frozenset(d.get("user_metadata", {}).keys())))

    def all_non_none_fields(self) -> Dict[str, Any]:
        d = object.__getattribute__(self, "_data")
        ans: Dict[str, Any] = {}
        for k in self.all_field_keys():
            v = d.get(k) if k in d else self.get(k, None)
            if v is None:
                continue
            if isinstance(v, (list, dict, tuple, set)) and len(v) == 0:
                continue
            ans[k] = v
        return ans

    def metadata_for_field(self, key: str) -> Optional[Dict[str, Any]]:
        d = object.__getattribute__(self, "_data")
        if key in d.get("user_metadata", {}):
            um = d["user_metadata"][key].copy()
            um.pop("#value#", None)
            um.pop("#extra#", None)
            return um
        return field_metadata.get(key)

    def get_standard_metadata(self, field: str, make_copy: bool) -> Optional[Dict[str, Any]]:
        if field in field_metadata and field_metadata[field].get("kind") == "field":
            return copy.deepcopy(field_metadata[field]) if make_copy else field_metadata[field]
        return None

    def get_all_standard_metadata(self, make_copy: bool) -> Mapping[str, Dict[str, Any]]:
        if not make_copy:
            return field_metadata
        return {k: copy.deepcopy(v) for k, v in field_metadata.items() if v.get("kind") == "field"}

    def get_all_user_metadata(self, make_copy: bool) -> Dict[str, Dict[str, Any]]:
        d = object.__getattribute__(self, "_data")
        um = d.get("user_metadata", {})
        return copy.deepcopy(um) if make_copy else um

    def get_user_metadata(self, field: str, make_copy: bool) -> Optional[Dict[str, Any]]:
        d = object.__getattribute__(self, "_data")
        um = d.get("user_metadata", {})
        if field not in um:
            return None
        return copy.deepcopy(um[field]) if make_copy else um[field]

    def set_all_user_metadata(self, um: Mapping[str, Mapping[str, Any]]) -> None:
        d = object.__getattribute__(self, "_data")
        out: Dict[str, Dict[str, Any]] = {}
        for k, meta in dict(um).items():
            meta = dict(meta)
            meta.setdefault("datatype", meta.get("datatype", "text"))
            meta.setdefault("name", meta.get("name", k))
            meta.setdefault("is_multiple", bool(meta.get("is_multiple", False)))
            meta.setdefault("kind", "field")
            meta.setdefault("#value#", meta.get("#value#"))
            meta.setdefault("#extra#", meta.get("#extra#"))
            out[k] = meta
        d["user_metadata"] = out

    def set_user_metadata(self, field: str, meta: Mapping[str, Any]) -> None:
        d = object.__getattribute__(self, "_data")
        meta = dict(meta)
        meta.setdefault("datatype", meta.get("datatype", "text"))
        meta.setdefault("name", meta.get("name", field))
        meta.setdefault("is_multiple", bool(meta.get("is_multiple", False)))
        meta.setdefault("kind", "field")
        meta.setdefault("#value#", meta.get("#value#"))
        meta.setdefault("#extra#", meta.get("#extra#"))
        d.setdefault("user_metadata", {})[field] = meta

    def remove_stale_user_metadata(self, custom_fields: Iterable[str] = ()) -> None:
        # No-op in the shim; calibre uses this when schema changes.
        return

    def deepcopy(self, class_generator=lambda: None) -> "Metadata":
        clone = None
        if class_generator is not None:
            try:
                clone = class_generator()
            except Exception:
                clone = None
        if clone is None:
            clone = Metadata(None)
        object.__setattr__(clone, "_data", copy.deepcopy(object.__getattribute__(self, "_data")))
        clone.__dict__.update({k: copy.deepcopy(v) for k, v in self.__dict__.items() if k != "_data"})
        return clone

    def deepcopy_metadata(self) -> "Metadata":
        return self.deepcopy(class_generator=lambda: self.__class__(None))

    def smart_update(self, other: object) -> None:
        for k in STANDARD_METADATA_FIELDS:
            if k in {"user_metadata"}:
                continue
            try:
                other_val = getattr(other, k)
            except Exception:
                continue
            if other_val is None:
                continue
            if self.is_null(k):
                setattr(self, k, copy.deepcopy(other_val))

        try:
            other_ids = getattr(other, "identifiers")
        except Exception:
            other_ids = None
        if other_ids:
            cur = dict(self.get_identifiers())
            for ik, iv in dict(other_ids).items():
                if iv is None:
                    continue
                if ik not in cur or cur[ik] in (None, ""):
                    cur[ik] = str(iv)
            self.set_identifiers(cur)

        try:
            other_langs = getattr(other, "languages")
        except Exception:
            other_langs = None
        if other_langs:
            ol = list(other_langs)
            if ol and ol != ["und"]:
                self.languages = ol

        if not getattr(self, "series", None):
            self.series_index = None

    def format_series_index(self, val: Any) -> str:
        try:
            f = float(val)
        except Exception:
            return ""
        if f.is_integer():
            return str(int(f))
        return f"{f:g}"

    def authors_from_string(self, raw: str) -> None:
        self.authors = string_to_authors(raw)

    def format_authors(self) -> Tuple[str, str]:
        authors = self.authors or []
        return ("Authors", authors_to_string(list(authors)))

    def format_tags(self) -> Tuple[str, str]:
        tags = self.tags or []
        return ("Tags", ", ".join(map(str, tags)))

    def format_rating(self) -> Tuple[str, str]:
        r = self.rating
        return ("Rating", "" if r is None else str(r))

    def format_field(self, key: str) -> Tuple[str, str]:
        meta = self.metadata_for_field(key) or {}
        name = meta.get("name", key)
        val = self.get(key, None)
        if val is None:
            return (name, "")
        if isinstance(val, list):
            return (name, ", ".join(map(str, val)))
        if isinstance(val, dict):
            return (name, ", ".join(f"{k}:{v}" for k, v in val.items()))
        if key == "series_index":
            return (name, self.format_series_index(val))
        return (name, str(val))

    def format_field_extended(self, key: str) -> Tuple[str, str, str]:
        name, val = self.format_field(key)
        return (name, val, "")

    def template_to_attribute(self, template: str) -> Optional[str]:
        if not template:
            return None
        m = _TEMPLATE_ATTR_RE.search(template)
        if m:
            return m.group(1)
        t = template.strip()
        if t in self.all_field_keys():
            return t
        return None

    def print_all_attributes(self) -> None:
        for k in sorted(self.all_field_keys()):
            v = self.get(k, None)
            print(f"{k}: {v!r}")

    def __unicode__representation__(self) -> str:
        title = self.title or ""
        authors = self.authors or []
        a = authors_to_string(list(authors)) if authors else ""
        return f"{title} [{a}]" if a else title

    def to_html(self) -> str:
        rows = []
        for k in ("title", "authors", "tags", "publisher", "pubdate", "series", "series_index", "languages", "identifiers"):
            name, val = self.format_field(k)
            if val:
                rows.append(f"<tr><td><b>{name}</b></td><td>{val}</td></tr>")
        for key in self.custom_field_keys():
            name, val = self.format_field(key)
            if val:
                rows.append(f"<tr><td><b>{name}</b></td><td>{val}</td></tr>")
        return "<table>" + "\n".join(rows) + "</table>"

    def __nonzero__(self) -> bool:
        return bool(self.title or (self.authors and any(self.authors)) or self.comments or (self.tags and any(self.tags)))

    def __bool__(self) -> bool:
        return self.__nonzero__()

    __str__ = __unicode__representation__
