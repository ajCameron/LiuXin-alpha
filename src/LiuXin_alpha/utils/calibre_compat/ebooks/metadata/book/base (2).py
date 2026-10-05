"""
Retain the broader legacy Calibre metadata container and field registry compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base (2) through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
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
    """
    Very small stand-in for calibre's FieldMetadata.

    Example:
        Exercise FieldMetadata through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    pass


def _now_utc() -> _dt.datetime:
    """
    Perform the now utc utility operation under explicit compatibility rules.

    Example:
        Exercise  now utc through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Clean identifier key under the documented compatibility and safety rules.

    Example:
        Exercise  clean identifier key through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :param typ: Value supplied for typ under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Calibre-compatible metadata container (subset).

    Example:
        Exercise Metadata through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """

    def __init__(
        self,
        title: Optional[str],
        authors: Iterable[str] = ("Unknown",),
        other: Optional[object] = None,
        template_cache: Any = None,
        formatter: Any = None,
    ):
        """
        Initialize and validate the Metadata state.

        Example:
            Exercise Metadata.  init   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param title: Value supplied for title under the utility contract.
        :param authors: Value supplied for authors under the utility contract.
        :param other: Value supplied for other under the utility contract.
        :param template_cache: Value supplied for template cache under the utility contract.
        :param formatter: Template formatter supplying evaluation services and context.
        :return: None; validated state is stored on the receiving object.
        """
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
        """
        Return or update whether is null holds for the compatibility value.

        Example:
            Exercise Metadata.is null through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :return: True when the documented condition holds; otherwise False.
        """
        d = object.__getattribute__(self, "_data")
        if field in TOP_LEVEL_IDENTIFIERS:
            return not bool(d["identifiers"].get(field))
        if field == "language":
            return not bool(d.get("languages") or [])
        if field not in d:
            return True
        return d[field] == NULL_VALUES.get(field)

    def set_null(self, field: str) -> None:
        """
        Set null under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.set null through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Expose getattribute behavior for the compatibility container.

        Example:
            Exercise Metadata.  getattribute   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Expose setattr behavior for the compatibility container.

        Example:
            Exercise Metadata.  setattr   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :param extra: Value supplied for extra under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
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
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise Metadata.  iter   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        return iter(d.get("user_metadata", {}))

    def has_key(self, field: str) -> bool:
        """
        Return or update whether has key holds for the compatibility value.

        Example:
            Exercise Metadata.has key through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :return: True when the documented condition holds; otherwise False.
        """
        return field in self.all_field_keys()

    def get(self, field: str, default: Any = None) -> Any:
        """
        Perform the get utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.get through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return getattr(self, field)
        except AttributeError:
            return default

    def get_extra(self, field: str, default: Any = None) -> Any:
        """
        Return extra under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.get extra through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        um = d.get("user_metadata", {})
        if field in um:
            return um[field].get("#extra#", default)
        raise AttributeError(f"Metadata object has no attribute named: {field!r}")

    def set(self, field: str, val: Any, extra: Any = None) -> None:
        """
        Perform the set utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.set through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :param extra: Value supplied for extra under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__setattr__(field, val, extra)

    def get_identifiers(self) -> Dict[str, str]:
        """
        Return identifiers under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.get identifiers through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        return dict(d.get("identifiers", {}) or {})

    def set_identifiers(self, identifiers: Optional[Mapping[str, Any]]) -> None:
        """
        Set identifiers under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.set identifiers through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param identifiers: Value supplied for identifiers under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Set identifier under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.set identifier through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param typ: Value supplied for typ under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        d = object.__getattribute__(self, "_data")
        typ = _clean_identifier_key(typ)
        if not typ:
            return
        if val is None or val == "":
            d["identifiers"].pop(typ, None)
        else:
            d["identifiers"][typ] = str(val)

    def has_identifier(self, typ: str) -> bool:
        """
        Return or update whether has identifier holds for the compatibility value.

        Example:
            Exercise Metadata.has identifier through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param typ: Value supplied for typ under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        typ = _clean_identifier_key(typ)
        return bool(object.__getattribute__(self, "_data")["identifiers"].get(typ))

    def standard_field_keys(self) -> frozenset[str]:
        """
        Perform the standard field keys utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.standard field keys through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return frozenset(STANDARD_METADATA_FIELDS)

    def custom_field_keys(self) -> Iterator[str]:
        """
        Perform the custom field keys utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.custom field keys through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        return iter(d.get("user_metadata", {}))

    def all_field_keys(self) -> frozenset[str]:
        """
        Perform the all field keys utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.all field keys through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        return frozenset(STANDARD_METADATA_FIELDS.union(frozenset(d.get("user_metadata", {}).keys())))

    def all_non_none_fields(self) -> Dict[str, Any]:
        """
        Perform the all non none fields utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.all non none fields through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the metadata for field utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.metadata for field through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        if key in d.get("user_metadata", {}):
            um = d["user_metadata"][key].copy()
            um.pop("#value#", None)
            um.pop("#extra#", None)
            return um
        return field_metadata.get(key)

    def get_standard_metadata(self, field: str, make_copy: bool) -> Optional[Dict[str, Any]]:
        """
        Return standard metadata under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.get standard metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :param make_copy: Value supplied for make copy under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if field in field_metadata and field_metadata[field].get("kind") == "field":
            return copy.deepcopy(field_metadata[field]) if make_copy else field_metadata[field]
        return None

    def get_all_standard_metadata(self, make_copy: bool) -> Mapping[str, Dict[str, Any]]:
        """
        Return all standard metadata under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.get all standard metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param make_copy: Value supplied for make copy under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not make_copy:
            return field_metadata
        return {k: copy.deepcopy(v) for k, v in field_metadata.items() if v.get("kind") == "field"}

    def get_all_user_metadata(self, make_copy: bool) -> Dict[str, Dict[str, Any]]:
        """
        Return all user metadata under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.get all user metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param make_copy: Value supplied for make copy under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        um = d.get("user_metadata", {})
        return copy.deepcopy(um) if make_copy else um

    def get_user_metadata(self, field: str, make_copy: bool) -> Optional[Dict[str, Any]]:
        """
        Return user metadata under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.get user metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :param make_copy: Value supplied for make copy under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        d = object.__getattribute__(self, "_data")
        um = d.get("user_metadata", {})
        if field not in um:
            return None
        return copy.deepcopy(um[field]) if make_copy else um[field]

    def set_all_user_metadata(self, um: Mapping[str, Mapping[str, Any]]) -> None:
        """
        Set all user metadata under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.set all user metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param um: Value supplied for um under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Set user metadata under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.set user metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param field: Metadata or template field addressed by the operation.
        :param meta: Value supplied for meta under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Remove stale user metadata under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.remove stale user metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param custom_fields: Value supplied for custom fields under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        return

    def deepcopy(self, class_generator=lambda: None) -> "Metadata":
        """
        Perform the deepcopy utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.deepcopy through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param class_generator: Value supplied for class generator under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the deepcopy metadata utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.deepcopy metadata through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.deepcopy(class_generator=lambda: self.__class__(None))

    def smart_update(self, other: object) -> None:
        """
        Perform the smart update utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.smart update through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param other: Value supplied for other under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Format series index under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.format series index through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            f = float(val)
        except Exception:
            return ""
        if f.is_integer():
            return str(int(f))
        return f"{f:g}"

    def authors_from_string(self, raw: str) -> None:
        """
        Split a Calibre author string into normalized author names.

        Example:
            Exercise Metadata.authors from string through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.authors = string_to_authors(raw)

    def format_authors(self) -> Tuple[str, str]:
        """
        Join author names using Calibre-compatible display rules.

        Example:
            Exercise Metadata.format authors through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        authors = self.authors or []
        return ("Authors", authors_to_string(list(authors)))

    def format_tags(self) -> Tuple[str, str]:
        """
        Join tag values using Calibre-compatible display rules.

        Example:
            Exercise Metadata.format tags through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tags = self.tags or []
        return ("Tags", ", ".join(map(str, tags)))

    def format_rating(self) -> Tuple[str, str]:
        """
        Render a numeric rating in the requested compatibility form.

        Example:
            Exercise Metadata.format rating through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        r = self.rating
        return ("Rating", "" if r is None else str(r))

    def format_field(self, key: str) -> Tuple[str, str]:
        """
        Format field under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.format field through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Format field extended under the documented compatibility and safety rules.

        Example:
            Exercise Metadata.format field extended through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        name, val = self.format_field(key)
        return (name, val, "")

    def template_to_attribute(self, template: str) -> Optional[str]:
        """
        Perform the template to attribute utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.template to attribute through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param template: Template expression parsed or evaluated.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the print all attributes utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.print all attributes through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for k in sorted(self.all_field_keys()):
            v = self.get(k, None)
            print(f"{k}: {v!r}")

    def __unicode__representation__(self) -> str:
        """
        Perform the unicode representation utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.  unicode  representation   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        title = self.title or ""
        authors = self.authors or []
        a = authors_to_string(list(authors)) if authors else ""
        return f"{title} [{a}]" if a else title

    def to_html(self) -> str:
        """
        Perform the to html utility operation under explicit compatibility rules.

        Example:
            Exercise Metadata.to html through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Expose nonzero behavior for the compatibility container.

        Example:
            Exercise Metadata.  nonzero   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: True when the documented condition holds; otherwise False.
        """
        return bool(self.title or (self.authors and any(self.authors)) or self.comments or (self.tags and any(self.tags)))

    def __bool__(self) -> bool:
        """
        Expose bool behavior for the compatibility container.

        Example:
            Exercise Metadata.  bool   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: True when the documented condition holds; otherwise False.
        """
        return self.__nonzero__()

    __str__ = __unicode__representation__


class MetaInformation(Metadata):
    """
    Backwards-compatible alias for calibre's historic :class:`MetaInformation`.

    Example:
        Exercise MetaInformation through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """

    pass
