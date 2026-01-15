"""
LiuXin "six"-like compatibility helpers (Python 3 only, typed, stdlib-only).

Why this exists:
- LiuXin still contains a fair amount of Python-2-era idioms (unicode/long,
  iteritems, etc.) even though the project is moving to Python 3.
- Historically this module wrapped `six` and a few `future` helpers.
- This rewrite keeps the same public surface area but removes third-party
  runtime dependencies and improves typing.

Design goals:
- Drop-in-ish replacement for the previous liuxin_six wrapper.
- Provide well-typed helpers for str/bytes conversions.
- Avoid surprising behaviour; fail loudly on unsupported input types.
"""

from __future__ import annotations

import builtins as __builtins__  # keep old attribute name available
import io
import pickle as _pickle
import urllib.parse as _urllib_parse
from typing import (
    Any,
    Callable,
    Iterator,
    Mapping,
    Tuple,
    TypeVar,
    Union,
    overload,
    cast,
)

# --------------------------------------------------------------------------------------
# Version flags (Python 3 only)
# --------------------------------------------------------------------------------------

PY2: bool = False
PY3: bool = True

# --------------------------------------------------------------------------------------
# Common "six"/"past" type aliases used by legacy code
# --------------------------------------------------------------------------------------

text_type = str
binary_type = bytes
string_types = (str,)
integer_types = (int,)

# Names used widely across old LiuXin / calibre-era code
six_unicode = str
six_unichar = chr
six_long = int

six_string_types = string_types
force_unicode = six_unicode  # legacy alias used for isinstance checks

# Convenience aliases sometimes imported as if they were builtins in Py2
basestring = str  # noqa: A001 (intentional legacy name)
unicode = str      # noqa: A001 (intentional legacy name)
long = int         # noqa: A001 (intentional legacy name)
unichr = chr       # noqa: A001 (intentional legacy name)

# --------------------------------------------------------------------------------------
# Dict iteration helpers (Py2-style names)
# --------------------------------------------------------------------------------------

K = TypeVar("K")
V = TypeVar("V")


def iteritems(target_dict: Mapping[K, V]) -> Iterator[Tuple[K, V]]:
    """Return an iterator over (key, value) pairs (Py2 compat)."""
    return iter(target_dict.items())


def iterkeys(target_dict: Mapping[K, Any]) -> Iterator[K]:
    """Return an iterator over keys (Py2 compat)."""
    return iter(target_dict.keys())


def itervalues(target_dict: Mapping[Any, V]) -> Iterator[V]:
    """Return an iterator over values (Py2 compat)."""
    return iter(target_dict.values())


# Historic wrappers used throughout LiuXin ("dict_iteritems as iteritems", etc.)
def dict_iteritems(target_dict: Mapping[K, V]) -> Iterator[Tuple[K, V]]:
    return iteritems(target_dict)


def dict_iterkeys(target_dict: Mapping[K, Any]) -> Iterator[K]:
    return iterkeys(target_dict)


def dict_itervalues(target_dict: Mapping[Any, V]) -> Iterator[V]:
    return itervalues(target_dict)


# --------------------------------------------------------------------------------------
# cmp() helper
# --------------------------------------------------------------------------------------

def force_cmp(x: Any, y: Any) -> int:
    """
    Python 3 replacement for the Python 2 built-in cmp(x, y).
    Returns: -1 if x<y, 0 if x==y, +1 if x>y
    """
    return (x > y) - (x < y)


six_cmp = force_cmp
cmp = force_cmp  # noqa: A001 (intentional legacy name)

# --------------------------------------------------------------------------------------
# Range / buffer / pickle / input / functional builtins
# --------------------------------------------------------------------------------------

memory_range = range

six_pickle = _pickle

# "buffer" doesn't exist on Py3; memoryview is the closest equivalent.
six_buffer = memoryview

# Input helpers
user_input = __builtins__.input
six_input = __builtins__.input

# Functional builtins (already lazy in Py3)
six_map = map
six_zip = zip
six_filter = filter

# --------------------------------------------------------------------------------------
# StringIO helpers (Py2 naming; Py3 uses io.StringIO)
# --------------------------------------------------------------------------------------

six_cStringIO = io.StringIO
six_basic_StringIO = io.StringIO
six_BytesIO = io.BytesIO

# --------------------------------------------------------------------------------------
# LZMA (stdlib when available; optional backport; otherwise raise on use)
# --------------------------------------------------------------------------------------

class _MissingModuleProxy:
    def __init__(self, module_name: str) -> None:
        self._module_name = module_name

    def __getattr__(self, item: str) -> Any:
        raise ImportError(
            f"Optional module '{self._module_name}' is not available; "
            f"tried stdlib and backports."
        )


try:
    import lzma as six_lzma  # type: ignore
except Exception:
    try:
        from backports import lzma as six_lzma  # type: ignore
    except Exception:
        six_lzma = cast(Any, _MissingModuleProxy("lzma"))

# --------------------------------------------------------------------------------------
# urllib.parse helpers (Py2 names preserved)
# --------------------------------------------------------------------------------------

six_urlparse = _urllib_parse.urlparse
six_unquote = _urllib_parse.unquote
six_urldefrag = _urllib_parse.urldefrag
six_urlunparse = _urllib_parse.urlunparse
six_urljoin = _urllib_parse.urljoin

# --------------------------------------------------------------------------------------
# Safer text/bytes conversion helpers (typed)
# --------------------------------------------------------------------------------------

BytesLike = Union[bytes, bytearray, memoryview]
TextOrBytes = Union[str, BytesLike]


@overload
def ensure_bytes(
    s: None, encoding: str = "utf-8", errors: str = "strict"
) -> None: ...
@overload
def ensure_bytes(
    s: bytes, encoding: str = "utf-8", errors: str = "strict"
) -> bytes: ...
@overload
def ensure_bytes(
    s: bytearray, encoding: str = "utf-8", errors: str = "strict"
) -> bytes: ...
@overload
def ensure_bytes(
    s: memoryview, encoding: str = "utf-8", errors: str = "strict"
) -> bytes: ...
@overload
def ensure_bytes(
    s: str, encoding: str = "utf-8", errors: str = "strict"
) -> bytes: ...


def ensure_bytes(
    s: Any, encoding: str = "utf-8", errors: str = "strict"
) -> Any:
    """
    Convert text/bytes-ish inputs to bytes.
    - str -> encoded
    - bytes -> unchanged
    - bytearray/memoryview -> copied to bytes
    - None -> None
    """
    if s is None:
        return None
    if isinstance(s, bytes):
        return s
    if isinstance(s, bytearray):
        return bytes(s)
    if isinstance(s, memoryview):
        return s.tobytes()
    if isinstance(s, str):
        return s.encode(encoding, errors)
    raise TypeError(f"ensure_bytes() expected str/bytes/bytearray/memoryview/None, got {type(s)!r}")


@overload
def ensure_text(
    s: None, encoding: str = "utf-8", errors: str = "strict"
) -> None: ...
@overload
def ensure_text(
    s: str, encoding: str = "utf-8", errors: str = "strict"
) -> str: ...
@overload
def ensure_text(
    s: bytes, encoding: str = "utf-8", errors: str = "strict"
) -> str: ...
@overload
def ensure_text(
    s: bytearray, encoding: str = "utf-8", errors: str = "strict"
) -> str: ...
@overload
def ensure_text(
    s: memoryview, encoding: str = "utf-8", errors: str = "strict"
) -> str: ...


def ensure_text(
    s: Any, encoding: str = "utf-8", errors: str = "strict"
) -> Any:
    """
    Convert text/bytes-ish inputs to str.
    - bytes/bytearray/memoryview -> decoded
    - str -> unchanged
    - None -> None
    """
    if s is None:
        return None
    if isinstance(s, str):
        return s
    if isinstance(s, (bytes, bytearray)):
        return bytes(s).decode(encoding, errors)
    if isinstance(s, memoryview):
        return s.tobytes().decode(encoding, errors)
    raise TypeError(f"ensure_text() expected str/bytes/bytearray/memoryview/None, got {type(s)!r}")


# In Py3, "ensure_str" is effectively the same as ensure_text.
ensure_str = ensure_text

# Small helpers mirroring common compatibility patterns
def b(s: Union[str, bytes], encoding: str = "latin-1", errors: str = "strict") -> bytes:
    """
    Create bytes from a string in a stable, byte-preserving way.
    Default encoding is latin-1 so codepoints 0..255 map 1:1 to bytes.
    """
    return ensure_bytes(s, encoding=encoding, errors=errors)


def u(s: Union[str, bytes], encoding: str = "utf-8", errors: str = "strict") -> str:
    """Create text (str) from bytes or return str unchanged."""
    return ensure_text(s, encoding=encoding, errors=errors)


# --------------------------------------------------------------------------------------
# Metaclass helpers (commonly provided by six)
# --------------------------------------------------------------------------------------

def add_metaclass(metaclass: type) -> Callable[[type], type]:
    """
    Class decorator that replaces a class with the same name/bases but using
    the given metaclass. Similar to six.add_metaclass.
    """
    def decorator(cls: type) -> type:
        attrs = dict(cls.__dict__)
        # these are created by type machinery, don't pass through
        attrs.pop("__dict__", None)
        attrs.pop("__weakref__", None)
        return metaclass(cls.__name__, cls.__bases__, attrs)

    return decorator


def with_metaclass(metaclass: type, *bases: type) -> type:
    """
    Create a base class using `metaclass` and `bases`.
    Typical use:
        class MyBase(with_metaclass(Meta, object)):
            ...
    """
    return metaclass("_WithMetaclassBase", bases or (object,), {})


__all__ = [
    # flags
    "PY2", "PY3",
    # types
    "text_type", "binary_type", "string_types", "integer_types",
    "six_unicode", "six_unichar", "six_long", "six_string_types", "force_unicode",
    "basestring", "unicode", "long", "unichr",
    # dict iteration
    "iteritems", "iterkeys", "itervalues",
    "dict_iteritems", "dict_iterkeys", "dict_itervalues",
    # cmp
    "force_cmp", "six_cmp", "cmp",
    # builtins / stdlib shims
    "memory_range", "six_pickle", "six_buffer",
    "user_input", "six_input",
    "six_map", "six_zip", "six_filter",
    "six_cStringIO", "six_basic_StringIO", "six_BytesIO",
    "six_lzma",
    # urllib helpers
    "six_urlparse", "six_unquote", "six_urldefrag", "six_urlunparse", "six_urljoin",
    # conversions
    "ensure_bytes", "ensure_text", "ensure_str", "b", "u",
    # metaclass helpers
    "add_metaclass", "with_metaclass",
]
