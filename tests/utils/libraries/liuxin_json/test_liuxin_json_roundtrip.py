"""Tests for LiuXinJSON (base64-wrapped JSON).

These tests guard against a Py3 regression where decoded JSON strings/keys
become `bytes` instead of `str`, which breaks stdlib json semantics and caused
preferences upgrade failures.

Focus:
- dumps() accepts str and bytes (bytes are coerced to str safely)
- loads() returns str keys/values (never bytes)
- round-trip for nested structures and awkward characters
"""

from __future__ import annotations

import pytest


@pytest.fixture()
def lxjson():
    from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

    return LiuXinJSON()


def _assert_no_bytes(obj):
    """Recursively assert the object tree contains no `bytes`."""
    if isinstance(obj, bytes):
        raise AssertionError("found bytes in decoded object")
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, bytes):
                raise AssertionError("found bytes key in decoded object")
            _assert_no_bytes(v)
    elif isinstance(obj, (list, tuple)):
        for x in obj:
            _assert_no_bytes(x)


def test_roundtrip_simple_dict(lxjson):
    obj = {"a": "b", "num": 3, "flag": True, "none": None, "list": [1, "two", False]}
    s = lxjson.dumps(obj)
    out = lxjson.loads(s)
    assert out == obj
    _assert_no_bytes(out)


def test_roundtrip_unicode_and_control_chars(lxjson):
    # Includes: unicode, newline, tab, and an embedded NUL.
    text = "café ☃\n\tNUL:\x00:end"
    obj = {"k": text, "nested": [text, {"inner": text}]}

    s = lxjson.dumps(obj)
    out = lxjson.loads(s)

    assert out == obj
    _assert_no_bytes(out)


def test_dumps_accepts_bytes_and_never_emits_bytes_keys_on_load(lxjson):
    # Preferences historically had dict keys and values as bytes (py2 legacy).
    obj = {
        b"eng": [b"A\\s+", b"The\\s+"],
        b"deu": [b"Der\\s+", b"Die\\s+"],
        b"nested": {b"k": b"v"},
    }

    s = lxjson.dumps(obj)
    out = lxjson.loads(s)

    # Keys/values should be text after load.
    _assert_no_bytes(out)
    assert sorted(out.keys()) == ["deu", "eng", "nested"]
    assert out["eng"] == ["A\\s+", "The\\s+"]
    assert out["nested"] == {"k": "v"}


def test_multiple_instances_dont_break_roundtrip():
    from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

    a = LiuXinJSON()
    b = LiuXinJSON()

    obj = {"x": "y"}
    assert a.loads(a.dumps(obj)) == obj
    assert b.loads(b.dumps(obj)) == obj


def test_rejects_non_jsonable_types_cleanly(lxjson):
    class X:
        pass

    with pytest.raises(TypeError):
        lxjson.dumps({"x": X()})
