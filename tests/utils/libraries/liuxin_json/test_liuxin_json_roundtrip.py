"""
Provide test liuxin json roundtrip utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test liuxin json roundtrip through a consuming regression::

        python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py
"""

from __future__ import annotations

import pytest


@pytest.fixture()
def lxjson():
    """
    Perform the lxjson utility operation under explicit compatibility rules.

    Example:
        Exercise lxjson through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

    return LiuXinJSON()


def _assert_no_bytes(obj):
    """
    Recursively assert the object tree contains no `bytes`.

    Example:
        Exercise  assert no bytes through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py


    :param obj: Value supplied for obj under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test roundtrip simple dict utility operation under explicit compatibility rules.

    Example:
        Exercise test roundtrip simple dict through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py


    :param lxjson: Value supplied for lxjson under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    obj = {"a": "b", "num": 3, "flag": True, "none": None, "list": [1, "two", False]}
    s = lxjson.dumps(obj)
    out = lxjson.loads(s)
    assert out == obj
    _assert_no_bytes(out)


def test_roundtrip_unicode_and_control_chars(lxjson):
    # Includes: unicode, newline, tab, and an embedded NUL.
    """
    Perform the test roundtrip unicode and control chars utility operation under explicit compatibility rules.

    Example:
        Exercise test roundtrip unicode and control chars through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py


    :param lxjson: Value supplied for lxjson under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    text = "café ☃\n\tNUL:\x00:end"
    obj = {"k": text, "nested": [text, {"inner": text}]}

    s = lxjson.dumps(obj)
    out = lxjson.loads(s)

    assert out == obj
    _assert_no_bytes(out)


def test_dumps_accepts_bytes_and_never_emits_bytes_keys_on_load(lxjson):
    # Preferences historically had dict keys and values as bytes (py2 legacy).
    """
    Perform the test dumps accepts bytes and never emits bytes keys on load utility operation under explicit compatibility rules.

    Example:
        Exercise test dumps accepts bytes and never emits bytes keys on load through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py


    :param lxjson: Value supplied for lxjson under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test multiple instances dont break roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise test multiple instances dont break roundtrip through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

    a = LiuXinJSON()
    b = LiuXinJSON()

    obj = {"x": "y"}
    assert a.loads(a.dumps(obj)) == obj
    assert b.loads(b.dumps(obj)) == obj


def test_rejects_non_jsonable_types_cleanly(lxjson):
    """
    Perform the test rejects non jsonable types cleanly utility operation under explicit compatibility rules.

    Example:
        Exercise test rejects non jsonable types cleanly through a consuming regression::

            python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py


    :param lxjson: Value supplied for lxjson under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    class X:
        """
        Provide the X utility contract with explicit state and cleanup behavior.

        Example:
            Exercise test rejects non jsonable types cleanly.X through a consuming regression::

                python -m pytest -q tests/utils/libraries/liuxin_json/test_liuxin_json_roundtrip.py
        """
        pass

    with pytest.raises(TypeError):
        lxjson.dumps({"x": X()})
