
"""
tests/utils/libraries/liuxin_json/test_liuxin_json_isolation.py

Isolation/regression tests for LiuXin's JSON fork.

The primary purpose of these tests is to ensure that importing or instantiating
`LiuXinJSON` never mutates the stdlib `json` module (or its submodules).

Why this matters:
  - pytest (and many other tools) rely on `json` behaving exactly like the
    standard library implementation.
  - accidental monkeypatching tends to surface as bizarre failures in unrelated
    code (e.g. pytest cache reads).
"""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
from pathlib import Path

import pytest


# Ensure the project's src/ is importable even when the package isn't installed.
_ROOT = None
try:
    _ROOT = Path(__file__).resolve()
    for _p in _ROOT.parents:
        if (_p / "src").exists() and (_p / "tests").exists():
            _ROOT = _p
            break
    else:
        _ROOT = Path.cwd().resolve()
except Exception:
    _ROOT = Path.cwd().resolve()

_SRC = str(Path(_ROOT) / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


def _project_root() -> Path:
    """Locate the project root from this test file path."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        # `src/` is a strong indicator of the repo root in this project.
        if (parent / "src").exists() and (parent / "tests").exists():
            return parent
    # Fallback: whatever pytest is running from.
    return Path.cwd().resolve()


def _run_in_subprocess(code: str) -> subprocess.CompletedProcess[str]:
    """Run a Python snippet in a fresh interpreter, ensuring src/ is on PYTHONPATH."""
    root = _project_root()
    env = os.environ.copy()
    src = str(root / "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(root),
        env=env,
        text=True,
        capture_output=True,
    )


def _assert_subprocess_ok(proc: subprocess.CompletedProcess[str]) -> None:
    if proc.returncode != 0:
        raise AssertionError(
            "Subprocess failed:\n"
            f"--- stdout ---\n{proc.stdout}\n"
            f"--- stderr ---\n{proc.stderr}\n"
        )


def test_stdlib_json_not_monkeypatched_by_import() -> None:
    """Importing liuxin_json must not mutate stdlib json globals."""
    code = r"""
import json
import json.decoder
import json.encoder

pre = {
    "default_decoder": id(json._default_decoder),
    "default_decoder_scan_once": id(getattr(json._default_decoder, "scan_once", None)),
    "decoder_scanstring": id(json.decoder.scanstring),
    "encoder_encode_basestring": id(json.encoder.encode_basestring),
    "encoder_encode_basestring_ascii": id(json.encoder.encode_basestring_ascii),
}

import LiuXin_alpha.utils.libraries.liuxin_json  # noqa: F401

post = {
    "default_decoder": id(json._default_decoder),
    "default_decoder_scan_once": id(getattr(json._default_decoder, "scan_once", None)),
    "decoder_scanstring": id(json.decoder.scanstring),
    "encoder_encode_basestring": id(json.encoder.encode_basestring),
    "encoder_encode_basestring_ascii": id(json.encoder.encode_basestring_ascii),
}

assert pre == post, (pre, post)
"""
    proc = _run_in_subprocess(code)
    _assert_subprocess_ok(proc)


def test_stdlib_json_not_monkeypatched_by_instantiation() -> None:
    """Instantiating LiuXinJSON must not mutate stdlib json globals."""
    code = r"""
import json
import json.decoder
import json.encoder

pre = {
    "default_decoder": id(json._default_decoder),
    "default_decoder_scan_once": id(getattr(json._default_decoder, "scan_once", None)),
    "decoder_scanstring": id(json.decoder.scanstring),
    "encoder_encode_basestring": id(json.encoder.encode_basestring),
    "encoder_encode_basestring_ascii": id(json.encoder.encode_basestring_ascii),
}

from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON
_ = LiuXinJSON()

post = {
    "default_decoder": id(json._default_decoder),
    "default_decoder_scan_once": id(getattr(json._default_decoder, "scan_once", None)),
    "decoder_scanstring": id(json.decoder.scanstring),
    "encoder_encode_basestring": id(json.encoder.encode_basestring),
    "encoder_encode_basestring_ascii": id(json.encoder.encode_basestring_ascii),
}

assert pre == post, (pre, post)
"""
    proc = _run_in_subprocess(code)
    _assert_subprocess_ok(proc)


def test_sys_modules_json_namespace_not_clobbered() -> None:
    """Nothing in sys.modules named json.* should point at the LiuXin clone."""
    code = r"""
import json
import sys

from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON
_ = LiuXinJSON()

assert sys.modules["json"] is json
assert sys.modules["json"].__name__ == "json"

bad = []
for name, mod in list(sys.modules.items()):
    if name == "json" or name.startswith("json."):
        path = getattr(mod, "__file__", "") or ""
        if "json_local_clone" in path or "LiuXin_alpha" in path:
            # stdlib json.* must not resolve into project files.
            bad.append((name, path))

assert not bad, f"stdlib json modules were clobbered: {bad}"
"""
    proc = _run_in_subprocess(code)
    _assert_subprocess_ok(proc)


def test_stdlib_json_error_type_is_stable() -> None:
    """A canary: invalid JSON should raise stdlib JSONDecodeError, not TypeError."""
    import json

    from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

    _ = LiuXinJSON()  # should not affect stdlib
    with pytest.raises(json.JSONDecodeError):
        json.loads("not valid json")


def test_liuxin_json_uses_clone_module_and_does_not_patch_stdlib_encoder() -> None:
    """The base64 patching must remain confined to the clone module."""
    import json
    import json.encoder

    from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON
    from LiuXin_alpha.utils.libraries import json_local_clone

    std_encode_ascii = json.encoder.encode_basestring_ascii

    lx = LiuXinJSON()
    assert lx.modded_json is json_local_clone
    assert json.encoder.encode_basestring_ascii is std_encode_ascii


def test_liuxin_json_round_trip_bytes_keys_and_values() -> None:
    """LiuXinJSON decodes strings (and keys) into bytes; stdlib keeps them as str."""
    import json

    from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

    lx = LiuXinJSON()
    dumped = lx.dumps({"a": "hello"})

    # stdlib json should still decode normally (strings in, strings out)
    std = json.loads(dumped)
    assert isinstance(next(iter(std.keys())), str)
    assert isinstance(next(iter(std.values())), str)

    # LiuXin should decode to bytes
    loaded = lx.loads(dumped)
    assert loaded == {b"a": b"hello"}


def test_stdlib_json_module_origin_is_stdlib_when_available() -> None:
    """Extra guard: stdlib json should resolve under sysconfig stdlib path (when `__file__` exists)."""
    import json

    stdlib_dir = Path(sysconfig.get_paths()["stdlib"]).resolve()
    json_file = getattr(json, "__file__", None)
    if not json_file:
        # Some builds may freeze stdlib modules.
        pytest.skip("json module has no __file__ (frozen stdlib)")

    assert Path(json_file).resolve().is_relative_to(stdlib_dir)
