from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest


def _install_liuxin_decompression_stubs(calls: list[tuple[str, str, str]]) -> None:
    liuxin = types.ModuleType("LiuXin")
    utils = types.ModuleType("LiuXin.utils")
    # Mark as packages so nested imports work.
    liuxin.__path__ = []  # type: ignore[attr-defined]
    utils.__path__ = []  # type: ignore[attr-defined]

    lx_libs = types.ModuleType("LiuXin.utils.lx_libraries")
    lx_libs.__path__ = []  # type: ignore[attr-defined]

    liuxin_six = types.ModuleType("LiuXin.utils.lx_libraries.liuxin_six")
    liuxin_six.six_unicode = lambda x, *a, **k: str(x)  # type: ignore[attr-defined]

    unrar = types.ModuleType("LiuXin.utils.decompression.unrar")
    def rar_extract(path: str, d: str) -> None:
        calls.append(("rar", path, d))

    unrar.extract = rar_extract  # type: ignore[attr-defined]

    libunzip = types.ModuleType("LiuXin.utils.libunzip")
    def zip_extract(path: str, d: str) -> None:
        calls.append(("zip", path, d))

    libunzip.extract = zip_extract  # type: ignore[attr-defined]

    # Overwrite any prior stubs so each test gets a fresh call capture.
    sys.modules["LiuXin"] = liuxin
    sys.modules["LiuXin.utils"] = utils
    sys.modules["LiuXin.utils.lx_libraries"] = lx_libs
    sys.modules["LiuXin.utils.lx_libraries.liuxin_six"] = liuxin_six
    decomp = types.ModuleType("LiuXin.utils.decompression")
    decomp.__path__ = []  # type: ignore[attr-defined]
    sys.modules["LiuXin.utils.decompression"] = decomp
    sys.modules["LiuXin.utils.decompression.unrar"] = unrar
    sys.modules["LiuXin.utils.libunzip"] = libunzip


def test_extract_dispatches_by_magic_header(tmp_path: Path) -> None:
    calls: list[tuple[str, str, str]] = []
    _install_liuxin_decompression_stubs(calls)

    from LiuXin_alpha.utils.decompression.archives import extract

    rar = tmp_path / "a.rar"
    rar.write_bytes(b"Rar" + b"x" * 100)
    out = tmp_path / "out"
    out.mkdir()

    extract(str(rar), str(out))
    assert calls and calls[-1][0] == "rar"

    zipf = tmp_path / "a.zip"
    zipf.write_bytes(b"PK\x03\x04" + b"x" * 100)
    extract(str(zipf), str(out))
    assert calls[-1][0] == "zip"


def test_extract_falls_back_to_extension_when_unknown_header(tmp_path: Path) -> None:
    calls: list[tuple[str, str, str]] = []
    _install_liuxin_decompression_stubs(calls)

    from LiuXin_alpha.utils.decompression.archives import extract

    p = tmp_path / "x.epub"
    p.write_bytes(b"???" + b"x" * 10)
    out = tmp_path / "out"
    out.mkdir()
    extract(str(p), str(out))
    assert calls[-1][0] == "zip"


def test_extract_raises_on_unknown_archive_type(tmp_path: Path) -> None:
    calls: list[tuple[str, str, str]] = []
    _install_liuxin_decompression_stubs(calls)

    from LiuXin_alpha.utils.decompression.archives import extract

    p = tmp_path / "x.unknown"
    p.write_bytes(b"???" + b"x" * 10)
    out = tmp_path / "out"
    out.mkdir()

    with pytest.raises(Exception):
        extract(str(p), str(out))