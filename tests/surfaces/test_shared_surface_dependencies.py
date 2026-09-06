"""Independent imports and unchanged contracts for shared surface primitives."""

import subprocess
import sys
from collections.abc import Mapping
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from LiuXin_alpha.surfaces import acquisition_types, presentation
from LiuXin_alpha.surfaces.core import CoreRow

SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src"


@pytest.mark.parametrize(
    "module",
    (
        "presentation",
        "acquisition_types",
        "read_model.api",
        "images.api",
        "catalog.api",
        "opds.api",
        "acquisition.api",
    ),
)
def test_shared_modules_import_without_web_applications(module: str) -> None:
    source = f"""
import importlib
import sys
sys.path.insert(0, {str(SOURCE_ROOT)!r})
importlib.import_module('LiuXin_alpha.surfaces.' + {module!r})
applications = ('web_readonly', 'web_readwrite', 'web_calibre_readonly', 'api_readonly', 'opds_readonly')
loaded = [name for name in sys.modules if any(
    name == 'LiuXin_alpha.surfaces.' + app or name.startswith('LiuXin_alpha.surfaces.' + app + '.')
    for app in applications
)]
assert not loaded, loaded
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", source],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_web_application_preserves_compatibility_exports() -> None:
    from LiuXin_alpha.surfaces.web_readonly import app

    assert app._escape is presentation.escape
    assert app._short_text is presentation.short_text
    assert app._row_value is presentation.row_value
    assert app._coerce_int is presentation.coerce_int
    assert app._CoreStoredFile is acquisition_types.CoreStoredFile
    assert app._ResolvedFileTarget is acquisition_types.ResolvedFileTarget


def test_presentation_preserves_escaping_newlines_and_truncation() -> None:
    assert presentation.escape(None) == ""
    assert (
        presentation.escape("<é & \"猫\" 'x'>")
        == "&lt;é &amp; &quot;猫&quot; &#x27;x&#x27;&gt;"
    )
    assert presentation.short_text(None) == ""
    assert presentation.short_text("é\r\n猫\rfin") == "é\n猫\nfin"
    assert presentation.short_text("abcdef", width=6) == "abcdef"
    assert presentation.short_text("abcdef", width=5) == "ab..."
    assert presentation.short_text("abcdef", width=0) == "..."


@pytest.mark.parametrize(
    ("raw", "default", "minimum", "maximum", "expected"),
    [
        (" 12 ", 5, 0, None, 12),
        (None, 5, 0, None, 5),
        ("bad", 5, 0, None, 5),
        ("-4", 5, 0, None, 0),
        ("12", 5, 0, 10, 10),
        ("bad", 15, 0, 10, 10),
        ("1", 0, 5, 3, 3),
    ],
)
def test_integer_fallback_and_clamping_remain_unchanged(
    raw, default, minimum, maximum, expected
) -> None:
    assert (
        presentation.coerce_int(raw, default=default, minimum=minimum, maximum=maximum)
        == expected
    )


def test_row_lookup_accepts_mapping_and_core_rows_and_preserves_fallback() -> None:
    values = {"title": "雪", "empty": None}
    row = CoreRow(table="works", row_id=7, values=values)
    for source in (values, row):
        assert presentation.row_value(source, "title") == "雪"
        assert presentation.row_value(source, "empty") is None
        assert presentation.row_value(source, "missing") is None

    class BrokenRow:
        def __getitem__(self, column: str) -> object:
            raise RuntimeError("lookup failed")

    with pytest.raises(RuntimeError, match="lookup failed"):
        presentation.row_value(BrokenRow(), "title")


class _Reader:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, int]] = []
        self.error = error

    def acquisition_read(
        self, kind: str, resource_id: int
    ) -> tuple[Mapping[str, object], bytes]:
        self.calls.append((kind, resource_id))
        if self.error is not None:
            raise self.error
        return {"name": "雪.epub"}, b"\x00\xffpayload"


def test_stored_file_forwards_requests_and_keeps_target_values_immutable() -> None:
    reader = _Reader()
    stored = acquisition_types.CoreStoredFile(reader, "file", 42)
    assert stored.read_bytes() == b"\x00\xffpayload"
    assert reader.calls == [("file", 42)]
    target = acquisition_types.ResolvedFileTarget(
        "redirect", "https://example.invalid/book", "雪.epub"
    )
    assert (target.mode, target.location, target.download_name) == (
        "redirect",
        "https://example.invalid/book",
        "雪.epub",
    )
    with pytest.raises(FrozenInstanceError):
        target.mode = "local"
    with pytest.raises(FrozenInstanceError):
        stored.resource_id = 99


def test_stored_file_propagates_reader_errors_without_reinterpreting_them() -> None:
    error = OSError("read failed")
    reader = _Reader(error)
    with pytest.raises(OSError) as raised:
        acquisition_types.CoreStoredFile(reader, "image", 7).read_bytes()
    assert raised.value is error
    assert reader.calls == [("image", 7)]
