"""
Verify independent surface imports, legacy export identity, and shared presentation/acquisition behavior.

Import checks run in isolated child interpreters; other tests use in-memory rows
and a recording reader, without opening databases or making acquisition requests.
"""

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
    """
    Import each shared module in an isolated interpreter and reject eager loading of web application owners.

    Example:
        >>> test_shared_modules_import_without_web_applications("presentation")  # doctest: +SKIP


    :param module: Parametrized dotted module name relative to LiuXin_alpha.surfaces.
    :return: None after the child succeeds without any checked web application in sys.modules.
    """
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
    """
    Keep legacy web helper and acquisition-record names bound to the exact shared implementations.

    Example:
        >>> test_web_application_preserves_compatibility_exports()


    :return: None after all six compatibility exports pass object-identity assertions.
    """
    from LiuXin_alpha.surfaces.web_readonly import app

    assert app._escape is presentation.escape
    assert app._short_text is presentation.short_text
    assert app._row_value is presentation.row_value
    assert app._coerce_int is presentation.coerce_int
    assert app._CoreStoredFile is acquisition_types.CoreStoredFile
    assert app._ResolvedFileTarget is acquisition_types.ResolvedFileTarget


def test_presentation_preserves_escaping_newlines_and_truncation() -> None:
    """
    Retain quoted HTML escaping, Unicode/newline preservation, and literal-dot truncation at narrow widths.

    Example:
        >>> test_presentation_preserves_escaping_newlines_and_truncation()


    :return: None after absent values, HTML metacharacters, newline normalization, and boundary widths are checked.
    """
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
    """
    Preserve fallback parsing and lower-then-upper clamping, including inconsistent bound order.

    Example:
        >>> test_integer_fallback_and_clamping_remain_unchanged("1", 0, 5, 3, 3)


    :param raw: Parametrized option text or None passed to the parser.
    :param default: Fallback integer used when parsing fails.
    :param minimum: Lower clamp applied before the upper clamp.
    :param maximum: Optional upper clamp, allowed to be below minimum in this regression case.
    :param expected: Exact final integer required by the existing behavior.
    :return: None after the actual parser result matches the parametrized expectation.
    """
    assert (
        presentation.coerce_int(raw, default=default, minimum=minimum, maximum=maximum)
        == expected
    )


def test_row_lookup_accepts_mapping_and_core_rows_and_preserves_fallback() -> None:
    """
    Accept dict/CoreRow subscription while distinguishing absent keys from unexpected access failures.

    Example:
        >>> test_row_lookup_accepts_mapping_and_core_rows_and_preserves_fallback()


    :return: None after stored/missing values and propagation from a failing row double are checked.
    """
    values = {"title": "雪", "empty": None}
    row = CoreRow(table="works", row_id=7, values=values)
    for source in (values, row):
        assert presentation.row_value(source, "title") == "雪"
        assert presentation.row_value(source, "empty") is None
        assert presentation.row_value(source, "missing") is None

    class BrokenRow:
        """
        Supply a row-shaped double whose subscription always fails with a non-KeyError exception.

        Example:
            >>> row = BrokenRow()  # doctest: +SKIP
        """

        def __getitem__(self, column: str) -> object:
            """
            Raise a simulated provider failure for every requested column.

            Example:
                >>> row["title"]  # doctest: +SKIP


            :param column: Requested column, deliberately ignored by this always-failing double.
            :return: No value; every subscription raises RuntimeError.
            :raises RuntimeError: Always, to ensure the presentation helper does not hide provider failures.
            """
            raise RuntimeError("lookup failed")

    with pytest.raises(RuntimeError, match="lookup failed"):
        presentation.row_value(BrokenRow(), "title")


class _Reader:
    """
    Record acquisition requests and return fixed binary content or raise an injected exception.

    The double performs no Core or storage access; calls are recorded before
    success/failure selection so forwarding can be asserted on both paths.

    Example:
        >>> reader = _Reader()
        >>> reader.acquisition_read("file", 7)[1].hex()
        '00ff7061796c6f6164'
    """

    def __init__(self, error: Exception | None = None) -> None:
        """
        Initialize an empty request log and retain the optional failure instance.

        Example:
            >>> _Reader().calls
            []


        :param error: Exception to raise unchanged on each read, or None for the fixed successful result.
        :return: None after storing the empty call list and error selection.
        """
        self.calls: list[tuple[str, int]] = []
        self.error = error

    def acquisition_read(
        self, kind: str, resource_id: int
    ) -> tuple[Mapping[str, object], bytes]:
        """
        Record the requested resource and return fixed metadata/bytes unless an error was injected.

        Example:
            >>> reader = _Reader()
            >>> reader.acquisition_read("file", 7)[0]
            {'name': '雪.epub'}
            >>> reader.calls
            [('file', 7)]


        :param kind: Resource kind appended unchanged to the request log.
        :param resource_id: Resource identifier appended unchanged to the request log.
        :return: Fixed filename metadata and binary payload, unless the retained error is raised.
        """
        self.calls.append((kind, resource_id))
        if self.error is not None:
            raise self.error
        return {"name": "雪.epub"}, b"\x00\xffpayload"


def test_stored_file_forwards_requests_and_keeps_target_values_immutable() -> None:
    """
    Forward a stored-resource read exactly and keep both acquisition value objects' attributes frozen.

    Example:
        >>> test_stored_file_forwards_requests_and_keeps_target_values_immutable()


    :return: None after request forwarding, delivery fields, and FrozenInstanceError assertions.
    """
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
    """
    Preserve the exact acquisition-reader exception after recording the attempted resource request.

    Example:
        >>> test_stored_file_propagates_reader_errors_without_reinterpreting_them()


    :return: None after failure identity and single-call forwarding are verified.
    """
    error = OSError("read failed")
    reader = _Reader(error)
    with pytest.raises(OSError) as raised:
        acquisition_types.CoreStoredFile(reader, "image", 7).read_bytes()
    assert raised.value is error
    assert reader.calls == [("image", 7)]
