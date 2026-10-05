"""
Provide test pml unicode framework utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pml unicode framework through a consuming regression::

        python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py
"""
from __future__ import annotations

import builtins
import importlib
import zipfile
from pathlib import Path

from tests.support.file_format_oeb import (
    build_text_output_book,
    install_minimal_stylizers,
    null_log,
    text_output_options,
)
from tests.support.file_format_unicode import assert_no_replacement_chars


SUPPORTED_ROUNDTRIP_FRAGMENTS = (
    "café",
    "Καλημ?ρα",
    "שלום",
    "cafe",
    "A",
)


def _pml_options(**overrides):
    """
    Perform the pml options operation under explicit file-format and conversion rules.

    Example:
        Exercise  pml options through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param overrides: Value supplied for overrides under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return text_output_options(
        pml_output_encoding="cp1252",
        full_image_depth=False,
        remove_paragraph_spacing=False,
        **overrides,
    )


def _assert_ascii_pml(rendered: str) -> None:
    """
    Perform the assert ascii pml operation under explicit file-format and conversion rules.

    Example:
        Exercise  assert ascii pml through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param rendered: Value supplied for rendered under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    rendered.encode("ascii", "strict")
    assert_no_replacement_chars(rendered, context="PML output")


def test_pmlmlizer_serializes_shared_oeb_with_pml_unicode_escapes(monkeypatch) -> None:
    """
    Perform the test pmlmlizer serializes shared oeb with pml unicode escapes operation under explicit file-format and conversion rules.

    Example:
        Exercise test pmlmlizer serializes shared oeb with pml unicode escapes through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    install_minimal_stylizers(monkeypatch)
    pmlml = importlib.import_module("LiuXin_alpha.file_formats.pml.pmlml")

    rendered_a = pmlml.PMLMLizer(null_log()).extract_content(build_text_output_book(), _pml_options())
    rendered_b = pmlml.PMLMLizer(null_log()).extract_content(build_text_output_book(), _pml_options())

    assert rendered_a == rendered_b
    _assert_ascii_pml(rendered_a)
    assert r"\a233" in rendered_a
    assert r"\U039A\U03B1\U03BB\U03B7\U03BC?\U03C1\U03B1" in rendered_a
    assert r"\U05E9\U05DC\U05D5\U05DD" in rendered_a
    assert "??????" in rendered_a
    assert r"\m=" in rendered_a
    assert r"\Bbold \U03A9\B" in rendered_a
    assert r"\iitalic \U05E9\U05DC\U05D5\U05DD\i" in rendered_a


def test_pmlmlizer_output_roundtrips_supported_foreign_language_fragments(monkeypatch) -> None:
    """
    Perform the test pmlmlizer output roundtrips supported foreign language fragments operation under explicit file-format and conversion rules.

    Example:
        Exercise test pmlmlizer output roundtrips supported foreign language fragments through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    install_minimal_stylizers(monkeypatch)
    pmlml = importlib.import_module("LiuXin_alpha.file_formats.pml.pmlml")
    converter = importlib.import_module("LiuXin_alpha.file_formats.pml.pmlconverter")

    pml = pmlml.PMLMLizer(null_log()).extract_content(build_text_output_book(), _pml_options())
    html = converter.pml_to_html(pml)

    for fragment in SUPPORTED_ROUNDTRIP_FRAGMENTS:
        assert fragment in html
    assert_no_replacement_chars(html, context="PML roundtrip")


def test_pml_output_writes_deterministic_pmlz_with_unicode_escaped_pml(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test pml output writes deterministic pmlz with unicode escaped pml operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml output writes deterministic pmlz with unicode escaped pml through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    install_minimal_stylizers(monkeypatch)
    pml_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.pml_output")

    def render_once(run_name: str) -> tuple[list[str], str]:
        """
        Perform the render once operation under explicit file-format and conversion rules.

        Example:
            Exercise test pml output writes deterministic pmlz with unicode escaped pml.render once through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


        :param run_name: Value supplied for run name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        output_path = tmp_path / f"{run_name}.pmlz"
        pml_output.PMLOutput(None).convert(build_text_output_book(), str(output_path), None, _pml_options(), null_log())
        with zipfile.ZipFile(output_path) as zf:
            names = sorted(zf.namelist())
            index = zf.read("index.pml").decode("cp1252", "strict")
        return names, index

    names, index = render_once("deterministic_1")
    second_names, second_index = render_once("deterministic_2")

    assert second_names == names
    assert second_index == index
    assert "index.pml" in names
    assert "index_img/cover.png" not in names
    _assert_ascii_pml(index)
    assert r"\a233" in index
    assert r"\U039A\U03B1\U03BB\U03B7\U03BC?\U03C1\U03B1" in index


def test_pml_output_reports_unsupported_character_replacement(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test pml output reports unsupported character replacement operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml output reports unsupported character replacement through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    install_minimal_stylizers(monkeypatch)
    pml_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.pml_output")

    opts = _pml_options()
    output_path = tmp_path / "lossy.pmlz"
    pml_output.PMLOutput(None).convert(
        build_text_output_book(),
        str(output_path),
        None,
        opts,
        null_log(),
    )

    with zipfile.ZipFile(output_path) as zf:
        index = zf.read("index.pml").decode("cp1252", "strict")

    assert "??????" in index
    report = opts.conversion_report
    events = [event for event in report.loss_events if event.code == "unsupported-character-replacement"]
    assert len(events) == 1
    event = events[0]
    assert event.phase == "pml-output"
    assert event.source_format == "oeb"
    assert event.target_format == "pmlz"
    assert event.edge_name == "oeb-to-pmlz"
    assert event.recoverable is True
    assert event.count >= 6
    assert event.details["replacement"] == "?"
    assert event.details["unique_characters"] >= 1
    assert any("U+4E16" in sample.codepoints for sample in event.samples)

    payload = report.to_mapping()
    assert payload["loss_event_count"] == 1
    assert payload["loss_events"][0]["code"] == "unsupported-character-replacement"


def test_pml_output_report_uses_explicit_conversion_edge(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test pml output report uses explicit conversion edge operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml output report uses explicit conversion edge through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    install_minimal_stylizers(monkeypatch)
    edges = importlib.import_module("LiuXin_alpha.file_formats.conversion.edges")
    pml_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.pml_output")

    opts = _pml_options(conversion_edge=edges.legacy_oeb_edge("html", "pmlz"))
    output_path = tmp_path / "lossy_with_edge.pmlz"
    pml_output.PMLOutput(None).convert(
        build_text_output_book(),
        str(output_path),
        None,
        opts,
        null_log(),
    )

    event = opts.conversion_report.loss_events[0]
    assert event.source_format == "html"
    assert event.target_format == "pmlz"
    assert event.edge_name == "legacy-oeb:html->pmlz"


def test_pml_output_skips_images_when_pillow_is_unavailable_with_minimal_log(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """
    Perform the test pml output skips images when pillow is unavailable with minimal log operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml output skips images when pillow is unavailable with minimal log through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    install_minimal_stylizers(monkeypatch)
    pml_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.pml_output")
    real_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        """
        Perform the blocked import operation under explicit file-format and conversion rules.

        Example:
            Exercise test pml output skips images when pillow is unavailable with minimal log.blocked import through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_unicode_framework.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param globals: Value supplied for globals under the utility contract.
        :param locals: Local template variables available during evaluation.
        :param fromlist: Value supplied for fromlist under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if name == "PIL" or name.startswith("PIL."):
            raise ImportError("Pillow intentionally blocked")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    output_path = tmp_path / "no_pillow.pmlz"
    pml_output.PMLOutput(None).convert(
        build_text_output_book(),
        str(output_path),
        None,
        _pml_options(),
        null_log(),
    )

    with zipfile.ZipFile(output_path) as zf:
        names = sorted(zf.namelist())
        index = zf.read("index.pml").decode("cp1252", "strict")

    assert "index.pml" in names
    assert "index_img/cover.png" not in names
    _assert_ascii_pml(index)
