"""
Provide test epub malformed hostile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test epub malformed hostile through a consuming regression::

        python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py
"""
from __future__ import annotations

import re
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.file_format_epub import NullLog, build_unicode_epub, rewrite_epub_zip


def _assert_epub_input_rejects_without_partial_output(
    monkeypatch,
    archive: Path,
    workdir: Path,
    match: str,
    input_cls=None,
) -> None:
    """
    Perform the assert epub input rejects without partial output operation under explicit file-format and conversion rules.

    Example:
        Exercise  assert epub input rejects without partial output through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param archive: Value supplied for archive under the utility contract.
    :param workdir: Value supplied for workdir under the utility contract.
    :param match: Value supplied for match under the utility contract.
    :param input_cls: Value supplied for input cls under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    workdir.mkdir()
    monkeypatch.chdir(workdir)

    plugin_cls = input_cls or EPUBInput
    log = NullLog()
    with archive.open("rb") as stream:
        with pytest.raises(ValueError, match=match):
            plugin_cls(None).convert(stream, SimpleNamespace(), "epub", log, {})

    assert not (workdir / "content.opf").exists()
    assert not (workdir / "OPS").exists()
    assert not (workdir / "META-INF").exists()

    preflight_messages = [message for message in log.messages if "EPUB preflight rejected" in message]
    assert preflight_messages
    assert re.search(match, preflight_messages[-1])


class WarnOnlyLog:
    """
    Provide the warnonlylog contract for validated ebook processing.

    Example:
        Exercise WarnOnlyLog through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the warnonlylog state.

        Example:
            Exercise WarnOnlyLog.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[str] = []

    def warn(self, message: str = "", *args) -> None:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise WarnOnlyLog.warn through a consuming regression::

                python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(message % args if args else message)


def _container_xml(opf_path: str = "OPS/content.opf", media_type: str = "application/oebps-package+xml") -> bytes:
    """
    Perform the container xml operation under explicit file-format and conversion rules.

    Example:
        Exercise  container xml through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param opf_path: Value supplied for opf path under the utility contract.
    :param media_type: Value supplied for media type under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    media_attr = f' media-type="{media_type}"' if media_type else ""
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
        "<rootfiles>"
        f'<rootfile full-path="{opf_path}"{media_attr}/>'
        "</rootfiles>"
        "</container>"
    ).encode("utf-8")


def _opf_without_manifest() -> bytes:
    """
    Perform the opf without manifest operation under explicit file-format and conversion rules.

    Example:
        Exercise  opf without manifest through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        '<spine><itemref idref="chapter"/></spine>'
        "</package>"
    ).encode("utf-8")


def _opf_without_spine() -> bytes:
    """
    Perform the opf without spine operation under explicit file-format and conversion rules.

    Example:
        Exercise  opf without spine through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        '<manifest><item id="chapter" href="text/chapter.xhtml" media-type="application/xhtml+xml"/></manifest>'
        "</package>"
    ).encode("utf-8")


def _opf_without_manifest_items() -> bytes:
    """
    Perform the opf without manifest items operation under explicit file-format and conversion rules.

    Example:
        Exercise  opf without manifest items through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        "<manifest/>"
        '<spine><itemref idref="chapter"/></spine>'
        "</package>"
    ).encode("utf-8")


def _opf_without_spine_itemrefs() -> bytes:
    """
    Perform the opf without spine itemrefs operation under explicit file-format and conversion rules.

    Example:
        Exercise  opf without spine itemrefs through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        '<manifest><item id="chapter" href="text/chapter.xhtml" media-type="application/xhtml+xml"/></manifest>'
        "<spine/>"
        "</package>"
    ).encode("utf-8")


@pytest.mark.parametrize(
    ("case_id", "remove", "replace", "match"),
    (
        ("missing_mimetype", ("mimetype",), {}, "missing required member"),
        ("invalid_mimetype", (), {"mimetype": b"text/plain"}, "invalid mimetype"),
        ("missing_container_xml", ("META-INF/container.xml",), {}, "missing required member"),
        ("malformed_container_xml", (), {"META-INF/container.xml": b"<container"}, "malformed META-INF/container.xml"),
        (
            "container_without_opf_rootfile",
            (),
            {"META-INF/container.xml": _container_xml(media_type="text/plain")},
            "does not reference an OPF package",
        ),
        (
            "container_missing_opf_package",
            (),
            {"META-INF/container.xml": _container_xml(opf_path="OPS/missing.opf")},
            "missing OPF package",
        ),
        (
            "malformed_opf_package",
            (),
            {"OPS/content.opf": b"<?xml version='1.0'?><package><metadata>"},
            "malformed OPF package",
        ),
        (
            "opf_wrong_root",
            (),
            {"OPS/content.opf": b"<?xml version='1.0'?><not-package/>"},
            "root is not <package>",
        ),
        (
            "opf_missing_manifest",
            (),
            {"OPS/content.opf": _opf_without_manifest()},
            "missing manifest",
        ),
        (
            "opf_missing_manifest_items",
            (),
            {"OPS/content.opf": _opf_without_manifest_items()},
            "missing manifest items",
        ),
        (
            "opf_missing_spine",
            (),
            {"OPS/content.opf": _opf_without_spine()},
            "missing spine",
        ),
        (
            "opf_missing_spine_itemrefs",
            (),
            {"OPS/content.opf": _opf_without_spine_itemrefs()},
            "missing spine itemrefs",
        ),
    ),
)
def test_epub_input_rejects_malformed_container_before_extraction(
    tmp_path: Path,
    monkeypatch,
    case_id: str,
    remove: tuple[str, ...],
    replace: dict[str, bytes],
    match: str,
) -> None:
    """
    Perform the test epub input rejects malformed container before extraction operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input rejects malformed container before extraction through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param case_id: Value supplied for case id under the utility contract.
    :param remove: Value supplied for remove under the utility contract.
    :param replace: Value supplied for replace under the utility contract.
    :param match: Value supplied for match under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    base = build_unicode_epub(tmp_path / "base.epub")
    hostile = tmp_path / f"{case_id}.epub"
    rewrite_epub_zip(base.path, hostile, remove=remove, replace=replace)

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / f"{case_id}_work",
        match,
    )


def test_epub_input_rejects_non_zip_payload_before_extraction(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test epub input rejects non zip payload before extraction operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input rejects non zip payload before extraction through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    hostile = tmp_path / "not_an_epub.epub"
    hostile.write_bytes("not an EPUB zip: Καλημέρα".encode("utf-8"))

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / "not_zip_work",
        "invalid ZIP",
    )


def test_epub_input_preflight_rejection_uses_warn_fallback(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test epub input preflight rejection uses warn fallback operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input preflight rejection uses warn fallback through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    hostile = tmp_path / "not_an_epub.epub"
    hostile.write_bytes(b"not an EPUB zip")
    workdir = tmp_path / "warn_only_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    log = WarnOnlyLog()

    with hostile.open("rb") as stream:
        with pytest.raises(ValueError, match="invalid ZIP"):
            EPUBInput(None).convert(stream, SimpleNamespace(), "epub", log, {})

    assert len(log.messages) == 1
    assert "EPUB preflight rejected" in log.messages[0]
    assert "invalid ZIP" in log.messages[0]


@pytest.mark.parametrize(
    ("case_id", "member_name"),
    (
        ("parent_escape", "../escape.txt"),
        ("nested_parent_escape", "OPS/../../escape.txt"),
        ("internal_parent_component", "OPS/text/../escape.xhtml"),
        ("absolute_path", "/absolute.txt"),
        ("drive_path", "C:/absolute.txt"),
    ),
)
def test_epub_input_rejects_unsafe_archive_member_paths_before_extraction(
    tmp_path: Path,
    monkeypatch,
    case_id: str,
    member_name: str,
) -> None:
    """
    Perform the test epub input rejects unsafe archive member paths before extraction operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input rejects unsafe archive member paths before extraction through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param case_id: Value supplied for case id under the utility contract.
    :param member_name: Value supplied for member name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    base = build_unicode_epub(tmp_path / "base.epub")
    hostile = tmp_path / f"{case_id}.epub"
    rewrite_epub_zip(base.path, hostile, add={member_name: b"unsafe"})

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / f"{case_id}_work",
        "unsafe path",
    )


def test_epub_input_rejects_too_many_archive_members_without_partial_output(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test epub input rejects too many archive members without partial output operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input rejects too many archive members without partial output through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    class StrictEPUBInput(EPUBInput):
        """
        Convert strictepubinput sources into the normalized OEB pipeline model.

        Example:
            Exercise test epub input rejects too many archive members without partial output.StrictEPUBInput through a consuming regression::

                python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py
        """
        max_archive_members = 8

    base = build_unicode_epub(tmp_path / "small.epub")
    hostile = tmp_path / "too_many_members.epub"
    rewrite_epub_zip(
        base.path,
        hostile,
        add={f"OPS/text/many/{i}.xhtml": b"<html/>" for i in range(12)},
    )

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / "too_many_work",
        "too many archive members",
        input_cls=StrictEPUBInput,
    )


def test_epub_input_rejects_oversized_archive_member_without_partial_output(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test epub input rejects oversized archive member without partial output operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input rejects oversized archive member without partial output through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    class StrictEPUBInput(EPUBInput):
        """
        Convert strictepubinput sources into the normalized OEB pipeline model.

        Example:
            Exercise test epub input rejects oversized archive member without partial output.StrictEPUBInput through a consuming regression::

                python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py
        """
        max_member_uncompressed_size = 10 * 1024

    base = build_unicode_epub(tmp_path / "small.epub")
    hostile = tmp_path / "oversized_member.epub"
    rewrite_epub_zip(base.path, hostile, add={"OPS/assets/big.bin": b"x" * (20 * 1024)})

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / "oversized_work",
        "member is too large",
        input_cls=StrictEPUBInput,
    )


def test_epub_input_rejects_excessive_total_expansion_without_partial_output(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test epub input rejects excessive total expansion without partial output operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input rejects excessive total expansion without partial output through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    class StrictEPUBInput(EPUBInput):
        """
        Convert strictepubinput sources into the normalized OEB pipeline model.

        Example:
            Exercise test epub input rejects excessive total expansion without partial output.StrictEPUBInput through a consuming regression::

                python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py
        """
        max_member_uncompressed_size = 100 * 1024
        max_total_uncompressed_size = 30 * 1024

    base = build_unicode_epub(tmp_path / "small.epub")
    hostile = tmp_path / "large_total.epub"
    rewrite_epub_zip(
        base.path,
        hostile,
        add={f"OPS/assets/chunk-{i}.bin": b"x" * (8 * 1024) for i in range(6)},
    )

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / "large_total_work",
        "expands to too much data",
        input_cls=StrictEPUBInput,
    )


def test_epub_input_rejects_suspicious_compression_ratio_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """
    Perform the test epub input rejects suspicious compression ratio without partial output operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input rejects suspicious compression ratio without partial output through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    class StrictEPUBInput(EPUBInput):
        """
        Convert strictepubinput sources into the normalized OEB pipeline model.

        Example:
            Exercise test epub input rejects suspicious compression ratio without partial output.StrictEPUBInput through a consuming regression::

                python -m pytest -q tests/file_formats/epub/test_epub_malformed_hostile.py
        """
        max_compression_ratio = 20
        min_compression_ratio_check_size = 32 * 1024

    base = build_unicode_epub(tmp_path / "small.epub")
    hostile = tmp_path / "ratio_bomb_shape.epub"
    rewrite_epub_zip(
        base.path,
        hostile,
        add={"OPS/assets/repeated.bin": b"0" * (128 * 1024)},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / "ratio_work",
        "suspicious compression ratio",
        input_cls=StrictEPUBInput,
    )
