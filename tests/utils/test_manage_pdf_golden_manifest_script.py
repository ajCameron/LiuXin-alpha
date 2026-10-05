"""
Provide test manage pdf golden manifest script utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test manage pdf golden manifest script through a consuming regression::

        python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    """
    Perform the repo root utility operation under explicit compatibility rules.

    Example:
        Exercise  repo root through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return Path(__file__).resolve().parents[2]


def _script_path() -> Path:
    """
    Perform the script path utility operation under explicit compatibility rules.

    Example:
        Exercise  script path through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _repo_root() / "scripts" / "manage_pdf_golden_manifest.py"


def _pdf_escape(value: str) -> str:
    """
    Perform the pdf escape utility operation under explicit compatibility rules.

    Example:
        Exercise  pdf escape through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _assemble_pdf(objects: list[bytes], *, info_obj_num: int) -> bytes:
    """
    Perform the assemble pdf utility operation under explicit compatibility rules.

    Example:
        Exercise  assemble pdf through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param objects: Value supplied for objects under the utility contract.
    :param info_obj_num: Value supplied for info obj num under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]

    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode("ascii")
        out += obj
        if not obj.endswith(b"\n"):
            out += b"\n"
        out += b"endobj\n"

    xref_pos = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode("ascii")
    out += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        out += f"{off:010d} 00000 n \n".encode("ascii")

    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R /Info {info_obj_num} 0 R >>\n".encode("ascii")
    )
    out += f"startxref\n{xref_pos}\n%%EOF\n".encode("ascii")
    return bytes(out)


def _build_pdf(*, title: str) -> bytes:
    """
    Perform the build pdf utility operation under explicit compatibility rules.

    Example:
        Exercise  build pdf through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param title: Value supplied for title under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pages_obj = b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>"
    page_obj = b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R >>"
    content_stream = b"<< /Length 31 >>\nstream\nBT /F1 24 Tf 100 100 Td (Hello) Tj ET\nendstream"
    info_obj = (
        f"<< /Title ({_pdf_escape(title)}) "
        f"/Author ({_pdf_escape('Golden Author A & Golden Author B')}) "
        f"/Subject ({_pdf_escape('Golden Subject')}) "
        f"/Keywords ({_pdf_escape('tag-one,9780306406157')}) "
        f"/Creator ({_pdf_escape('Golden Creator')}) "
        f"/Producer ({_pdf_escape('Golden Producer')}) >>"
    ).encode("utf-8")

    catalog_obj = b"<< /Type /Catalog /Pages 2 0 R >>"
    objects = [catalog_obj, pages_obj, page_obj, content_stream, info_obj]
    return _assemble_pdf(objects, info_obj_num=5)


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    """
    Perform the run utility operation under explicit compatibility rules.

    Example:
        Exercise  run through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param args: Positional values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return subprocess.run(
        [sys.executable, str(_script_path()), *args],
        capture_output=True,
        text=True,
    )


def _write_manifest(path: Path, payload: dict) -> None:
    """
    Perform the write manifest utility operation under explicit compatibility rules.

    Example:
        Exercise  write manifest through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_manage_pdf_manifest_add_then_verify(tmp_path: Path) -> None:
    """
    Perform the test manage pdf manifest add then verify utility operation under explicit compatibility rules.

    Example:
        Exercise test manage pdf manifest add then verify through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture_dir = tmp_path / "pdf_golden"
    fixture_dir.mkdir(parents=True)
    manifest = fixture_dir / "manifest.json"
    _write_manifest(manifest, {"cases": []})

    fixture = fixture_dir / "fake_001.pdf"
    fixture.write_bytes(_build_pdf(title="Golden PDF Case A"))

    add_proc = _run(
        "--repo-root",
        str(_repo_root()),
        "--fixture-dir",
        str(fixture_dir),
        "--manifest",
        str(manifest),
        "--add",
        fixture.name,
    )
    assert add_proc.returncode == 0, add_proc.stdout + add_proc.stderr
    assert "ADDED:" in add_proc.stdout

    data = json.loads(manifest.read_text(encoding="utf-8"))
    assert len(data["cases"]) == 1
    case = data["cases"][0]
    assert case["path"] == "fake_001.pdf"
    assert case["expected"]["title"] == "Golden PDF Case A"
    assert case["expected"]["authors"] == ["Golden Author A", "Golden Author B"]
    assert case["expected"]["identifiers"]["isbn"] == "9780306406157"

    verify_proc = _run(
        "--repo-root",
        str(_repo_root()),
        "--fixture-dir",
        str(fixture_dir),
        "--manifest",
        str(manifest),
        "--verify",
    )
    assert verify_proc.returncode == 0, verify_proc.stdout + verify_proc.stderr
    assert "validated 1 PDF golden cases" in verify_proc.stdout


def test_manage_pdf_manifest_verify_strict_set_fails_for_untracked(tmp_path: Path) -> None:
    """
    Perform the test manage pdf manifest verify strict set fails for untracked utility operation under explicit compatibility rules.

    Example:
        Exercise test manage pdf manifest verify strict set fails for untracked through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture_dir = tmp_path / "pdf_golden"
    fixture_dir.mkdir(parents=True)
    manifest = fixture_dir / "manifest.json"
    _write_manifest(manifest, {"cases": []})

    tracked = fixture_dir / "tracked.pdf"
    tracked.write_bytes(_build_pdf(title="Tracked"))
    untracked = fixture_dir / "untracked.pdf"
    untracked.write_bytes(_build_pdf(title="Untracked"))

    add_proc = _run(
        "--repo-root",
        str(_repo_root()),
        "--fixture-dir",
        str(fixture_dir),
        "--manifest",
        str(manifest),
        "--add",
        tracked.name,
    )
    assert add_proc.returncode == 0, add_proc.stdout + add_proc.stderr

    verify_proc = _run(
        "--repo-root",
        str(_repo_root()),
        "--fixture-dir",
        str(fixture_dir),
        "--manifest",
        str(manifest),
        "--verify",
        "--strict-set",
    )
    assert verify_proc.returncode == 1
    assert "untracked PDF fixtures" in verify_proc.stdout


def test_manage_pdf_manifest_rebuild_updates_expected_after_fixture_change(tmp_path: Path) -> None:
    """
    Perform the test manage pdf manifest rebuild updates expected after fixture change utility operation under explicit compatibility rules.

    Example:
        Exercise test manage pdf manifest rebuild updates expected after fixture change through a consuming regression::

            python -m pytest -q tests/utils/test_manage_pdf_golden_manifest_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture_dir = tmp_path / "pdf_golden"
    fixture_dir.mkdir(parents=True)
    manifest = fixture_dir / "manifest.json"
    _write_manifest(manifest, {"cases": []})

    fixture = fixture_dir / "rebuild_case.pdf"
    fixture.write_bytes(_build_pdf(title="Title Before"))

    add_proc = _run(
        "--repo-root",
        str(_repo_root()),
        "--fixture-dir",
        str(fixture_dir),
        "--manifest",
        str(manifest),
        "--add",
        fixture.name,
    )
    assert add_proc.returncode == 0, add_proc.stdout + add_proc.stderr
    before = json.loads(manifest.read_text(encoding="utf-8"))
    before_title = before["cases"][0]["expected"]["title"]
    before_sha = before["cases"][0]["sha256"]
    assert before_title == "Title Before"

    fixture.write_bytes(_build_pdf(title="Title After"))

    rebuild_proc = _run(
        "--repo-root",
        str(_repo_root()),
        "--fixture-dir",
        str(fixture_dir),
        "--manifest",
        str(manifest),
        "--rebuild",
    )
    assert rebuild_proc.returncode == 0, rebuild_proc.stdout + rebuild_proc.stderr
    after = json.loads(manifest.read_text(encoding="utf-8"))
    after_title = after["cases"][0]["expected"]["title"]
    after_sha = after["cases"][0]["sha256"]
    assert after_title == "Title After"
    assert after_sha != before_sha
