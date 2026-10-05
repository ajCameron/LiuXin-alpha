"""
Provide test manage opf golden manifest script utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test manage opf golden manifest script through a consuming regression::

        python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py
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

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return Path(__file__).resolve().parents[2]


def _script_path() -> Path:
    """
    Perform the script path utility operation under explicit compatibility rules.

    Example:
        Exercise  script path through a consuming regression::

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _repo_root() / "scripts" / "manage_opf_golden_manifest.py"


def _opf3_bytes(title: str) -> bytes:
    """
    Perform the opf3 bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  opf3 bytes through a consuming regression::

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


    :param title: Value supplied for title under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return f"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="3.0">
  <metadata>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <dc:title>{title}</dc:title>
    <dc:creator id="creator">Test Author</dc:creator>
    <dc:language>en</dc:language>
    <meta property="dcterms:modified">2020-01-01T00:00:00Z</meta>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
  </manifest>
  <spine>
    <itemref idref="chap1"/>
  </spine>
</package>
""".encode("utf-8")


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    """
    Perform the run utility operation under explicit compatibility rules.

    Example:
        Exercise  run through a consuming regression::

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


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

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_manage_opf_manifest_add_then_verify(tmp_path: Path) -> None:
    """
    Perform the test manage opf manifest add then verify utility operation under explicit compatibility rules.

    Example:
        Exercise test manage opf manifest add then verify through a consuming regression::

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture_dir = tmp_path / "opf_golden"
    fixture_dir.mkdir(parents=True)
    manifest = fixture_dir / "manifest.json"
    _write_manifest(manifest, {"cases": []})

    fixture = fixture_dir / "fake_001.opf"
    fixture.write_bytes(_opf3_bytes("Golden Title A"))

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
    assert case["path"] == "fake_001.opf"
    assert "sha256" in case
    assert case["expected"]["version_major"] == 3
    assert case["expected"]["title"] == "Golden Title A"
    assert case["expected"]["authors"] == ["Test Author"]

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
    assert "validated 1 OPF golden cases" in verify_proc.stdout


def test_manage_opf_manifest_verify_strict_set_fails_for_untracked(tmp_path: Path) -> None:
    """
    Perform the test manage opf manifest verify strict set fails for untracked utility operation under explicit compatibility rules.

    Example:
        Exercise test manage opf manifest verify strict set fails for untracked through a consuming regression::

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture_dir = tmp_path / "opf_golden"
    fixture_dir.mkdir(parents=True)
    manifest = fixture_dir / "manifest.json"
    _write_manifest(manifest, {"cases": []})

    tracked = fixture_dir / "tracked.opf"
    tracked.write_bytes(_opf3_bytes("Tracked"))
    untracked = fixture_dir / "untracked.opf"
    untracked.write_bytes(_opf3_bytes("Untracked"))

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
    assert "untracked OPF fixtures" in verify_proc.stdout


def test_manage_opf_manifest_rebuild_updates_expected_after_fixture_change(tmp_path: Path) -> None:
    """
    Perform the test manage opf manifest rebuild updates expected after fixture change utility operation under explicit compatibility rules.

    Example:
        Exercise test manage opf manifest rebuild updates expected after fixture change through a consuming regression::

            python -m pytest -q tests/utils/test_manage_opf_golden_manifest_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fixture_dir = tmp_path / "opf_golden"
    fixture_dir.mkdir(parents=True)
    manifest = fixture_dir / "manifest.json"
    _write_manifest(manifest, {"cases": []})

    fixture = fixture_dir / "rebuild_case.opf"
    fixture.write_bytes(_opf3_bytes("Title Before"))

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

    fixture.write_bytes(_opf3_bytes("Title After"))

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
