"""
Provide test manage html ingest fixture hashes script utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test manage html ingest fixture hashes script through a consuming regression::

        python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py
"""
from __future__ import annotations

import hashlib
import runpy
import subprocess
import sys
from pathlib import Path


def _legacy_hash_bytes(data: bytes) -> str:
    """
    Perform the legacy hash bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  legacy hash bytes through a consuming regression::

            python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return hashlib.sha512(data).hexdigest() + str(len(data))


def _script_path() -> Path:
    """
    Perform the script path utility operation under explicit compatibility rules.

    Example:
        Exercise  script path through a consuming regression::

            python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return Path(__file__).resolve().parents[2] / "scripts" / "manage_html_ingest_fixture_hashes.py"


def _write_manifest(path: Path, mapping: dict[str, str]) -> None:
    """
    Perform the write manifest utility operation under explicit compatibility rules.

    Example:
        Exercise  write manifest through a consuming regression::

            python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param mapping: Value supplied for mapping under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "from __future__ import annotations\n",
        "import hashlib\n",
        "from pathlib import Path\n",
        "EXPECTED_HTML_INGEST_FIXTURE_HASHES = {\n",
    ]
    for name, value in sorted(mapping.items()):
        lines.append(f'    "{name}": "{value}",\n')
    lines.extend(
        [
            "}\n",
            "def legacy_sha512_size_hash(path: Path) -> str:\n",
            "    h = hashlib.sha512()\n",
            '    with path.open("rb") as stream:\n',
            "        while True:\n",
            "            chunk = stream.read(1024 * 1024)\n",
            "            if not chunk:\n",
            "                break\n",
            "            h.update(chunk)\n",
            "    return h.hexdigest() + str(path.stat().st_size)\n",
        ]
    )
    path.write_text("".join(lines), encoding="utf-8")


def _mk_fake_repo(tmp_path: Path) -> tuple[Path, Path, Path]:
    """
    Perform the mk fake repo utility operation under explicit compatibility rules.

    Example:
        Exercise  mk fake repo through a consuming regression::

            python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    repo = tmp_path / "repo"
    (repo / "src" / "LiuXin_alpha").mkdir(parents=True)
    (repo / "tests").mkdir(parents=True)
    fixture_dir = repo / "tests" / "fixtures" / "html_ingest"
    fixture_dir.mkdir(parents=True)
    manifest = repo / "tests" / "support" / "html_ingest_fixture_hashes.py"
    return repo, fixture_dir, manifest


def test_manage_html_ingest_fixture_hashes_revalidate_ok(tmp_path: Path) -> None:
    """
    Perform the test manage html ingest fixture hashes revalidate ok utility operation under explicit compatibility rules.

    Example:
        Exercise test manage html ingest fixture hashes revalidate ok through a consuming regression::

            python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    repo, fixture_dir, manifest = _mk_fake_repo(tmp_path)
    fixture = fixture_dir / "case_001.html"
    payload = b"fixture-bytes-1\n"
    fixture.write_bytes(payload)

    _write_manifest(manifest, {fixture.name: _legacy_hash_bytes(payload)})

    proc = subprocess.run(
        [
            sys.executable,
            str(_script_path()),
            "--repo-root",
            str(repo),
            "--fixtures-dir",
            str(fixture_dir),
            "--manifest",
            str(manifest),
            "--revalidate",
        ],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "validated 1 tracked fixture hashes" in proc.stdout


def test_manage_html_ingest_fixture_hashes_add_writes_new_entry(tmp_path: Path) -> None:
    """
    Perform the test manage html ingest fixture hashes add writes new entry utility operation under explicit compatibility rules.

    Example:
        Exercise test manage html ingest fixture hashes add writes new entry through a consuming regression::

            python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    repo, fixture_dir, manifest = _mk_fake_repo(tmp_path)
    fixture = fixture_dir / "case_002.html"
    payload = b"fixture-bytes-2\n"
    fixture.write_bytes(payload)
    expected_hash = _legacy_hash_bytes(payload)

    _write_manifest(manifest, {})

    proc = subprocess.run(
        [
            sys.executable,
            str(_script_path()),
            "--repo-root",
            str(repo),
            "--fixtures-dir",
            str(fixture_dir),
            "--manifest",
            str(manifest),
            "--add",
            fixture.name,
        ],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "ADDED: case_002.html" in proc.stdout

    scope = runpy.run_path(str(manifest))
    mapping = scope["EXPECTED_HTML_INGEST_FIXTURE_HASHES"]
    assert mapping["case_002.html"] == expected_hash


def test_manage_html_ingest_fixture_hashes_revalidate_strict_set_fails_on_extra(tmp_path: Path) -> None:
    """
    Perform the test manage html ingest fixture hashes revalidate strict set fails on extra utility operation under explicit compatibility rules.

    Example:
        Exercise test manage html ingest fixture hashes revalidate strict set fails on extra through a consuming regression::

            python -m pytest -q tests/utils/test_manage_html_ingest_fixture_hashes_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    repo, fixture_dir, manifest = _mk_fake_repo(tmp_path)
    tracked = fixture_dir / "tracked_case.html"
    tracked_payload = b"tracked\n"
    tracked.write_bytes(tracked_payload)

    extra = fixture_dir / "extra_case.html"
    extra.write_bytes(b"extra\n")

    _write_manifest(manifest, {tracked.name: _legacy_hash_bytes(tracked_payload)})

    proc = subprocess.run(
        [
            sys.executable,
            str(_script_path()),
            "--repo-root",
            str(repo),
            "--fixtures-dir",
            str(fixture_dir),
            "--manifest",
            str(manifest),
            "--revalidate",
            "--strict-set",
        ],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 1
    assert "Untracked fixture files" in proc.stdout
