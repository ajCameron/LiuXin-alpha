"""
Provide test comic rar extraction boundary utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test comic rar extraction boundary through a consuming regression::

        python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py
"""
from __future__ import annotations

import io
import os
from pathlib import Path

import pytest

from LiuXin_alpha.utils.decompression import unrar


def _rar_header(
    filename: str,
    *,
    is_directory: bool = False,
    is_symlink: bool = False,
    is_label: bool = False,
    has_password: bool = False,
) -> dict[str, object]:
    """
    Perform the rar header operation under explicit file-format and conversion rules.

    Example:
        Exercise  rar header through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


    :param filename: Filename used for type inference or archive output.
    :param is_directory: Value supplied for is directory under the utility contract.
    :param is_symlink: Value supplied for is symlink under the utility contract.
    :param is_label: Value supplied for is label under the utility contract.
    :param has_password: Value supplied for has password under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return {
        "filename": filename,
        "is_directory": is_directory,
        "is_symlink": is_symlink,
        "is_label": is_label,
        "has_password": has_password,
    }


class _FakeRarFile:
    """
    Provide the fakerarfile contract for validated ebook processing.

    Example:
        Exercise  FakeRarFile through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py
    """
    def __init__(self, entries: list[tuple[dict[str, object], bytes]]) -> None:
        """
        Initialize and validate the fakerarfile state.

        Example:
            Exercise  FakeRarFile.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


        :param entries: Value supplied for entries under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.entries = entries
        self.index = 0
        self.current_item_calls = 0
        self.process_calls: list[tuple[str, bool]] = []

    @property
    def current_item(self):
        """
        Perform the current item operation under explicit file-format and conversion rules.

        Example:
            Exercise  FakeRarFile.current item through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.current_item_calls += 1
        if self.current_item_calls > len(self.entries) + 3:
            raise AssertionError("RAR extraction loop did not advance")
        if self.index >= len(self.entries):
            raise EOFError("End of RAR file")
        return self.entries[self.index][0]

    def process_current_item(self, extract_to=None):
        """
        Perform the process current item operation under explicit file-format and conversion rules.

        Example:
            Exercise  FakeRarFile.process current item through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


        :param extract_to: Value supplied for extract to under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.index >= len(self.entries):
            raise EOFError("End of RAR file")
        header, payload = self.entries[self.index]
        self.index += 1
        self.process_calls.append((str(header["filename"]), extract_to is not None))
        if extract_to is not None:
            extract_to.write(payload)


@pytest.mark.parametrize(
    "member_name",
    [
        "../escape.png",
        "pages/../../escape.png",
        "pages/../escape.png",
        "/absolute.png",
        "C:/absolute.png",
        "C:\\absolute.png",
        "",
        ".",
    ],
)
def test_unrar_safe_path_rejects_unsafe_member_names(tmp_path: Path, member_name: str) -> None:
    """
    Perform the test unrar safe path rejects unsafe member names operation under explicit file-format and conversion rules.

    Example:
        Exercise test unrar safe path rejects unsafe member names through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param member_name: Value supplied for member name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert unrar.safe_path(tmp_path / "extract", member_name) is None


def test_unrar_safe_path_accepts_nested_unicode_member(tmp_path: Path) -> None:
    """
    Perform the test unrar safe path accepts nested unicode member operation under explicit file-format and conversion rules.

    Example:
        Exercise test unrar safe path accepts nested unicode member through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    base = tmp_path / "extract"
    resolved = unrar.safe_path(base, "pages/深/01_世界.png")

    assert resolved is not None
    assert os.path.commonpath([str(base.resolve()), resolved]) == str(base.resolve())
    assert resolved.endswith(os.path.join("pages", "深", "01_世界.png"))


def test_unrar_stream_extract_skips_unsafe_useful_members_and_continues(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """
    Perform the test unrar stream extract skips unsafe useful members and continues operation under explicit file-format and conversion rules.

    Example:
        Exercise test unrar stream extract skips unsafe useful members and continues through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fake = _FakeRarFile(
        [
            (_rar_header("../escape.png"), b"bad"),
            (_rar_header("pages/../still_escape.png"), b"bad"),
            (_rar_header("pages/深/01_世界.png"), b"\x89PNG safe"),
        ]
    )
    monkeypatch.setattr(unrar, "RARFile", lambda stream: fake)

    out_dir = tmp_path / "extract"
    unrar.stream_extract(io.BytesIO(b"Rar!"), out_dir)

    assert not (tmp_path / "escape.png").exists()
    assert not (out_dir / "still_escape.png").exists()
    assert (out_dir / "pages" / "深" / "01_世界.png").read_bytes() == b"\x89PNG safe"
    assert fake.process_calls == [
        ("../escape.png", False),
        ("pages/../still_escape.png", False),
        ("pages/深/01_世界.png", True),
    ]


def test_unrar_stream_extract_skips_non_useful_members(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test unrar stream extract skips non useful members operation under explicit file-format and conversion rules.

    Example:
        Exercise test unrar stream extract skips non useful members through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_rar_extraction_boundary.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fake = _FakeRarFile(
        [
            (_rar_header("pages", is_directory=True), b""),
            (_rar_header("pages/link.png", is_symlink=True), b"symlink target"),
            (_rar_header("pages/locked.png", has_password=True), b"locked"),
            (_rar_header("pages/02.png"), b"\x89PNG safe"),
        ]
    )
    monkeypatch.setattr(unrar, "RARFile", lambda stream: fake)

    out_dir = tmp_path / "extract"
    unrar.stream_extract(io.BytesIO(b"Rar!"), out_dir)

    assert (out_dir / "pages").is_dir()
    assert not (out_dir / "pages" / "link.png").exists()
    assert not (out_dir / "pages" / "locked.png").exists()
    assert (out_dir / "pages" / "02.png").read_bytes() == b"\x89PNG safe"
    assert fake.process_calls == [
        ("pages", False),
        ("pages/link.png", False),
        ("pages/locked.png", False),
        ("pages/02.png", True),
    ]
