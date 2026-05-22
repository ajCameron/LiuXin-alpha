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
    return {
        "filename": filename,
        "is_directory": is_directory,
        "is_symlink": is_symlink,
        "is_label": is_label,
        "has_password": has_password,
    }


class _FakeRarFile:
    def __init__(self, entries: list[tuple[dict[str, object], bytes]]) -> None:
        self.entries = entries
        self.index = 0
        self.current_item_calls = 0
        self.process_calls: list[tuple[str, bool]] = []

    @property
    def current_item(self):
        self.current_item_calls += 1
        if self.current_item_calls > len(self.entries) + 3:
            raise AssertionError("RAR extraction loop did not advance")
        if self.index >= len(self.entries):
            raise EOFError("End of RAR file")
        return self.entries[self.index][0]

    def process_current_item(self, extract_to=None):
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
    assert unrar.safe_path(tmp_path / "extract", member_name) is None


def test_unrar_safe_path_accepts_nested_unicode_member(tmp_path: Path) -> None:
    base = tmp_path / "extract"
    resolved = unrar.safe_path(base, "pages/深/01_世界.png")

    assert resolved is not None
    assert os.path.commonpath([str(base.resolve()), resolved]) == str(base.resolve())
    assert resolved.endswith(os.path.join("pages", "深", "01_世界.png"))


def test_unrar_stream_extract_skips_unsafe_useful_members_and_continues(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
