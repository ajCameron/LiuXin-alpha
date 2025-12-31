from __future__ import annotations

import re

import pytest


def test_safe_path_to_name_basic_posix_adds_hash_and_is_stable() -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    a = safe_path_to_name("/var/lib/My Project/file.txt")
    b = safe_path_to_name("/var/lib/My Project/file.txt")

    # Stable across calls
    assert a == b
    # Should be filename-safe-ish
    assert " " not in a
    assert "/" not in a
    assert "\\" not in a
    # Hash suffix present and looks like hex
    assert re.search(r"-[0-9a-f]{10}$", a)


def test_safe_path_to_name_disables_hash() -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    s = safe_path_to_name("/a/b/c.txt", add_hash=False)
    assert "-" not in s  # no appended -{hash}


def test_safe_path_to_name_windows_drive_and_unc_are_tokenized() -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    drive = safe_path_to_name(r"C:\\Users\\Alex\\My File.pdf", add_hash=False, lowercase=True)
    # Drive letter becomes a token
    assert drive.startswith("c__")
    assert "my_file.pdf" in drive

    unc = safe_path_to_name(r"\\\\server\\share\\dir\\file.txt", add_hash=False)
    assert unc.startswith("UNC_server_share__")


def test_safe_path_to_name_reserved_device_names_are_avoided() -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    # If the *entire* name is a reserved word, it should be prefixed.
    s = safe_path_to_name("CON", add_hash=False)
    assert s.startswith("_")


def test_safe_path_to_name_diacritics_stripped_when_unicode_disallowed() -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    s = safe_path_to_name("/tmp/Michaël fällen/naïve.txt", add_hash=False, allow_unicode=False)
    # ASCII-only output
    assert s.isascii()
    assert "michael" in s.lower()


def test_safe_path_to_name_allows_unicode_when_enabled() -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    s = safe_path_to_name("/tmp/Michaël/naïve.txt", add_hash=False, allow_unicode=True)
    assert "ï" in s or "ë" in s


def test_safe_path_to_name_truncates_but_preserves_hash() -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    raw = "/" + "/".join(["longlonglong"] * 50)
    name = safe_path_to_name(raw, max_len=40, hash_len=10, add_hash=True)
    assert len(name) <= 40
    assert re.search(r"-[0-9a-f]{10}$", name)


@pytest.mark.parametrize(
    "kwargs, exc",
    [
        ({"max_len": 7}, ValueError),
        ({"hash_len": 3}, ValueError),
        ({"sep": ""}, ValueError),
    ],
)
def test_safe_path_to_name_validates_parameters(kwargs: dict, exc: type[Exception]) -> None:
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    with pytest.raises(exc):
        safe_path_to_name("/tmp/x", **kwargs)
