"""
Provide test safe path to name utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test safe path to name through a consuming regression::

        python -m pytest -q tests/utils/text/test_safe_path_to_name.py
"""
from __future__ import annotations

import re

import pytest


def test_safe_path_to_name_basic_posix_adds_hash_and_is_stable() -> None:
    """
    Perform the test safe path to name basic posix adds hash and is stable utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name basic posix adds hash and is stable through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test safe path to name disables hash utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name disables hash through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    s = safe_path_to_name("/a/b/c.txt", add_hash=False)
    assert "-" not in s  # no appended -{hash}


def test_safe_path_to_name_windows_drive_and_unc_are_tokenized() -> None:
    """
    Perform the test safe path to name windows drive and unc are tokenized utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name windows drive and unc are tokenized through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    drive = safe_path_to_name(r"C:\\Users\\Alex\\My File.pdf", add_hash=False, lowercase=True)
    # Drive letter becomes a token
    assert drive.startswith("c__")
    assert "my_file.pdf" in drive

    unc = safe_path_to_name(r"\\\\server\\share\\dir\\file.txt", add_hash=False)
    assert unc.startswith("UNC_server_share__")


def test_safe_path_to_name_reserved_device_names_are_avoided() -> None:
    """
    Perform the test safe path to name reserved device names are avoided utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name reserved device names are avoided through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    # If the *entire* name is a reserved word, it should be prefixed.
    s = safe_path_to_name("CON", add_hash=False)
    assert s.startswith("_")


def test_safe_path_to_name_diacritics_stripped_when_unicode_disallowed() -> None:
    """
    Perform the test safe path to name diacritics stripped when unicode disallowed utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name diacritics stripped when unicode disallowed through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    s = safe_path_to_name("/tmp/Michaël fällen/naïve.txt", add_hash=False, allow_unicode=False)
    # ASCII-only output
    assert s.isascii()
    assert "michael" in s.lower()


def test_safe_path_to_name_allows_unicode_when_enabled() -> None:
    """
    Perform the test safe path to name allows unicode when enabled utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name allows unicode when enabled through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    s = safe_path_to_name("/tmp/Michaël/naïve.txt", add_hash=False, allow_unicode=True)
    assert "ï" in s or "ë" in s


def test_safe_path_to_name_truncates_but_preserves_hash() -> None:
    """
    Perform the test safe path to name truncates but preserves hash utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name truncates but preserves hash through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test safe path to name validates parameters utility operation under explicit compatibility rules.

    Example:
        Exercise test safe path to name validates parameters through a consuming regression::

            python -m pytest -q tests/utils/text/test_safe_path_to_name.py


    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :param exc: Value supplied for exc under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

    with pytest.raises(exc):
        safe_path_to_name("/tmp/x", **kwargs)
