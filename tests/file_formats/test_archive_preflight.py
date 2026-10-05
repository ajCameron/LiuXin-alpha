"""
Provide test archive preflight utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test archive preflight through a consuming regression::

        python -m pytest -q tests/file_formats/test_archive_preflight.py
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from LiuXin_alpha.file_formats.archive_preflight import (
    ArchivePreflightError,
    normalized_zip_member_name,
    validate_zip_member_infos,
)


def info(filename, file_size=128, compress_size=64):
    """
    Perform the info operation under explicit file-format and conversion rules.

    Example:
        Exercise info through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :param filename: Filename used for type inference or archive output.
    :param file_size: Value supplied for file size under the utility contract.
    :param compress_size: Value supplied for compress size under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return SimpleNamespace(
        filename=filename,
        file_size=file_size,
        compress_size=compress_size,
    )


def test_zip_member_preflight_returns_normalized_name_map() -> None:
    """
    Perform the test zip member preflight returns normalized name map operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip member preflight returns normalized name map through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert validate_zip_member_infos(
        (
            info("OPS/Text/chapter.xhtml"),
            info("OPS/Images/"),
        ),
        container_label="fixture file",
        member_label="fixture archive",
    ) == {
        "OPS/Text/chapter.xhtml": "OPS/Text/chapter.xhtml",
        "OPS/Images": "OPS/Images/",
    }


@pytest.mark.parametrize(
    "filename",
    (
        "../escape.txt",
        "OPS/../escape.txt",
        "/absolute.txt",
        "C:/absolute.txt",
        "C:\\absolute.txt",
        ".",
        "",
    ),
)
def test_zip_member_preflight_rejects_unsafe_paths(filename: str) -> None:
    """
    Perform the test zip member preflight rejects unsafe paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip member preflight rejects unsafe paths through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :param filename: Filename used for type inference or archive output.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ArchivePreflightError, match="unsafe path"):
        normalized_zip_member_name(filename, member_label="fixture archive")


def test_zip_member_preflight_uses_requested_error_type() -> None:
    """
    Perform the test zip member preflight uses requested error type operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip member preflight uses requested error type through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    class FixtureError(ValueError):
        """
        Report a fixtureerror encountered while processing an ebook format.

        Example:
            Exercise test zip member preflight uses requested error type.FixtureError through a consuming regression::

                python -m pytest -q tests/file_formats/test_archive_preflight.py
        """
        pass

    with pytest.raises(FixtureError, match="fixture file has too many archive members"):
        validate_zip_member_infos(
            (info("a.txt"), info("b.txt")),
            container_label="fixture file",
            member_label="fixture archive",
            error_type=FixtureError,
            max_archive_members=1,
        )


def test_zip_member_preflight_can_preserve_skip_unsafe_policy() -> None:
    """
    Perform the test zip member preflight can preserve skip unsafe policy operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip member preflight can preserve skip unsafe policy through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    names = validate_zip_member_infos(
        (
            info("Pictures/valid.png"),
            info("Pictures/../../escape.txt"),
        ),
        container_label="fixture file",
        member_label="fixture archive",
        allow_unsafe_paths=True,
    )

    assert names == {"Pictures/valid.png": "Pictures/valid.png"}


def test_zip_member_preflight_still_budgets_skipped_unsafe_paths() -> None:
    """
    Perform the test zip member preflight still budgets skipped unsafe paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip member preflight still budgets skipped unsafe paths through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ArchivePreflightError, match="member is too large"):
        validate_zip_member_infos(
            (info("../escape.bin", file_size=2048, compress_size=64),),
            container_label="fixture file",
            member_label="fixture archive",
            allow_unsafe_paths=True,
            max_member_uncompressed_size=1024,
        )


@pytest.mark.parametrize(
    ("attrs", "match"),
    (
        ({"file_size": 2048, "compress_size": 64}, "member is too large"),
        ({"file_size": 2048, "compress_size": 64}, "expands to too much data"),
        ({"file_size": 128, "compress_size": 0}, "invalid compressed size"),
        ({"file_size": 128 * 1024, "compress_size": 128}, "suspicious compression ratio"),
    ),
)
def test_zip_member_preflight_rejects_archive_budget_shapes(attrs: dict[str, int], match: str) -> None:
    """
    Perform the test zip member preflight rejects archive budget shapes operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip member preflight rejects archive budget shapes through a consuming regression::

            python -m pytest -q tests/file_formats/test_archive_preflight.py


    :param attrs: Value supplied for attrs under the utility contract.
    :param match: Value supplied for match under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    kwargs = {
        "max_member_uncompressed_size": 1024,
        "max_total_uncompressed_size": 1024 * 1024,
        "max_compression_ratio": 20,
        "min_compression_ratio_check_size": 32 * 1024,
    }
    if match == "expands to too much data":
        kwargs["max_member_uncompressed_size"] = 4096
        kwargs["max_total_uncompressed_size"] = 1024
    if match == "suspicious compression ratio":
        kwargs["max_member_uncompressed_size"] = 256 * 1024
        kwargs["max_total_uncompressed_size"] = 512 * 1024

    with pytest.raises(ArchivePreflightError, match=match):
        validate_zip_member_infos(
            (info("payload.bin", **attrs),),
            container_label="fixture file",
            member_label="fixture archive",
            **kwargs,
        )
