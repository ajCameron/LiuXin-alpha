from __future__ import annotations

from types import SimpleNamespace

import pytest

from LiuXin_alpha.file_formats.archive_preflight import (
    ArchivePreflightError,
    normalized_zip_member_name,
    validate_zip_member_infos,
)


def info(filename, file_size=128, compress_size=64):
    return SimpleNamespace(
        filename=filename,
        file_size=file_size,
        compress_size=compress_size,
    )


def test_zip_member_preflight_returns_normalized_name_map() -> None:
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
    with pytest.raises(ArchivePreflightError, match="unsafe path"):
        normalized_zip_member_name(filename, member_label="fixture archive")


def test_zip_member_preflight_uses_requested_error_type() -> None:
    class FixtureError(ValueError):
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
