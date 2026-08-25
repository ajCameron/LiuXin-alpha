"""
Backend-neutral assertions for exact Unicode storage paths and bytes.
"""

from __future__ import annotations

import dataclasses

from collections.abc import Callable, Iterable
from typing import Any

from LiuXin_alpha.storage.api import FileInfo, Location, StoreAPI
from tests.fixtures.storage_unicode import StoragePathCase


UNICODE_CONTRACT_BACKEND_KINDS = frozenset(
    {
        "encrypted",
        "filesystem",
        "ftp_readonly",
        "http_readonly",
        "iso_readonly",
        "iso_writable",
        "native_html_readonly",
        "on_disk_calibre_like",
        "on_disk_existing_managed_drive",
        "on_disk_existing_unmanaged_drive",
        "on_disk_flat",
        "rclone_http_readonly",
        "rclone_writable",
        "rar_build",
        "rar_readonly",
        "sevenzip_readonly",
        "s3",
        "single_file_sqlite",
        "squashfs_build",
        "squashfs_readonly",
        "tar_readonly",
        "tar_writable",
        "wget_html_readonly",
        "zip_readonly",
        "zip_writable",
    }
)


@dataclasses.dataclass(slots=True, frozen=True)
class UnicodePathContractResult:
    """
    Values observed while exercising one Unicode path case.

    Example:
        >>> result.location.key  # doctest: +SKIP
        'books/novel.epub'
    """

    location: Location
    info: FileInfo
    uri: str | None


def exercise_unicode_path_case(
    store: StoreAPI,
    case: StoragePathCase,
    *,
    key: str | None = None,
    seed: Callable[[str, bytes], Any] | None = None,
    check_uri_round_trip: bool = False,
    check_filename_hint: bool = True,
) -> UnicodePathContractResult:
    """
    Require exact addressing, inventory, stat, range, and byte behavior.

    Example:
        >>> result = exercise_unicode_path_case(store, case)  # doctest: +SKIP


    :param store:
    :param case:
    :param key:
    :param seed:
    :param check_uri_round_trip:
    :param check_filename_hint:
    :return:
    """

    expected_key = case.key if key is None else key
    stored = None if seed is None else seed(expected_key, case.payload)
    discovered = [
        location
        for location in store.iter_locations()
        if location.key == expected_key
    ]
    assert len(discovered) == 1, (
        f"{store.store_kind} inventory did not return exactly one {expected_key!r}"
    )
    location = discovered[0]
    assert store.locate(expected_key) == location
    if stored is not None:
        assert stored.location == location
    info = store.stat_file(location)
    assert info.location == location
    assert info.size == len(case.payload)
    if check_filename_hint:
        assert info.hints.suggested_filename == case.filename
    assert store.read_file(location) == case.payload
    assert store.read_file(info) == case.payload
    if case.payload:
        offset = min(1, len(case.payload))
        length = min(7, len(case.payload) - offset)
        assert store.read_file(location, offset=offset, length=length) == (
            case.payload[offset : offset + length]
        )
    uri = store.location_uri(location)
    if check_uri_round_trip:
        assert uri is not None
        assert store.location_from_uri(uri) == location
    return UnicodePathContractResult(location=location, info=info, uri=uri)


def exercise_unicode_path_cases(
    store: StoreAPI,
    cases: Iterable[StoragePathCase],
    *,
    key_for_case: Callable[[StoragePathCase], str] | None = None,
    seed: Callable[[str, bytes], Any] | None = None,
    check_uri_round_trip: bool = False,
    check_filename_hint: bool = True,
) -> tuple[UnicodePathContractResult, ...]:
    """
    Exercise several cases already present together in one backend.

    Example:
        >>> results = exercise_unicode_path_cases(store, cases)  # doctest: +SKIP


    :param store:
    :param cases:
    :param key_for_case:
    :param seed:
    :param check_uri_round_trip:
    :param check_filename_hint:
    :return:
    """

    case_list = tuple(cases)
    if seed is not None:
        for case in case_list:
            expected_key = case.key if key_for_case is None else key_for_case(case)
            seed(expected_key, case.payload)
    results = []
    for case in case_list:
        expected_key = case.key if key_for_case is None else key_for_case(case)
        results.append(
            exercise_unicode_path_case(
                store,
                case,
                key=expected_key,
                check_uri_round_trip=check_uri_round_trip,
                check_filename_hint=check_filename_hint,
            )
        )
    return tuple(results)


__all__ = [
    "UNICODE_CONTRACT_BACKEND_KINDS",
    "UnicodePathContractResult",
    "exercise_unicode_path_case",
    "exercise_unicode_path_cases",
]
