"""
Assert exact opaque-key addressing and byte behavior across configured Stores.

The helpers operate on borrowed real backends or test doubles, optionally seed
bytes, and raise ordinary assertions for observed mismatches. They do not own
cleanup or roll back seeded objects. UNICODE_CONTRACT_BACKEND_KINDS records the
expected family coverage set; membership alone is not proof that a backend or
an optional dependency was exercised in a particular test run.
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
    Retain the routed location, metadata, and optional URI observed by a path check. This frozen
    dataclass adds no validation, byte snapshot, or independent evidence checks. The helper returns
    the exact objects it observed; the backend can change after the assertions complete.

    Example:
        >>> result = exercise_unicode_path_case(store, case)  # doctest: +SKIP
        >>> result.location == result.info.location  # doctest: +SKIP
        True


    :ivar location: The single matching inventory Location, also compared with store.locate for the requested key.
    :ivar info: FileInfo returned by stat_file and checked against the expected location, payload size, and optional filename hint.
    :ivar uri: Value returned by location_uri, possibly None unless a URI round trip was required.
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
    Check one exact key through inventory, addressing, metadata, reads, and optional URI parsing.
    Select case.key unless key is supplied, optionally call seed once, then exhaust inventory and
    require exactly one equal key. Compare store.locate with that Location and, when seed returns
    non-None, compare its location attribute too. Stat must agree on Location and byte count; the
    optional filename check always uses case.filename, even for an overridden key.

    Require exact full reads through both Location and FileInfo. A nonempty payload also checks one
    slice starting at byte 1, up to seven bytes long; a one-byte payload checks a zero-length slice
    at EOF. Always call location_uri, optionally requiring a non-None URI that parses back to the
    same Location. These sequential checks acquire no version snapshot and do not exhaust every
    range boundary. Assertion/backend errors propagate, and any seeded bytes remain for caller
    cleanup.

    Example:
        >>> result = exercise_unicode_path_case(store, case, check_uri_round_trip=True)  # doctest: +SKIP


    :param store: Borrowed Store whose inventory, stat, addressing, read, and URI methods are exercised.
    :param case: Expected key/filename and exact payload bytes, without mutation by this helper.
    :param key: Optional exact inventory key override; None uses case.key and an empty string remains an explicit override.
    :param seed: Optional callback called with selected key and payload before inspection; non-None results must expose the expected location.
    :param check_uri_round_trip: Whether to require and parse the URI; location_uri is still called when False.
    :param check_filename_hint: Whether suggested_filename must equal case.filename, independent of a key override.
    :return: A UnicodePathContractResult after all selected assertions pass; seeded objects and backend lifetime remain caller-owned.
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
    Seed all requested cases before checking them in their original order. Materialize cases into a
    tuple first. If seed is supplied, compute each key and seed every payload before any contract
    assertion; discard seed return values. Then recompute each key and invoke the single-case
    checker without seed, so bulk execution does not compare seeder-return locations.

    With both seed and key_for_case, the key callback runs twice per case and must remain consistent
    across the two phases. No cases or keys are deduplicated. A failure can follow earlier
    writes/checks without cleanup or a returned partial result; an empty input returns an empty
    tuple without calling the Store. URI and filename checks use the single-case semantics.

    Example:
        >>> exercise_unicode_path_cases(None, ())
        ()


    :param store: Borrowed Store containing, or receiving, all cases before checks begin.
    :param cases: Iterable eagerly consumed into ordered case references before seeding.
    :param key_for_case: Optional deterministic mapping from each case to its Store key; None uses case.key.
    :param seed: Optional callback receiving key and payload during the initial seed-all phase; its return value is ignored.
    :param check_uri_round_trip: Whether each checker must parse its non-None URI back to the discovered Location.
    :param check_filename_hint: Whether every stat filename hint must match the original case filename.
    :return: A tuple of completed UnicodePathContractResult objects in input order, only after all cases pass.
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
