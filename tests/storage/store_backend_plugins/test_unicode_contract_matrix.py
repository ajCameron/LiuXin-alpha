"""
Compare registry coverage declarations and run exact Unicode checks on local plugins.

The family-set assertion tracks expected registry membership without executing
every backend. The parametrized behavioral matrix separately exercises four
filesystem-derived plugin families with eight exact key/payload cases, including
URI round trips. Other backend families have their own adjacent contract tests.
"""

from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like import (
    OnDiskCalibreLikeStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_flat import (
    OnDiskFlatStorageBackend,
)
from tests.fixtures.storage_unicode import (
    StoragePathCase,
    TORTURED_UNICODE_PATH_CASES,
)
from tests.storage.contracts.unicode_paths import (
    UNICODE_CONTRACT_BACKEND_KINDS,
    exercise_unicode_path_case,
)


def test_every_registered_backend_kind_has_unicode_contract_coverage() -> None:
    """
    Compare the exact set of default registry kinds with the manually maintained Unicode coverage
    set. A newly registered or removed kind changes this assertion; the test does not discover or
    execute per-backend behavioral tests or verify optional dependency availability.

    Example:
        >>> test_every_registered_backend_kind_has_unicode_contract_coverage()  # doctest: +SKIP


    :return: None when declared coverage and registry kind sets agree.
    """
    assert {descriptor.kind for descriptor in DEFAULT_BACKEND_REGISTRY} == (
        UNICODE_CONTRACT_BACKEND_KINDS
    )


@pytest.mark.parametrize(
    "case",
    TORTURED_UNICODE_PATH_CASES,
    ids=lambda case: case.case_id,
)
@pytest.mark.parametrize(
    "backend_kind",
    (
        "on_disk_existing_managed_drive",
        "on_disk_existing_unmanaged_drive",
        "on_disk_flat",
        "on_disk_calibre_like",
    ),
)
def test_filesystem_backend_kinds_obey_unicode_path_contract(
    tmp_path: pathlib.Path,
    backend_kind: str,
    case: StoragePathCase,
) -> None:
    """
    Exercise one Unicode case against a real filesystem-derived Store. For an unmanaged read-only
    root, create the exact file before constructing the backend. For managed, flat, and Calibre-like
    Stores, construct the backend and seed through store_bytes. Require the shared inventory,
    stat/filename, full/range-read, and URI-round-trip assertions. Each parametrized invocation has
    its own temporary root; no remote service, persistent catalogue, or process restart is involved.

    Example:
        >>> test_filesystem_backend_kinds_obey_unicode_path_contract(tmp_path, backend_kind, case)  # doctest: +SKIP


    :param tmp_path: Temporary directory owning the case's real backend root and bytes.
    :param backend_kind: Parametrized managed, unmanaged, flat, or Calibre-like plugin family.
    :param case: One of the eight exact Unicode path/payload fixtures, with its filename hint preserved.
    :return: None after the selected backend passes the shared Unicode path and URI assertions.
    """
    root = tmp_path / backend_kind
    seed = None
    if backend_kind == "on_disk_existing_unmanaged_drive":
        target = root.joinpath(*case.key.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(case.payload)
        store = OnDiskUnmanagedStorageBackend(root)
    else:
        backend_type = {
            "on_disk_existing_managed_drive": OnDiskExistingManagedStorageBackend,
            "on_disk_flat": OnDiskFlatStorageBackend,
            "on_disk_calibre_like": OnDiskCalibreLikeStorageBackend,
        }[backend_kind]
        store = backend_type(root)
        seed = lambda key, payload: store.store_bytes(payload, location=key)

    exercise_unicode_path_case(
        store,
        case,
        seed=seed,
        check_uri_round_trip=True,
    )
