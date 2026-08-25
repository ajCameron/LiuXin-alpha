"""Apply the generic Unicode contract to filesystem-derived backend kinds."""

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
