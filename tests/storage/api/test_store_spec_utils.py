from __future__ import annotations

import json

from dataclasses import dataclass

from LiuXin_alpha.storage.api.info_containers_api import StoreSpec
from LiuXin_alpha.storage.store_spec_utils import store_spec_from_row, store_spec_to_row_dict


def test_store_spec_from_row_parses_mapping_values_and_defaults() -> None:
    spec = store_spec_from_row(
        {
            "store_id": "17",
            "store_uuid": "  ",
            "store_name": "  Primary Store  ",
            "store_kind": "",
            "store_root_uri": "  https://example.invalid/root  ",
            "store_tags_json": '[" sci-fi ", "", "archive"]',
            "store_default_replication_policy_id": "5",
            "store_default_backup_policy_id": "not-an-int",
            "store_supports_active_replica_mode": "yes",
            "store_supports_backup_replica_mode": "off",
            "store_supports_archive_replica_mode": "unknown",
            "store_is_read_only": "1",
            "store_supports_folders": "",
        }
    )

    assert spec.store_id == 17
    assert spec.store_uuid == "store-17"
    assert spec.store_name == "Primary Store"
    assert spec.store_kind == "unknown"
    assert spec.store_root_uri == "https://example.invalid/root"
    assert spec.store_url == "https://example.invalid/root"
    assert spec.store_tags == ("sci-fi", "archive")
    assert spec.store_default_replication_policy_id == 5
    assert spec.store_default_backup_policy_id is None
    assert spec.store_supports_active_replica_mode is True
    assert spec.store_supports_backup_replica_mode is False
    assert spec.store_supports_archive_replica_mode is True
    assert spec.store_is_read_only is True
    assert spec.store_supports_folders is False


@dataclass
class FakeRowObject:
    store_name: str | None = None
    store_kind: str | None = None
    store_root_uri: str | None = None
    store_tags: str | None = None


def test_store_spec_from_row_supports_attribute_rows_and_fallback_id() -> None:
    spec = store_spec_from_row(
        FakeRowObject(
            store_name=None,
            store_kind="  ftp_readonly  ",
            store_root_uri="",
            store_tags="raw-tag-value",
        ),
        fallback_store_id=9,
    )

    assert spec.store_id == 9
    assert spec.store_uuid == "store-9"
    assert spec.store_name == "9"
    assert spec.store_kind == "ftp_readonly"
    assert spec.store_root_uri is None
    assert spec.store_url == ""
    assert spec.store_tags == ("raw-tag-value",)


def test_store_spec_to_row_dict_serializes_fields_and_bools() -> None:
    spec = StoreSpec(
        store_id=3,
        store_uuid="uuid-3",
        store_name="Store Three",
        store_kind="on_disk_flat",
        store_url="file:///fallback",
        store_access_protocol="file",
        store_root_uri="file:///actual",
        store_failure_domain="rack-a",
        store_region="eu-west",
        store_tags=("ebooks", "nightly"),
        store_default_replication_policy_id=11,
        store_default_backup_policy_id=12,
        store_supports_active_replica_mode=True,
        store_supports_backup_replica_mode=False,
        store_supports_archive_replica_mode=True,
        store_operational_role="backup",
        store_is_read_only=True,
        store_supports_folders=False,
        store_policy_json='{"policy":"strict"}',
        store_scratch="/tmp/cache",
    )

    row = store_spec_to_row_dict(spec)

    assert row["store_name"] == "Store Three"
    assert row["store_kind"] == "on_disk_flat"
    assert row["store_root_uri"] == "file:///actual"
    assert json.loads(row["store_tags_json"]) == ["ebooks", "nightly"]
    assert row["store_supports_active_replica_mode"] == 1
    assert row["store_supports_backup_replica_mode"] == 0
    assert row["store_supports_archive_replica_mode"] == 1
    assert row["store_is_read_only"] == 1
    assert row["store_supports_folders"] == 0


def test_store_spec_to_row_dict_filters_allowed_columns_and_skips_none() -> None:
    spec = StoreSpec(
        store_id=None,
        store_uuid=None,
        store_name="Filtered",
        store_kind="native_html_readonly",
        store_url="https://example.invalid/",
        store_access_protocol=None,
        store_root_uri=None,
        store_policy_json=None,
    )

    row = store_spec_to_row_dict(spec, allowed_columns=("store_name", "store_root_uri", "store_kind"))

    assert row == {
        "store_name": "Filtered",
        "store_root_uri": "https://example.invalid/",
        "store_kind": "native_html_readonly",
    }
