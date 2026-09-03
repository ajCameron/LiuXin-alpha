"""Public convenience construction for durable Store configuration."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api


def test_filesystem_configuration_accepts_paths_and_preserves_rich_fields(
    tmp_path: Path,
) -> None:
    store_ref = uuid4()
    host_ref = uuid4()
    device_ref = uuid4()
    root = tmp_path / "tortured 😀 Store"

    configuration = api.StoreConfiguration.filesystem(
        "primary",
        root,
        store_uuid=store_ref,
        failure_domain="local-disk",
        region="home",
        host=host_ref,
        device=device_ref,
        tags={"fast", "local"},
        modes=("active", "backup"),
        operational_role="live",
        options={"allocation_prefix": "managed"},
    )

    assert configuration.store_uuid == store_ref
    assert configuration.store_name == "primary"
    assert configuration.store_kind == "filesystem"
    assert configuration.store_access_protocol == "file"
    assert configuration.store_root_uri == root.resolve().as_uri()
    assert configuration.store_failure_domain == "local-disk"
    assert configuration.store_region == "home"
    assert configuration.store_host_uuid == host_ref
    assert configuration.store_device_uuid == device_ref
    assert set(configuration.store_tags) == {"fast", "local"}
    assert configuration.supported_replica_modes == {
        api.ReplicaMode.ACTIVE,
        api.ReplicaMode.BACKUP,
    }
    assert configuration.operational_role == "live"
    assert configuration.backend_options == (("allocation_prefix", "managed"),)


def test_filesystem_configuration_normalizes_plain_paths_and_checks_schemes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "plain path"
    from_plain_text = api.StoreConfiguration.filesystem("plain", str(root))
    from_file_uri = api.StoreConfiguration.filesystem(
        "uri",
        root.resolve().as_uri(),
    )

    assert from_plain_text.store_root_uri == root.resolve().as_uri()
    assert from_file_uri.store_root_uri == root.resolve().as_uri()
    assert from_plain_text.store_uuid != from_file_uri.store_uuid
    with pytest.raises(ValueError, match="local path or file URI"):
        api.StoreConfiguration.filesystem("remote", "https://example.test/root")


def test_generic_configuration_factory_preserves_remote_endpoints() -> None:
    configuration = api.StoreConfiguration.for_backend(
        "archive",
        "s3",
        "s3://books/archive",
        protocol="s3",
        read_only=True,
        folders=False,
        options=(
            ("endpoint_url", "https://objects.example.test"),
            ("addressing_style", "path"),
        ),
    )

    assert configuration.store_root_uri == "s3://books/archive"
    assert configuration.store_access_protocol == "s3"
    assert configuration.read_only
    assert not configuration.supports_folders
    assert configuration.backend_options == (
        ("endpoint_url", "https://objects.example.test"),
        ("addressing_style", "path"),
    )
