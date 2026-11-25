"""Exercise exact recipe execution with real pinned tools and managed byte stores."""

import dataclasses
import hashlib
import shutil
from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.workflows.recreation import recreate_asset, restore_asset
from tests.storage.api2.test_storage_manager_api2 import (
    ARCHIVE_STORE_UUID,
    MAIN_STORE_UUID,
    TransientStorageManager,
    _MemoryStore,
    _simulate_replica_loss,
)


@pytest.fixture
def recipe_setup():
    executable = shutil.which("cp")
    if executable is None:
        pytest.skip("This real-executor fixture requires the POSIX cp tool")
    store = _MemoryStore(MAIN_STORE_UUID)
    manager = TransientStorageManager(
        store_registrations=((store.configuration, store),)
    )
    source = manager.ingest_bytes(b"source bytes")
    # Distinct source/result identities are needed by the provenance registry.
    # The recorded recipe transforms an empty file to the declared exact output.
    result = manager.ingest_bytes(b"")
    binary = Path(executable)
    tool = manager.ingest_bytes(binary.read_bytes()).asset_record
    recipe = api.ReproductionRecipe(
        recipe_type="test-cp",
        reproducibility=api.Reproducibility.EXACT,
        complete=True,
        inputs=(
            api.ReproductionRecipeInputReference(
                0,
                source.asset_record.digital_asset_id,
                source.asset_record.size_bytes,
                source.asset_record.digests,
                "input.bin",
            ),
        ),
        executor=api.ReproductionRecipeArtifactReference(
            "cp",
            api.Digest("sha256", hashlib.sha256(binary.read_bytes()).hexdigest()),
            digital_asset_id=tool.digital_asset_id,
        ),
        command=("cp", "--attributes-only", "input.bin", "output.bin"),
        output_path="output.bin",
        expected_output_size=0,
        expected_output_digests=result.asset_record.digests,
    )
    record = manager.record_digital_asset_derivation(
        api.DigitalAssetDerivationDeclaration(
            result.asset_record.digital_asset_id,
            (
                api.DigitalAssetDerivationSourceReference(
                    0, digital_asset_id=source.asset_record.digital_asset_id
                ),
            ),
            api.DigitalAssetDerivationKind.CONVERT,
            recipe=recipe,
        )
    )
    _simulate_replica_loss(manager, result.replica_record.replica_id)
    return manager, result.asset_record, record, store


def test_real_pinned_execution_restores_the_original_asset(recipe_setup):
    manager, asset, record, store = recipe_setup
    result = recreate_asset(manager, asset.digital_asset_id)
    assert result["completed_derivations"] == [record.digital_asset_derivation_id]
    replica = manager.select_replica(asset.digital_asset_id)
    assert replica.state is api.ReplicaState.VERIFIED
    with manager.get(replica.location) as stream:
        assert stream.read() == b""


def test_wrong_output_never_publishes_a_replacement(recipe_setup, monkeypatch):
    manager, asset, record, store = recipe_setup
    changed = dataclasses.replace(
        record,
        declaration=dataclasses.replace(
            record.declaration,
            recipe=dataclasses.replace(
                record.declaration.recipe, command=("cp", "input.bin", "output.bin")
            ),
        ),
    )
    plan = manager.plan_digital_asset_recreation(asset.digital_asset_id)
    monkeypatch.setattr(
        manager,
        "plan_digital_asset_recreation",
        lambda identity: dataclasses.replace(plan, steps=(changed,)),
    )
    with pytest.raises(api.StoreIntegrityError, match="size"):
        recreate_asset(manager, asset.digital_asset_id)
    assert not manager.plan_digital_asset_recreation(
        asset.digital_asset_id
    ).already_available


def test_restore_streams_and_verifies_without_removing_source():
    source, target = _MemoryStore(MAIN_STORE_UUID), _MemoryStore(ARCHIVE_STORE_UUID)
    manager = TransientStorageManager(
        store_registrations=(
            (source.configuration, source),
            (target.configuration, target),
        )
    )
    original = manager.ingest_bytes(b"retained original")
    restored = restore_asset(
        manager, original.asset_record.digital_asset_id, destination=ARCHIVE_STORE_UUID
    )
    assert restored.state is api.ReplicaState.VERIFIED
    assert restored.digital_asset_id == original.asset_record.digital_asset_id
    with manager.get(original.replica_record.location) as stream:
        assert stream.read() == b"retained original"
