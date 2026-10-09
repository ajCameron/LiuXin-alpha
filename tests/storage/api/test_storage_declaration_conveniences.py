"""
Verify storage convenience façades construct declarations and delegate once.
"""

from unittest.mock import Mock, call, sentinel
from uuid import UUID

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.api.storage_manager_api.convenience_api import (
    StorageConvenienceBase,
)


def test_repository_convenience_apis_are_public_exports() -> None:
    """
    Keep all four typed repository convenience surfaces at the broad API boundary.

    Example:
        >>> test_repository_convenience_apis_are_public_exports()


    :return: None after the expected public names and objects are verified.
    """

    names = {
        "CompositeDigitalAssetRepositoryConvenienceAPI",
        "DigitalAssetDerivationRepositoryConvenienceAPI",
        "DigitalAssetRepositoryConvenienceAPI",
        "ReplicaRepositoryConvenienceAPI",
    }

    assert names <= set(api.__all__)
    assert all(getattr(api, name).__name__ == name for name in names)


def test_database_repositories_expose_the_convenience_protocols() -> None:
    """
    Keep shipped persistence adapters on the extended repository surfaces.

    Import implementations locally so API-module collection does not eagerly load the storage
    manager before namespace-isolation tests run.

    Example:
        >>> test_database_repositories_expose_the_convenience_protocols()


    :return: None after each concrete repository is paired with its convenience protocol.
    """

    from LiuXin_alpha.storage.storage_manager.database_unit_of_work import (
        DatabaseCompositeRepository,
        DatabaseDerivationRepository,
        DatabaseDigitalAssetRepository,
        DatabaseReplicaRepository,
    )

    pairs = (
        (DatabaseDigitalAssetRepository, api.DigitalAssetRepositoryConvenienceAPI),
        (DatabaseReplicaRepository, api.ReplicaRepositoryConvenienceAPI),
        (
            DatabaseCompositeRepository,
            api.CompositeDigitalAssetRepositoryConvenienceAPI,
        ),
        (
            DatabaseDerivationRepository,
            api.DigitalAssetDerivationRepositoryConvenienceAPI,
        ),
    )

    assert all(issubclass(repository, protocol) for repository, protocol in pairs)
    assert all(callable(repository.add) for repository, _protocol in pairs)
    assert all(not hasattr(repository, "create") for repository, _protocol in pairs)
    assert callable(DatabaseCompositeRepository.replace)
    assert not hasattr(DatabaseCompositeRepository, "replace_values")


def test_database_repository_facades_reach_renamed_concrete_operations() -> None:
    """
    Exercise inherited add and replace façades against every affected concrete adapter.

    The lower metadata repository is a recording stand-in: it allocates identities and accepts
    upserts without opening a database transaction. This verifies concrete method resolution and
    declaration translation, not durability or reference validation.

    Example:
        >>> test_database_repository_facades_reach_renamed_concrete_operations()


    :return: None after Replica, Composite, and derivation records reach their lower upserts.
    """

    from LiuXin_alpha.storage.storage_manager.database_unit_of_work import (
        DatabaseCompositeRepository,
        DatabaseDerivationRepository,
        DatabaseReplicaRepository,
    )

    metadata_repository = Mock()
    metadata_repository.allocate_record_id.side_effect = [12, 3, 4]
    location = api.Location(UUID(int=1), "objects/book")

    replica_repository = DatabaseReplicaRepository(metadata_repository)
    replica = replica_repository.add(api.DigitalAssetID(7), location)

    assert replica.replica_id == 12
    assert replica.digital_asset_id == 7
    assert replica.location == location
    metadata_repository.upsert_replica.assert_called_once_with(replica)

    membership = api.CompositeDigitalAssetMembership(api.DigitalAssetID(7), 0)
    composite_repository = DatabaseCompositeRepository(metadata_repository)
    composite = composite_repository.add((membership,), name="book")
    metadata_repository.get_composite.return_value = composite
    replacement = composite_repository.replace(
        composite.composite_digital_asset_id,
        (membership,),
        name="renamed book",
        if_revision=composite.revision,
    )

    assert replacement.composite_digital_asset_id == 3
    assert replacement.name == "renamed book"
    assert metadata_repository.upsert_composite.call_args_list == [
        call(composite),
        call(replacement),
    ]

    source = api.DigitalAssetDerivationSourceReference(
        0,
        digital_asset_id=api.DigitalAssetID(7),
    )
    derivation_repository = DatabaseDerivationRepository(metadata_repository)
    derivation = derivation_repository.add(
        api.DigitalAssetID(8),
        (source,),
        api.DigitalAssetDerivationKind.EXTRACT,
    )

    assert derivation.digital_asset_derivation_id == 4
    assert derivation.declaration.sources == (source,)
    metadata_repository.upsert_derivation.assert_called_once_with(derivation)


def test_repository_add_facades_delegate_asset_and_replica_declarations() -> None:
    """
    Check Asset and Replica repository conveniences without invoking persistence.

    Example:
        >>> test_repository_add_facades_delegate_asset_and_replica_declarations()


    :return: None after exact declaration delegation is verified.
    """

    asset_repository = Mock()
    asset_repository.add_from_declaration.return_value = sentinel.asset_record
    digest = api.Digest("sha256", "abcd")
    metadata = api.DigitalAssetMetadata(name="book")

    result = api.DigitalAssetRepositoryConvenienceAPI.add(
        asset_repository,
        4,
        [digest],
        metadata=metadata,
        replication_policy_id=api.ReplicationPolicyID(2),
        backup_policy_id=api.BackupPolicyID(3),
    )

    assert result is sentinel.asset_record
    asset_repository.add_from_declaration.assert_called_once_with(
        api.DigitalAssetDeclaration(
            4,
            (digest,),
            metadata,
            api.ReplicationPolicyID(2),
            api.BackupPolicyID(3),
        )
    )

    replica_repository = Mock()
    replica_repository.add_from_declaration.return_value = sentinel.replica_record
    location = api.Location(UUID(int=1), "objects/book")
    observation = api.ReplicaObservation(
        api.ReplicaState.PRESENT,
        observed_size_bytes=4,
    )

    result = api.ReplicaRepositoryConvenienceAPI.add(
        replica_repository,
        api.DigitalAssetID(7),
        location,
        mode=api.ReplicaMode.BACKUP,
        observation=observation,
    )

    assert result is sentinel.replica_record
    replica_repository.add_from_declaration.assert_called_once_with(
        api.ReplicaDeclaration(
            api.DigitalAssetID(7),
            location,
            mode=api.ReplicaMode.BACKUP,
            observation=observation,
        )
    )


def test_composite_repository_facades_delegate_complete_declarations() -> None:
    """
    Check Composite creation and replacement share their precise repository paths.

    Example:
        >>> test_composite_repository_facades_delegate_complete_declarations()


    :return: None after add and replacement declarations are compared.
    """

    repository = Mock()
    repository.add_from_declaration.return_value = sentinel.created
    repository.replace_from_declaration.return_value = sentinel.replaced
    membership = api.CompositeDigitalAssetMembership(api.DigitalAssetID(7), 0)
    declaration = api.CompositeDigitalAssetDeclaration(
        (membership,),
        name="book",
        attributes=(("edition", "first"),),
    )

    created = api.CompositeDigitalAssetRepositoryConvenienceAPI.add(
        repository,
        [membership],
        name="book",
        attributes=[("edition", "first")],
    )
    replaced = api.CompositeDigitalAssetRepositoryConvenienceAPI.replace(
        repository,
        api.CompositeDigitalAssetID(3),
        [membership],
        name="book",
        attributes=[("edition", "first")],
        if_revision="c-old",
    )

    assert created is sentinel.created
    assert replaced is sentinel.replaced
    repository.add_from_declaration.assert_called_once_with(declaration)
    repository.replace_from_declaration.assert_called_once_with(
        api.CompositeDigitalAssetID(3),
        declaration,
        if_revision="c-old",
    )


def test_derivation_repository_add_delegates_complete_provenance() -> None:
    """
    Check derivation convenience materializes sources into one declaration.

    Example:
        >>> test_derivation_repository_add_delegates_complete_provenance()


    :return: None after the derivation declaration and result are verified.
    """

    repository = Mock()
    repository.add_from_declaration.return_value = sentinel.derivation_record
    source = api.DigitalAssetDerivationSourceReference(
        0,
        digital_asset_id=api.DigitalAssetID(7),
        role="source",
    )

    result = api.DigitalAssetDerivationRepositoryConvenienceAPI.add(
        repository,
        api.DigitalAssetID(8),
        [source],
        api.DigitalAssetDerivationKind.EXTRACT,
        operator="LiuXin",
        workflow_reference="conversion:42",
    )

    assert result is sentinel.derivation_record
    repository.add_from_declaration.assert_called_once_with(
        api.DigitalAssetDerivationDeclaration(
            result_digital_asset_id=api.DigitalAssetID(8),
            sources=(source,),
            kind=api.DigitalAssetDerivationKind.EXTRACT,
            operator="LiuXin",
            workflow_reference="conversion:42",
        )
    )


def test_manager_replace_composite_facade_delegates_declaration() -> None:
    """
    Check ordinary Composite inputs reach the existing declaration operation.

    Example:
        >>> test_manager_replace_composite_facade_delegates_declaration()


    :return: None after identity normalization and declaration delegation are verified.
    """

    manager = Mock()
    manager.replace_composite_digital_asset.return_value = sentinel.composite

    result = StorageConvenienceBase.replace_composite(
        manager,
        api.CompositeDigitalAssetID(3),
        {"book.epub": api.DigitalAssetID(7)},
        name="package",
        attributes={"edition": "first"},
        if_revision="c-old",
    )

    assert result is sentinel.composite
    manager.replace_composite_digital_asset.assert_called_once_with(
        api.CompositeDigitalAssetID(3),
        api.CompositeDigitalAssetDeclaration(
            (
                api.CompositeDigitalAssetMembership(
                    api.DigitalAssetID(7),
                    0,
                    logical_path="book.epub",
                ),
            ),
            name="package",
            attributes=(("edition", "first"),),
        ),
        if_revision="c-old",
    )


def test_backup_workflow_facades_delegate_durable_intent() -> None:
    """
    Check workflow construction and persistence use one declaration representation.

    Example:
        >>> test_backup_workflow_facades_delegate_durable_intent()


    :return: None after workflow reconstruction and persistence delegation are verified.
    """

    source = api.BackupSourceDeclaration(
        api.BackupSourceKind.LOCAL_PATH,
        "/books/book.epub",
        archive_path="books/book.epub",
    )
    expected = api.BackupWorkflowDeclaration(
        "nightly",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        "nightly.sqsh",
        sources=(source,),
        options=(("compression", "zstd"),),
    )
    workflow_type = type(
        "RecordingWorkflow",
        (),
        {"from_declaration": Mock(return_value=sentinel.workflow)},
    )
    create = api.BackupWorkflowAPI.__dict__["create"].__func__

    workflow = create(
        workflow_type,
        "nightly",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        "nightly.sqsh",
        sources=[source],
        options={"compression": "zstd"},
        storage_manager=sentinel.manager,
    )

    assert workflow is sentinel.workflow
    workflow_type.from_declaration.assert_called_once_with(
        expected,
        storage_manager=sentinel.manager,
    )

    repository = Mock()
    repository.save_workflow_declaration.return_value = api.WorkflowID(9)
    workflow_id = api.BackupWorkflowRepositoryAPI.save_workflow(
        repository,
        "nightly",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        "nightly.sqsh",
        sources=[source],
        options={"compression": "zstd"},
    )

    assert workflow_id == 9
    repository.save_workflow_declaration.assert_called_once_with(
        expected,
        workflow_id=None,
        status=api.WorkflowStatus.DRAFT,
    )


def test_backup_source_presence_facade_delegates_source_declaration() -> None:
    """
    Check backup presence convenience constructs the retained source evidence.

    Example:
        >>> test_backup_source_presence_facade_delegates_source_declaration()


    :return: None after source evidence and presence options are verified.
    """

    repository = Mock()
    repository.record_backup_presence.return_value = True

    created = api.BackupWorkflowRepositoryAPI.record_backup_source_presence(
        repository,
        api.WorkflowID(9),
        sentinel.registration,
        api.BackupSourceKind.LOCAL_PATH,
        "/books/book.epub",
        archive_path="books/book.epub",
        expected_size=4,
        protected=False,
    )

    assert created is True
    repository.record_backup_presence.assert_called_once_with(
        api.WorkflowID(9),
        sentinel.registration,
        api.BackupSourceDeclaration(
            api.BackupSourceKind.LOCAL_PATH,
            "/books/book.epub",
            archive_path="books/book.epub",
            expected_size=4,
        ),
        archive_path="books/book.epub",
        protected=False,
        immutable=True,
    )
