"""Prove command identities survive response loss, rollback and runtime reopening."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreError
from LiuXin_alpha.databases import command_journal
from tests.core.test_metadata_workflow_boundary import public_client
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize(
    "mode", (None, "schema_backed", "database_backed", "numpy_vectorized")
)
@pytest.mark.parametrize("remote", (False, True))
def test_response_loss_and_reopen_do_not_repeat_create(metadata_db, mode, remote):
    command = CoreCommand("metadata.create", {"data": {"title": "Only once"}})
    for _reopening in range(2):
        with CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime:
            with public_client(runtime, remote, timeout_seconds=120) as (client, _):
                first = client.execute_command(command).result
                second = client.execute_command(command).result
                assert first["result"] == second["result"]
                assert client.query("metadata.search", {})["total_count"] == 1
                outcome = client.query(
                    "commands.outcome", {"command_id": command.command_id}
                )
                assert outcome["effects"] == "committed"
                assert outcome["result"]["result"] == first["result"]
                assert outcome["cache_observation"] == "historical"


def test_conflicting_identity_is_rejected_before_second_effect(metadata_db):
    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        command = CoreCommand("metadata.create", {"data": {"title": "First"}})
        runtime.execute_command(command)
        with pytest.raises(CoreError) as error:
            runtime.execute_command(
                replace(command, payload={"data": {"title": "Second"}})
            )
        assert error.value.code == "command_conflict"
        assert len(runtime.catalog.works.list()) == 1


@pytest.mark.parametrize("commit", (False, True))
def test_outer_transaction_owns_mutation_and_receipt(metadata_db, commit):
    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        command = CoreCommand("metadata.create", {"data": {"title": "Pending"}})
        try:
            with metadata_db.macros.transaction():
                value = runtime.execute_command(command).result
                assert value["persistence"] == "pending"
                if not commit:
                    raise RuntimeError("rollback")
        except RuntimeError:
            assert not commit
        outcome = runtime.query("commands.outcome", {"command_id": command.command_id})
        assert outcome["state"] == ("succeeded" if commit else "not_found")
        runtime.execute_command(command)
        assert len(runtime.catalog.works.list()) == 1


@pytest.mark.parametrize(
    "mode", (None, "schema_backed", "database_backed", "numpy_vectorized")
)
def test_failed_result_encoding_rolls_back_and_replays_rejection(
    metadata_db, monkeypatch, mode
):
    with CoreRuntime(
        library=SimpleNamespace(database=metadata_db), cache_type=mode
    ) as runtime:
        command = CoreCommand("metadata.create", {"data": {"title": "No effect"}})
        original = runtime.services.metadata.works.create
        broken = Mock(side_effect=lambda data: replace(original(data), result=object()))
        monkeypatch.setattr(runtime.services.metadata.works, "create", broken)
        for _ in range(2):
            with pytest.raises(CoreError) as error:
                runtime.execute_command(command)
            assert error.value.details["effects"] == "none"
        assert broken.call_count == 1
        assert not runtime.catalog.works.list()
        outcome = runtime.query("commands.outcome", {"command_id": command.command_id})
        assert outcome["state"] == "failed"


def test_lookup_is_passive_and_unknown_is_not_rollback_evidence(metadata_db):
    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        assert runtime.query("commands.outcome", {"command_id": "unknown"}) == {
            "command_id": "unknown",
            "state": "not_found",
            "effects": "unknown",
        }
        with metadata_db.macros.read_transaction() as connection:
            assert not command_journal.present(connection)


def test_input_is_detached_before_started_subscribers(metadata_db):
    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        payload = {"data": {"title": "Before"}}
        command = CoreCommand("metadata.create", payload)
        runtime.subscribe(lambda event: payload["data"].update(title="After"))
        value = runtime.execute_command(command).result
        assert (
            runtime.query("metadata.read_for_edit", {"entity_id": value["result"]})[
                "fields"
            ]["title"]
            == "Before"
        )


def test_pruned_identity_never_executes_again(metadata_db):
    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        command = CoreCommand("metadata.create", {"data": {"title": "Retained"}})
        runtime.execute_command(command)
        runtime.command("commands.prune", {"command_ids": [command.command_id]})
        with pytest.raises(CoreError) as failure:
            runtime.execute_command(command)
        assert failure.value.code == "command_result_pruned"
        assert len(runtime.catalog.works.list()) == 1
        assert (
            runtime.query("commands.outcome", {"command_id": command.command_id})[
                "effects"
            ]
            == "committed"
        )


def test_pending_lookup_never_claims_commit(metadata_db):
    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        command = CoreCommand("metadata.create", {"data": {"title": "Pending"}})
        with metadata_db.macros.transaction():
            runtime.execute_command(command)
            assert (
                runtime.query("commands.outcome", {"command_id": command.command_id})[
                    "effects"
                ]
                == "pending"
            )


def test_committed_refresh_failure_remains_recoverable(metadata_db, monkeypatch):
    with CoreRuntime(
        library=SimpleNamespace(database=metadata_db), cache_type="schema_backed"
    ) as runtime:
        _ = runtime.services.metadata
        command = CoreCommand("metadata.create", {"data": {"title": "Committed"}})
        with monkeypatch.context() as broken:
            broken.setattr(
                runtime.cache,
                "reload_data",
                Mock(side_effect=OSError("publication fails")),
            )
            with pytest.raises(CoreError) as failure:
                runtime.execute_command(command)
            assert failure.value.details["canonical_write_committed"] is True
            assert (
                runtime.query("commands.outcome", {"command_id": command.command_id})[
                    "effects"
                ]
                == "committed"
            )
        runtime.execute_command(command)
        assert len(runtime.catalog.works.list()) == 1
