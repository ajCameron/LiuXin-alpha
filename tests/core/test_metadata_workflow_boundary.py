"""Exercise native metadata through the same public local and HTTP contracts."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core import CoreHttpDaemon, CoreRuntime
from LiuXin_alpha.core.authorization import CoreTransportPolicy
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.proxies.remote import RemoteCoreClient, RemoteProxyError
from LiuXin_alpha.databases.transaction_participants import after_commit
from tests.databases.caches.test_metadata_api import MODES
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@contextmanager
def public_client(runtime, remote):
    if not remote:
        yield runtime, CoreHandlerError
        return
    daemon = CoreHttpDaemon(
        runtime, policy=CoreTransportPolicy.trusted("core-review-test")
    )
    daemon.start()
    try:
        yield (
            RemoteCoreClient(endpoint=daemon.base_url, bearer_token="core-review-test"),
            RemoteProxyError,
        )
    finally:
        daemon.stop()


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_native_search_select_edit_save_and_readback(
    metadata_db, mode, remote, monkeypatch
):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote) as (client, error_type),
    ):
        created = client.command(
            "catalog.entity.create",
            {"repository": "works", "data": {"title": "Core review workflow"}},
        )
        identity = created["entity_id"]
        page = client.query("metadata.search", {"query": "work.title:workflow"})
        assert page["total_count"] == 1
        assert page["references"] == [{"level": "work", "entity_id": identity}]
        assert page["source"] == ("database" if mode is None else "cache")
        selected = client.query("metadata.read_many", {"entity_ids": [identity]})
        assert selected["entities"][0]["work"]["work_title"] == "Core review workflow"
        snapshot = client.query("metadata.read_for_edit", {"entity_id": identity})
        payload = {
            "entity_id": identity,
            "data": {"title": "Reviewed and saved"},
            "expected_revision": snapshot["revision"],
        }
        original_update = runtime.catalog.works.update
        update = Mock(wraps=original_update)
        monkeypatch.setattr(runtime.catalog.works, "update", update)
        reconcile = Mock(wraps=runtime.services.reconcile)
        monkeypatch.setattr(runtime.services, "reconcile", reconcile)
        result = client.command("metadata.update", payload)
        assert result["result"] == identity and result["persistence"] == "committed"
        assert result["cache"] == ("not_configured" if mode is None else "current")
        with pytest.raises(error_type) as stale:
            client.command("metadata.update", payload)
        assert stale.value.code == "metadata_conflict"
        assert stale.value.details["expected"] == snapshot["revision"]
        assert update.call_count == 1
        saved = client.query("metadata.read_many", {"entity_ids": [identity]})
        assert saved["entities"][0]["work"]["work_title"] == "Reviewed and saved"
        assert (
            client.query("metadata.search", {"query": "work.title:workflow"})[
                "total_count"
            ]
            == 0
        )
        assert (
            client.query("metadata.search", {"query": "work.title:Reviewed"})[
                "total_count"
            ]
            == 1
        )

        def publication_failure():
            raise OSError("publication failed after commit")

        def save_then_fail_publication(*args, **kwargs):
            result = original_update(*args, **kwargs)
            after_commit(metadata_db.macros, publication_failure)
            return result

        with monkeypatch.context() as broken:
            if mode in (None, "database_backed"):
                # These modes have no snapshot reload. Inject a real transaction
                # completion failure rather than breaking an in-memory counter
                # that is also used before persistence and during dirty marking.
                broken.setattr(update, "side_effect", save_then_fail_publication)
            else:
                broken.setattr(
                    runtime.cache,
                    "reload_data",
                    Mock(side_effect=OSError("refresh failed after commit")),
                )
            with pytest.raises(error_type) as committed:
                client.command(
                    "metadata.update",
                    {
                        "entity_id": identity,
                        "data": {"title": "Durable after failure"},
                    },
                )
        assert committed.value.code == "metadata_reconciliation_failed"
        assert committed.value.details["canonical_write_committed"] is True
        assert committed.value.details["receipt"]["result"] == identity
        assert update.call_count == 2
        assert (
            runtime.catalog.works.require(identity)["work_title"]
            == "Durable after failure"
        )
        recovered = client.query("metadata.read_many", {"entity_ids": [identity]})
        assert recovered["entities"][0]["work"]["work_title"] == "Durable after failure"
        reconcile.assert_not_called()


@pytest.mark.parametrize("remote", (False, True))
@pytest.mark.parametrize("during_iteration", (False, True))
def test_required_search_read_failure_is_never_an_empty_success(
    remote, during_iteration
):
    database = object()

    def broken_rows():
        raise OSError("required table unreadable")
        yield  # pragma: no cover

    reader = SimpleNamespace(database=database)
    reader.get_all_rows = (
        Mock(return_value=broken_rows())
        if during_iteration
        else Mock(side_effect=OSError("required table unreadable"))
    )
    with (
        CoreRuntime(
            library=SimpleNamespace(database=database), read_source=reader
        ) as runtime,
        public_client(runtime, remote) as (client, error_type),
    ):
        with pytest.raises(error_type) as failed:
            client.query("search.global", {"text": "anything", "tables": ["works"]})
        assert failed.value.code == "handler_error"
        assert failed.value.details["exception_type"] == "OSError"
