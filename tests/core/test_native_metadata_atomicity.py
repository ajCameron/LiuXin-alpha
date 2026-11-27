"""Exercise batch serialization, transaction ownership and competing client edits."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core import CoreRuntime
from tests.core.test_metadata_workflow_boundary import public_client
from tests.core.test_native_metadata_completion import ERRORS, records, result, step
from tests.databases.caches.test_metadata_api import MODES
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_unserializable_batch_result_rolls_back_canonical_edits(
    metadata_db, mode, remote, monkeypatch
):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        original = runtime.services.metadata.works.create

        def invalid_result(data):
            return replace(original(data), result=object())

        with monkeypatch.context() as broken:
            broken.setattr(runtime.services.metadata.works, "create", invalid_result)
            with pytest.raises(ERRORS) as failure:
                client.command(
                    "metadata.batch",
                    {
                        "operations": [
                            step(
                                "bad",
                                "metadata.create",
                                data={"title": "Must roll back"},
                            )
                        ]
                    },
                )
        assert failure.value.details["operation_index"] == 0
        assert failure.value.details["exception_type"] == "CoreWireError"
        assert client.query("metadata.search", {})["total_count"] == 0
        assert not runtime.catalog.works.list()
        result(client, "metadata.create", data={"title": "Healthy after rollback"})
        assert client.query("metadata.search", {})["total_count"] == 1


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("rollback", (False, True))
def test_batch_respects_caller_transaction_and_reconciles_once(
    metadata_db, mode, rollback, monkeypatch
):
    with CoreRuntime(
        library=SimpleNamespace(database=metadata_db), cache_type=mode
    ) as runtime:
        _ = runtime.services.metadata
        reload = None
        if mode in ("schema_backed", "numpy_vectorized"):
            reload = Mock(wraps=runtime.cache.storage.reload)
            monkeypatch.setattr(runtime.cache.storage, "reload", reload)
        try:
            with metadata_db.macros.transaction():
                receipt = runtime.command(
                    "metadata.batch",
                    {
                        "operations": [
                            step(
                                str(i),
                                "metadata.create",
                                data={"title": f"Pending {i}"},
                            )
                            for i in range(3)
                        ]
                    },
                )
                assert receipt["persistence"] == "pending"
                assert receipt["cache"] == (
                    "not_configured" if mode is None else "deferred"
                )
                assert all("persistence" not in entry for entry in receipt["result"])
                if reload is not None:
                    reload.assert_not_called()
                if rollback:
                    raise RuntimeError("caller rolls back")
        except RuntimeError:
            assert rollback
        assert runtime.query("metadata.search", {})["total_count"] == (
            0 if rollback else 3
        )
        if reload is not None:
            assert reload.call_count == 1


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_competing_clients_cannot_both_apply_one_batch_revision(
    metadata_db, mode, remote
):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        work = result(client, "metadata.create", data={"title": "Before"})
        observed = client.query("metadata.read_for_edit", {"entity_id": work})[
            "revision"
        ]
        barrier = Barrier(2)

        def edit(index):
            barrier.wait(timeout=10)
            try:
                client.command(
                    "metadata.batch",
                    {
                        "expected": [observed],
                        "operations": [
                            step(
                                "work",
                                "metadata.update",
                                entity_id=work,
                                data={"title": f"Winner {index}"},
                            ),
                            step(
                                "tag",
                                "metadata.shared.create",
                                kind="tags",
                                data={"name": f"Winner {index}"},
                            ),
                        ],
                    },
                )
                return index, "committed"
            except ERRORS as error:
                return index, error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(edit, (0, 1)))
        assert sorted(state for _, state in outcomes) == [
            "committed",
            "metadata_conflict",
        ]
        winner = next(index for index, state in outcomes if state == "committed")
        assert (
            client.query("metadata.read_for_edit", {"entity_id": work})["fields"][
                "title"
            ]
            == f"Winner {winner}"
        )
        assert [
            record["fields"]["name"]
            for record in records(client, "metadata.shared.list", kind="tags")
        ] == [f"Winner {winner}"]


@pytest.mark.parametrize("mode", (None, "numpy_vectorized"))
def test_full_size_batch_preserves_operation_order_and_all_results(metadata_db, mode):
    with CoreRuntime(
        library=SimpleNamespace(database=metadata_db), cache_type=mode
    ) as runtime:
        receipt = runtime.command(
            "metadata.batch",
            {
                "operations": [
                    step(str(i), "metadata.create", data={"title": f"Bounded {i:03}"})
                    for i in range(100)
                ]
            },
        )
        assert receipt["persistence"] == "committed"
        assert [entry["key"] for entry in receipt["result"]] == [
            str(i) for i in range(100)
        ]
        ids = [entry["result"] for entry in receipt["result"]]
        assert len(set(ids)) == 100
        hydrated = runtime.query("metadata.read_many", {"entity_ids": ids})
        assert [entry["work"]["work_title"] for entry in hydrated["entities"]] == [
            f"Bounded {i:03}" for i in range(100)
        ]


@pytest.mark.parametrize(
    "mode", ("schema_backed", "database_backed", "numpy_vectorized")
)
@pytest.mark.parametrize("remote", (False, True))
def test_schema_verification_failure_preserves_committed_batch_receipt(
    metadata_db, mode, remote, monkeypatch
):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        _ = runtime.services.metadata
        original = metadata_db.driver.schema_read_scope

        @contextmanager
        def changed_schema():
            with original():
                yield
            raise OSError("schema changed before cache publication")

        with monkeypatch.context() as failed:
            failed.setattr(metadata_db.driver, "schema_read_scope", changed_schema)
            with pytest.raises(ERRORS) as error:
                client.command(
                    "metadata.batch",
                    {
                        "operations": [
                            step("saved", "metadata.create", data={"title": "Durable"})
                        ]
                    },
                )
        assert error.value.code == "metadata_reconciliation_failed"
        assert error.value.details["canonical_write_committed"] is True
        receipt = error.value.details["receipt"]
        assert receipt["persistence"] == "committed" and receipt["cache"] == "dirty"
        work = receipt["result"][0]["result"]
        assert runtime.catalog.works.require(work)["work_title"] == "Durable"
        assert (
            client.query("metadata.read_many", {"entity_ids": [work]})["entities"][0][
                "work"
            ]["work_title"]
            == "Durable"
        )
        assert client.query("metadata.search", {})["total_count"] == 1
