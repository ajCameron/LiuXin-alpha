"""
Characterize program-service coercion, projection, jobs, storage repair, and mutation/error boundaries.

Recording doubles isolate the adapters from real jobs, databases, Stores, and
network access. These tests exercise documented behavior rather than interpreting
an adapter's optimistic receipt flags as independent persistence verification.
"""

import importlib
import inspect
import re
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import BinaryIO
from unittest.mock import MagicMock, Mock
from uuid import UUID

import pytest

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services import (
    catalog,
    database,
    discovery,
    metadata,
    payloads,
    storage_integrity,
    storage_recovery,
    storage_repair,
    storage_status,
    stores,
)
from LiuXin_alpha.storage.api import ReplicaMode, ReplicaState, StoreConfiguration


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ([" a ", "", "a", "A", None], ["a", "A", "None"]),
        ("  a b  ", ["a b"]),
        (b"AB", ["65", "66"]),
        ([], []),
    ],
)
def test_text_lists_stringify_deduplicate_and_keep_case(
    value: object, expected: list[str]
) -> None:
    """
    Retain the helper's distinct scalar-string, byte-iterable, blank, duplicate, and case behavior.

    Example:
        >>> test_text_lists_stringify_deduplicate_and_keep_case(b"AB", ["65", "66"])


    :param value: Parametrized text or iterable field presented to _text_list.
    :param expected: Exact ordered normalized strings expected from that field.
    :return: None after checking normalization and rejection of Mapping/None fields.
    """
    assert payloads._text_list({"tags": value}, "tags") == expected
    for invalid in ({"name": "not an array"}, None):
        with pytest.raises(CoreDispatchError):
            payloads._text_list({"tags": invalid}, "tags")


def test_plain_uses_first_accepted_metadata_adapter() -> None:
    """
    Skip a non-Mapping adapter result, accept the next Mapping, and avoid lower-priority projections.

    Example:
        >>> test_plain_uses_first_accepted_metadata_adapter()


    :return: None after checking adapter invocation order, Mapping recursion, and untouched later adapters.
    """
    first = Mock(return_value=["not a mapping"])
    second = Mock(return_value={"tags": ("a", "b")})
    later = Mock(return_value={"wrong": True})
    source = SimpleNamespace(
        to_mapping=first,
        to_dict=second,
        to_calibre=later,
        row_dict={"also_wrong": True},
    )
    assert payloads.plain(source) == {"tags": ["a", "b"]}
    first.assert_called_once_with()
    second.assert_called_once_with()
    later.assert_not_called()
    assert payloads._metadata_projection(
        SimpleNamespace(to_calibre=Mock(return_value=None))
    ) == (
        True,
        None,
    )


def test_plain_does_not_swallow_metadata_adapter_errors() -> None:
    """
    Propagate a present adapter's exception instead of silently retrying another projection.

    Example:
        >>> test_plain_does_not_swallow_metadata_adapter_errors()


    :return: None after the first adapter failure is preserved and the fallback remains uncalled.
    """
    fallback = Mock(return_value={"title": "fallback"})
    source = SimpleNamespace(
        to_mapping=Mock(side_effect=ValueError("invalid metadata")),
        to_dict=fallback,
    )
    with pytest.raises(ValueError, match="invalid metadata"):
        payloads.plain(source)
    fallback.assert_not_called()


def test_plain_retains_opaque_values_and_filters_attribute_fallback() -> None:
    """
    Preserve opaque/byte objects while stringifying Mapping keys and filtering fallback attributes.

    Example:
        >>> test_plain_retains_opaque_values_and_filters_attribute_fallback()


    :return: None after identity, colliding-key precedence, and private/callable attribute filtering checks.
    """
    opaque = object()
    content = b"abc"
    assert payloads.plain(opaque) is opaque
    assert payloads.plain(content) is content
    assert payloads.plain({1: "first", "1": "second"}) == {"1": "second"}
    source = SimpleNamespace(public=("a", "b"), _private="hidden", callback=Mock())
    assert payloads.plain(source) == {"public": ["a", "b"]}


def test_job_submission_receipt_reports_requested_options() -> None:
    """
    Distinguish requested job settings from effective backend choice or completion status.

    The recording manager accepts everything: explicit None label becomes text,
    a false timeout becomes 0.0, and a nonempty no-output string becomes True.

    Example:
        >>> test_job_submission_receipt_reports_requested_options()


    :return: None after verifying worker identity, shallow argument copying, options, and default timeout sentinel.
    """
    submit = Mock(return_value="job-7")
    runtime = SimpleNamespace(job_manager=SimpleNamespace(submit=submit))
    arguments = {"formats": ["EPUB"]}
    receipt = payloads._job_submit(
        runtime,
        {
            "label": None,
            "job_timeout_s": False,
            "job_backend": "thread",
            "job_no_output": "false",
        },
        function_name="run_conversion_job",
        kwargs=arguments,
        default_label="convert",
    )
    request = submit.call_args.args[0]
    assert request.module_name == "LiuXin_alpha.core.workflow_jobs"
    assert request.function_name == "run_conversion_job"
    assert request.kwargs == arguments and request.kwargs is not arguments
    assert request.kwargs["formats"] is arguments["formats"]
    assert submit.call_args.kwargs == {
        "timeout": 0.0,
        "no_output": True,
        "backend": "thread",
        "label": "None",
    }
    assert receipt == {
        "job_id": "job-7",
        "label": "None",
        "backend": "thread",
        "timeout_s": 0.0,
        "no_output": True,
    }
    default = payloads._job_submit(
        runtime,
        {},
        function_name="run_conversion_job",
        kwargs={},
        default_label="convert",
    )
    assert default["timeout_s"] is None and default["backend"] == ""
    assert submit.call_args.kwargs["timeout"] == -1.0


@pytest.mark.parametrize("offset", [0, 1])
def test_recovery_paging_trims_only_requested_state(offset: int) -> None:
    """
    Filter normalized request state against unstripped record state and cap the page independently of total.

    Example:
        >>> test_recovery_paging_trims_only_requested_state(1)


    :param offset: Start at the one matching row or just beyond the filtered end.
    :return: None after filtered total, capped bounds, end-of-list completeness, and null-limit assertions are checked.
    """
    manager = Mock()
    manager.list_ingest_operations.return_value = [
        {"state": "pending", "id": 1},
        {"state": " pending ", "id": 2},
        {"state": "done", "id": 3},
    ]
    runtime = SimpleNamespace(library=SimpleNamespace(storage=manager))
    query = SimpleNamespace(
        payload={"state": " PENDING ", "limit": 20_000, "offset": offset}
    )
    result = storage_recovery.storage_recovery_list(runtime, query)
    assert result["total"] == 1 and result["limit"] == 10_000
    assert result["offset"] == offset and result["complete"] is True
    assert result["operations"] == (
        [{"state": "pending", "id": 1}] if offset == 0 else []
    )
    with pytest.raises(AssertionError):
        storage_recovery.storage_recovery_list(
            runtime, SimpleNamespace(payload={"limit": None})
        )


def test_store_update_checks_topology_before_write_but_status_afterward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Reject topology edits with live claims and preserve a post-update status failure for an allowed name edit.

    Both mutations use recording doubles; no actual Store or canonical row is changed.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_store_update_checks_topology_before_write_but_status_afterward(patch)


    :param monkeypatch: Fixture replacing shared durable Store resolution for this test only.
    :return: None after checking no update on unsafe topology and an already-called update before status failure.
    """
    configuration = StoreConfiguration(
        UUID(int=1), "archive", "memory", "memory://archive"
    )
    monkeypatch.setattr(
        stores, "_store_configuration", Mock(return_value=configuration)
    )
    manager = Mock()
    manager.iter_replica_records.return_value = [
        SimpleNamespace(state=ReplicaState.VERIFIED)
    ]
    manager.update_store.return_value = configuration
    manager.get_store.return_value.status.side_effect = RuntimeError(
        "status failed after update"
    )
    runtime = SimpleNamespace(library=SimpleNamespace(storage=manager))
    with pytest.raises(CoreDispatchError, match="live Replica claims"):
        stores.storage_store_update(
            runtime,
            SimpleNamespace(
                payload={"store": "archive", "changes": {"root": "memory://new"}}
            ),
        )
    manager.update_store.assert_not_called()
    with pytest.raises(RuntimeError, match="status failed after update"):
        stores.storage_store_update(
            runtime,
            SimpleNamespace(
                payload={"store": "archive", "changes": {"name": "new name"}}
            ),
        )
    manager.update_store.assert_called_once()
    assert manager.update_store.call_args.args[1].store_name == "new name"


def test_global_search_skips_call_failures_but_not_iteration_failures() -> None:
    """
    Skip an initial table-read error, count a Unicode match at limit zero, and expose later iterator errors.

    Example:
        >>> test_global_search_skips_call_failures_but_not_iteration_failures()


    :return: None after verifying observed-table reporting, count-only scanning, and the iteration-error boundary.
    """
    reader = Mock()
    reader.get_all_rows.side_effect = [
        RuntimeError("table unavailable"),
        [{"title": "Straße"}],
    ]
    runtime = SimpleNamespace(read_source=reader)
    query = SimpleNamespace(
        payload={"text": "STRASSE", "tables": ["broken", "works"], "limit": 0}
    )
    result = catalog.search_global(runtime, query)
    assert result["tables"] == ["works"] and result["total"] == 1
    assert result["items"] == [] and result["has_more"] is True
    iterator = MagicMock()
    iterator.__iter__.side_effect = RuntimeError("iteration failed")
    reader.get_all_rows.side_effect = None
    reader.get_all_rows.return_value = iterator
    with pytest.raises(RuntimeError, match="iteration failed"):
        catalog.search_global(runtime, query)


def test_agent_role_filter_requires_exact_projected_link_code() -> None:
    """
    Normalize the requested Agent role while requiring exact link-code case and retaining the echoed request text.

    Example:
        >>> test_agent_role_filter_requires_exact_projected_link_code()


    :return: None after only the matching Mapping link survives the projected role filter.
    """
    rows = [
        {"id": 1, "_catalog_link": {"type": "aut"}},
        {"id": 2, "_catalog_link": {"type": "AUT"}},
        {"id": 3},
        "not a projected Mapping",
    ]
    agents = SimpleNamespace(list_for_wemi=Mock(return_value=rows))
    runtime = SimpleNamespace(
        catalog=SimpleNamespace(repositories=SimpleNamespace(agents=agents))
    )
    query = SimpleNamespace(
        payload={"level": " Work ", "entity_id": 1, "role": " AUTHOR "}
    )
    result = catalog.catalog_agents_list(runtime, query)
    assert result["level"] == "work" and result["role"] == " AUTHOR "
    assert result["agents"] == [rows[0]] and result["count"] == 1


def test_store_description_falls_back_only_during_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Use durable configuration after a Core lookup error but propagate a live Store's status failure.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_store_description_falls_back_only_during_lookup(patch)


    :param monkeypatch: Fixture replacing live and durable Store resolvers with recording doubles.
    :return: None after fallback metadata and the live-status error boundary are asserted.
    """
    configuration = StoreConfiguration(
        UUID(int=1), "archive", "memory", "memory://archive"
    )
    live_lookup = Mock(side_effect=CoreDispatchError("not loaded"))
    durable_lookup = Mock(return_value=configuration)
    monkeypatch.setattr(stores, "_store", live_lookup)
    monkeypatch.setattr(stores, "_store_configuration", durable_lookup)
    query = SimpleNamespace(payload={"store": "archive"})
    result = stores.storage_store_get(object(), query)
    assert result["loaded"] is False and result["status"]["available"] is False
    assert result["store_uuid"] == UUID(int=1)
    durable_lookup.assert_called_once()
    live_lookup.side_effect = None
    live_lookup.return_value = SimpleNamespace(
        configuration=configuration,
        status=Mock(side_effect=RuntimeError("live status failed")),
    )
    durable_lookup.reset_mock()
    with pytest.raises(RuntimeError, match="live status failed"):
        stores.storage_store_get(object(), query)
    durable_lookup.assert_not_called()


@pytest.mark.parametrize("use_backend", [False, True])
def test_vacuum_adapter_documents_selected_callable_without_retry(
    monkeypatch: pytest.MonkeyPatch, use_backend: bool
) -> None:
    """
    Preserve vacuum routing and failure propagation while documenting the transient capability-adapter class.

    Only recording mocks are invoked; no database or backend is compacted.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_vacuum_adapter_documents_selected_callable_without_retry(patch, True)


    :param monkeypatch: Fixture wrapping the capability checker to capture its transient adapter instance.
    :param use_backend: Select a missing database callable or a callable that takes precedence over the backend.
    :return: None after checking class metadata, selected callable identity, receipt, and absence of retries.
    """
    direct = Mock()
    backend = Mock()
    db = SimpleNamespace(
        vacuum=None if use_backend else direct,
        backend=SimpleNamespace(vacuum=backend),
    )
    runtime = SimpleNamespace(database=db)
    checker = Mock(wraps=payloads._callable)
    monkeypatch.setattr(database, "_callable", checker)
    assert database.database_vacuum(runtime, None) == {"vacuumed": True}
    target = checker.call_args.args[0]
    selected = backend if use_backend else direct
    unused = direct if use_backend else backend
    selected.assert_called_once_with()
    unused.assert_not_called()
    assert type(target).__name__ == "_VacuumTarget"
    assert type(target).__bases__ == (object,)
    assert type(target).__dict__["vacuum"] is selected
    assert target.vacuum is selected
    description = inspect.getdoc(type(target))
    assert description.startswith("Carry the selected database vacuum callable")
    assert "Example:" in description and "descriptor binding" in description
    selected.side_effect = RuntimeError("vacuum failed")
    with pytest.raises(RuntimeError, match="vacuum failed"):
        database.database_vacuum(runtime, None)
    assert selected.call_count == 2
    unused.assert_not_called()


def test_migration_plan_ok_does_not_imply_available_identity_audit() -> None:
    """
    Retain an unhealthy identity status inside a collision-free plan with no suggested migrations.

    Example:
        >>> test_migration_plan_ok_does_not_imply_available_identity_audit()


    :return: None after unavailable-audit, no-action, status-health, and plan-ok semantics are distinguished.
    """
    db = SimpleNamespace(
        get_tables=Mock(return_value=["storage_schema_migrations"]),
        get_all_rows=Mock(
            return_value=[
                {"migration_id": "storage-0001-migration-ledger"},
                {"migration_id": "storage-0002-ingest-journal"},
            ]
        ),
        audit_normalized_identities=Mock(side_effect=RuntimeError("audit unavailable")),
    )
    result = database.database_migrations_plan(SimpleNamespace(database=db), None)
    assert result["ok"] is True and result["status"]["ok"] is False
    assert result["actions"] == [] and result["blocked_by_collisions"] == 0
    identities = result["status"]["normalized_identities"]
    assert identities["report"]["available"] is False
    assert identities["rows_needing_update"] is None


def test_job_log_offsets_count_bytes_and_eof_is_read_time(tmp_path: Path) -> None:
    """
    Split a UTF-8 character at a page boundary and distinguish read-time EOF from later file growth.

    Example:
        >>> test_job_log_offsets_count_bytes_and_eof_is_read_time(directory)  # doctest: +SKIP


    :param tmp_path: Isolated pytest directory containing this test's log file only.
    :return: None after byte offsets, replacement characters, lookahead, and newly appended output are checked.
    """
    log_path = tmp_path / "job.log"
    log_path.write_bytes("éZ".encode())
    lookup = Mock(return_value=SimpleNamespace(log_path=log_path))
    runtime = SimpleNamespace(job_manager=SimpleNamespace(get=lookup))
    first = discovery.jobs_log_read(
        runtime, SimpleNamespace(payload={"job_id": "job-1", "max_bytes": 1})
    )
    assert first["text"] == "�" and first["next_offset"] == 1
    assert first["eof"] is False and first["available"] is True
    second = discovery.jobs_log_read(
        runtime,
        SimpleNamespace(payload={"job_id": "job-1", "offset": 1, "max_bytes": 2}),
    )
    assert second["text"] == "�Z" and second["next_offset"] == 3
    assert second["eof"] is True
    with log_path.open("ab") as stream:
        stream.write(b"!")
    later = discovery.jobs_log_read(
        runtime, SimpleNamespace(payload={"job_id": "job-1", "offset": 3})
    )
    assert later["text"] == "!" and later["next_offset"] == 4


def test_metadata_writer_error_can_follow_partial_file_change(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    Expose a callback-reported writer failure without claiming that an in-place metadata edit was rolled back.

    A test-local writer modifies only a temporary file; real format plugins are not run.

    Example:
        >>> test_metadata_writer_error_can_follow_partial_file_change(patch, directory)  # doctest: +SKIP


    :param monkeypatch: Fixture replacing writer capability, metadata hydration, and writer implementation.
    :param tmp_path: Isolated pytest directory holding the disposable book file.
    :return: None after checking the reported error and retained partial bytes on disk.
    """
    from LiuXin_alpha.customize import ui

    def failing_writer(
        stream: BinaryIO,
        value: object,
        file_type: str,
        *,
        report_error: Callable[[object, str, str], None],
    ) -> None:
        """
        Overwrite one byte and report a failure through the callback without raising directly.

        Example:
            >>> failing_writer(stream, value, "epub", report_error=callback)  # doctest: +SKIP


        :param stream: Temporary book stream opened in-place by the adapter.
        :param value: Metadata sentinel forwarded unchanged to the error callback.
        :param file_type: Adapter-selected format forwarded to the callback.
        :param report_error: Collector invoked after the simulated partial write.
        :return: None after modifying the stream and recording the diagnostic.
        """
        stream.write(b"X")
        report_error(value, file_type, "simulated partial rewrite")

    monkeypatch.setattr(ui, "can_set_metadata", Mock(return_value=True))
    monkeypatch.setattr(ui, "set_file_type_metadata", failing_writer)
    monkeypatch.setattr(metadata, "_metadata_for_write", Mock(return_value=object()))
    path = tmp_path / "book.epub"
    path.write_bytes(b"original")
    with pytest.raises(CoreDispatchError) as caught:
        metadata.metadata_file_write(
            object(), SimpleNamespace(payload={"path": str(path)})
        )
    assert caught.value.code == "metadata_file_write_failed"
    assert caught.value.details["errors"] == ["simulated partial rewrite"]
    assert path.read_bytes() == b"Xriginal"


def test_reconcile_reload_failure_consumes_budget_but_not_final_health() -> None:
    """
    Count a failed reload attempt against the action limit while deriving final ok solely from later status.

    Example:
        >>> test_reconcile_reload_failure_consumes_budget_but_not_final_health()


    :return: None after the failed action, skipped verification, truncation, and healthy final receipt are checked.
    """
    manager = Mock()
    manager.get_operational_status.side_effect = [
        SimpleNamespace(healthy=False),
        SimpleNamespace(healthy=True),
    ]
    manager.reload_stores.side_effect = RuntimeError("reload failed")
    manager.iter_replica_records.return_value = [
        SimpleNamespace(replica_id=1, state="unverified")
    ]
    result = storage_integrity.storage_reconcile_apply(
        SimpleNamespace(library=SimpleNamespace(storage=manager)),
        SimpleNamespace(payload={"max_actions": 1}),
    )
    assert result["ok"] is True and result["actions_truncated"] is True
    assert result["actions"] == [
        {"action": "reload_stores", "ok": False, "error": "reload failed"}
    ]
    manager.verify_replica.assert_not_called()


@pytest.mark.parametrize("failed_first", [False, True])
def test_repair_counts_attempts_and_only_estimates_successful_transfers(
    monkeypatch: pytest.MonkeyPatch, failed_first: bool
) -> None:
    """
    Charge failed copies to the action budget but not transferred-byte estimates, and allow success for a scanned subset.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_repair_counts_attempts_and_only_estimates_successful_transfers(patch, True)


    :param monkeypatch: Fixture supplying before/after plans without real storage planning or copying.
    :param failed_first: Fail the first of two attempts and leave a third action outside the limit, or complete one copy.
    :return: None after attempt/byte accounting, truncation, delegated verification, and subset-only ok are checked.
    """
    action = {
        "action": "replicate_digital_asset",
        "digital_asset_id": 1,
        "destination_store_ref": str(UUID(int=2)),
        "mode": "active",
        "estimated_bytes": 7,
    }
    before = {"actions": [action] * (3 if failed_first else 1), "blocked": True}
    after = {"actions": [], "action_count": 0, "blocked": False, "complete": False}
    planner = Mock(side_effect=[before, after])
    monkeypatch.setattr(storage_repair, "_storage_repair_plan_payload", planner)
    manager = Mock()
    manager.replicate_digital_asset.side_effect = (
        [RuntimeError("copy failed"), {}] if failed_first else [{}]
    )
    result = storage_repair.storage_repair_apply(
        SimpleNamespace(library=SimpleNamespace(storage=manager)),
        SimpleNamespace(payload={"max_actions": 2, "max_transfer_bytes": 7}),
    )
    assert result["transferred_bytes"] == 7
    assert result["actions_applied"] == (2 if failed_first else 1)
    assert result["actions_failed"] == int(failed_first)
    assert result["actions_truncated"] is failed_first
    assert result["ok"] is not failed_first
    assert result["after"]["complete"] is False
    assert all(
        call.kwargs["verify"] is True
        for call in manager.replicate_digital_asset.call_args_list
    )


def test_store_status_uses_observations_and_declared_sizes() -> None:
    """
    Prefer observed availability/writability to offline/read-only hints and retain unclamped, declared-size accounting.

    Example:
        >>> test_store_status_uses_observations_and_declared_sizes()


    :return: None after checking observation precedence, capacity sentinels, per-claim versus logical bytes, and unknown Assets.
    """
    store_ref = UUID(int=1)
    configuration = StoreConfiguration(
        store_ref, "archive", "memory", "memory://archive", read_only=True
    )
    observation = SimpleNamespace(
        available=True,
        writable=True,
        total_bytes=100,
        free_bytes=200,
        object_count=None,
        checked_at=None,
        message=None,
        warnings=["free-text warning only"],
    )
    replicas = [
        SimpleNamespace(
            replica_id=index,
            digital_asset_id=asset_id,
            mode=ReplicaMode.ACTIVE,
            state=ReplicaState.VERIFIED,
        )
        for index, asset_id in enumerate([1, 1, 99], start=1)
    ]
    context = storage_status._StatusContext(
        storage_status._StoreInventory(
            {store_ref: configuration}, {store_ref: {"online_status": "offline"}}, [], 1
        ),
        {},
        {store_ref: observation},
        set(),
        None,
        {1: 7},
        {store_ref: replicas},
        SimpleNamespace(issues=[]),
    )
    result = storage_status._render_store_status(configuration, context)
    assert result["read_only"] is True and result["online_status"] == "offline"
    assert result["available"] is True and result["writable"] is True
    assert result["health"] == "healthy"
    assert result["registered"] is False and result["configuration_in_sync"] is True
    assert result["logical_bytes"] == 7 and result["replica_bytes"] == 14
    assert result["unaccounted_replicas"] == 1
    assert result["used_bytes"] == -100 and result["free_percent"] == 200.0
    observation.total_bytes = 0
    assert (
        storage_status._render_store_status(configuration, context)["free_percent"]
        is None
    )
    observation.total_bytes, observation.free_bytes = 100, None
    assert (
        storage_status._render_store_status(configuration, context)["free_percent"]
        is None
    )


def test_program_protocol_documentation_matches_its_service_owners() -> None:
    """
    Resolve every handler protocol's service link and compare its summary, parameter/result fields, and argument names.

    This checks documentation and callable structure without invoking workflows.
    Facade static aliases must continue to expose their service functions' docstrings.

    Example:
        >>> test_program_protocol_documentation_matches_its_service_owners()


    :return: None after checking all 80 declared endpoint methods and the facade's static alias documentation.
    """
    from LiuXin_alpha.core.program_api import CoreProgramAPI
    from LiuXin_alpha.core.program_endpoints import handlers

    checked = 0
    for family in handlers.ProgramEndpointHandlers.__mro__:
        if family.__module__ != handlers.__name__:
            continue
        for name, method in vars(family).items():
            if name.startswith("_") or not inspect.isfunction(method):
                continue
            description = inspect.getdoc(method)
            assert description is not None
            links = re.findall(r":func:`([^`]+)`", description)
            assert len(links) == 1
            module_name, function_name = links[0].rsplit(".", 1)
            assert module_name.startswith("LiuXin_alpha.core.program_services.")
            assert function_name == name
            owner = getattr(importlib.import_module(module_name), function_name)
            owner_description = inspect.getdoc(owner)
            assert owner_description is not None
            assert description.splitlines()[0] == owner_description.splitlines()[0]
            assert (
                description[description.index(":param runtime:") :]
                == owner_description[owner_description.index(":param runtime:") :]
            )
            assert list(inspect.signature(method).parameters)[1:] == list(
                inspect.signature(owner).parameters
            )
            checked += 1
    assert checked == 80
    for name, alias in vars(CoreProgramAPI).items():
        if isinstance(alias, staticmethod):
            function = getattr(CoreProgramAPI, name)
            assert function is alias.__func__
            assert inspect.getdoc(function) == inspect.getdoc(alias.__func__)
            assert ":return:" in inspect.getdoc(function)
