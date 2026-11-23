"""Regress the contract defects identified by the whole-Core review."""

from __future__ import annotations

import importlib
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreDispatchError, CoreHandlerError
from LiuXin_alpha.core.settings import redacted_manifest


@pytest.mark.parametrize("stable", (False, True))
@pytest.mark.parametrize("replace_at", ("started", "running"))
def test_command_keeps_its_captured_result_contract(
    core_runtime_factory, stable, replace_at
):
    runtime = core_runtime_factory()
    entered, release = Event(), Event()
    opaque = object()

    def old_handler(core, command):
        entered.set()
        assert release.wait(5)
        return opaque

    def replace():
        runtime.register_command_handler(
            "review.replace",
            lambda core, command: {"new": True},
            transport_stable=not stable,
        )

    runtime.register_command_handler(
        "review.replace", old_handler, transport_stable=stable
    )
    if replace_at == "started":
        runtime.subscribe(
            lambda event: replace() if event.event_type == "command.started" else None
        )
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            pending = pool.submit(
                runtime.execute_command, CoreCommand("review.replace")
            )
            assert entered.wait(3)
            if replace_at == "running":
                replace()
            release.set()
            if stable:
                with pytest.raises(CoreHandlerError, match="serialization"):
                    pending.result(timeout=3)
            else:
                assert pending.result(timeout=3).result is opaque
        finally:
            release.set()
            runtime.shutdown()


@pytest.mark.parametrize(
    "module",
    (
        "application_api",
        "database_semantics_api",
        "storage_graph_api",
        "browse_api",
        "program_services.payloads",
    ),
)
@pytest.mark.parametrize(
    "value", (True, 1.9, 1.0, "1", None, float("nan"), float("inf"))
)
def test_required_ids_are_strict_wire_integers(module, value):
    owner = importlib.import_module("LiuXin_alpha.core." + module)
    with pytest.raises(CoreDispatchError) as failure:
        owner._required_int({"id": value}, "id")
    assert failure.value.code == "invalid_payload"


@pytest.mark.parametrize(
    "module",
    (
        "application_api",
        "database_semantics_api",
        "storage_graph_api",
        "program_services.payloads",
    ),
)
@pytest.mark.parametrize("value", (None, True, 1, {}, [], " "))
def test_required_text_does_not_stringify_other_types(module, value):
    owner = importlib.import_module("LiuXin_alpha.core." + module)
    with pytest.raises(CoreDispatchError) as failure:
        owner._required_text({"name": value}, "name")
    assert failure.value.code == "invalid_payload"


def test_discovery_covers_every_registered_route_and_late_extension(
    core_runtime_factory,
):
    runtime = core_runtime_factory()
    try:
        runtime.register_query_handler(
            "extension.local", lambda core, query: {}, tags=("extension",)
        )
        api = runtime.describe_api(include_targets=False)
        actual = {
            (row["kind"], row["name"])
            for kind in ("commands", "queries")
            for row in api[kind]
        }
        result = runtime.query("capabilities.list")
        discovered = {
            (route["kind"], name)
            for name, detail in result["operations"].items()
            for route in detail["routes"]
        }
        assert discovered == actual
        assert result["inventory_complete"] is True
        assert "complete_program_boundary" not in result
        assert result["operations"]["extension.local"]["call_modes"] == ["direct"]
        assert result["operations"]["metadata.search"]["registered"] is True
        assert (
            result["operations"]["maintenance.clean"]["availability"] == "unavailable"
        )
        assert result["operations"]["maintenance.clean"]["reason"]
    finally:
        runtime.shutdown()


def test_manifest_redaction_copies_nested_containers_and_scrubs_urls():
    nested = {"password": "synthetic-nested"}
    original = {
        "extra": [nested],
        "database_metadata": {"auth": nested},
        "database": "postgres://reader:synthetic-authority@[::1]:5432/books?password=synthetic-query&sslmode=require#synthetic-fragment",
        "links": ["https://example.test/?access_token=synthetic-link"],
        "malformed": "https://reader:synthetic-bad@example.test:bad/books",
        "protocol_relative": "//reader:synthetic-relative@example.test/books",
    }
    result = redacted_manifest(original)
    rendered = repr(result)
    assert "synthetic-" not in rendered
    assert "[::1]:5432" in result["database"]
    assert "sslmode=require" in result["database"]
    assert result["extra"] is not original["extra"]
    assert original["extra"][0]["password"] == "synthetic-nested"


def test_manifest_redaction_handles_cycles_without_leaking_values():
    values = {"password": "synthetic-cycle"}
    values["child"] = values
    result = redacted_manifest(values)
    assert result["password"] == "<redacted>"
    assert "synthetic-" not in repr(result)
    assert result["child"] == "<redacted>"


@pytest.mark.parametrize("role", ("read-only", "read-write", "trusted"))
def test_route_inventory_and_validation_across_transport_roles(
    core_runtime_factory, role
):
    """Discovery does not grant access; validation and authority both precede effects."""
    from LiuXin_alpha.core import CoreHttpDaemon
    from LiuXin_alpha.core.authorization import CoreTransportPolicy
    from LiuXin_alpha.core.description import CorePayloadFieldDescription as Field
    from LiuXin_alpha.core.proxies.remote import RemoteCoreClient, RemoteProxyError

    runtime = core_runtime_factory()
    effects, events = [], []
    runtime.subscribe(events.append)
    runtime.register_command_handler(
        "review.mutate",
        lambda core, command: effects.append(dict(command.payload)),
        payload_fields=(
            Field("id", required=True, field_type="integer"),
            Field("enabled", required=True, field_type="boolean"),
            Field("timeout", field_type="number|null"),
        ),
        remote_capabilities=("write",),
    )
    runtime.register_query_handler("review.private", lambda core, query: None)
    token = "contract-review-token"
    policy = CoreTransportPolicy.for_access(role, token)
    daemon = CoreHttpDaemon(runtime, policy=policy)
    daemon.start()
    client = RemoteCoreClient(endpoint=daemon.base_url, bearer_token=token)
    try:
        direct = runtime.query("capabilities.list")
        assert client.query("capabilities.list") == direct
        assert direct["operations"]["review.private"]["call_modes"] == ["direct"]
        routes = direct["operations"]["invoke"]["routes"]
        assert {route["kind"] for route in routes} == {"command", "query"}
        assert all(route["remote_capabilities"] for route in routes)
        for payload in (
            {"id": 1.9, "enabled": True},
            {"id": True, "enabled": True},
            {"id": "1", "enabled": True},
            {"id": 1, "enabled": "false"},
            {"id": 1, "enabled": None},
            {"enabled": True},
            {"id": 1, "enabled": True, "timeout": "2"},
        ):
            with pytest.raises(CoreDispatchError) as local:
                runtime.execute_command(CoreCommand("review.mutate", payload))
            assert local.value.code == "invalid_payload"
            with pytest.raises(RemoteProxyError) as remote:
                client.execute_command(CoreCommand("review.mutate", payload))
            assert remote.value.code == (
                "permission_denied" if role == "read-only" else "invalid_payload"
            )
        assert effects == [] and events == []
        valid = CoreCommand(
            "review.mutate", {"id": 1, "enabled": False, "timeout": None}
        )
        if role == "read-only":
            with pytest.raises(RemoteProxyError):
                client.execute_command(valid)
            assert effects == []
        else:
            client.execute_command(valid)
            assert effects == [dict(valid.payload)]
    finally:
        daemon.stop()
        runtime.shutdown()


@pytest.mark.parametrize("payload", (None, [], "string", {1: "value"}))
@pytest.mark.parametrize("kind", ("command", "query"))
def test_direct_payload_requires_an_object_with_string_keys(
    core_runtime_factory, payload, kind
):
    from LiuXin_alpha.core.queries import CoreQuery

    runtime = core_runtime_factory()
    effects = []
    try:
        register = getattr(runtime, "register_" + kind + "_handler")
        register(
            "review.shape",
            lambda core, envelope: effects.append(envelope),
            remote_capabilities=("read",),
        )
        envelope = CoreCommand if kind == "command" else CoreQuery
        with pytest.raises(CoreDispatchError) as failure:
            getattr(runtime, "execute_" + kind)(
                envelope("review.shape", payload),
                capabilities=frozenset({"read", "write"}),
            )
        assert failure.value.code == "invalid_payload"
        assert effects == []
    finally:
        runtime.shutdown()


@pytest.mark.parametrize("value", (1.9, True, "7"))
def test_builtin_nested_identity_validation_precedes_storage_effects(
    core_runtime_factory, value
):
    from unittest.mock import Mock

    runtime = core_runtime_factory()
    verify = Mock()
    runtime.library.storage.verify_digital_asset = verify
    try:
        with pytest.raises(CoreHandlerError) as failure:
            runtime.execute_command(
                CoreCommand(
                    "storage.asset.verify", {"asset_id": 1, "replica_ids": [value]}
                )
            )
        assert failure.value.code == "invalid_payload"
        verify.assert_not_called()
    finally:
        runtime.shutdown()


def test_failed_registration_keeps_handler_authority_and_prerequisites(
    core_runtime_factory,
):
    from LiuXin_alpha.core.description import CorePayloadFieldDescription as Field

    runtime = core_runtime_factory()
    try:
        runtime.register_query_handler(
            "review.replace",
            lambda core, query: "original",
            remote_capabilities=("read",),
            requirements=("Configured backend",),
        )
        for fields in ((Field("id", field_type="int"),), (Field("id"), Field("id"))):
            with pytest.raises(ValueError):
                runtime.register_query_handler(
                    "review.replace",
                    lambda core, query: "replacement",
                    payload_fields=fields,
                )
        assert runtime.query("review.replace") == "original"
        row = runtime.query("capabilities.list")["operations"]["review.replace"][
            "routes"
        ][0]
        assert row["requirements"] == ["Configured backend"]
        assert row["remote_capabilities"] == ["read"]
        assert row["availability"] == "conditional" and row["readiness"] == "not_probed"
    finally:
        runtime.shutdown()


def test_discovery_observes_injected_cleanup_without_constructing_services(
    core_runtime_factory,
):
    from types import SimpleNamespace

    runtime = core_runtime_factory()
    try:
        before = runtime.services._maintenance
        assert before is None
        assert (
            runtime.query("capabilities.list")["operations"]["maintenance.clean"][
                "availability"
            ]
            == "unavailable"
        )
        assert runtime.services._maintenance is before
        runtime.database.maintenance = SimpleNamespace(clean=lambda *args: None)
        assert (
            runtime.query("capabilities.list")["operations"]["maintenance.clean"][
                "availability"
            ]
            == "available"
        )
        assert runtime.services._maintenance is before
        runtime.register_query_handler(
            "review.unavailable",
            lambda core, query: {},
            capability_check=lambda core: "Plugin not installed",
        )
        row = runtime.query("capabilities.list")["operations"]["review.unavailable"][
            "routes"
        ][0]
        assert (
            row["implementation"] == "unavailable"
            and row["reason"] == "Plugin not installed"
        )
    finally:
        runtime.shutdown()


@pytest.mark.parametrize(
    "value", (float("nan"), float("inf"), -float("inf"), 10**500, True, "1")
)
def test_numbers_reject_nonfinite_and_coercible_values(value):
    from LiuXin_alpha.core.payloads import number_value

    with pytest.raises(CoreDispatchError) as failure:
        number_value(value, "timeout")
    assert failure.value.code == "invalid_payload"


@pytest.mark.parametrize("value", ("single", b"bytes", [None], [1], {"key": "value"}))
def test_text_arrays_do_not_stringify_other_types(value):
    from LiuXin_alpha.core.program_services.payloads import _text_list

    with pytest.raises(CoreDispatchError) as failure:
        _text_list({"names": value}, "names")
    assert failure.value.code == "invalid_payload"


@pytest.mark.parametrize(
    "url",
    (
        "https://example.test/?%74oken=synthetic-one&token=synthetic-two&safe=ok",
        "https://example.test/?redirect=https%3A%2F%2Finner.test%2F%3Fsecret%3Dsynthetic-inner",
        "https://reader:synthetic-password@[bad-ip/books",
        "https://reader:synthetic-password@example.test:99999/books",
        "https://example.test/?Authorization=synthetic-token",
        "host=example.test password=synthetic-dsn dbname=books",
    ),
)
def test_manifest_redaction_handles_encoded_repeated_nested_and_malformed_secrets(url):
    result = redacted_manifest({"value": url})
    assert "synthetic-" not in repr(result)


@pytest.mark.parametrize(
    "field,value",
    (
        ("read_only", "false"),
        ("read_only", None),
        ("replication_policy_id", 1.9),
        ("backup_policy_id", True),
        ("name", None),
        ("root", 123),
    ),
)
def test_store_edits_reject_coercible_controls(field, value):
    from LiuXin_alpha.core.program_services.stores import _update_field_value

    with pytest.raises(CoreDispatchError) as failure:
        _update_field_value(field, value)
    assert failure.value.code == "invalid_payload"


@pytest.mark.parametrize("value", ("false", 1, None))
def test_column_policy_flag_is_validated_before_persistence(value):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from LiuXin_alpha.core.program_services.schema import schema_column_update

    database = SimpleNamespace(
        get_column_metadata=Mock(return_value=SimpleNamespace(case_sensitive=False)),
        set_column_metadata=Mock(),
    )
    runtime = SimpleNamespace(database=database)
    with pytest.raises(CoreDispatchError) as failure:
        schema_column_update(
            runtime,
            CoreCommand(
                "schema.column.update",
                {
                    "table": "works",
                    "column": "work_title",
                    "policy": {"case_sensitive": value},
                },
            ),
        )
    assert failure.value.code == "invalid_payload"
    database.set_column_metadata.assert_not_called()
