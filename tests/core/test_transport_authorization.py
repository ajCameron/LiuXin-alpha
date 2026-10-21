"""Prove HTTP authority at dispatch with actual handlers and observable state."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from LiuXin_alpha.core import CoreCommand, CoreHttpDaemon, CoreQuery
from LiuXin_alpha.core.authorization import CoreAuthorizationError, CoreTransportPolicy
from LiuXin_alpha.core.proxies.local import LocalCoreClient
from LiuXin_alpha.core.proxies.remote import (
    RemoteCoreClient,
    RemoteLibraryProxy,
    RemoteProxyError,
)
from LiuXin_alpha.core.readonly import ReadOnlyCoreClient

TOKEN = "test-transport-token"


def test_deployment_role_lookup_keeps_policy_in_core():
    """The CLI selects a role; Core alone defines and validates its authority."""
    assert CoreTransportPolicy.for_access("read-only") == CoreTransportPolicy()
    assert CoreTransportPolicy.for_access(
        "read-write", TOKEN
    ) == CoreTransportPolicy.read_write(TOKEN)
    assert CoreTransportPolicy.for_access(
        "trusted", TOKEN, allowed_hosts=("Example:8765",)
    ).allowed_hosts == ("example:8765",)
    with pytest.raises(ValueError, match="bearer token"):
        CoreTransportPolicy.for_access("trusted")
    with pytest.raises(ValueError, match="Unknown Core deployment role"):
        CoreTransportPolicy.for_access("other", TOKEN)


@pytest.mark.parametrize("token", [TOKEN + "\n", TOKEN + "\x7f", TOKEN + "雪"])
def test_invalid_credentials_fail_without_echoing_the_secret(token):
    with pytest.raises(ValueError) as failure:
        RemoteCoreClient(endpoint="http://127.0.0.1:8765", bearer_token=token)
    assert TOKEN not in str(failure.value)


@contextmanager
def hosted(runtime, policy=None):
    """Own one real listener and its runtime for each failure-isolated case."""
    daemon = CoreHttpDaemon(runtime, policy=policy)
    daemon.start()
    try:
        yield daemon
    finally:
        daemon.stop()
        runtime.shutdown()


def request(
    daemon, name=None, payload=None, *, kind="command", headers=None, path=None
):
    """Send an unmediated HTTP request without relying on a client's UI restrictions."""
    connection = HTTPConnection(*daemon.server_address, timeout=5)
    try:
        body = (
            None
            if name is None
            else json.dumps(
                {
                    "name": name,
                    "payload": payload or {},
                    "capabilities": ["read", "write", "admin", "execute", "invoke"],
                }
            )
        )
        request_headers = {"Content-Type": "application/json"}
        request_headers.update(headers or {})
        connection.request(
            "GET" if body is None else "POST",
            path or f"/rpc/{kind}",
            body=body,
            headers=request_headers,
        )
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


def install_value_routes(runtime):
    """Declare the example's reader and writer separately from their serialization."""
    runtime.register_query_handler(
        "example.value",
        lambda owner, query: owner.library.database.get_value(),
        remote_capabilities=("read",),
    )
    runtime.register_command_handler(
        "example.value",
        lambda owner, command: owner.library.database.set_value(
            command.payload["value"]
        ),
        remote_capabilities=("write",),
    )


@pytest.mark.parametrize(
    ("kind", "name", "payload"),
    [
        (
            "query",
            "invoke",
            {"target": "database", "method": "set_value", "args": [999]},
        ),
        (
            "command",
            "invoke",
            {"target": "database", "method": "set_value", "args": [999]},
        ),
        ("command", "admin.row.delete", {"table": "works", "row_id": 1}),
        (
            "command",
            "preferences.set",
            {"key": "mksquashfs_path", "value": "/tmp/executable"},
        ),
        ("command", "storage.refresh", {}),
        ("command", "read-source.refresh", {}),
        ("command", "shutdown", {}),
        ("command", "example.value", {"value": 999}),
    ],
)
def test_default_policy_refuses_named_bypasses_without_effects(
    core_runtime_factory, kind, name, payload
):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    events = []
    runtime.subscribe(events.append)
    with hosted(runtime) as daemon:
        status, body = request(daemon, name, payload, kind=kind)
        assert status == 403
        assert body["error_code"] == "permission_denied"
        assert runtime.library.database.value == 7
        assert events == []
        assert request(daemon, "example.value", kind="query")[1]["result"] == 7


def test_authenticated_catalogue_role_preserves_direct_rpc_parity(core_runtime_factory):
    runtime = core_runtime_factory()
    install_value_routes(runtime)
    with hosted(runtime, CoreTransportPolicy.read_write(TOKEN)) as daemon:
        headers = {"Authorization": f"Bearer {TOKEN}"}
        direct = runtime.execute_command(
            CoreCommand("example.value", {"value": 23})
        ).result
        status, body = request(daemon, "example.value", {"value": 23}, headers=headers)
        assert status == 200
        assert body["result"] == direct == 23
        direct_read = runtime.execute_query(CoreQuery("example.value")).result
        assert (
            request(daemon, "example.value", kind="query", headers=headers)[1]["result"]
            == direct_read
        )
        assert (
            request(
                daemon,
                "invoke",
                {"target": "database", "method": "set_value", "args": [99]},
                kind="query",
                headers=headers,
            )[0]
            == 403
        )
        assert runtime.library.database.value == 23


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong-token"}])
def test_authentication_precedes_execution(core_runtime_factory, headers):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    with hosted(runtime, CoreTransportPolicy.read_write(TOKEN)) as daemon:
        status, body = request(daemon, "example.value", {"value": 9}, headers=headers)
        assert status == 401
        assert body["error_code"] == "authentication_required"
        assert runtime.library.database.value == 7


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"Origin": "https://untrusted.invalid"}, 403),
        ({"Origin": "null"}, 403),
        ({"Sec-Fetch-Site": "cross-site"}, 403),
        ({"Host": "rebound.invalid"}, 403),
        ({"Content-Type": "text/plain"}, 415),
        ({"Content-Type": "application/x-www-form-urlencoded"}, 415),
    ],
)
def test_origin_host_and_media_failures_have_no_effects(
    core_runtime_factory, headers, expected
):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    with hosted(runtime, CoreTransportPolicy.read_write(TOKEN)) as daemon:
        headers = {"Authorization": f"Bearer {TOKEN}", **headers}
        assert (
            request(daemon, "example.value", {"value": 9}, headers=headers)[0]
            == expected
        )
        assert runtime.library.database.value == 7


def test_unannotated_and_replaced_handlers_fail_closed(core_runtime_factory):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    with hosted(runtime) as daemon:
        runtime.register_query_handler(
            "example.value",
            lambda owner, query: owner.library.database.set_value(999),
            transport_stable=True,
        )
        assert request(daemon, "example.value", kind="query")[0] == 403
        assert runtime.library.database.value == 7


def test_command_cannot_opt_itself_into_a_readonly_deployment(core_runtime_factory):
    runtime = core_runtime_factory(initial_value=7)
    runtime.register_command_handler(
        "misdeclared.command",
        lambda owner, command: owner.library.database.set_value(999),
        remote_capabilities=("read",),
    )
    with hosted(runtime) as daemon:
        assert request(daemon, "misdeclared.command")[0] == 403
        assert runtime.library.database.value == 7


def test_explicit_trusted_policy_can_invoke(core_runtime_factory):
    runtime = core_runtime_factory(initial_value=7)
    with hosted(runtime, CoreTransportPolicy.trusted(TOKEN)) as daemon:
        status, body = request(
            daemon,
            "invoke",
            {
                "target": "database",
                "method": "set_value",
                "args": [9],
            },
            kind="query",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        assert status == 200
        assert body["result"] == runtime.library.database.value == 9


def test_read_role_cannot_refresh_introspect_targets_or_read_global_events(
    core_runtime_factory,
):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    with hosted(runtime) as daemon:
        assert (
            request(daemon, "example.value", {"refresh": True}, kind="query")[0] == 403
        )
        assert request(daemon, path="/api/describe?include_targets=1")[0] == 403
        assert request(daemon, path="/events/next?timeout=0")[0] == 403
        status, body = request(daemon, path="/api/describe")
        assert status == 200
        assert "targets" not in body["result"]


def test_runtime_authorization_is_before_handler_and_events(core_runtime_factory):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    events = []
    runtime.subscribe(events.append)
    try:
        with pytest.raises(CoreAuthorizationError):
            runtime.execute_command(
                CoreCommand("example.value", {"value": 99}),
                capabilities=frozenset({"read"}),
            )
        assert runtime.library.database.value == 7
        assert events == []
    finally:
        runtime.shutdown()


def test_writable_and_nonlocal_policies_require_credentials(core_runtime_factory):
    with pytest.raises(ValueError, match="bearer token"):
        CoreTransportPolicy(frozenset({"read", "write"}))
    runtime = core_runtime_factory()
    try:
        with pytest.raises(ValueError, match="bearer token"):
            CoreHttpDaemon(runtime, host="0.0.0.0")
    finally:
        runtime.shutdown()


def test_authenticated_existing_clients_use_server_policy(core_runtime_factory):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    with hosted(runtime, CoreTransportPolicy.read_write(TOKEN)) as daemon:
        client = RemoteCoreClient(endpoint=daemon.base_url, bearer_token=TOKEN)
        assert client.command("example.value", {"value": 9}) == 9
        assert client.query("example.value") == 9
        with pytest.raises(RemoteProxyError) as refusal:
            client.command(
                "preferences.set",
                {"key": "mksquashfs_path", "value": "/tmp/executable"},
            )
        assert refusal.value.status_code == 403
        with pytest.raises(RemoteProxyError) as unauthenticated:
            RemoteCoreClient(endpoint=daemon.base_url).query("example.value")
        assert unauthenticated.value.status_code == 401


def test_legacy_proxy_children_keep_explicit_trusted_credentials(core_runtime_factory):
    runtime = core_runtime_factory(initial_value=7)
    with hosted(runtime, CoreTransportPolicy.trusted(TOKEN)) as daemon:
        proxy = RemoteLibraryProxy(endpoint=daemon.base_url, bearer_token=TOKEN)
        assert proxy.database.set_value(19) == 19
        assert proxy.database.get_value() == 19
        assert proxy.core.health()["core_uuid"] == runtime.core_uuid


@pytest.mark.parametrize("kind", ["runtime", "local", "remote"])
def test_restricted_reader_blocks_all_write_forms(core_runtime_factory, kind):
    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    with hosted(runtime, CoreTransportPolicy.trusted(TOKEN)) as daemon:
        client = {
            "runtime": runtime,
            "local": LocalCoreClient(runtime),
            "remote": RemoteCoreClient(endpoint=daemon.base_url, bearer_token=TOKEN),
        }[kind]
        reader = ReadOnlyCoreClient(client)
        assert reader.query("example.value") == 7
        with pytest.raises(CoreAuthorizationError):
            reader.command("example.value", {"value": 99})
        with pytest.raises(CoreAuthorizationError):
            reader.query(
                "invoke", {"target": "database", "method": "set_value", "args": [99]}
            )
        with pytest.raises(CoreAuthorizationError):
            reader.query("example.value", {"refresh": True})
        with pytest.raises(CoreAuthorizationError):
            reader.shutdown()
        assert runtime.library.database.value == 7


def test_readonly_surface_and_injected_model_share_restricted_client(
    core_runtime_factory,
):
    from LiuXin_alpha.surfaces.core import CoreSurfaceModel
    from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication
    from LiuXin_alpha.surfaces.web_readwrite.app import ReadWriteWebApplication

    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    model = CoreSurfaceModel(runtime)
    app = ReadOnlyWebApplication(runtime, model=model)
    writable = ReadWriteWebApplication(runtime)
    try:
        assert app.core.query("example.value") == 7
        for reader in (app.core, app.model.core):
            with pytest.raises(CoreAuthorizationError):
                reader.command("example.value", {"value": 99})
        assert model.core is runtime
        assert runtime.library.database.value == 7
        assert writable.core.command("example.value", {"value": 9}) == 9
    finally:
        app.close()
        writable.close()
        runtime.shutdown()


def test_trusted_http_still_obeys_storage_last_copy_policy(tmp_path):
    from LiuXin_alpha.core import CoreRuntime
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.library import Library
    from LiuXin_alpha.storage.durable_manager import StorageManager
    from LiuXin_alpha.storage.stores import FilesystemStore

    with Database(
        metadata={"database_path": str(tmp_path / "catalog.sqlite")},
        db_type="SQLite",
        create=True,
        backup=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as database:
        store = FilesystemStore(tmp_path / "bytes")
        with StorageManager(db=database, stores=(store,)) as manager:
            asset = manager.store_bytes(b"only surviving book", store=store)
            replica = next(
                manager.iter_replica_records(digital_asset_id=asset.digital_asset_id)
            )
            manager.verify_replica(replica.replica_id, calculate_digests=True)
            before = manager.get_replica_record(replica.replica_id)
            runtime = CoreRuntime(library=Library(database=database, storage=manager))
            with hosted(runtime, CoreTransportPolicy.trusted(TOKEN)) as daemon:
                status, body = request(
                    daemon,
                    "storage.file.delete",
                    {"replica_id": int(replica.replica_id)},
                    headers={"Authorization": f"Bearer {TOKEN}"},
                )
                assert status == 400
                assert "freshly verified copy" in body["error"]
                assert manager.get_replica_record(replica.replica_id) == before
                with store.get(replica.location) as stream:
                    assert stream.read() == b"only surviving book"


def test_all_builtin_registrations_declare_remote_authority(core_runtime_factory):
    runtime = core_runtime_factory()
    try:
        description = runtime.describe_api(include_targets=False)
        missing = [
            entry["name"]
            for kind in ("commands", "queries")
            for entry in description[kind]
            if entry["remote_capabilities"] is None
        ]
        assert missing == []
    finally:
        runtime.shutdown()


def test_failed_registration_cannot_pair_a_new_handler_with_old_authority(
    core_runtime_factory,
):
    class InvalidSummary:
        def __str__(self):
            raise ValueError("invalid description")

    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    with hosted(runtime) as daemon:
        with pytest.raises(ValueError, match="invalid description"):
            runtime.register_query_handler(
                "example.value",
                lambda owner, query: owner.library.database.set_value(999),
                summary=InvalidSummary(),
                remote_capabilities=("admin",),
            )
        status, body = request(daemon, "example.value", kind="query")
        assert status == 200
        assert body["result"] == runtime.library.database.value == 7


def test_authenticated_client_does_not_forward_credentials_on_redirect(
    core_runtime_factory,
):
    runtime = core_runtime_factory()
    received = []
    runtime.register_query_handler(
        "health",
        lambda owner, query: received.append(query) or {},
        remote_capabilities=("read",),
    )
    with hosted(runtime, CoreTransportPolicy.trusted(TOKEN)) as destination:

        class Redirect(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", destination.health_url)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_POST = do_GET

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
        thread = Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        thread.start()
        try:
            client = RemoteCoreClient(
                endpoint="http://{}:{}".format(*server.server_address),
                bearer_token=TOKEN,
            )
            with pytest.raises(RemoteProxyError) as failure:
                client.health()
            assert failure.value.status_code == 302
            assert received == []
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


def test_cli_token_files_connect_to_the_served_role_without_disclosing_token(
    core_runtime_factory,
    tmp_path,
    monkeypatch,
    capsys,
):
    from LiuXin_alpha.surfaces.cli import core_cli
    from LiuXin_alpha.surfaces.cli.app import build_parser
    from LiuXin_alpha.surfaces.core import open_surface_core_from_args

    runtime = core_runtime_factory(initial_value=7)
    install_value_routes(runtime)
    token_file = tmp_path / "role.token"
    token_file.write_text(TOKEN + "\n")
    ready_file = tmp_path / "ready.json"
    ready = Event()
    original_emit = core_cli.emit_bytes

    def publish_ready(*args, **kwargs):
        original_emit(*args, **kwargs)
        ready.set()

    monkeypatch.setattr(core_cli, "emit_bytes", publish_ready)
    monkeypatch.setattr(
        core_cli,
        "open_surface_core_from_args",
        lambda *args, **kwargs: SimpleNamespace(
            runtime=runtime,
            owns_runtime=True,
            close=runtime.shutdown,
        ),
    )
    args = build_parser().parse_args(
        [
            "core",
            "serve",
            "--database",
            str(tmp_path / "borrowed.sqlite"),
            "--port",
            "0",
            "--access",
            "read-write",
            "--auth-token-file",
            str(token_file),
            "--ready-file",
            str(ready_file),
            "--stop-after",
            "3",
        ]
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(core_cli.cmd_core_serve, args)
        assert ready.wait(10)
        readiness = json.loads(ready_file.read_text())
        client_args = build_parser().parse_args(
            [
                "core",
                "health",
                "--core-endpoint",
                readiness["endpoint"],
                "--core-token-file",
                str(token_file),
            ]
        )
        with open_surface_core_from_args(client_args) as session:
            assert session.client.command("example.value", {"value": 9}) == 9
            with pytest.raises(RemoteProxyError) as denied:
                session.client.command("shutdown")
            assert denied.value.status_code == 403
        assert future.result(timeout=10) == 0
    assert readiness["authentication"] is True
    assert readiness["capabilities"] == ["read", "write"]
    assert TOKEN not in ready_file.read_text()
    output = capsys.readouterr()
    assert TOKEN not in output.out + output.err
    assert runtime.library.database.value == 9
