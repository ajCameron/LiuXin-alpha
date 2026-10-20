"""Exercise the production audit route, independent services and durable evidence."""

import base64
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import dataclass
from threading import Event
from types import SimpleNamespace

import pytest

from LiuXin_alpha.core import CoreHttpDaemon, CoreRuntime
from LiuXin_alpha.core.authorization import CoreTransportPolicy
from LiuXin_alpha.core.proxies.local import LocalCoreClient
from LiuXin_alpha.core.proxies.remote import RemoteCoreClient
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager


@dataclass
class Report:
    healthy: bool = True


class GatedStorage:
    def __init__(self):
        self.entered = Event()
        self.release = Event()
        self.verified = []

    def iter_replica_records(self):
        return [
            SimpleNamespace(replica_id=i, digital_asset_id=10 + i, state="present")
            for i in (1, 2)
        ]

    def verify_replica(self, replica_id, *, calculate_digests):
        self.entered.set()
        assert self.release.wait(10)
        self.verified.append(replica_id)
        return Report()


@pytest.fixture
def audit_runtime(tmp_path):
    from LiuXin_alpha.core.durable_jobs import DurableJobService

    storage = GatedStorage()
    lifetime = []
    database = SimpleNamespace(uuid="library-1", type="SQLite", metadata={})
    worker_library = SimpleNamespace(db=database, storage=storage)

    @contextmanager
    def open_library():
        lifetime.append("opened")
        try:
            yield SimpleNamespace(
                library=worker_library, core_services=SimpleNamespace(preferences={})
            )
        finally:
            lifetime.append("closed")

    durable = DurableJobService(
        path=tmp_path / "jobs.sqlite",
        library_uuid="library-1",
        open_library=open_library,
    )
    utility = InMemoryJobManager(max_workers=1, default_backend="serial")
    runtime = CoreRuntime(
        library=SimpleNamespace(database=database, storage=SimpleNamespace()),
        job_manager=utility,
        durable_jobs=durable,
        preferences={},
    )
    try:
        yield runtime, durable, storage, lifetime, utility
    finally:
        storage.release.set()
        runtime.shutdown()
        durable.stop(wait=True)
        utility.shutdown(wait=True, cancel_pending=True)


@pytest.fixture(params=["local", "http"])
def clients(request, audit_runtime):
    runtime = audit_runtime[0]
    if request.param == "local":
        yield LocalCoreClient(runtime), LocalCoreClient(runtime)
        return
    daemon = CoreHttpDaemon(
        runtime, policy=CoreTransportPolicy.trusted("durable-audit-test")
    )
    daemon.start()
    try:
        yield tuple(
            RemoteCoreClient(
                endpoint=daemon.base_url, bearer_token="durable-audit-test"
            )
            for _ in range(2)
        )
    finally:
        audit_runtime[2].release.set()
        daemon.stop()


def test_audit_wait_preserves_dispatch_and_cancellation_receipt(audit_runtime, clients):
    runtime, durable, storage, lifetime, _utility = audit_runtime
    submitter, observer = clients
    receipt = submitter.command("storage.audit.start", {"request_key": "audit-once"})
    job_id = receipt["job_id"]
    assert job_id.startswith("durable:")
    assert storage.entered.wait(4)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            waiter = pool.submit(submitter.query, "jobs.result", {"job_id": job_id})
            assert (
                pool.submit(observer.query, "health").result(timeout=3)["core_uuid"]
                == runtime.core_uuid
            )
            running = observer.query("jobs.get", {"job_id": job_id})["job"]
            assert running["state"] == "running"
            assert running["durable"]["progress"]["total"] == 2
            assert observer.command("jobs.cancel", {"job_id": job_id})["cancelled"]
            assert not waiter.done()
            storage.release.set()
            result = waiter.result(timeout=5)["execution"]
        finally:
            storage.release.set()
    assert result["ok"] is False
    assert result["result"]["checked"] == 1
    assert result["result"]["cancelled"] is True
    assert storage.verified == [1]
    assert lifetime == ["opened", "closed"]
    finished = observer.query("jobs.get", {"job_id": job_id})["job"]
    assert finished["state"] == "cancelled"
    assert finished["durable"]["cancel_requested"] is True
    page = observer.query("jobs.events", {"job_id": job_id, "limit": 2})
    assert len(page["events"]) == 2 and page["has_more"]
    following = observer.query(
        "jobs.events", {"job_id": job_id, "after_event_id": page["next_cursor"]}
    )
    assert all(event["event_id"] > page["next_cursor"] for event in following["events"])
    assert any(event["event_kind"] == "log" for event in following["events"])
    repeated = submitter.command("storage.audit.start", {"request_key": "audit-once"})
    assert repeated["job_id"] == job_id
    assert len(durable.repository.list_runs()) == 1


def test_shutdown_keeps_active_audit_and_borrowed_utility_alive(audit_runtime):
    runtime, durable, storage, lifetime, utility = audit_runtime
    job_id = runtime.command("storage.audit.start", {"request_key": "shutdown-audit"})[
        "job_id"
    ]
    assert storage.entered.wait(4)
    runtime.shutdown()
    assert lifetime == ["opened"]
    storage.release.set()
    assert durable.result(job_id, timeout=5).ok
    assert lifetime == ["opened", "closed"]
    from LiuXin_alpha.utils.jobs import JobRequest

    utility_id = utility.submit(
        JobRequest(module_name="math", function_name="sqrt", args=(25,)), no_output=True
    )
    assert utility.result(utility_id, timeout=5).result == 5


def test_new_core_process_reports_interrupted_audit_without_replaying_it(audit_runtime):
    runtime, durable, storage, _lifetime, _utility = audit_runtime
    job_id = runtime.command("storage.audit.start", {"request_key": "interrupted"})[
        "job_id"
    ]
    assert storage.entered.wait(4)
    # Simulate a dead owner's last persisted lease while its test thread stays gated.
    run_id = durable.get(job_id).durable["run_id"]
    with closing(durable.repository.connect()) as connection, connection:
        connection.execute(
            "UPDATE job_runs SET lease_expires_timestamp_ep_k=1 WHERE job_run_id=?",
            (run_id,),
        )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json, sys
from types import SimpleNamespace
from LiuXin_alpha.core.durable_jobs import DurableJobService
from LiuXin_alpha.core.runtime import CoreRuntime
def refuse_open():
    raise AssertionError("Inspection must not replay an audit")
service = DurableJobService(path=sys.argv[1], library_uuid="library-1", open_library=refuse_open)
db = SimpleNamespace(type="SQLite", metadata={})
runtime = CoreRuntime(library=SimpleNamespace(database=db, storage=None), durable_jobs=service, preferences={})
try:
    print(json.dumps(runtime.query("jobs.get", {"job_id": sys.argv[2]})))
finally:
    runtime.shutdown()
""",
            str(durable.path),
            job_id,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    recovered = json.loads(result.stdout)["job"]
    assert recovered["state"] == "aborted"
    assert "effects" in recovered["execution"]["traceback"]
    storage.release.set()
    durable.stop(wait=True)
    assert durable.get(job_id).state == "aborted"
    assert len(durable.repository.list_runs()) == 1


@pytest.mark.parametrize("db_type", ["SQLite", "SQLite_apsw"])
def test_factory_uses_independent_library_and_reopens_audit_receipt(tmp_path, db_type):
    from LiuXin_alpha.core.factory import create_core

    path = tmp_path / "library.sqlite"
    root = tmp_path / "store"
    root.mkdir()
    with create_core(
        database_path=path, db_type=db_type, create=True, enable_maintenance=False
    ) as runtime:
        assert not runtime.durable_jobs.path.exists()
        saved = runtime.command(
            "storage.store.save",
            {
                "store": {
                    "store_name": "audit-source",
                    "store_kind": "filesystem",
                    "store_access_protocol": "file",
                    "store_root_uri": str(root),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                    "store_operational_role": "live",
                }
            },
        )
        runtime.command(
            "storage.refresh",
            {"startup_on_add": True, "clear_existing": True, "strict": True},
        )
        runtime.command("storage.default.set", {"store": saved["store"]["store_name"]})
        stored = runtime.command(
            "storage.file.put",
            {
                "content_base64": base64.b64encode(b"durable audit evidence").decode(
                    "ascii"
                ),
                "original_name": "audit.txt",
            },
        )
        before = runtime.library.storage.get_replica_record(stored["replica_id"])
        accepted = runtime.command(
            "storage.audit.start", {"request_key": "real-library"}
        )
        execution = runtime.query(
            "jobs.result", {"job_id": accepted["job_id"], "timeout_s": 10}
        )["execution"]
        assert execution["ok"] is True, execution
        receipt = execution["result"]
        assert receipt["checked"] == 1
        assert receipt["results"][0]["replica_id"] == stored["replica_id"]
        assert receipt["healthy"] == 1
        # A real separate graph persisted the observation; the main manager reloads it.
        after = runtime.library.storage.get_replica_record(stored["replica_id"])
        assert after.revision != before.revision
        assert after.observation.checked_at is not None
        ledger = runtime.durable_jobs.path
    with create_core(
        database_path=path, db_type=db_type, enable_maintenance=False
    ) as reopened:
        assert reopened.durable_jobs.path == ledger
        found = reopened.query("jobs.get", {"job_id": accepted["job_id"]})["job"]
        assert found["state"] == "succeeded"
        repeated = reopened.command(
            "storage.audit.start", {"request_key": "real-library"}
        )
        assert repeated["job_id"] == accepted["job_id"]
        assert (
            reopened.query(
                "jobs.result", {"job_id": accepted["job_id"], "timeout_s": 0}
            )["execution"]["result"]
            == receipt
        )


def test_ledger_owner_and_namespace_refuse_another_library(audit_runtime):
    from LiuXin_alpha.core.durable_jobs import DurableJobService

    runtime, durable, storage, _lifetime, _utility = audit_runtime
    storage.release.set()
    job_id = runtime.command("storage.audit.start", {"request_key": "owned"})["job_id"]
    assert durable.result(job_id, timeout=5).ok
    wrong = DurableJobService(
        path=durable.path, library_uuid="another-library", open_library=lambda: None
    )
    with pytest.raises(ValueError, match="different owner"):
        wrong.get(job_id)
    with pytest.raises(KeyError):
        durable.get("durable:other-ledger:1")


def test_core_refuses_to_claim_an_existing_unowned_job_ledger(audit_runtime):
    from LiuXin_alpha.jobs.models import JobDefinition
    from LiuXin_alpha.jobs.repository import JobRepository

    _runtime, durable, _storage, _lifetime, _utility = audit_runtime
    repository = JobRepository(durable.path)
    queued = repository.submit_job(
        JobDefinition(
            job_kind="other.application", job_name="Existing work", payload_json="{}"
        ),
        request_key="existing",
    )
    with pytest.raises(ValueError, match="dedicated ledger"):
        _ = durable.repository
    assert repository.get_run(queued.job_run_id) == queued


def test_audit_retry_requires_effect_acknowledgement_and_keeps_ancestry(audit_runtime):
    runtime, durable, storage, _lifetime, _utility = audit_runtime
    first = runtime.command("storage.audit.start", {"request_key": "first"})["job_id"]
    assert storage.entered.wait(4)
    runtime.command("jobs.cancel", {"job_id": first})
    storage.release.set()
    assert not durable.result(first, timeout=5).ok
    from LiuXin_alpha.core.errors import CoreHandlerError

    with pytest.raises(CoreHandlerError, match="effects"):
        runtime.command("jobs.retry", {"job_id": first, "request_key": "retry"})
    payload = {
        "job_id": first,
        "request_key": "retry",
        "acknowledge_possible_effects": True,
    }
    successor = runtime.command("jobs.retry", payload)
    assert successor["job"]["durable"]["attempt_number"] == 2
    assert successor["retry_of_job_id"] == first
    assert durable.result(successor["job_id"], timeout=5).ok
    assert runtime.command("jobs.retry", payload)["job_id"] == successor["job_id"]


def test_disconnected_http_client_does_not_cancel_durable_audit(audit_runtime):
    from LiuXin_alpha.core.proxies.remote import RemoteProxyError

    runtime, durable, storage, _lifetime, _utility = audit_runtime
    job_id = runtime.command("storage.audit.start", {"request_key": "disconnect"})[
        "job_id"
    ]
    assert storage.entered.wait(4)
    daemon = CoreHttpDaemon(
        runtime, policy=CoreTransportPolicy.trusted("disconnect-audit")
    )
    daemon.start()
    try:
        client = RemoteCoreClient(
            endpoint=daemon.base_url,
            bearer_token="disconnect-audit",
            timeout_seconds=0.2,
        )
        with pytest.raises(RemoteProxyError):
            client.query("jobs.result", {"job_id": job_id})
        assert runtime.query("health")["core_uuid"] == runtime.core_uuid
        assert durable.get(job_id).state == "running"
        assert not durable.get(job_id).durable["cancel_requested"]
        storage.release.set()
        assert durable.result(job_id, timeout=5).ok
    finally:
        storage.release.set()
        daemon.stop()


def test_read_only_authority_cannot_create_ledger_or_submit(audit_runtime):
    from LiuXin_alpha.core import CoreCommand, CoreQuery
    from LiuXin_alpha.core.authorization import CoreAuthorizationError

    runtime, durable, _storage, _lifetime, _utility = audit_runtime
    with pytest.raises(CoreAuthorizationError):
        runtime.execute_command(
            CoreCommand("storage.audit.start", {"request_key": "denied"}),
            capabilities=frozenset({"read"}),
        )
    with pytest.raises(CoreAuthorizationError):
        runtime.execute_query(
            CoreQuery("jobs.events", {"job_id": "durable:unknown:1"}),
            capabilities=frozenset({"read"}),
        )
    assert not durable.path.exists()


def test_relative_ledger_keeps_its_location_after_working_directory_changes(
    tmp_path, monkeypatch
):
    from LiuXin_alpha.core.factory import create_core

    monkeypatch.chdir(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    with create_core(
        database_path=tmp_path / "library.sqlite",
        durable_job_path="audit.sqlite",
        create=True,
        enable_maintenance=False,
    ) as runtime:
        monkeypatch.chdir(other)
        accepted = runtime.command(
            "storage.audit.start", {"request_key": "relative-ledger"}
        )
        execution = runtime.query(
            "jobs.result", {"job_id": accepted["job_id"], "timeout_s": 10}
        )["execution"]
        assert execution["ok"] is True
        assert (tmp_path / "audit.sqlite").is_file()
        assert not (other / "audit.sqlite").exists()


@pytest.mark.parametrize("same_path", ["direct", "hardlink"])
def test_factory_refuses_a_ledger_aliasing_the_catalogue(tmp_path, same_path):
    from LiuXin_alpha.core.factory import create_core

    path = tmp_path / "library.sqlite"
    with create_core(database_path=path, create=True, enable_maintenance=False):
        pass
    target = path
    if same_path == "hardlink":
        target = tmp_path / "alias.sqlite"
        target.hardlink_to(path)
    with pytest.raises(ValueError, match="separate"):
        create_core(
            database_path=path, durable_job_path=target, enable_maintenance=False
        )


def test_replaced_library_is_rejected_before_verification(audit_runtime, monkeypatch):
    runtime, durable, storage, lifetime, _utility = audit_runtime
    original_open = durable._handler.open_library

    @contextmanager
    def changed_library():
        with original_open() as services:
            services.library.db.uuid = "replacement-library"
            yield services

    monkeypatch.setattr(durable._handler, "open_library", changed_library)
    job_id = runtime.command("storage.audit.start", {"request_key": "wrong-database"})[
        "job_id"
    ]
    result = durable.result(job_id, timeout=5)
    assert not result.ok
    assert "identity changed" in result.traceback
    assert not storage.entered.is_set()
    assert lifetime == ["opened", "closed"]
