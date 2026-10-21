"""Exercise concurrent job clients while preserving shared database serialization."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, RLock

import pytest

from LiuXin_alpha.core import CoreHttpDaemon, CoreQuery
from LiuXin_alpha.core.authorization import CoreAuthorizationError, CoreTransportPolicy
from LiuXin_alpha.core.errors import CoreHandlerError, CoreShutdownError
from LiuXin_alpha.core.proxies.local import LocalCoreClient
from LiuXin_alpha.core.proxies.remote import RemoteCoreClient, RemoteProxyError
from LiuXin_alpha.utils.jobs import JobRequest
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager
from tests.core.test_core_runtime_jobs_phase2 import _build_runtime_with_manager


def blocked_job(started, release):
    """Keep the real serial worker running until the test releases it."""
    started.set()
    if not release.wait(15):
        raise RuntimeError("Test did not release its worker.")
    return "finished"


class ObservedManager(InMemoryJobManager):
    """Expose wait entry without replacing the manager's execution/state behavior."""

    def __init__(self):
        super().__init__(max_workers=1, default_backend="serial")
        self.wait_entered = Event()

    def wait(self, job_id, *, timeout=None):
        self.wait_entered.set()
        return super().wait(job_id, timeout=timeout)


@pytest.fixture
def running_job():
    manager = ObservedManager()
    runtime = _build_runtime_with_manager(manager)
    started, release = Event(), Event()
    try:
        job_id = manager.submit(
            JobRequest(
                module_name=__name__,
                function_name="blocked_job",
                args=(started, release),
            ),
            no_output=True,
        )
        assert started.wait(5)
        yield runtime, manager, job_id, release
    finally:
        release.set()
        manager.shutdown(wait=True, cancel_pending=True)
        runtime.shutdown()


@pytest.fixture(params=("local", "http"))
def clients(request, running_job):
    runtime, _manager, _job_id, release = running_job
    if request.param == "local":
        yield LocalCoreClient(runtime), LocalCoreClient(runtime)
        return
    token = "job-concurrency-test"
    daemon = CoreHttpDaemon(runtime, policy=CoreTransportPolicy.trusted(token))
    daemon.start()
    try:
        yield tuple(
            RemoteCoreClient(endpoint=daemon.base_url, bearer_token=token)
            for _ in range(2)
        )
    finally:
        release.set()
        daemon.stop()


@pytest.mark.parametrize("route", ("jobs.wait", "jobs.result"))
def test_waiting_client_does_not_block_health_progress_or_cancel(
    running_job, clients, route
):
    """The second client completes before the deliberately blocked worker does."""
    runtime, manager, job_id, release = running_job
    waiter, observer = clients

    def inspect_and_cancel():
        assert observer.query("health")["core_uuid"] == runtime.core_uuid
        assert (
            observer.query("jobs.get", {"job_id": job_id})["job"]["state"] == "running"
        )
        return observer.command("jobs.cancel", {"job_id": job_id})

    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            pending = pool.submit(waiter.query, route, {"job_id": job_id})
            assert manager.wait_entered.wait(3)
            cancellation = pool.submit(inspect_and_cancel).result(timeout=3)
            assert cancellation["cancelled"] is True
            # Serial execution cannot preempt a running function.
            assert not pending.done()
            release.set()
            result = pending.result(timeout=3)
            if route == "jobs.wait":
                assert result["job"]["state"] == "succeeded"
            else:
                assert result["execution"]["result"] == "finished"
        finally:
            release.set()


@pytest.mark.parametrize("route", ("jobs.wait", "jobs.result"))
def test_wait_authority_timeout_and_shutdown_preserve_manager_ownership(
    running_job, route
):
    runtime, manager, job_id, release = running_job
    query = CoreQuery(route, {"job_id": job_id, "timeout_s": 0})
    with pytest.raises(CoreAuthorizationError):
        runtime.execute_query(query, capabilities=frozenset({"read"}))
    assert not manager.wait_entered.is_set()
    if route == "jobs.wait":
        assert runtime.execute_query(query).result["job"]["state"] == "running"
    else:
        with pytest.raises(CoreHandlerError, match="no execution payload yet"):
            runtime.execute_query(query)
    assert manager.get(job_id).state == "running"
    with pytest.raises(CoreHandlerError, match="Unknown job"):
        runtime.query(route, {"job_id": "absent", "timeout_s": 0})

    manager.wait_entered.clear()
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            pending = pool.submit(runtime.query, route, {"job_id": job_id})
            assert manager.wait_entered.wait(3)
            assert pool.submit(runtime.shutdown).result(timeout=3) == 0
            assert not pending.done()
            with pytest.raises(CoreShutdownError):
                runtime.query("health")
            release.set()
            pending.result(timeout=3)
            # This runtime borrowed its manager; shutdown cannot close it.
            next_job = manager.submit(
                JobRequest(module_name="math", function_name="sqrt", args=(9,))
            )
            assert manager.result(next_job, timeout=3).result == 3
        finally:
            release.set()


@pytest.mark.parametrize("route", ("jobs.wait", "jobs.result"))
def test_disconnected_http_wait_leaves_job_running_and_dispatch_available(
    running_job, route
):
    runtime, manager, job_id, release = running_job
    token = "disconnected-job-client"
    daemon = CoreHttpDaemon(runtime, policy=CoreTransportPolicy.trusted(token))
    daemon.start()
    try:
        waiter = RemoteCoreClient(
            endpoint=daemon.base_url, bearer_token=token, timeout_seconds=0.25
        )
        observer = RemoteCoreClient(
            endpoint=daemon.base_url, bearer_token=token, timeout_seconds=3
        )
        with pytest.raises(RemoteProxyError):
            waiter.query(route, {"job_id": job_id})
        assert manager.wait_entered.wait(3)
        assert observer.query("health")["core_uuid"] == runtime.core_uuid
        assert (
            observer.query("jobs.get", {"job_id": job_id})["job"]["state"] == "running"
        )
    finally:
        release.set()
        daemon.stop()


class ObservedLock:
    """Signal an attempted acquisition before it blocks on the real reentrant lock."""

    def __init__(self):
        self.lock = RLock()
        self.attempted = Event()

    def __enter__(self):
        self.attempted.set()
        self.lock.acquire()
        return self

    def __exit__(self, *args):
        self.lock.release()


@pytest.mark.parametrize("route", ("example.read", "jobs.wait", "jobs.result"))
def test_replaced_queries_default_to_serialized_database_access(
    core_runtime_factory, route
):
    """Replacement restores the safe default, including formerly concurrent names."""
    runtime = core_runtime_factory()
    lock = runtime._command_lock = ObservedLock()
    entered = Event()
    runtime.register_query_handler(route, lambda owner, query: entered.set())
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            with lock:
                lock.attempted.clear()
                pending = pool.submit(runtime.query, route)
                assert lock.attempted.wait(3)
                assert not entered.is_set()
            pending.result(timeout=3)
            assert entered.is_set()
        finally:
            runtime.shutdown()


@pytest.mark.parametrize("kind", ("command", "query"))
def test_queued_handlers_refuse_execution_after_shutdown(core_runtime_factory, kind):
    runtime = core_runtime_factory()
    lock = runtime._command_lock = ObservedLock()
    entered = Event()
    events = []
    runtime.subscribe(events.append)
    getattr(runtime, f"register_{kind}_handler")(
        "example.queued", lambda owner, envelope: entered.set()
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        with lock:
            lock.attempted.clear()
            pending = pool.submit(getattr(runtime, kind), "example.queued")
            assert lock.attempted.wait(3)
            runtime.shutdown()
        with pytest.raises(CoreShutdownError):
            pending.result(timeout=3)
    assert not entered.is_set()
    if kind == "command":
        assert [event.event_type for event in events].count("command.failed") == 1


def test_query_replacement_keeps_inflight_result_policy(core_runtime_factory):
    runtime = core_runtime_factory()
    entered, release = Event(), Event()
    opaque = object()

    def old_handler(owner, query):
        entered.set()
        assert release.wait(5)
        return opaque

    runtime.register_query_handler(
        "example.replace", old_handler, transport_stable=False
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            pending = pool.submit(runtime.query, "example.replace")
            assert entered.wait(3)
            runtime.register_query_handler(
                "example.replace", lambda owner, query: {"new": True}
            )
            release.set()
            assert pending.result(timeout=3) is opaque
            assert runtime.query("example.replace") == {"new": True}
        finally:
            release.set()
            runtime.shutdown()


@pytest.mark.parametrize("backend", ("SQLite", "SQLite_apsw"))
def test_database_commands_keep_separate_serial_transactions(tmp_path, backend):
    """The job exception must not let another handler enter an active DB transaction."""
    from LiuXin_alpha.core import CoreRuntime
    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.library.library import Library

    if backend == "SQLite_apsw":
        pytest.importorskip("apsw")
    with Database(
        metadata={"database_path": str(tmp_path / "dispatch.sqlite")},
        db_type=backend,
        create=True,
        backup=False,
        enable_storage_manager=False,
    ) as db:
        runtime = CoreRuntime(library=Library(database=db))
        lock = runtime._command_lock = ObservedLock()
        first_entered, second_entered, release = Event(), Event(), Event()

        def first_write(owner, command):
            with db.macros.transaction():
                row_id = db.macros.insert_row("works", {"work_title": "First"})
                first_entered.set()
                assert release.wait(10)
                return row_id

        def second_write(owner, command):
            second_entered.set()
            with db.macros.transaction():
                return db.macros.insert_row("works", {"work_title": "Second"})

        runtime.register_command_handler("example.first", first_write)
        runtime.register_command_handler("example.second", second_write)
        with ThreadPoolExecutor(max_workers=2) as pool:
            try:
                first = pool.submit(runtime.command, "example.first")
                assert first_entered.wait(3)
                lock.attempted.clear()
                second = pool.submit(runtime.command, "example.second")
                assert lock.attempted.wait(3)
                assert not second_entered.is_set()
                release.set()
                first_id, second_id = first.result(timeout=3), second.result(timeout=3)
                assert db.macros.get_row("works", first_id)["work_title"] == "First"
                assert db.macros.get_row("works", second_id)["work_title"] == "Second"
                assert first_id != second_id
            finally:
                release.set()
        runtime.shutdown()
