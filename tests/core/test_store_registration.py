"""Exercise one-shot registration ownership across the four discovery modes."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.core import store_registration
from LiuXin_alpha.ingest.models import RemoteHtmlRegistrationReport
from LiuXin_alpha.storage.reconcile.models import UnmanagedDiskRegistrationReport


@pytest.mark.parametrize(
    "helper,operation,module,options,report_type",
    (
        (
            "register_existing_disk_with_database_path",
            "register_native_disk",
            "LiuXin_alpha.ingest.native_disk",
            {
                "disk_path": Path("books"),
                "compute_hash": False,
                "follow_symlinks": True,
            },
            UnmanagedDiskRegistrationReport,
        ),
        (
            "register_rclone_http_readonly_with_database_path",
            "register_rclone_http_readonly_store_files",
            "LiuXin_alpha.storage.reconcile.store_db_sync",
            {
                "remote_url": "https://example.test/books/",
                "rclone_args": ("--fast-list",),
                "capture_hashes": True,
            },
            UnmanagedDiskRegistrationReport,
        ),
        (
            "register_wget_html_readonly_with_database_path",
            "register_wget_html_readonly_store_files",
            "LiuXin_alpha.ingest.remote_html",
            {
                "remote_url": "https://example.test/books/",
                "wget_args": ("--quiet",),
                "incremental_db_writes": False,
            },
            RemoteHtmlRegistrationReport,
        ),
        (
            "register_native_html_readonly_with_database_path",
            "register_native_html_readonly_store_files",
            "LiuXin_alpha.ingest.remote_html",
            {
                "remote_url": "https://example.test/books/",
                "max_pages": 17,
                "max_depth": 2,
            },
            RemoteHtmlRegistrationReport,
        ),
    ),
)
@pytest.mark.parametrize("outcome", ("success", "startup", "registration", "cleanup"))
@pytest.mark.parametrize("target", ("sqlite", "url", "service"))
def test_registration_uses_one_owned_graph(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    helper: str,
    operation: str,
    module: str,
    options: dict[str, Any],
    report_type: type[UnmanagedDiskRegistrationReport]
    | type[RemoteHtmlRegistrationReport],
    outcome: str,
    target: str,
) -> None:
    """Forward policy, observe progress inside the graph, and close before return.

    Startup failure cannot run registration; later failures cannot repeat it or
    skip cleanup. The lifecycle owner's own rollback tests cover its internals.
    """
    events: list[str] = []
    database = object()
    report = report_type(1, "books", "Books")
    path = {
        "sqlite": str(tmp_path / "existing.sqlite"),
        "url": "postgresql://localhost/selected",
        "service": "service=selected",
    }[target]
    backend = "SQLite_APSW" if target == "sqlite" else "PostgreSQL"
    metadata = None if target == "sqlite" else {"schema": "selected_schema"}
    forwarded = {
        **options,
        "ebook_extensions": (),
        "attach_store_links": False,
        "refresh_storage_manager": False,
    }

    @contextmanager
    def open_graph(**kwargs: Any) -> Iterator[SimpleNamespace]:
        assert kwargs == {
            "database_path": path,
            "db_type": backend,
            "database_metadata": metadata,
            "create": False,
            "backup": False,
            "enable_maintenance": False,
        }
        events.append("open")
        if outcome == "startup":
            raise RuntimeError("startup failed")
        try:
            yield SimpleNamespace(database=database, library=database)
        finally:
            events.append("close")
            if outcome == "cleanup":
                raise RuntimeError("cleanup failed")

    def progress(event: str, current: object, details: dict[str, object]) -> None:
        assert current is report
        assert event == "scan"
        assert details == {"seen": 1}
        assert events == ["open", "register"]
        events.append("progress")

    def register(db: object, **kwargs: Any) -> object:
        assert db is database
        assert all(kwargs[key] == value for key, value in forwarded.items())
        assert kwargs["progress_callback"] is progress
        events.append("register")
        kwargs["progress_callback"]("scan", report, {"seen": 1})
        if outcome == "registration":
            raise RuntimeError("registration failed")
        return report

    monkeypatch.setattr(store_registration.LibraryServices, "open", open_graph)
    monkeypatch.setattr(import_module(module), operation, register)
    invoke = getattr(store_registration, helper)
    if outcome == "success":
        assert (
            invoke(
                database_path=str(path),
                db_type=backend,
                database_metadata=metadata,
                progress_callback=progress,
                **forwarded,
            )
            is report
        )
    else:
        with pytest.raises(RuntimeError, match=f"{outcome} failed"):
            invoke(
                database_path=str(path),
                db_type=backend,
                database_metadata=metadata,
                progress_callback=progress,
                **forwarded,
            )
    assert events == (
        ["open"] if outcome == "startup" else ["open", "register", "progress", "close"]
    )
