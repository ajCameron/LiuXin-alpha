"""Exclude surviving executors across retries of one durable logical action."""

from __future__ import annotations

import hashlib
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.jobs.repository import JobRepository


@contextmanager
def execution_guard(repository: JobRepository, run_id: int) -> Iterator[None]:
    """Take a nonblocking OS lock shared by every attempt in a retry lineage.

    Lease expiry cannot release this lock. The file is retained permanently;
    unlinking it while another executor holds it would defeat exclusion. The
    SQLite ledger and these lock files must live on a local filesystem.
    """
    run = repository.get_run(run_id)
    visited: set[int | None] = set()
    while run.retry_of_job_run_id is not None:
        if run.job_run_id in visited or len(visited) > 256:
            raise ValueError("Invalid or excessive retry ancestry")
        visited.add(run.job_run_id)
        run = repository.get_run(run.retry_of_job_run_id)
    directory = repository.sqlite_path.with_name(
        repository.sqlite_path.name + ".execution-locks"
    )
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / f"{repository.ledger_uuid}-{run.job_run_id}.lock"
    with _locked_file(path):
        yield


@contextmanager
def resource_guard(repository: JobRepository, key: str | None) -> Iterator[None]:
    """Exclude concurrent effects on a handler-selected resource across command IDs."""
    if key is None:
        yield
        return
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    directory = repository.sqlite_path.with_name(
        repository.sqlite_path.name + ".execution-locks"
    )
    directory.mkdir(mode=0o700, exist_ok=True)
    with _locked_file(directory / f"{repository.ledger_uuid}-resource-{digest}.lock"):
        yield


@contextmanager
def _locked_file(path: Path) -> Iterator[None]:
    with path.open("a+b") as handle:
        if sys.platform == "win32":
            import msvcrt

            handle.seek(0, os.SEEK_END)
            if not handle.tell():
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
