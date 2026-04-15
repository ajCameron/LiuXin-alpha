"""Backup workflow contracts.

Backup workflows are orchestration objects that sit *above* raw store plugins.
They coordinate designation of source files/locations, staging, sealing, resume
state, and final backup artifacts. They should not be confused with raw storage
plugins or with the top-level storage manager.
"""

from __future__ import annotations

import abc
from typing import Callable, Self

from LiuXin_alpha.storage.api.backup_workflow_models import (
    BackupSourceSpec,
    BackupWorkflowResult,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
)
from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class BackupWorkflowAPI(abc.ABC):
    """Contract for one resumable backup/export workflow.

    A workflow owns the *process* of creating a backup artifact. Raw store
    plugins own byte access. Store containers own configured stores. The
    storage manager chooses stores. Backup workflows sit beside those concerns
    and can therefore track planning, progress, and resume state explicitly.
    """

    @property
    @abc.abstractmethod
    def workflow_kind(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def workflow_name(self) -> str:
        ...

    @abc.abstractmethod
    def build_spec(self) -> BackupWorkflowSpec:
        ...

    @abc.abstractmethod
    def progress(self) -> BackupWorkflowResumeState:
        ...

    @abc.abstractmethod
    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        ...

    @abc.abstractmethod
    def designate_location(
        self,
        source_location: StoreLocationMixinAPI,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        ...

    @abc.abstractmethod
    def run_next(self) -> BackupWorkflowResumeState:
        ...

    @abc.abstractmethod
    def run_to_completion(self) -> BackupWorkflowResult:
        ...

    @abc.abstractmethod
    def cancel(self) -> BackupWorkflowResumeState:
        ...

    @classmethod
    @abc.abstractmethod
    def from_resume_state(
        cls,
        resume_state: BackupWorkflowResumeState,
        *,
        location_loader: Callable[[str], StoreLocationMixinAPI] | None = None,
    ) -> Self:
        ...
