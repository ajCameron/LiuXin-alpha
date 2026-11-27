"""Define when a saved backup can safely re-enter staging after executor loss."""

from __future__ import annotations

from LiuXin_alpha.storage.api import BackupWorkflowCheckpoint, WorkflowStatus


def can_resume_staging(checkpoint: BackupWorkflowCheckpoint) -> bool:
    """Permit only owned, content-pinned staging before the publication boundary.

    Finalization can leave an artifact without a persisted seal milestone. Such
    checkpoints require explicit inspection; this predicate never adopts output.
    Re-entering a permitted checkpoint must verify all previously staged sources.
    """
    declaration = checkpoint.declaration
    return (
        checkpoint.status is WorkflowStatus.RUNNING
        and checkpoint.next_source_index < len(declaration.sources)
        and checkpoint.output_artifact_reference is None
        and bool(declaration.option_map().get("staging_workspace"))
        and all(
            source.expected_size is not None and source.expected_digest is not None
            for source in declaration.sources
        )
        and len(checkpoint.source_reports) == checkpoint.next_source_index
    )
