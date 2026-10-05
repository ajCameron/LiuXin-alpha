"""
Export concrete sealed-container Asset provenance orchestration.

SealedArtifactWorkflow catalogues existing image bytes and records ordered member/recipe
evidence through a borrowed manager. Physical archive building and completed-image Store
registration are provided separately by the backup and backend packages.
"""

from LiuXin_alpha.storage.workflows.sealed_artifact_workflow import (
    SealedArtifactWorkflow,
)


__all__ = ["SealedArtifactWorkflow"]
