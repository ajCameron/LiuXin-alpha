"""Own a backup's private workspace independently of its caller's staging parent."""

from __future__ import annotations

import pathlib
import shutil
import tempfile
from dataclasses import dataclass
from uuid import UUID

from LiuXin_alpha.storage.api import StorePreconditionFailed


@dataclass(frozen=True)
class BackupStagingWorkspace:
    """Keep ownership evidence outside the members that become the archive."""

    path: pathlib.Path
    builder_uuid: UUID

    @property
    def members(self) -> pathlib.Path:
        """Return the only directory exposed to the build Store."""
        return self.path / "members"

    @property
    def _marker(self) -> pathlib.Path:
        """Return the ownership record, which must never become an archive member."""
        return self.path / ".liuxin-backup-owner"

    @property
    def _identity(self) -> str:
        """Bind the versioned ownership record to the persisted builder identity."""
        return f"liuxin-backup-staging-v1\n{self.builder_uuid}\n"

    @classmethod
    def create(cls, parent: str | None, builder_uuid: UUID) -> BackupStagingWorkspace:
        """Exclusively allocate a workspace under the supplied parent or system scratch."""
        directory = None
        if parent is not None:
            directory = pathlib.Path(parent).expanduser().resolve()
            directory.mkdir(parents=True, exist_ok=True)
        workspace = cls(
            pathlib.Path(
                tempfile.mkdtemp(prefix="liuxin-backup-", dir=directory)
            ).resolve(),
            builder_uuid,
        )
        try:
            workspace.members.mkdir()
            with workspace._marker.open("x", encoding="utf-8") as marker:
                marker.write(workspace._identity)
        except BaseException:
            shutil.rmtree(workspace.path)
            raise
        return workspace

    def validate(self, staging_root: pathlib.Path) -> None:
        """Refuse missing, redirected or replaced staging before writes or cleanup."""
        try:
            valid = (
                self.path.is_dir()
                and not self.path.is_symlink()
                and self.path.resolve() == self.path
                and staging_root == self.members
                and self.members.is_dir()
                and not self.members.is_symlink()
                and self._marker.is_file()
                and not self._marker.is_symlink()
                and self._marker.read_text(encoding="utf-8") == self._identity
            )
        except (OSError, UnicodeError):
            valid = False
        if not valid:
            raise StorePreconditionFailed(
                "backup staging ownership cannot be verified; retain the workspace "
                "for inspection instead of recreating or cleaning it."
            )

    def cleanup(self, staging_root: pathlib.Path) -> None:
        """Remove only this verified workspace; surface cleanup failures to the workflow."""
        self.validate(staging_root)
        shutil.rmtree(self.path)
