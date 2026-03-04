from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryDirectory


def _looks_like_project_root(path: Path) -> bool:
    return (path / ".git").exists() and (path / "src" / "LiuXin_alpha").is_dir()


def choose_conversion_workdir(prefix: str) -> str:
    """
    Return a writable conversion workdir.

    Most conversion code writes intermediate files to CWD by design. When a
    converter is invoked directly from the repository root this can pollute the
    checkout with transient files (`metadata.opf`, `index.xhtml`, etc.). In
    that case we redirect work to a persistent temp directory instead.
    """
    cwd = Path.cwd()
    if _looks_like_project_root(cwd):
        workdir = PersistentTemporaryDirectory(prefix=prefix)
        default_log.info(
            f"Using temporary conversion workdir instead of project root: {workdir}"
        )
        return workdir
    return str(cwd)

