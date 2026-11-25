"""Restore registered bytes and execute pinned exact recipes before publication.

Recipe execution is privileged application work, not a sandbox for untrusted tools.
Executors, inputs and dependencies are verified before execution. Only an output
matching its registered identity may be published. Interrupted publication remains
uncertain and is never automatically repeated by this owner.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from urllib.parse import unquote, urlparse

from LiuXin_alpha.storage import api


def restore_asset(
    manager: Any,
    asset_id: int,
    *,
    destination: Any = None,
    source_replica_id: int | None = None,
    checkpoint: Callable[[], None] = lambda: None,
) -> Any:
    """Create and verify an active copy of an existing Asset without deleting its source."""
    checkpoint()
    if source_replica_id is None:
        # Restoration must work when only a backup, archive or adopted source
        # remains. Normal foreground reads deliberately select active copies.
        for source_mode in (
            api.ReplicaMode.ACTIVE,
            api.ReplicaMode.BACKUP,
            api.ReplicaMode.ARCHIVE,
            api.ReplicaMode.UNMANAGED,
            api.ReplicaMode.CACHE,
            api.ReplicaMode.TRANSIENT,
        ):
            try:
                source_replica_id = manager.select_replica(
                    asset_id, mode=source_mode
                ).replica_id
                break
            except api.NoReadableReplica:
                continue
        if source_replica_id is None:
            raise api.NoReadableReplica(
                f"Digital Asset {asset_id} has no readable restoration source"
            )
    replica = manager.replicate_digital_asset(
        asset_id,
        destination_store_ref=destination,
        source_replica_id=source_replica_id,
        mode=api.ReplicaMode.ACTIVE,
        verify=True,
    )
    if replica.observation.state is not api.ReplicaState.VERIFIED:
        raise api.StoreIntegrityError(
            "Restored replica did not verify; retain publication for inspection"
        )
    return replica


def _member(root: Path, name: str) -> Path:
    member = PurePosixPath(name)
    if member.is_absolute() or ".." in member.parts or "\\" in name:
        raise ValueError("Recipe path must remain inside its workspace")
    path = root.joinpath(*member.parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _copy_verified(
    source: BinaryIO,
    target: Path,
    *,
    expected_size: int | None,
    digests: tuple[Any, ...],
    max_bytes: int,
    checkpoint: Callable[[], None],
) -> int:
    if not digests or max_bytes < 0:
        raise ValueError("Recipe material requires identity and a nonnegative budget")
    hashers = {item.algorithm: hashlib.new(item.algorithm) for item in digests}
    total = 0
    with target.open("xb") as output:
        while True:
            checkpoint()
            chunk = source.read(min(1024 * 1024, max_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes or (
                expected_size is not None and total > expected_size
            ):
                raise api.StoreIntegrityError(
                    "Recipe material exceeds its expected size or budget"
                )
            output.write(chunk)
            for hasher in hashers.values():
                hasher.update(chunk)
    if expected_size is not None and total != expected_size:
        raise api.StoreIntegrityError("Recipe material has the wrong size")
    if any(hashers[item.algorithm].hexdigest() != item.value for item in digests):
        raise api.StoreIntegrityError("Recipe material digest changed")
    return total


def _artifact_source(manager: Any, artifact: Any) -> AbstractContextManager[BinaryIO]:
    if artifact.digital_asset_id is not None:
        replica = manager.select_replica(artifact.digital_asset_id)
        return manager.get(replica.location)
    uri = urlparse(artifact.uri or "")
    if uri.scheme != "file" or uri.netloc not in ("", "localhost"):
        raise ValueError(
            "Pinned executor/dependencies require a managed Asset or local file URI"
        )
    return Path(unquote(uri.path)).open("rb")


def recreate_asset(
    manager: Any,
    asset_id: int,
    *,
    destination: Any = None,
    max_steps: int = 32,
    max_bytes: int = 1024**4,
    checkpoint: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    """Execute a bounded exact plan, verifying each result before managed ingestion.

    The copied pinned executable replaces argv[0]; remaining arguments are literal
    argv values, without shell evaluation or interpolation. Environment variables
    come from the captured recipe. Ordinary inherited credentials are excluded.
    Completed prerequisite publications survive failure of a later step.
    """
    if type(max_steps) is not int or not 1 <= max_steps <= 32:
        raise ValueError("max_steps must be between 1 and 32")
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")
    plan = manager.plan_digital_asset_recreation(asset_id)
    if not plan.can_recreate_exactly or len(plan.steps) > max_steps:
        raise api.StorePreconditionFailed(
            "No bounded exact recreation plan is available"
        )
    completed = []
    consumed = 0
    for step in plan.steps:
        checkpoint()
        recipe = step.declaration.recipe
        if (
            recipe is None
            or not recipe.complete
            or recipe.reproducibility is not api.Reproducibility.EXACT
        ):
            raise api.StorePreconditionFailed(
                "Recreation requires a complete exact recipe"
            )
        if recipe.executor is None or not recipe.command or recipe.output_path is None:
            raise api.StorePreconditionFailed(
                "Recipe is missing its executor, command or output"
            )
        with tempfile.TemporaryDirectory(prefix="liuxin-recreation-") as workspace:
            root = Path(workspace)
            for source in recipe.inputs:
                replica = manager.select_replica(source.digital_asset_id)
                with manager.get(replica.location) as stream:
                    consumed += _copy_verified(
                        stream,
                        _member(root, source.logical_path),
                        expected_size=source.size_bytes,
                        digests=source.digests,
                        max_bytes=max_bytes - consumed,
                        checkpoint=checkpoint,
                    )
            artifacts = (recipe.executor, *recipe.dependencies)
            executable = None
            for artifact in artifacts:
                target = _member(root, artifact.name)
                with _artifact_source(manager, artifact) as stream:
                    consumed += _copy_verified(
                        stream,
                        target,
                        expected_size=None,
                        digests=(artifact.digest,),
                        max_bytes=max_bytes - consumed,
                        checkpoint=checkpoint,
                    )
                if artifact is recipe.executor:
                    target.chmod(0o700)
                    executable = target
            environment = json.loads(recipe.environment_json)
            if not isinstance(environment, dict) or any(
                not isinstance(k, str) or not isinstance(v, str)
                for k, v in environment.items()
            ):
                raise ValueError(
                    "Recipe environment must contain string keys and values"
                )
            directory = _member(root, recipe.working_directory)
            directory.mkdir(parents=True, exist_ok=True)
            output = _member(root, recipe.output_path)
            if output.exists() or output.is_symlink():
                raise ValueError("Recipe output overlaps staged material")
            checkpoint()
            process = subprocess.Popen(
                [str(executable), *recipe.command[1:]],
                cwd=directory,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=os.name == "posix",
            )
            try:
                while process.poll() is None:
                    checkpoint()
                    time.sleep(0.1)
                if process.returncode != 0:
                    raise RuntimeError(
                        f"Pinned executor exited with status {process.returncode}"
                    )
            finally:
                if process.poll() is None:
                    if os.name == "posix":
                        import signal

                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait()
            if (
                output.is_symlink()
                or not output.is_file()
                or not output.resolve().is_relative_to(root)
            ):
                raise api.StoreIntegrityError(
                    "Recipe did not produce a confined regular output"
                )
            record = manager.get_digital_asset_record(
                step.declaration.result_digital_asset_id
            )
            verified = root / ".verified-output"
            with output.open("rb") as stream:
                consumed += _copy_verified(
                    stream,
                    verified,
                    expected_size=record.size_bytes,
                    digests=record.digests,
                    max_bytes=max_bytes - consumed,
                    checkpoint=checkpoint,
                )
            checkpoint()
            with verified.open("rb") as stream:
                result = manager.ingest_stream(
                    stream,
                    expected_size=record.size_bytes,
                    expected_digests=record.digests,
                    preferred_store_ref=destination,
                    verify=True,
                )
            if result.asset_record.digital_asset_id != record.digital_asset_id:
                raise api.StoreIntegrityError(
                    "Recreated output resolved to a different registered Asset"
                )
            if result.replica_record.state is not api.ReplicaState.VERIFIED:
                raise api.StoreIntegrityError(
                    "Recreated output was published without successful verification"
                )
            completed.append(int(step.digital_asset_derivation_id))
    return {
        "asset_id": asset_id,
        "completed_derivations": completed,
        "already_available": plan.already_available,
        "materialized_bytes": consumed,
    }
