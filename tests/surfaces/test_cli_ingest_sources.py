"""Keep the automatic source shorthand protected and preserve explicit legacy routing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import uuid4

import pytest

from LiuXin_alpha.ingest.source_mount_paths import managed_mount_target
from LiuXin_alpha.surfaces.cli import ingest_runs
from LiuXin_alpha.surfaces.cli.app import main as cli_main
from LiuXin_alpha.surfaces.cli.storage_commands import (
    ingest,
    ingest_mount,
    ingest_sources,
)
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_options import (
    add_storage_ingest_arguments,
)


def _arguments(*arguments: str) -> argparse.Namespace:
    """Use the actual shared parser for all option combinations."""
    parser = argparse.ArgumentParser()
    add_storage_ingest_arguments(parser)
    return parser.parse_args(arguments)


@pytest.mark.parametrize(
    "kind,filesystem", [("directory", None), ("image", "squashfs"), ("device", "ext4")]
)
def test_positional_source_always_selects_protected_ingest(
    monkeypatch, tmp_path: Path, kind, filesystem
) -> None:
    """Translate the ergonomic form into one of the already verified mount paths."""
    source = tmp_path / "source with spaces"
    monkeypatch.setattr(
        ingest_sources, "detect_source", lambda path: (kind, filesystem)
    )
    received = []
    monkeypatch.setattr(
        ingest_mount, "launch_mounted_ingest", lambda args: received.append(args) or 0
    )
    monkeypatch.setattr(
        ingest, "_cmd_storage_ingest", lambda args: pytest.fail("unprotected ingest")
    )
    assert ingest.cmd_storage_ingest(_arguments(str(source))) == 0
    (args,) = received
    assert args.mount_source_read_only is True
    assert args.source is None
    if kind == "directory":
        assert args.source_root == str(source)
        assert not args.source_mount_managed
    else:
        assert args.source_root == str(managed_mount_target(source))
        assert args.source_mount_managed is True
        assert args.source_filesystem == filesystem
        assert getattr(
            args, "source_image" if kind == "image" else "source_device"
        ) == str(source)
    assert not source.exists()


@pytest.mark.parametrize(
    "options",
    [
        [],
        ["SOURCE", "--source-root", "folder"],
        ["SOURCE", "--source-image", "image"],
        ["SOURCE", "--mount-point", "target"],
        ["SOURCE", "--source-filesystem", "ext4"],
    ],
)
def test_conflicting_or_absent_source_does_not_start_ingest(
    monkeypatch, options
) -> None:
    """Do not hide conflicting selectors or weaken directory protection to accept options."""
    monkeypatch.setattr(
        ingest_sources, "detect_source", lambda path: ("directory", None)
    )
    monkeypatch.setattr(
        ingest_mount, "launch_mounted_ingest", lambda args: pytest.fail("launched")
    )
    monkeypatch.setattr(
        ingest, "_cmd_storage_ingest", lambda args: pytest.fail("unprotected ingest")
    )
    assert ingest.cmd_storage_ingest(_arguments(*options)) == 2


@pytest.mark.parametrize(
    "prefix", [["storage", "ingest"], ["ingest"], ["ingest", "--source"]]
)
def test_public_shortcuts_share_automatic_directory_protection(
    monkeypatch, tmp_path: Path, prefix
) -> None:
    """All supported positional/source shorthand front doors reach the same mount launcher."""
    seen = []
    monkeypatch.setattr(
        ingest_mount, "launch_mounted_ingest", lambda args: seen.append(args) or 0
    )
    assert cli_main([*prefix, str(tmp_path), "--discover-only"]) == 0
    assert seen[0].source_root == str(tmp_path)
    assert seen[0].mount_source_read_only


def test_explicit_source_root_retains_opt_in_mounting(
    monkeypatch, tmp_path: Path
) -> None:
    """Existing scripts can continue using the explicit directory interface on every OS."""
    monkeypatch.setattr(ingest, "_cmd_storage_ingest", lambda args: 17)
    monkeypatch.setattr(
        ingest_mount,
        "launch_mounted_ingest",
        lambda args: pytest.fail("unexpected mount"),
    )
    assert ingest.cmd_storage_ingest(_arguments("--source-root", str(tmp_path))) == 17


def test_unknown_image_fails_before_creating_outputs(tmp_path: Path, capsys) -> None:
    """A .squashfs extension alone must not result in mounting or catalogue/log writes."""
    source = tmp_path / "fake.squashfs"
    source.write_bytes(b"not a filesystem")
    existing = set(tmp_path.iterdir())
    assert (
        cli_main(
            [
                "storage",
                "ingest",
                str(source),
                "--database",
                str(tmp_path / "catalogue"),
                "--log-directory",
                str(tmp_path / "logs"),
            ]
        )
        == 2
    )
    assert "ERROR:" in capsys.readouterr().err
    assert set(tmp_path.iterdir()) == existing
    assert source.read_bytes() == b"not a filesystem"


def test_explicit_target_overrides_managed_destination(
    monkeypatch, tmp_path: Path
) -> None:
    """Support an operator-selected stable mount point without creating it implicitly."""
    source, target = tmp_path / "image", tmp_path / "target"
    monkeypatch.setattr(
        ingest_sources, "detect_source", lambda path: ("image", "squashfs")
    )
    args = _arguments(str(source), "--mount-point", str(target))
    ingest_sources.prepare_source_arguments(args)
    assert args.source_root == str(target)
    assert args.mount_point is None and not args.source_mount_managed
    assert not target.exists()


@pytest.mark.parametrize(
    "option,kind", [("--source-image", "image"), ("--source-device", "device")]
)
def test_explicit_attachment_can_detect_filesystem_and_allocate_target(
    monkeypatch, tmp_path: Path, option: str, kind: str
) -> None:
    """Explicit attachment selectors can omit both filesystem and mount destination."""
    source = tmp_path / "source"
    monkeypatch.setattr(ingest_sources, "detect_source", lambda path: (kind, "ext4"))
    args = _arguments(option, str(source))
    ingest_sources.prepare_source_arguments(args)
    assert args.source_filesystem == "ext4"
    assert args.source_root == str(managed_mount_target(source))
    assert args.source_mount_managed


def test_resume_reuses_managed_target_without_reinterpreting_positional_source(
    tmp_path: Path,
) -> None:
    """Replay the pinned kind/filesystem and stable path, recreating the managed target if needed."""
    source = tmp_path / "image"
    target = managed_mount_target(source)
    event_log = tmp_path / "run.jsonl"
    event_log.write_text(
        json.dumps(
            {
                "context": {
                    "event": "cli_started",
                    "details": {
                        "mode": "ingest",
                        "source_root": str(target),
                        "database": str(tmp_path / "catalogue"),
                        "arguments": {
                            "source": None,
                            "source_image": str(source),
                            "source_filesystem": "squashfs",
                            "source_mount_managed": True,
                            "mount_source_read_only": True,
                        },
                    },
                }
            }
        )
        + "\n"
    )
    args = ingest_runs._resume_namespace(
        argparse.Namespace(run_id=str(uuid4())), {"event_log": str(event_log)}
    )
    ingest_sources.prepare_source_arguments(args)
    assert args.source_root == str(target)
    assert args.source_mount_managed and args.mount_source_read_only
    assert args.source is None
