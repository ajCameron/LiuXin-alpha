"""Protect CLI composition, standalone imports, and historical entry points."""

from __future__ import annotations

import argparse
import importlib
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.cli import (
    app,
    completion,
    parsers,
    squashfs,
    squashfs_commands,
    squashfs_parsers,
)
from LiuXin_alpha.surfaces.cli.parser_types import CompletionSubparsers
from LiuXin_alpha.surfaces.core import CoreRow, CoreSurfaceModel

ROOT = Path(__file__).resolve().parents[2]
PREFIX = "LiuXin_alpha.surfaces.cli"


@pytest.mark.parametrize(
    "module",
    [
        "",
        ".parser_types",
        ".parsers",
        ".completion",
        ".squashfs_commands",
        ".squashfs_parsers",
        ".squashfs",
    ],
)
def test_cold_import_does_not_load_application_entry_points(module: str) -> None:
    forbidden = {f"{PREFIX}.app"}
    if module != ".squashfs":
        forbidden.add(f"{PREFIX}.squashfs")
    if module not in {".completion"}:
        forbidden.add(f"{PREFIX}.completion")
    if module in {
        "",
        ".parser_types",
        ".squashfs",
        ".squashfs_commands",
        ".squashfs_parsers",
    }:
        forbidden.add(f"{PREFIX}.parsers")
    command = (
        "import importlib, sys\n"
        f"importlib.import_module({(PREFIX + module)!r})\n"
        f"assert not ({forbidden!r} & set(sys.modules)), {forbidden!r} & set(sys.modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_completion_registrar_is_explicit_and_called_once() -> None:
    calls = []

    def register(subparsers: CompletionSubparsers) -> None:
        calls.append(subparsers)
        completion.build_completion_parser(subparsers)

    parser = parsers.create_parser(register_completion=register)
    assert len(calls) == 1
    args = parser.parse_args(["completion", "fish"])
    assert args.handler is completion.cmd_completion
    assert args.shell == "fish"
    assert args.output == "-"
    assert args.replace_output is False
    assert completion._command_tree(parser) == completion._command_tree(
        app.build_parser()
    )


@pytest.mark.parametrize("module", [PREFIX, f"{PREFIX}.squashfs"])
@pytest.mark.parametrize(
    "argv", [None, ["metadata", "inspect", "--help"], ["postgres", "--help"]]
)
def test_compatibility_main_forwards_exact_arguments_and_exit_code(
    monkeypatch, module: str, argv: list[str] | None
) -> None:
    seen = []

    def dispatch(selected: list[str] | None = None) -> int:
        seen.append(selected)
        return 37

    monkeypatch.setattr(app, "main", dispatch)
    assert importlib.import_module(module).main(argv) == 37
    assert len(seen) == 1 and seen[0] is argv


def test_squashfs_compatibility_exports_are_the_actual_owners() -> None:
    assert squashfs.build_squashfs_parser is squashfs_parsers.build_squashfs_parser
    for name in (
        "cmd_publish_store",
        "cmd_publish_from_ids",
        "cmd_provenance",
        "_run_job",
        "_build_provenance_payload",
    ):
        assert getattr(squashfs, name) is getattr(squashfs_commands, name)
    parser = app.build_parser()
    args = parser.parse_args(["squashfs", "provenance", "--file-id", "7", "--json"])
    assert args.handler is squashfs_commands.cmd_provenance
    assert args.file_id == 7 and args.store_id is None and args.json is True


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
def test_standalone_completion_needs_no_application_factory(
    monkeypatch, capsys, shell: str
) -> None:
    tree = completion._command_tree(app.build_parser())
    expected = getattr(completion, f"_{shell}")(tree)

    def fail():
        raise AssertionError("Completion must not call back into the application.")

    monkeypatch.setattr(app, "build_parser", fail)
    args = argparse.Namespace(shell=shell, output="-", replace_output=False)
    assert completion.cmd_completion(args) == 0
    assert capsys.readouterr().out == expected


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
def test_cli_completion_writes_the_same_script(tmp_path: Path, shell: str) -> None:
    target = tmp_path / f"liuxin.{shell}"
    tree = completion._command_tree(app.build_parser())
    expected = getattr(completion, f"_{shell}")(tree).encode()
    assert app.main(["completion", shell, "--output", str(target)]) == 0
    assert target.read_bytes() == expected
    assert app.main(["completion", shell, "--output", str(target)]) == 2
    assert target.read_bytes() == expected
    assert (
        app.main(["completion", shell, "--output", str(target), "--replace-output"])
        == 0
    )
    assert target.read_bytes() == expected


def test_completion_preserves_each_alias_path_and_nested_options() -> None:
    parser = app.build_parser()
    tree = completion._command_tree(parser)

    def walk(current, path):
        for action in current._actions:
            if isinstance(action, argparse._SubParsersAction):
                for name, child in action.choices.items():
                    assert (*path, name) in tree
                    assert set(tree[(*path, name)]) >= {
                        option
                        for entry in child._actions
                        for option in entry.option_strings
                    }
                    walk(child, (*path, name))

    walk(parser, ())
    assert "--system-root" in tree[()]
    assert "--profile" in tree[()]
    assert "--file-ids-file" in tree[("squashfs", "publish-from-ids")]


@pytest.mark.parametrize("entry", [app.main, squashfs.main])
def test_complete_entry_points_keep_help_and_invalid_argument_exits(
    entry, capsys
) -> None:
    with pytest.raises(SystemExit) as exc:
        entry(["metadata", "--help"])
    assert exc.value.code == 0
    assert "metadata" in capsys.readouterr().out
    with pytest.raises(SystemExit) as exc:
        entry(["completion", "unsupported-shell"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_global_selectors_keep_their_validation_and_position_independence(
    capsys,
) -> None:
    assert app.main(["completion", "bash", "--profile", "example"]) == 0
    script = capsys.readouterr().out
    assert app.main(["--profile=example", "completion", "bash"]) == 0
    assert capsys.readouterr().out == script
    with pytest.raises(SystemExit) as exc:
        app.main(
            ["completion", "bash", "--profile", "example", "--system-root", "root"]
        )
    assert exc.value.code == 2
    assert "mutually exclusive" in capsys.readouterr().err


def test_squashfs_job_waiting_uses_named_core_operations(monkeypatch) -> None:
    core = create_autospec(CoreClientAPI, instance=True)
    core.command.return_value = {"job_id": "job-7"}
    core.query.side_effect = [
        {"job": {"state": "running"}},
        {"job": {"state": "succeeded"}},
        {"execution": {"ok": True, "result": {"verified_files": 1}}},
    ]
    waits = []
    monkeypatch.setattr(squashfs_commands.time, "sleep", waits.append)
    assert squashfs_commands._run_job(
        core, "backup.squashfs.publish-files.start", {"file_ids": [7]}
    ) == {"verified_files": 1}
    core.command.assert_called_once_with(
        "backup.squashfs.publish-files.start", {"file_ids": [7]}
    )
    assert [call.args[0] for call in core.query.call_args_list] == [
        "jobs.get",
        "jobs.get",
        "jobs.result",
    ]
    assert waits == [0.1]


def test_provenance_wire_shape_preserves_absent_metadata() -> None:
    model = create_autospec(CoreSurfaceModel, instance=True)
    model.table_names.return_value = ("files", "file_derivations")
    model.rows.return_value = [
        CoreRow(
            table="file_derivations",
            row_id=3,
            values={
                "file_derivation_id": 3,
                "file_derivation_parent_file_id": 1,
                "file_derivation_child_file_id": 2,
                "file_derivation_kind": None,
                "file_derivation_note": None,
            },
        )
    ]
    rows = {
        row_id: CoreRow(
            table="files",
            row_id=row_id,
            values={
                "file_id": row_id,
                "file_store_id": 10 + row_id,
                "file_storage_key": f"{row_id}.epub",
                "file_name": None,
                "file_size_bytes": None,
                "file_hash_sha256": None,
            },
        )
        for row_id in (1, 2)
    }
    model.row.side_effect = lambda table, row_id: rows.get(row_id)
    payload = squashfs_commands._build_provenance_payload(
        model, store_id=None, file_id=1
    )
    assert payload == {
        "query": {"store_id": None, "file_id": 1},
        "edge_count": 1,
        "edges": [
            {
                "file_derivation_id": 3,
                "kind": None,
                "note": None,
                "parent_file": rows[1].values,
                "child_file": rows[2].values,
            }
        ],
    }
