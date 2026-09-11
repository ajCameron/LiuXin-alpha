"""
Protect CLI composition, standalone imports, completion, and compatibility exports.

Cold-import checks use isolated interpreter processes; parser/completion tests
inspect or render the grammar without starting Core. SquashFS job/provenance
cases use autospecced collaborators rather than real storage jobs. Embedded
Python snippets remain subprocess fixtures, not documentation declarations.
"""

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
    """
    Check one cold CLI import against its allowed parser/application dependencies.

    Spawn a fresh interpreter with the source directory as PYTHONPATH and a
    ninety-second timeout. The forbidden set varies by selected module, allowing
    dependencies that are intrinsic to that module's role.

    Example:
        >>> test_cold_import_does_not_load_application_entry_points('.parser_types')  # doctest: +SKIP


    :param module: Parametrized suffix appended to the CLI package name, or empty for its root.
    :return: None; assert the subprocess succeeds without forbidden imported modules.
    """
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
    """
    Verify callback injection registers completion once and preserves the full tree.

    Check parsed shell/output defaults and handler identity, then compare the
    independently assembled grammar with the application's parser.

    Example:
        >>> test_completion_registrar_is_explicit_and_called_once()


    :return: None; assert registration count, parser defaults, and command-tree parity.
    """
    calls = []

    def register(subparsers: CompletionSubparsers) -> None:
        """
        Record the registration collection before delegating to the real builder.

        Example:
            >>> register(subparsers)  # doctest: +SKIP


        :param subparsers: Root subparser collection supplied by grammar construction.
        :return: None; append the collection and register completion in place.
        """
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
    """
    Check lazy compatibility dispatch preserves argument identity and exit code.

    Patch the application entry point, then call the package or SquashFS facade.
    No real argument parsing or command execution occurs through the stub.

    Example:
        >>> test_compatibility_main_forwards_exact_arguments_and_exit_code(monkeypatch, PREFIX, None)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the patched application dispatcher.
    :param module: Importable compatibility module whose main is exercised.
    :param argv: Parametrized token list or None, expected to pass through by identity.
    :return: None; assert one dispatch call and unchanged sentinel return status.
    """
    seen = []

    def dispatch(selected: list[str] | None = None) -> int:
        """
        Capture the original argument object and supply a recognizable exit code.

        Example:
            >>> dispatch(None)  # doctest: +SKIP
            37


        :param selected: Forwarded token list or None retained without copying.
        :return: Sentinel status 37 after appending selected to the enclosing list.
        """
        seen.append(selected)
        return 37

    monkeypatch.setattr(app, "main", dispatch)
    assert importlib.import_module(module).main(argv) == 37
    assert len(seen) == 1 and seen[0] is argv


def test_squashfs_compatibility_exports_are_the_actual_owners() -> None:
    """
    Verify SquashFS compatibility names are aliases to their implementation owners.

    Also parse provenance arguments and check handler identity and option values;
    no provenance query or storage operation is executed.

    Example:
        >>> test_squashfs_compatibility_exports_are_the_actual_owners()


    :return: None; assert export identities and provenance parser bindings.
    """
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
    """
    Render completion without calling back into the application parser factory.

    Compute expected text first, replace that factory with a failing stub, then
    invoke standalone completion and compare captured stdout exactly.

    Example:
        >>> test_standalone_completion_needs_no_application_factory(monkeypatch, capsys, 'bash')  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring the replaced application factory.
    :param capsys: Capture fixture for the emitted completion script.
    :param shell: Parametrized bash, zsh, or fish renderer selector.
    :return: None; assert zero status and unchanged script text without factory recursion.
    """
    tree = completion._command_tree(app.build_parser())
    expected = getattr(completion, f"_{shell}")(tree)

    def fail():
        """
        Fail immediately if standalone completion re-enters application assembly.

        Example:
            >>> fail()  # doctest: +SKIP


        :return: Never returns normally.
        :raises AssertionError: Any call violates the standalone-completion boundary.
        """
        raise AssertionError("Completion must not call back into the application.")

    monkeypatch.setattr(app, "build_parser", fail)
    args = argparse.Namespace(shell=shell, output="-", replace_output=False)
    assert completion.cmd_completion(args) == 0
    assert capsys.readouterr().out == expected


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
def test_cli_completion_writes_the_same_script(tmp_path: Path, shell: str) -> None:
    """
    Check completion-file contents, no-clobber refusal, and explicit replacement.

    Use the real CLI dispatcher and temporary filesystem output, but do not
    install or execute the generated shell script.

    Example:
        >>> test_cli_completion_writes_the_same_script(tmp_path, 'fish')  # doctest: +SKIP


    :param tmp_path: Isolated output directory supplied by pytest.
    :param shell: Parametrized renderer name also used as the output extension.
    :return: None; assert output parity and expected zero/two/zero command statuses.
    """
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
    """
    Walk the real grammar and require every alias path and option in its tree.

    These assertions cover the intermediate tree, not equal completion depth
    in every rendered shell format. Explicitly check global selectors and a
    nested SquashFS option after the recursive walk.

    Example:
        >>> test_completion_preserves_each_alias_path_and_nested_options()


    :return: None; assert path coverage and option-set inclusion at each visited node.
    """
    parser = app.build_parser()
    tree = completion._command_tree(parser)

    def walk(current, path):
        """
        Recursively compare child parser actions with the enclosing completion tree.

        Revisit shared parser objects under each alias spelling; there is no
        cycle guard because the installed grammar is expected to be a tree.

        Example:
            >>> walk(parser, ())  # doctest: +SKIP


        :param current: ArgumentParser whose subparser actions are traversed.
        :param path: Command-name tuple leading to current, excluding the executable.
        :return: None; assert each child path and its option spellings before recursing.
        """
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
    """
    Check help and invalid-shell argparse exits through both complete entry points.

    Example:
        >>> test_complete_entry_points_keep_help_and_invalid_argument_exits(app.main, capsys)  # doctest: +SKIP


    :param entry: Parametrized application or SquashFS compatibility main function.
    :param capsys: Pytest stdout/stderr capture used to inspect help and diagnostics.
    :return: None; assert help exits zero and invalid shell selection exits two.
    """
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
    """
    Compare profile selectors around completion and reject conflicting global sources.

    Completion does not open the selected profile; this checks normalization and
    mutual exclusion, not profile existence or Core connection resolution.

    Example:
        >>> test_global_selectors_keep_their_validation_and_position_independence(capsys)  # doctest: +SKIP


    :param capsys: Pytest capture fixture for completion text and parser diagnostics.
    :return: None; assert identical scripts and conflicting-selector SystemExit two.
    """
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
    """
    Check the SquashFS-specific job runner's query sequence and result unwrapping.

    Use an autospecced Core with running then succeeded states and replace sleep
    with a recorder. This covers the separate SquashFS runner, not common.wait_for_job.

    Example:
        >>> test_squashfs_job_waiting_uses_named_core_operations(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture replacing sleep so the test never waits.
    :return: None; assert command payload, jobs.get/result ordering, delay, and unwrapped result.
    """
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
    """
    Verify provenance projection retains None metadata and complete endpoint values.

    Supply autospecced model reads for one derivation and two file rows. No real
    database, archive, or byte-storage operation participates in this case.

    Example:
        >>> test_provenance_wire_shape_preserves_absent_metadata()


    :return: None; assert exact query, edge count, and parent/child payload structure.
    """
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
