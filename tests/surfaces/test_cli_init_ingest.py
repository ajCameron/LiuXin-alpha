"""
Exercise first-run initialization, wizard routing, and concise local ingestion.

Most cases replace Core/PostgreSQL operations with recording fakes while retaining
real temporary layout/manifest files. The final round trip uses actual local Core
and ingestion, then verifies repeat initialization retains later Store registrations.
The PostgreSQL wizard case does not connect to a live server.
"""

from __future__ import annotations

import json
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

import LiuXin_alpha.surfaces.cli.storage_commands.ingest_config as storage_cli_ingest_config
from LiuXin_alpha.surfaces.cli import initialize as init_cli
from LiuXin_alpha.surfaces.cli.app import main as cli_main
from LiuXin_alpha.surfaces.cli.storage_commands import parsers as storage_parsers


class _Core:
    """
    Supply deterministic initializer receipts and record shallow command payloads.

    Only initializer operations are accepted. The Store-list fixture deliberately
    reports count one with an empty stores array; it is not a consistent database
    model and does not execute persistence or storage refresh.

    Example:
        >>> core = _Core()
        >>> core.query("health", {})["shutdown"]
        False


    :ivar commands: Ordered command names and shallow-copied request dictionaries.
    """

    def __init__(self) -> None:
        """
        Create an independent command history for this initializer fake.

        Example:
            >>> _Core().commands
            []


        :return: None; commands starts as a new empty list.
        """
        self.commands: list[tuple[str, dict[str, Any]]] = []

    def query(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Return fixed health or Store-count data without inspecting the payload.

        Example:
            >>> _Core().query("storage.stores.list", {})
            {'stores': [], 'count': 1}


        :param name: Exact health or storage.stores.list query token.
        :param payload: Request mapping accepted but not read or recorded.
        :return: Fresh deterministic receipt for the selected operation.
        :raises AssertionError: The initializer issues an unsupported query.
        """
        if name == "health":
            return {
                "core_uuid": "core-1",
                "core_version": "2.0.0",
                "api_version": "2.0",
                "shutdown": False,
            }
        if name == "storage.stores.list":
            return {"stores": [], "count": 1}
        raise AssertionError(name)

    def command(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Record a command before returning its fixed save/refresh/default receipt.

        Copy only the outer payload; nested Store values remain shared. Unknown
        commands are recorded before failing, and malformed known payloads may raise.

        Example:
            >>> core = _Core()
            >>> core.command("storage.default.set", {"store": "primary"})["selected"]
            True


        :param name: Exact initializer command token to recognize.
        :param payload: Request to shallow-copy into commands before dispatch.
        :return: Synthetic Store-save, refresh-report, or default-selection receipt.
        :raises AssertionError: The command is not one of the three supported tokens.
        """
        values = dict(payload)
        self.commands.append((name, values))
        if name == "storage.store.save":
            return {"store": values["store"]}
        if name == "storage.refresh":
            return {"refreshed": True, "report": {"loaded_stores": 1, "issues": []}}
        if name == "storage.default.set":
            return {
                "selected": True,
                "store_uuid": "store-1",
                "store_name": values["store"],
            }
        raise AssertionError(name)


@contextmanager
def _open_fake(args: Any, **_kwargs: Any):
    """
    Touch the selected database path and yield the injected fake Core.

    The resulting file is not a SQLite database; parents must already exist.
    Context exit adds no cleanup or exception suppression.

    Example:
        >>> with _open_fake(prepared_args) as core:  # doctest: +SKIP
        ...     assert core is prepared_args._test_core


    :param args: Mutable namespace containing database and an injected _test_core.
    :param _kwargs: Ignored Core factory options, including create/composition flags.
    :return: Context manager yielding the injected fake after touching the file.
    """
    Path(args.database).touch(exist_ok=True)
    yield args._test_core


def _answers(*values: str):
    """
    Build an input replacement that consumes a fixed sequence without displaying prompts.

    Example:
        >>> answer = _answers("yes", "no")
        >>> answer("Apply?"), answer("Again?")
        ('yes', 'no')


    :param values: Answers returned in order, unchanged, one per input call.
    :return: Callable that ignores its prompt and raises StopIteration when exhausted.
    """
    remaining = iter(values)

    def answer(_prompt: str) -> str:
        """
        Consume the next enclosing scripted answer without printing its prompt.

        Example:
            >>> _answers("selected")("Choice")
            'selected'


        :param _prompt: Input prompt text, deliberately ignored by the test helper.
        :return: Next scripted string; exhaustion propagates as StopIteration.
        """
        return next(remaining)

    return answer


def test_init_creates_idempotent_system_layout_and_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Assert first-run layout, private manifest, ordered Store setup, and next-action hints.

    This case invokes initialization once against a fake; repeat-open preservation
    is exercised by the separate real round-trip test, not established here.

    Example:
        >>> test_init_creates_idempotent_system_layout_and_manifest(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated parent for the created system-root fixture.
    :param monkeypatch: Pytest fixture replacing the Core opener with a recording fake.
    :param capsys: Capture used to decode the JSON command receipt.
    :return: None after layout, permissions, dispatch, and suggestion assertions pass.
    """
    core = _Core()

    def opener(args: Any, **kwargs: Any):
        """
        Inject this test's recording Core and return its file-touching context manager.

        Example:
            >>> context = opener(parsed_args, create=True)  # doctest: +SKIP


        :param args: Initializer namespace mutated with the enclosing _test_core.
        :param kwargs: Core factory options forwarded to the permissive fake opener.
        :return: Context manager that touches the database and yields the shared fake.
        """
        args._test_core = core
        return _open_fake(args, **kwargs)

    monkeypatch.setattr(init_cli, "open_cli_core", opener)
    root = tmp_path / "system"

    assert cli_main(["init", str(root)]) == 0
    result = json.loads(capsys.readouterr().out)

    assert result["ok"] is True
    assert result["database_created"] is True
    assert result["store"] == {
        "kind": "filesystem",
        "name": "primary",
        "root": str(root / "store"),
        "saved": True,
    }
    assert (root / "catalogue.sqlite").is_file()
    assert (root / "store").is_dir()
    assert (root / "ingest-materialized").is_dir()
    assert (root / "logs" / "ingest").is_dir()
    manifest_path = root / init_cli.SYSTEM_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["format"] == init_cli.SYSTEM_MANIFEST_FORMAT
    assert manifest["database"] == str(root / "catalogue.sqlite")
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o600
    assert [name for name, _payload in core.commands] == [
        "storage.store.save",
        "storage.refresh",
        "storage.default.set",
    ]
    assert result["next"]["ingest"] == [
        "liuxin",
        "ingest",
        "/path/to/source",
        "--system-root",
        str(root),
    ]
    assert result["next"]["connect"] == ["liuxin", "connect", str(root)]


def test_init_wizard_can_choose_apsw_system_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Exercise automatic attended wizard routing and APSW selection using fake Core.

    Assert the plan and trailing compact JSON separately; no real APSW driver is
    constructed, though the temporary layout and manifest are actually written.

    Example:
        >>> test_init_wizard_can_choose_apsw_system_root(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated parent for the scripted wizard's system root.
    :param monkeypatch: Fixture supplying TTY status, answers, and fake Core opening.
    :param capsys: Capture separating prompt/plan text from the final JSON line.
    :return: None after driver, layout, default Store, and manifest assertions pass.
    """
    core = _Core()

    def opener(args: Any, **kwargs: Any):
        """
        Attach the wizard test's fake Core before returning the touching context.

        Example:
            >>> context = opener(parsed_args, enable_storage_manager=True)  # doctest: +SKIP


        :param args: Mutable post-wizard initializer arguments receiving _test_core.
        :param kwargs: Factory options accepted and forwarded without interpretation.
        :return: Fake context manager, without opening APSW or another real driver.
        """
        args._test_core = core
        return _open_fake(args, **kwargs)

    root = tmp_path / "guided-system"
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(init_cli, "open_cli_core", opener)
    monkeypatch.setattr(
        "builtins.input",
        _answers("2", str(root), "yes", "yes"),
    )

    # An attended bare `liuxin init` enters the same wizard automatically.
    assert cli_main(["init", "--compact"]) == 0

    output = capsys.readouterr().out
    result = json.loads(output.splitlines()[-1])
    assert "Initialization plan" in output
    assert result["db_type"] == "APSW"
    assert result["database"] == str(root / "catalogue.sqlite")
    assert result["default_store"]["selected"] is True
    assert (root / init_cli.SYSTEM_MANIFEST_NAME).is_file()


def test_init_wizard_postgres_initializes_checks_and_redacts_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Check PostgreSQL wizard delegation, redacted output, and private environment-file output.

    The initializer is fake, so this proves forwarded check/connection flags and
    the real environment writer, not live server readiness. Scripted input does
    not simulate terminal echo of typed passwords. The file assertion excludes
    the password-variable name, not every possible secret-bearing representation.

    Example:
        >>> test_init_wizard_postgres_initializes_checks_and_redacts_target(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Parent for the chosen system-root and environment-file paths.
    :param monkeypatch: Fixture supplying TTY/driver availability, answers, and fake init.
    :param capsys: Capture used to inspect plan/initializer output for the example secret.
    :return: None after forwarding, redaction, file-mode, and variable-name assertions.
    """
    received: dict[str, Any] = {}

    def fake_postgres_init(args: Any) -> int:
        """
        Snapshot the delegated namespace and print synthetic readiness success.

        Example:
            >>> fake_postgres_init(postgres_args)  # doctest: +SKIP
            PostgreSQL readiness checks passed
            0


        :param args: PostgreSQL initializer namespace copied into the enclosing receipt dict.
        :return: Zero without contacting PostgreSQL or creating a system manifest.
        """
        received.update(vars(args))
        print("PostgreSQL readiness checks passed")
        return 0

    environment_file = tmp_path / "liuxin-postgres.env"
    system_root = tmp_path / "liuxin-postgres-system"
    target = "postgresql://owner:very-secret@example.invalid/liuxin"
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(init_cli, "postgres_driver_is_available", lambda: True)
    monkeypatch.setattr(init_cli, "cmd_postgres_init", fake_postgres_init)
    monkeypatch.setattr(
        "builtins.input",
        _answers(
            "3",
            "1",
            target,
            "liuxin_catalogue",
            str(system_root),
            "yes",
            str(environment_file),
            "yes",
        ),
    )

    assert cli_main(["init", "--wizard"]) == 0

    captured = capsys.readouterr()
    combined_output = captured.out + captured.err
    assert "PostgreSQL initialization plan" in captured.out
    assert "PostgreSQL readiness checks passed" in captured.out
    assert "very-secret" not in combined_output
    assert "owner:***@example.invalid" in captured.out
    assert received["url"] == target
    assert received["service"] is None
    assert received["schema"] == "liuxin_catalogue"
    assert received["system_root"] == str(system_root)
    assert received["check"] is True
    assert received["no_password_prompt"] is False
    assert environment_file.is_file()
    assert stat.S_IMODE(environment_file.stat().st_mode) == 0o600
    assert "LIUXIN_POSTGRES_PASSWORD" not in environment_file.read_text(
        encoding="utf-8"
    )


def test_init_wizard_hints_when_postgres_support_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Assert the CLI translates missing PostgreSQL support into status two and install guidance.

    Example:
        >>> test_init_wizard_hints_when_postgres_support_is_not_installed(monkeypatch, capsys)  # doctest: +SKIP


    :param monkeypatch: Fixture selecting PostgreSQL while reporting its driver unavailable.
    :param capsys: Capture used to verify the dependency hint on stderr.
    :return: None after checking failure status and the optional-extra guidance.
    """
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(init_cli, "postgres_driver_is_available", lambda: False)
    monkeypatch.setattr("builtins.input", _answers("3"))

    assert cli_main(["init", "--wizard"]) == 2

    captured = capsys.readouterr()
    assert ".[postgres]" in captured.err
    assert "PostgreSQL Python support" in captured.err


def test_init_wizard_cancellation_makes_no_layout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Decline the displayed local plan and verify the chosen root was never created.

    Example:
        >>> test_init_wizard_cancellation_makes_no_layout(tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated parent for the cancelled, nonexistent layout.
    :param monkeypatch: Fixture supplying attended status and answers ending in refusal.
    :param capsys: Capture used to check the cancellation message.
    :return: None after status-one, absent-root, and cancellation-output assertions.
    """
    root = tmp_path / "cancelled-system"
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(
        "builtins.input",
        _answers("1", str(root), "yes", "no"),
    )

    assert cli_main(["init", "--wizard"]) == 1
    assert not root.exists()
    assert "cancelled" in capsys.readouterr().out.casefold()


def test_init_wizard_requires_an_interactive_terminal(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Verify explicit wizard use refuses noninteractive input through the CLI error boundary.

    Example:
        >>> test_init_wizard_requires_an_interactive_terminal(monkeypatch, capsys)  # doctest: +SKIP


    :param monkeypatch: Fixture forcing the input-interactivity check to false.
    :param capsys: Capture used to assert the terminal requirement on stderr.
    :return: None after refusal status and diagnostic assertions pass.
    """
    monkeypatch.setattr(init_cli, "_stdin_is_interactive", lambda: False)

    assert cli_main(["init", "--wizard"]) == 2
    assert "interactive terminal" in capsys.readouterr().err


def test_system_manifest_populates_safe_mixed_ingest_defaults(tmp_path: Path) -> None:
    """
    Check manifest-derived database/materialization/log defaults and existing-database policy.

    The touched database is only an existence fixture, not a valid SQLite schema.
    This case applies defaults without opening Core or executing ingest.

    Example:
        >>> test_system_manifest_populates_safe_mixed_ingest_defaults(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated root containing the manifest and empty catalogue fixture.
    :return: None after all selected paths and require_existing_database are asserted.
    """
    root = tmp_path / "system"
    root.mkdir()
    database = root / "catalogue.sqlite"
    database.touch()
    manifest = {
        "format": "liuxin.system",
        "version": 1,
        "database": str(database),
        "db_type": "SQLite",
        "materialization_root": str(root / "materialized"),
        "log_directory": str(root / "logs"),
    }
    (root / init_cli.SYSTEM_MANIFEST_NAME).write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    args = type(
        "Args",
        (),
        {
            "__doc__": """
            Hold mutable selections for the manifest-default application fixture.

            Class attributes provide absent initial overrides; applying defaults
            writes instance attributes without opening the catalogue.

            Example:
                >>> type(args).__name__  # doctest: +SKIP
                'Args'


            :ivar system_root: Existing temporary root selecting the fixture manifest.
            :ivar database: Initially None, then the manifest's catalogue path.
            :ivar materialization_root: Initially None, then the manifest staging path.
            :ivar log_directory: Initially None, then the manifest log path.
            :ivar require_existing_database: Initially false, enabled after profile defaults.
            """,
            "system_root": str(root),
            "database": None,
            "materialization_root": None,
            "log_directory": None,
            "require_existing_database": False,
        },
    )()

    storage_cli_ingest_config._apply_system_root_defaults(args)

    assert args.database == str(database)
    assert args.materialization_root == str(root / "materialized")
    assert args.log_directory == str(root / "logs")
    assert args.require_existing_database is True


def test_concise_ingest_source_expands_to_mixed_ingest_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify positional and options-first ingest shortcuts reach the mixed-ingest handler.

    Record parsed arguments without creating source/root directories or running
    ingestion; this checks routing and option preservation, not import behavior.

    Example:
        >>> test_concise_ingest_source_expands_to_mixed_ingest_surface(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Parent supplying prospective source and system-root paths.
    :param monkeypatch: Fixture replacing the parser-bound storage-ingest handler.
    :return: None after both shortcut forms preserve source/root and strict selection.
    """
    received: dict[str, Any] = {}

    def handler(args: Any) -> int:
        """
        Snapshot the routed ingest namespace without executing the workflow.

        Example:
            >>> handler(parsed_ingest_args)  # doctest: +SKIP
            0


        :param args: Namespace from shortcut normalization and ordinary parser dispatch.
        :return: Zero after updating the enclosing received dictionary.
        """
        received.update(vars(args))
        return 0

    monkeypatch.setattr(storage_parsers, "cmd_storage_ingest", handler)
    source = tmp_path / "mess"
    root = tmp_path / "system"

    assert (
        cli_main(["ingest", str(source), "--system-root", str(root), "--strict"]) == 0
    )
    assert received["source_root"] == str(source)
    assert received["system_root"] == str(root)
    assert received["strict"] is True
    assert received["storage_command"] == "ingest"

    received.clear()
    assert (
        cli_main(
            [
                "ingest",
                "--system-root",
                str(root),
                "--source",
                str(source),
            ]
        )
        == 0
    )
    assert received["source_root"] == str(source)
    assert received["system_root"] == str(root)


def test_init_rejects_database_inside_managed_store(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Verify path-backed init rejects placing its catalogue under the managed Store root.

    Example:
        >>> test_init_rejects_database_inside_managed_store(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Parent supplying a proposed Store and nested database location.
    :param capsys: Capture used to check the protected-path error message.
    :return: None after status-two and containment-diagnostic assertions pass.
    """
    store = tmp_path / "store"
    rc = cli_main(
        [
            "init",
            "--database",
            str(store / "catalogue.sqlite"),
            "--store-root",
            str(store),
        ]
    )
    assert rc == 2
    assert "database must not be inside" in capsys.readouterr().err


def test_real_init_then_concise_ingest_round_trip(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Initialize real local Core, ingest one file, and reopen without losing later Stores.

    The .epub fixture contains arbitrary bytes; assertions concern adoption,
    catalogue assets, durable report/log paths, and repeat initialization, not
    valid ebook parsing or conversion. Store count after reopening must retain
    at least the primary and later-ingest registrations.

    Example:
        >>> test_real_init_then_concise_ingest_round_trip(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated parent for the actual catalogue, Store, source, and logs.
    :param capsys: Capture used to decode each init/ingest command's JSON result.
    :return: None after initial creation, adoption, report paths, and repeat-open checks.
    """
    root = tmp_path / "system"
    source = tmp_path / "incoming"
    source.mkdir()
    (source / "book.epub").write_bytes(b"first-run-book")

    assert cli_main(["init", str(root), "--compact"]) == 0
    initialized = json.loads(capsys.readouterr().out)
    assert initialized["database_created"] is True
    assert initialized["store_count"] == 1
    assert initialized["default_store"]["selected"] is True

    assert (
        cli_main(
            [
                "ingest",
                str(source),
                "--system-root",
                str(root),
                "--no-nested-containers",
                "--no-console-progress",
                "--compact-json",
            ]
        )
        == 0
    )
    ingested = json.loads(capsys.readouterr().out)
    assert ingested["mode"] == "ingest"
    assert ingested["ok"] is True
    assert ingested["report"]["files_examined"] == 1
    assert ingested["report"]["files_adopted"] == 1
    assert ingested["report"]["assets_created"] == 1
    assert Path(ingested["report_file"]).is_file()
    assert Path(ingested["event_log"]).is_file()

    assert cli_main(["init", str(root), "--compact"]) == 0
    reopened = json.loads(capsys.readouterr().out)
    assert reopened["database_created"] is False
    assert reopened["store_count"] >= 2
    assert reopened["default_store"]["selected"] is True
