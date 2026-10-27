"""Keep web CLI modes explicit and use Core's existing managed-job and query routes."""

import json
from contextlib import contextmanager

import pytest

from LiuXin_alpha.surfaces.cli.app import main as cli_main
from LiuXin_alpha.surfaces.cli import ingest_web


@pytest.fixture
def calls(monkeypatch):
    received = []

    class Core:
        def query(self, operation, payload):
            received.append((operation, payload))
            return {"files": []}

    @contextmanager
    def open_core(*args, **kwargs):
        yield Core()

    def submit(core, operation, payload, args):
        received.append((operation, payload))
        return {"execution": {"ok": True, "result": {"status": "complete"}}}

    monkeypatch.setattr(ingest_web, "open_cli_core", open_core)
    monkeypatch.setattr(ingest_web, "submit_job", submit)
    return received


@pytest.mark.parametrize(
    "flags,mode",
    [([], "register"), (["--register-only"], "register"), (["--download"], "download")],
)
def test_web_modes_use_same_job_owner(calls, flags, mode, capsys):
    assert cli_main(["ingest", "web", "https://books.test/", *flags]) == 0
    operation, payload = calls[0]
    assert operation == "ingest.web.start"
    assert payload["options"] == {"root_url": "https://books.test/", "mode": mode}
    assert json.loads(capsys.readouterr().out)["execution"]["ok"]


def test_list_uses_query_and_resume_replays_exact_controls(calls):
    assert cli_main(["ingest", "web", "https://books.test/", "--list"]) == 0
    assert calls[-1] == ("ingest.web.files", {"root_url": "https://books.test/"})
    run_id = "680698b3-176d-4c04-a710-707eb023564e"
    assert cli_main(["ingest", "web", "--resume", run_id]) == 0
    assert calls[-1] == ("ingest.web.start", {"resume": run_id})
    count = len(calls)
    assert cli_main(["ingest", "web", "--resume", run_id, "--max-files", "1"]) != 0
    assert len(calls) == count


def test_partial_acquisition_returns_nonzero_shell_status(calls, monkeypatch):
    monkeypatch.setattr(
        ingest_web,
        "submit_job",
        lambda *args: {
            "execution": {
                "ok": True,
                "result": {"status": "partial", "errors": ["short read"]},
            }
        },
    )
    assert cli_main(["ingest", "web", "https://books.test/", "--download"]) == 1
