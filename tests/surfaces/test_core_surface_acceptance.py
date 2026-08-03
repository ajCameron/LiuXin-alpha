from __future__ import annotations

import io
import urllib.parse

from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.core import CoreHttpDaemon, RemoteCoreClient, create_core
from LiuXin_alpha.surfaces.api_readonly.app import ApiReadOnlyApplication
from LiuXin_alpha.surfaces.opds_readonly.app import OpdsReadOnlyApplication
from LiuXin_alpha.surfaces.terminal.text_browser import TextDatabaseBrowser
from LiuXin_alpha.surfaces.tkinter_gui.backend import TkGuiBackend
from LiuXin_alpha.surfaces.tkinter_gui.session import TkGuiSession
from LiuXin_alpha.surfaces.tkinter_gui.state import TkGuiConfig
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication
from LiuXin_alpha.surfaces.web_readwrite.app import ReadWriteWebApplication


def _call_wsgi(
    app,
    path: str,
    *,
    method: str = "GET",
    form: dict[str, str] | None = None,
) -> tuple[str, dict[str, str], bytes]:
    environ: dict[str, object] = {}
    setup_testing_defaults(environ)
    environ["REQUEST_METHOD"] = method
    environ["PATH_INFO"] = path
    if form is not None:
        payload = urllib.parse.urlencode(form).encode("utf-8")
        environ["CONTENT_TYPE"] = "application/x-www-form-urlencoded"
        environ["CONTENT_LENGTH"] = str(len(payload))
        environ["wsgi.input"] = io.BytesIO(payload)
    captured: dict[str, object] = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    result = app(environ, start_response)
    try:
        body = b"".join(result)
    finally:
        close = getattr(result, "close", None)
        if callable(close):
            close()
    return (
        str(captured["status"]),
        dict(captured["headers"]),
        body,
    )


def _exercise_surface_client(client, *, label: str, tmp_path: Path) -> None:
    web = ReadOnlyWebApplication(client)
    status, _headers, body = _call_wsgi(web, "/tables/works")
    assert status == "200 OK"
    assert b"Seed Work" in body

    api = ApiReadOnlyApplication(client)
    status, _headers, body = _call_wsgi(api, "/api/works")
    assert status == "200 OK"
    assert b"Seed Work" in body

    opds = OpdsReadOnlyApplication(client)
    status, _headers, body = _call_wsgi(opds, "/opds")
    assert status == "200 OK"
    assert b"application/atom+xml" in dict(_headers).get(
        "Content-Type",
        "",
    ).encode("utf-8")

    writable = ReadWriteWebApplication(client)
    status, headers, _body = _call_wsgi(
        writable,
        "/tables/works/new",
        method="POST",
        form={
            "work_title": "{} Work".format(label),
            "work_canonical_title": "{} Work".format(label),
            "work_sort_title": "{} Work".format(label),
        },
    )
    assert status == "302 Found"
    assert headers["Location"].startswith("/tables/works/")

    output = io.StringIO()
    browser = TextDatabaseBrowser(
        client,
        output=output,
        history_file=tmp_path / "{}-history".format(label),
    )
    assert browser.run_commands(("use works", "count", "browse 5 0")) == 0
    assert "Seed Work" in output.getvalue()

    session = TkGuiSession.from_client(
        client,
        config=TkGuiConfig(
            core_endpoint="http://core.invalid"
            if label == "rpc"
            else None,
        ),
    )
    backend = TkGuiBackend.from_session(session)
    assert backend.page_rows("works", limit=10).total_count >= 2
    backend.close()


def test_all_application_surfaces_accept_direct_and_rpc_core(
    tmp_path: Path,
) -> None:
    runtime = create_core(
        database_path=tmp_path / "surface-core.sqlite",
        create=True,
        backup=False,
        storage_startup_on_add=False,
        enable_maintenance=False,
        repair_bootstrap_rows=False,
    )
    runtime.command(
        "admin.row.create",
        {
            "table": "works",
            "values": {
                "work_title": "Seed Work",
                "work_canonical_title": "Seed Work",
                "work_sort_title": "Seed Work",
            },
        },
    )
    daemon = CoreHttpDaemon(
        runtime,
        endpoint_namespace="surface-acceptance",
    )
    daemon.start()
    try:
        _exercise_surface_client(
            runtime,
            label="direct",
            tmp_path=tmp_path,
        )
        _exercise_surface_client(
            RemoteCoreClient(
                endpoint=daemon.base_url,
                timeout_seconds=30.0,
            ),
            label="rpc",
            tmp_path=tmp_path,
        )
    finally:
        daemon.stop()
        runtime.shutdown()
