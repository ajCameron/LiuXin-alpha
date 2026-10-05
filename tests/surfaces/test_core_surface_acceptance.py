"""
Check application-surface compatibility with one direct Core and its loopback RPC client.

The acceptance path exercises generic web, JSON API, OPDS, writable web, terminal,
and Tk backend adapters against the same temporary catalogue. WSGI requests stay
in process, while RemoteCoreClient uses a real ephemeral HTTP daemon. No browser,
curses screen, or Tk window is displayed. Each transport creates one work through
the writable surface, so the checks share intentionally evolving catalogue state.
"""

from __future__ import annotations

import io
import urllib.parse
from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.core import CoreHttpDaemon, RemoteCoreClient, create_core
from LiuXin_alpha.surfaces.api_readonly.app import ApiReadOnlyApplication
from LiuXin_alpha.surfaces.opds_readonly.app import OpdsReadOnlyApplication
from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser
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
    """
    Invoke WSGI with optional encoded form data and close the collected response.

    Path is copied verbatim into PATH_INFO, not split into query components.
    A non-None form, including an empty dict, supplies URL-encoded UTF-8 bytes
    with matching media/length headers. Response headers collapse to a dict,
    and byte-joining failures still trigger a callable iterable closer.

    Example:
        >>> status, headers, body = _call_wsgi(app, "/tables/works")  # doctest: +SKIP


    :param app: WSGI surface accepting an environment and two-argument response callback.
    :param path: Literal request path, without an independently parsed query string.
    :param method: Request method retained unchanged in the environment.
    :param form: Optional scalar form fields serialized without changing the method.
    :return: Status string, collapsed headers, and concatenated response bytes.
    """
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
        """
        Capture status and headers for the enclosing acceptance request.

        This minimal callback accepts no exc_info argument and returns no write
        callable; it implements only the shape used by the exercised surfaces.

        Example:
            >>> start_response("200 OK", [("Content-Type", "text/html")])  # doctest: +SKIP


        :param status: HTTP status line to retain.
        :param headers: Header pairs converted to a dict, discarding duplicate entries.
        :return: None after updating the enclosing captured mapping.
        """
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
    """
    Exercise six adapters through a supplied Core client, including one catalogue write.

    Web/API/OPDS reads must expose Seed Work. Writable web creates a label-named
    work, terminal commands browse the catalogue, and a Tk session/backend reads
    a page then closes. The exact rpc label adds an intentionally nonresolving
    endpoint hint to Tk configuration; the supplied client still performs the
    actual calls. No interactive UI is displayed.

    Example:
        >>> _exercise_surface_client(core_client, label="direct", tmp_path=tmp_path)  # doctest: +SKIP


    :param client: Direct runtime or remote Core client shared by the six adapters.
    :param label: Work-name/history suffix and selector for the rpc-only Tk endpoint hint.
    :param tmp_path: Directory for the terminal client's transport-specific history path.
    :return: None after read/write and presentation assertions; failures propagate.
    """
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
    """
    Verify the same application adapters accept local and HTTP Core clients.

    Create and seed a temporary catalogue, start an ephemeral loopback daemon,
    then exercise direct and remote clients sequentially against shared state.
    Once startup succeeds, finally stops the daemon and shuts down Core even
    when an assertion fails. Daemon startup precedes that cleanup try block,
    so this test requires permitted local sockets rather than a mocked transport.

    Example:
        >>> test_all_application_surfaces_accept_direct_and_rpc_core(tmp_path)  # doctest: +SKIP


    :param tmp_path: Isolated directory for the catalogue and both terminal history paths.
    :return: None after both transport paths satisfy the cross-surface contracts.
    """
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
