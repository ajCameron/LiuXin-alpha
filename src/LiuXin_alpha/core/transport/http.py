"""HTTP command/query and long-poll event transport for `CoreRuntime`."""

from __future__ import annotations

import dataclasses
import json
import threading
import urllib.parse

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional

from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import core_error_details
from LiuXin_alpha.core.events import CoreEvent
from LiuXin_alpha.core.queries import CoreQuery
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.core.wire import to_wire


class CoreHttpDaemon:
    """Host one `CoreRuntime` over a small HTTP JSON API."""

    def __init__(
        self,
        runtime: CoreRuntime,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        endpoint_namespace: str | None = None,
    ) -> None:
        self.runtime = runtime
        self.host = str(host)
        self.port = int(port)
        self.endpoint_namespace = self._normalize_namespace(endpoint_namespace)

        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._unsubscribe: Callable[[], None] | None = None

        self._event_lock = threading.Condition()
        self._event_sequence = 0
        self._events: list[tuple[int, dict[str, Any]]] = []
        self._running = False

    @staticmethod
    def _normalize_namespace(namespace: str | None) -> str:
        token = str(namespace or "").strip().strip("/")
        return token

    @property
    def is_running(self) -> bool:
        return bool(self._running)

    @property
    def server_address(self) -> tuple[str, int]:
        if self._server is None:
            raise RuntimeError("Daemon has not started yet.")
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    @property
    def base_path(self) -> str:
        if not self.endpoint_namespace:
            return ""
        return "/" + self.endpoint_namespace

    @property
    def base_url(self) -> str:
        host, port = self.server_address
        return "http://{}:{}{}".format(host, port, self.base_path)

    @property
    def health_url(self) -> str:
        return self.base_url + "/health"

    @property
    def describe_url(self) -> str:
        return self.base_url + "/api/describe"

    @property
    def query_url(self) -> str:
        return self.base_url + "/rpc/query"

    @property
    def command_url(self) -> str:
        return self.base_url + "/rpc/command"

    @property
    def events_next_url(self) -> str:
        return self.base_url + "/events/next"

    def start(self) -> None:
        if self._running:
            return

        daemon = self

        class _Handler(BaseHTTPRequestHandler):
            server_version = "LiuXinCoreHTTP/0.1"
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:
                # Keep transport tests deterministic and quiet.
                del format, args
                return

            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
                self.send_response(int(status))
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(data)
                self.wfile.flush()

            def _send_core_error(
                self,
                status: int,
                exc: BaseException,
            ) -> None:
                code, details = core_error_details(exc)
                self._send_json(
                    status,
                    {
                        "ok": False,
                        "error": str(exc),
                        "error_code": code,
                        "error_details": to_wire(details),
                    },
                )

            def _read_json_body(self) -> dict[str, Any]:
                raw_length = self.headers.get("Content-Length", "0").strip()
                try:
                    content_length = int(raw_length)
                except Exception:
                    raise ValueError("Invalid Content-Length header.")
                if content_length <= 0:
                    raise ValueError("Request body cannot be empty.")
                raw = self.rfile.read(content_length)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except Exception as exc:
                    raise ValueError("Malformed JSON body: {}".format(exc))
                if not isinstance(payload, dict):
                    raise ValueError("JSON payload must be an object.")
                return payload

            def _resolve_relative_path(self) -> str | None:
                parsed = urllib.parse.urlparse(self.path)
                raw_path = str(parsed.path or "")
                prefix = daemon.base_path
                if prefix:
                    if raw_path == prefix:
                        return "/"
                    if raw_path.startswith(prefix + "/"):
                        return raw_path[len(prefix) :]
                    return None
                return raw_path or "/"

            @staticmethod
            def _safe_int(value: str, default: int) -> int:
                try:
                    return int(value)
                except Exception:
                    return int(default)

            @staticmethod
            def _safe_float(value: str, default: float) -> float:
                try:
                    return float(value)
                except Exception:
                    return float(default)

            @staticmethod
            def _safe_bool(value: str, default: bool) -> bool:
                token = str(value).strip().lower()
                if token in {"1", "true", "yes", "on"}:
                    return True
                if token in {"0", "false", "no", "off"}:
                    return False
                return bool(default)

            def do_GET(self) -> None:
                rel_path = self._resolve_relative_path()
                if rel_path is None:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Unknown endpoint namespace."})
                    return

                parsed = urllib.parse.urlparse(self.path)
                query = urllib.parse.parse_qs(parsed.query, keep_blank_values=False)

                if rel_path == "/health":
                    try:
                        result = daemon.runtime.execute_query(CoreQuery(name="health")).result
                    except Exception as exc:
                        self._send_core_error(
                            HTTPStatus.INTERNAL_SERVER_ERROR,
                            exc,
                        )
                        return
                    self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
                    return

                if rel_path == "/api/describe":
                    payload: dict[str, Any] = {
                        "include_targets": self._safe_bool(query.get("include_targets", ["1"])[0], True),
                    }
                    target = str(query.get("target", [""])[0]).strip()
                    if target:
                        payload["target"] = target
                    try:
                        result = daemon.runtime.execute_query(CoreQuery(name="api.describe", payload=payload)).result
                    except Exception as exc:
                        self._send_core_error(
                            HTTPStatus.INTERNAL_SERVER_ERROR,
                            exc,
                        )
                        return
                    self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
                    return

                if rel_path == "/events/next":
                    after = self._safe_int(query.get("after", ["0"])[0], 0)
                    timeout = self._safe_float(query.get("timeout", ["10"])[0], 10.0)
                    timeout = max(0.0, min(timeout, 60.0))
                    result = daemon.get_next_event(after=after, timeout=timeout)
                    self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
                    return

                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Unknown endpoint."})

            def do_POST(self) -> None:
                rel_path = self._resolve_relative_path()
                if rel_path is None:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Unknown endpoint namespace."})
                    return

                try:
                    body = self._read_json_body()
                except ValueError as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                    return

                if rel_path == "/rpc/query":
                    try:
                        query_name = str(body.get("name", "")).strip()
                        if not query_name:
                            raise ValueError("Query name cannot be blank.")
                        payload = body.get("payload", {})
                        query_id = body.get("query_id")
                        correlation_id = body.get("correlation_id")
                        query_envelope_kwargs: dict[str, Any] = {
                            "name": query_name,
                            "payload": dict(payload if isinstance(payload, dict) else {}),
                        }
                        if query_id is not None:
                            query_envelope_kwargs["query_id"] = str(query_id)
                        if correlation_id is not None:
                            query_envelope_kwargs["correlation_id"] = str(correlation_id)
                        query_envelope = CoreQuery(**query_envelope_kwargs)
                    except Exception as exc:
                        self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Bad query payload: {}".format(exc)})
                        return

                    try:
                        query_result = daemon.runtime.execute_query(
                            query_envelope
                        )
                    except Exception as exc:
                        self._send_core_error(HTTPStatus.BAD_REQUEST, exc)
                        return
                    self._send_json(
                        HTTPStatus.OK,
                        dataclasses.asdict(query_result),
                    )
                    return

                if rel_path == "/rpc/command":
                    try:
                        command_name = str(body.get("name", "")).strip()
                        if not command_name:
                            raise ValueError("Command name cannot be blank.")
                        payload = body.get("payload", {})
                        command_id = body.get("command_id")
                        correlation_id = body.get("correlation_id")
                        command_envelope_kwargs: dict[str, Any] = {
                            "name": command_name,
                            "payload": dict(payload if isinstance(payload, dict) else {}),
                        }
                        if command_id is not None:
                            command_envelope_kwargs["command_id"] = str(command_id)
                        if correlation_id is not None:
                            command_envelope_kwargs["correlation_id"] = str(correlation_id)
                        command_envelope = CoreCommand(
                            **command_envelope_kwargs
                        )
                    except Exception as exc:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Bad command payload: {}".format(exc)}
                        )
                        return

                    try:
                        command_result = daemon.runtime.execute_command(
                            command_envelope
                        )
                    except Exception as exc:
                        self._send_core_error(HTTPStatus.BAD_REQUEST, exc)
                        return
                    self._send_json(
                        HTTPStatus.OK,
                        dataclasses.asdict(command_result),
                    )
                    return

                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Unknown endpoint."})

        server = ThreadingHTTPServer((self.host, self.port), _Handler)
        server.daemon_threads = True
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
        self._thread.start()

        self._unsubscribe = self.runtime.subscribe(self._on_runtime_event)
        self._running = True

    def stop(self) -> None:
        if not self._running:
            return

        self._running = False

        if self._unsubscribe is not None:
            try:
                self._unsubscribe()
            except Exception:
                pass
            self._unsubscribe = None

        with self._event_lock:
            self._event_lock.notify_all()

        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception:
                pass
            try:
                self._server.server_close()
            except Exception:
                pass
            self._server = None

        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _on_runtime_event(self, event: CoreEvent) -> None:
        event_payload = dataclasses.asdict(event)
        with self._event_lock:
            self._event_sequence += 1
            self._events.append((self._event_sequence, event_payload))
            # Keep a small ring of recent events for late joiners.
            if len(self._events) > 1000:
                self._events = self._events[-1000:]
            self._event_lock.notify_all()

    def get_next_event(self, *, after: int, timeout: float) -> dict[str, Any]:
        after_seq = int(after)
        timeout_s = float(timeout)

        def _find_next() -> tuple[int, dict[str, Any]] | None:
            for seq, payload in self._events:
                if seq > after_seq:
                    return seq, payload
            return None

        with self._event_lock:
            found = _find_next()
            if found is None and self._running and timeout_s > 0:
                self._event_lock.wait(timeout=timeout_s)
                found = _find_next()

        if found is None:
            return {"event": None, "next_sequence": after_seq}

        seq, payload = found
        return {"event": {"sequence": seq, **payload}, "next_sequence": seq}

    def __enter__(self) -> "CoreHttpDaemon":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        tb: Any,
    ) -> None:
        del exc_type, exc, tb
        self.stop()


__all__ = ["CoreHttpDaemon"]
