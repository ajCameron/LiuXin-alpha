"""
Host a borrowed Core runtime through threaded HTTP JSON routes and bounded in-memory event history.

Provides health, API descriptions, named command/query envelopes, and one-event
long polling. Namespace prefixes route URLs; they are not authentication or
authorization boundaries. This adapter supplies neither TLS nor access control.
Declared request bodies are size-limited, but socket reads have no added deadline
and event history is not a durable or lossless delivery service.
"""

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
    """
    Manage a threaded HTTP listener and event subscription for one existing runtime.

    Construction stores configuration without binding a socket. Start/stop control
    the transport, not runtime shutdown. Retains up to 1,000 recent events with
    daemon-local sequence numbers; stopping/restarting does not reset that history.
    Lifecycle methods are not serialized against concurrent starts/stops.

    Example:
        >>> from unittest.mock import Mock
        >>> daemon = CoreHttpDaemon(Mock(), endpoint_namespace="/library/")
        >>> daemon.base_path, daemon.is_running
        ('/library', False)
    """

    def __init__(
        self,
        runtime: CoreRuntime,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        endpoint_namespace: str | None = None,
        max_request_bytes: int = 1024 * 1024 * 1024,
    ) -> None:
        """
        Borrow a runtime, normalize listener settings, and initialize empty event/lifecycle state.

        Host/port are not bound or availability-checked. Only the integer-converted
        body limit is range-validated here. The default limit is one GiB of declared
        request data, not a global memory or concurrent-request budget.

        Example:
            >>> from unittest.mock import Mock
            >>> CoreHttpDaemon(Mock(), max_request_bytes=1024).max_request_bytes
            1024


        :param runtime: Existing runtime providing envelope execution and event subscription.
        :param host: Bind-host text, defaulting to loopback and stringified without address validation.
        :param port: Integer-coerced bind port; zero requests an OS-selected ephemeral port at start.
        :param endpoint_namespace: Optional URL prefix normalized by stripping whitespace and outer slashes.
        :param max_request_bytes: Positive integer limit on declared Content-Length before reading a JSON body.
        :return: ``None`` after storing configuration and initializing the condition/event buffer.
        :raises ValueError: If the converted request limit is below one or an integer conversion fails.
        """
        self.runtime = runtime
        self.host = str(host)
        self.port = int(port)
        self.endpoint_namespace = self._normalize_namespace(endpoint_namespace)
        self.max_request_bytes = int(max_request_bytes)
        if self.max_request_bytes < 1:
            raise ValueError("max_request_bytes must be >= 1.")

        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._unsubscribe: Callable[[], None] | None = None

        self._event_lock = threading.Condition()
        self._event_sequence = 0
        self._events: list[tuple[int, dict[str, Any]]] = []
        self._running = False

    @staticmethod
    def _normalize_namespace(namespace: str | None) -> str:
        """
        Strip outer whitespace and slashes from an optional namespace without escaping or validating interior text.

        Example:
            >>> CoreHttpDaemon._normalize_namespace(" /libraries/demo/ ")
            'libraries/demo'


        :param namespace: Optional URL prefix; falsey values become empty text before normalization.
        :return: Prefix text without outer slashes, or empty text for an unnamespaced listener.
        """
        token = str(namespace or "").strip().strip("/")
        return token

    def validate_request_body_length(self, content_length: int) -> int:
        """
        Integer-convert a declared body length and require it to be positive and within the configured limit.

        This validates the declaration, not actual bytes received or reading time.

        Example:
            >>> from unittest.mock import Mock
            >>> CoreHttpDaemon(Mock(), max_request_bytes=10).validate_request_body_length(10)
            10


        :param content_length: Declared request-body byte count, coerced to an integer.
        :return: Validated positive length, inclusive of the configured maximum.
        :raises ValueError: For an empty/negative/oversized declaration or invalid integer conversion.
        """

        length = int(content_length)
        if length <= 0:
            raise ValueError("Request body cannot be empty.")
        if length > self.max_request_bytes:
            raise ValueError(
                "Request body exceeds the configured {} byte limit.".format(
                    self.max_request_bytes
                )
            )
        return length

    @property
    def is_running(self) -> bool:
        """
        Return the lifecycle flag without probing listener or worker-thread health.

        Example:
            >>> from unittest.mock import Mock
            >>> CoreHttpDaemon(Mock()).is_running
            False


        :return: Boolean running flag set only after listener startup and event subscription succeed.
        """
        return bool(self._running)

    @property
    def server_address(self) -> tuple[str, int]:
        """
        Return the stored server's bound host and port, including an assigned ephemeral port.

        Example:
            >>> address = daemon.server_address  # doctest: +SKIP


        :return: Host text and integer port from the current server object, without a connectivity probe.
        :raises RuntimeError: If no server object is retained, including after stop.
        """
        if self._server is None:
            raise RuntimeError("Daemon has not started yet.")
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    @property
    def base_path(self) -> str:
        """
        Prefix the stored namespace with one slash, using empty text for an unnamespaced endpoint.

        Example:
            >>> from unittest.mock import Mock
            >>> CoreHttpDaemon(Mock(), endpoint_namespace="books").base_path
            '/books'


        :return: Literal namespace routing prefix; interior characters are not URL-escaped.
        """
        if not self.endpoint_namespace:
            return ""
        return "/" + self.endpoint_namespace

    @property
    def base_url(self) -> str:
        """
        Format an HTTP URL from the bound listener address and literal namespace prefix.

        This advertises the bind address, not a separately configured public origin
        or guaranteed reachable client address.

        Example:
            >>> url = daemon.base_url  # doctest: +SKIP


        :return: Plain HTTP base URL for the retained server address.
        :raises RuntimeError: If the server has not started or has been cleared by stop.
        """
        host, port = self.server_address
        return "http://{}:{}{}".format(host, port, self.base_path)

    @property
    def health_url(self) -> str:
        """
        Append the GET health route to the current bound base URL.

        Example:
            >>> url = daemon.health_url  # doctest: +SKIP


        :return: Health URL; computing it requires a retained server object.
        """
        return self.base_url + "/health"

    @property
    def describe_url(self) -> str:
        """
        Append the GET API-description route to the current bound base URL.

        Example:
            >>> url = daemon.describe_url  # doctest: +SKIP


        :return: Introspection URL before optional query-string filters are added.
        """
        return self.base_url + "/api/describe"

    @property
    def query_url(self) -> str:
        """
        Append the POST query-envelope route to the bound base URL.

        Example:
            >>> url = daemon.query_url  # doctest: +SKIP


        :return: Named query RPC URL, requiring a retained server address.
        """
        return self.base_url + "/rpc/query"

    @property
    def command_url(self) -> str:
        """
        Append the POST command-envelope route to the bound base URL.

        Example:
            >>> url = daemon.command_url  # doctest: +SKIP


        :return: Named command RPC URL, without validating any particular command route.
        """
        return self.base_url + "/rpc/command"

    @property
    def events_next_url(self) -> str:
        """
        Append the GET one-event polling route to the bound base URL.

        Example:
            >>> url = daemon.events_next_url  # doctest: +SKIP


        :return: Polling URL before after/timeout query parameters are appended.
        """
        return self.base_url + "/events/next"

    def start(self) -> None:
        """
        Bind a threaded listener, launch its serving thread, and subscribe to runtime events.

        An already-running flag makes this a no-op. The handler class captures
        this daemon and exposes only the configured routes. The server/thread are
        installed before subscribing, and running is marked last; a later failure
        can leave a listener behind while stop still sees a false running flag.
        Startup does not provide rollback or authenticate clients.

        Example:
            >>> daemon.start()  # doctest: +SKIP


        :return: ``None`` after successful startup, or immediately when already marked running.
        """
        if self._running:
            return

        daemon = self

        class _Handler(BaseHTTPRequestHandler):
            """
            Serve this daemon's HTTP/1.1 routes with JSON responses and suppressed request logging.

            The class is created per start call and closes over its owning daemon.
            Request execution uses the borrowed runtime's own synchronization.

            Example:
                >>> handler = _Handler(request, address, server)  # doctest: +SKIP
            """

            server_version = "LiuXinCoreHTTP/0.1"
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:
                """
                Suppress standard handler log output without interpolating the supplied message.

                Example:
                    >>> handler.log_message("request %s", "ignored")  # doctest: +SKIP


                :param format: Ignored standard-library log format string.
                :param args: Ignored values normally interpolated into that log message.
                :return: ``None`` without writing a transport log entry.
                """
                # Keep transport tests deterministic and quiet.
                del format, args
                return

            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                """
                Encode an entire JSON response and send its status, byte length, close header, and body.

                Standard json defaults apply, with sorted keys and unescaped
                Unicode. No strict wire conversion or response-size limit is added.
                Encoding happens before headers; socket failures can occur after
                a partial response and are not recovered here.

                Example:
                    >>> handler._send_json(200, {"ok": True})  # doctest: +SKIP


                :param status: HTTP status code converted to an integer for send_response.
                :param payload: Dictionary serializable by the standard JSON encoder.
                :return: ``None`` after writing and flushing the UTF-8 response bytes.
                """
                data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode(
                    "utf-8"
                )
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
                """
                Classify a Core failure, wire-encode its details, and send a structured error envelope.

                The exception message remains text. Classification, wire conversion,
                encoding, and socket-write errors can themselves propagate.

                Example:
                    >>> handler._send_core_error(400, error)  # doctest: +SKIP


                :param status: HTTP status selected by the route's error policy.
                :param exc: Failure whose code/details and message are exposed to the caller.
                :return: ``None`` after sending a false-ok response with error metadata.
                """
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
                """
                Validate Content-Length, read that many requested bytes, and parse a UTF-8 JSON object.

                Missing/zero, negative, malformed, and oversized declarations are
                rejected before reading. No read deadline or actual-length equality
                check is added; an early EOF with otherwise valid JSON can parse.
                Chunked transfer decoding is not implemented by this helper.

                Example:
                    >>> payload = handler._read_json_body()  # doctest: +SKIP


                :return: Parsed dictionary body without validating endpoint-specific fields.
                :raises ValueError: For invalid declared length, malformed UTF-8/JSON, or nonobject JSON.
                """
                raw_length = self.headers.get("Content-Length", "0").strip()
                try:
                    content_length = int(raw_length)
                except Exception:
                    raise ValueError("Invalid Content-Length header.")
                content_length = daemon.validate_request_body_length(content_length)
                raw = self.rfile.read(content_length)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except Exception as exc:
                    raise ValueError("Malformed JSON body: {}".format(exc))
                if not isinstance(payload, dict):
                    raise ValueError("JSON payload must be an object.")
                return payload

            def _resolve_relative_path(self) -> str | None:
                """
                Remove the literal namespace prefix from the parsed URL path when it matches a complete segment boundary.

                Query text is ignored. Percent escapes are not decoded and interior
                slashes/segments are not normalized. Exact namespace matches become
                slash; paths outside a configured prefix return None.

                Example:
                    >>> path = handler._resolve_relative_path()  # doctest: +SKIP


                :return: Relative route path, slash for an empty/root match, or ``None`` for another namespace.
                """
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
                """
                Integer-convert a query value or convert the supplied default after an ordinary conversion failure.

                No sign/range constraint is applied, and failure to convert the
                fallback is not caught again.

                Example:
                    >>> _Handler._safe_int("bad", 0)  # doctest: +SKIP
                    0


                :param value: Query text to parse as an integer.
                :param default: Fallback value converted to int when parsing fails.
                :return: Parsed or fallback integer.
                """
                try:
                    return int(value)
                except Exception:
                    return int(default)

            @staticmethod
            def _safe_float(value: str, default: float) -> float:
                """
                Float-convert a query value or its fallback without rejecting nonfinite numbers.

                Example:
                    >>> _Handler._safe_float("bad", 10.0)  # doctest: +SKIP
                    10.0


                :param value: Query text to parse as a float.
                :param default: Fallback converted to float after a conversion exception.
                :return: Parsed or fallback float, with range policy left to the route.
                """
                try:
                    return float(value)
                except Exception:
                    return float(default)

            @staticmethod
            def _safe_bool(value: str, default: bool) -> bool:
                """
                Parse common case-insensitive boolean words, truth-converting the fallback for unknown tokens.

                Example:
                    >>> _Handler._safe_bool(" YES ", False)  # doctest: +SKIP
                    True


                :param value: Query value stringified, stripped, and lowercased before matching.
                :param default: Fallback truth value for tokens outside the accepted true/false sets.
                :return: Parsed true/false value or bool(default).
                """
                token = str(value).strip().lower()
                if token in {"1", "true", "yes", "on"}:
                    return True
                if token in {"0", "false", "no", "off"}:
                    return False
                return bool(default)

            def do_GET(self) -> None:
                """
                Serve health, introspection, and one-event polling, returning 404 for unmatched namespaces/routes.

                Uses the first nonblank value of repeated query parameters. Health
                and description handler failures become structured 500 responses;
                event retrieval and response-writing failures are not caught by
                those blocks. HTTP polling timeouts are clamped to zero through
                sixty seconds after permissive parsing.

                Example:
                    >>> handler.do_GET()  # doctest: +SKIP


                :return: ``None`` after sending the selected response or error response.
                """
                rel_path = self._resolve_relative_path()
                if rel_path is None:
                    self._send_json(
                        HTTPStatus.NOT_FOUND,
                        {"ok": False, "error": "Unknown endpoint namespace."},
                    )
                    return

                parsed = urllib.parse.urlparse(self.path)
                query = urllib.parse.parse_qs(parsed.query, keep_blank_values=False)

                if rel_path == "/health":
                    try:
                        result = daemon.runtime.execute_query(
                            CoreQuery(name="health")
                        ).result
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
                        "include_targets": self._safe_bool(
                            query.get("include_targets", ["1"])[0], True
                        ),
                    }
                    target = str(query.get("target", [""])[0]).strip()
                    if target:
                        payload["target"] = target
                    try:
                        result = daemon.runtime.execute_query(
                            CoreQuery(name="api.describe", payload=payload)
                        ).result
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

                self._send_json(
                    HTTPStatus.NOT_FOUND, {"ok": False, "error": "Unknown endpoint."}
                )

            def do_POST(self) -> None:
                """
                Read a JSON envelope and dispatch named query/command routes, exposing validation and execution failures as 400s.

                Wrong namespaces return 404 before body reading. Other unknown
                routes still read/validate a body before their 404, so malformed
                bodies can instead receive 400. Non-dictionary payload fields are
                silently replaced with empty dictionaries; names are stripped and
                non-None IDs/correlation values stringified. Runtime exceptions
                become structured errors, while final asdict/encoding/write errors
                occur outside the execution catch blocks.

                Example:
                    >>> handler.do_POST()  # doctest: +SKIP


                :return: ``None`` after sending an envelope, route error, or payload/execution error response.
                """
                rel_path = self._resolve_relative_path()
                if rel_path is None:
                    self._send_json(
                        HTTPStatus.NOT_FOUND,
                        {"ok": False, "error": "Unknown endpoint namespace."},
                    )
                    return

                try:
                    body = self._read_json_body()
                except ValueError as exc:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)}
                    )
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
                            "payload": dict(
                                payload if isinstance(payload, dict) else {}
                            ),
                        }
                        if query_id is not None:
                            query_envelope_kwargs["query_id"] = str(query_id)
                        if correlation_id is not None:
                            query_envelope_kwargs["correlation_id"] = str(
                                correlation_id
                            )
                        query_envelope = CoreQuery(**query_envelope_kwargs)
                    except Exception as exc:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {"ok": False, "error": "Bad query payload: {}".format(exc)},
                        )
                        return

                    try:
                        query_result = daemon.runtime.execute_query(query_envelope)
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
                            "payload": dict(
                                payload if isinstance(payload, dict) else {}
                            ),
                        }
                        if command_id is not None:
                            command_envelope_kwargs["command_id"] = str(command_id)
                        if correlation_id is not None:
                            command_envelope_kwargs["correlation_id"] = str(
                                correlation_id
                            )
                        command_envelope = CoreCommand(**command_envelope_kwargs)
                    except Exception as exc:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "ok": False,
                                "error": "Bad command payload: {}".format(exc),
                            },
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

                self._send_json(
                    HTTPStatus.NOT_FOUND, {"ok": False, "error": "Unknown endpoint."}
                )

        server = ThreadingHTTPServer((self.host, self.port), _Handler)
        server.daemon_threads = True
        self._server = server
        self._thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True
        )
        self._thread.start()

        self._unsubscribe = self.runtime.subscribe(self._on_runtime_event)
        self._running = True

    def stop(self) -> None:
        """
        Clear running state, unsubscribe, wake event waiters, and attempt listener shutdown and bounded thread join.

        A false running flag makes this a no-op, including after partially failed
        startup. Unsubscribe/server-close errors are suppressed. The final serving
        thread join waits at most two seconds; request threads are daemon threads
        and are not explicitly awaited here. The runtime and event history remain.

        Example:
            >>> from unittest.mock import Mock
            >>> CoreHttpDaemon(Mock()).stop()


        :return: ``None`` after cleanup attempts or immediately when not marked running.
        """
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
        """
        Materialize an event as a detached dataclass dictionary, assign a sequence, and retain only the newest 1,000 records.

        Conversion precedes the condition lock; failures leave sequence/history
        unchanged. The callback does not require a running listener. Enqueue wakes
        all waiters, and old records are dropped without a separate gap marker.

        Example:
            >>> from unittest.mock import Mock
            >>> daemon = CoreHttpDaemon(Mock())
            >>> daemon._on_runtime_event(CoreEvent("event-1", "core-1", "ready", "2026-09-08T00:00:00Z"))
            >>> daemon.get_next_event(after=0, timeout=0)["next_sequence"]
            1


        :param event: Dataclass event converted with asdict before being stored in the daemon's history.
        :return: ``None`` after appending, trimming history if necessary, and notifying event waiters.
        """
        event_payload = dataclasses.asdict(event)
        with self._event_lock:
            self._event_sequence += 1
            self._events.append((self._event_sequence, event_payload))
            # Keep a small ring of recent events for late joiners.
            if len(self._events) > 1000:
                self._events = self._events[-1000:]
            self._event_lock.notify_all()

    def get_next_event(self, *, after: int, timeout: float) -> dict[str, Any]:
        """
        Return the first retained event after a cursor, optionally waiting once for an event notification.

        Does not consume history. Old cursors can skip discarded records without
        a gap indication. If no record is available, waits once only when running
        and timeout is positive; notifications/spurious wakeups can return early
        without an event. Direct calls do not apply the HTTP route's 60-second
        clamp. Returned event dictionaries are new but nested payload values remain
        shared with stored history.

        Example:
            >>> from unittest.mock import Mock
            >>> CoreHttpDaemon(Mock()).get_next_event(after=7, timeout=0)
            {'event': None, 'next_sequence': 7}


        :param after: Cursor integer-coerced without nonnegative validation; only later sequence numbers qualify.
        :param timeout: Maximum duration in seconds for a single condition wait, float-coerced without range/finite validation.
        :return: Event plus its next cursor, or None event with the unchanged supplied cursor.
        """
        after_seq = int(after)
        timeout_s = float(timeout)

        def _find_next() -> tuple[int, dict[str, Any]] | None:
            """
            Scan retained history in order for the first sequence greater than the captured cursor.

            The caller holds the condition lock; this helper acquires no lock or
            copy and returns the stored payload object directly.

            Example:
                >>> found = _find_next()  # doctest: +SKIP


            :return: First qualifying sequence/payload pair, or ``None`` if history has no later event.
            """
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
        """
        Start the listener and return this daemon for a with block.

        Failed startup propagates before context-manager exit cleanup can run.

        Example:
            >>> with daemon as running:  # doctest: +SKIP
            ...     print(running.base_url)


        :return: This daemon after start returns successfully.
        """
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        tb: Any,
    ) -> None:
        """
        Stop the transport on context exit without suppressing the with block's exception.

        The borrowed runtime is not shut down by this operation.

        Example:
            >>> daemon.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Ignored exception type from the with block.
        :param exc: Ignored exception instance from the with block.
        :param tb: Ignored traceback from the with block.
        :return: ``None`` after stop, leaving exception propagation to the context-manager protocol.
        """
        del exc_type, exc, tb
        self.stop()


__all__ = ["CoreHttpDaemon"]
