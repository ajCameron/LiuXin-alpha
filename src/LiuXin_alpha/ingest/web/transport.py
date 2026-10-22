"""Share scoped HTTP requests, robots decisions and pacing across native crawl and acquisition."""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from http.client import HTTPMessage
from typing import IO, Any
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

from LiuXin_alpha.ingest.sources.html_common import (
    is_within_root_scope,
    normalize_http_url,
)
from LiuXin_alpha.ingest.web.budget import WebBudget
from LiuXin_alpha.ingest.web.models import WebIngestOptions


class _ScopedRedirects(urllib.request.HTTPRedirectHandler):
    """Check each redirect before urllib sends the next request."""

    def __init__(self, transport: SiteTransport) -> None:
        self.transport = transport

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        """Reject a redirect outside scope before sending its follow-up request."""
        self.transport.check_scope(newurl)
        if newurl != self.transport.robots_url:
            self.transport.check_robots(newurl)
        self.transport.pace()
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class SiteTransport:
    """Permit read requests inside the selected root and retain response provenance.

    This is a scoped operator workflow, not an isolation boundary against DNS
    changes or a hostile remote caller. Each request and redirect is paced;
    unknown robots failures stop acquisition instead of silently allowing it.
    """

    def __init__(self, options: WebIngestOptions, budget: WebBudget) -> None:
        self.options, self.budget = options, budget
        root = urlsplit(options.root_url)
        self.robots_url = urlunsplit((root.scheme, root.netloc, "/robots.txt", "", ""))
        self._robots: RobotFileParser | None = None
        self._robots_loading = False
        self._next_request = 0.0
        self._opener = urllib.request.build_opener(_ScopedRedirects(self))
        self.last_response: dict[str, Any] = {}

    def check_scope(self, url: str) -> None:
        """Only the root's robots document is allowed outside its path prefix."""
        normalized = normalize_http_url(url)
        if normalized == self.robots_url:
            return
        if normalized is None or not is_within_root_scope(
            self.options.root_url, normalized, span_hosts=False, no_parent=True
        ):
            raise ValueError("HTTP request or redirect leaves the selected web source")

    def pace(self) -> None:
        """Share one serial request schedule for page, robots and file requests."""
        self.budget.tick()
        self.budget.pause(max(0.0, self._next_request - time.monotonic()))
        interval = (
            0.0
            if not self.options.requests_per_hour
            else 3600 / self.options.requests_per_hour
        )
        self._next_request = time.monotonic() + interval

    def check_robots(self, url: str) -> None:
        """Allow missing robots rules, but retain explicit denials and service failures."""
        if not self.options.respect_robots:
            return
        if self._robots is None:
            self._load_robots()
        assert self._robots is not None
        if not self._robots.can_fetch(self.options.user_agent, url):
            raise PermissionError("robots.txt disallows this source URL")

    def _load_robots(self) -> None:
        """Read a bounded rules document without recursively applying its own rules."""
        if self._robots_loading:
            raise ValueError(
                "robots.txt redirected to a path requiring unknown robots rules"
            )
        self._robots_loading = True
        parser = RobotFileParser(self.robots_url)
        try:
            self.pace()
            request = urllib.request.Request(
                self.robots_url, headers={"User-Agent": self.options.user_agent}
            )
            try:
                with self._opener.open(
                    request, timeout=self.options.timeout_s
                ) as response:
                    payload = response.read(256 * 1024 + 1)
                if len(payload) > 256 * 1024:
                    raise ValueError("robots.txt exceeds the configured rules limit")
                parser.parse(payload.decode("utf-8", errors="replace").splitlines())
            except urllib.error.HTTPError as error:
                error.close()
                if error.code in {404, 410}:
                    parser.parse([])
                elif error.code in {401, 403}:
                    parser.parse(["User-agent: *", "Disallow: /"])
                else:
                    raise
            self._robots = parser
        finally:
            self._robots_loading = False

    def open(
        self, request: urllib.request.Request, timeout: float | None = None
    ) -> Any:
        """Open a read response for the HTTP driver or native discovery source."""
        self.check_scope(request.full_url)
        if request.get_method() not in {"GET", "HEAD"}:
            raise ValueError("web acquisition only permits GET and HEAD")
        self.check_robots(request.full_url)
        self.pace()
        request.add_header("User-Agent", self.options.user_agent)
        request.add_header("Accept-Encoding", "identity")
        response = self._opener.open(request, timeout=timeout or self.options.timeout_s)
        try:
            self.check_scope(response.geturl())
            if (
                response.headers.get("Content-Encoding", "identity").lower()
                != "identity"
            ):
                raise ValueError("server ignored identity transfer encoding")
            self.last_response = {
                "requested_url": request.full_url,
                "final_url": response.geturl(),
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_type": response.headers.get("Content-Type"),
                "content_length": response.headers.get("Content-Length"),
            }
            return response
        except BaseException:
            response.close()
            raise
