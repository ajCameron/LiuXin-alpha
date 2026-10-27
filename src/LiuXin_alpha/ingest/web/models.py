"""Validated controls and explicit receipts for remote registration and acquisition."""

from __future__ import annotations

import dataclasses
import math
from pathlib import PurePosixPath
from typing import Any, Literal
from urllib.parse import unquote, urlsplit

from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
from LiuXin_alpha.ingest.sources.html_common import (
    looks_like_html_page_url,
    normalize_http_url,
)


@dataclasses.dataclass(frozen=True)
class WebIngestOptions:
    """Bound one site walk and, when requested, its retained downloads.

    Registration records observations without fetching ebook payloads. Crawling
    still reads navigation pages. Downloads are serial and stage complete objects
    before storage publication; limits apply to actual bytes, not only headers.
    """

    root_url: str
    mode: Literal["register", "download"] = "register"
    crawler: Literal["native", "wget"] = "native"
    destination: str | None = None
    state_directory: str | None = None
    registered_only: bool = False
    dry_run: bool = False
    extensions: tuple[str, ...] = ()
    include: tuple[str, ...] = ()
    max_depth: int = 5
    max_pages: int = 1000
    max_files: int = 1000
    max_observed_urls: int = 100_000
    max_html_bytes: int = 2_000_000
    max_file_bytes: int = 256 * 1024 * 1024
    max_total_bytes: int = 2 * 1024 * 1024 * 1024
    min_free_bytes: int = 1024 * 1024 * 1024
    requests_per_hour: float | None = None
    timeout_s: float = 30.0
    max_seconds: float = 3600.0
    user_agent: str = "LiuXin/1.0 (+bounded library acquisition)"
    respect_robots: bool = True

    def __post_init__(self) -> None:
        """Reject ambiguous or unbounded settings before network or catalogue work."""
        if self.requests_per_hour is None:
            from LiuXin_alpha.ingest.sources.crawler_defaults import (
                get_default_crawler_http_requests_per_hour,
            )

            object.__setattr__(
                self, "requests_per_hour", get_default_crawler_http_requests_per_hour()
            )
        root = normalize_http_url(self.root_url)
        if root is None:
            raise ValueError(
                "root_url must be an HTTP(S) URL without credentials or secret parameters"
            )
        object.__setattr__(self, "root_url", root)
        if self.mode not in {"register", "download"} or self.crawler not in {
            "native",
            "wget",
        }:
            raise ValueError("select register/download mode and native/wget crawler")
        for name in ("registered_only", "dry_run", "respect_robots"):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be a boolean")
        self._validate_limits()
        self._validate_selectors()

    def _validate_limits(self) -> None:
        """Reject negative, fractional or nonfinite budgets before any I/O."""
        for name in (
            "max_depth",
            "max_pages",
            "max_files",
            "max_observed_urls",
            "max_html_bytes",
            "max_file_bytes",
            "max_total_bytes",
            "min_free_bytes",
        ):
            value = getattr(self, name)
            minimum = 0 if name in {"max_depth", "min_free_bytes"} else 1
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        for name in ("requests_per_hour", "timeout_s", "max_seconds"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (float, int))
                or not math.isfinite(value)
            ):
                raise ValueError(f"{name} must be finite")
            if value < 0 or (name != "requests_per_hour" and value == 0):
                raise ValueError(
                    f"{name} must be positive (only request pacing permits zero)"
                )

    def _validate_selectors(self) -> None:
        """Normalize immutable filters and reject options that have no meaning in this mode."""
        self._validate_text()
        for name in ("extensions", "include"):
            values = getattr(self, name)
            if not isinstance(values, (tuple, list)) or not all(
                isinstance(value, str) and value for value in values
            ):
                raise ValueError(f"{name} must contain nonempty strings")
            object.__setattr__(self, name, tuple(values))
        if not self.user_agent.strip() or any(
            char in self.user_agent for char in "\r\n"
        ):
            raise ValueError("user_agent must be a nonempty single line")
        if self.mode == "register" and self.destination is not None:
            raise ValueError("a destination is only used with download mode")

    def _validate_text(self) -> None:
        """Refuse invalid optional paths/identities before resolving a destination."""
        from uuid import UUID

        if not isinstance(self.user_agent, str):
            raise ValueError("user_agent must be a string")
        for name in ("state_directory", "destination"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be a nonempty string")
        if self.destination is not None:
            object.__setattr__(self, "destination", str(UUID(self.destination)))

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> WebIngestOptions:
        """Use the same validation for CLI, Core, and saved-run reconstruction."""
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible controls suitable for exact replay."""
        return dataclasses.asdict(self)

    def accepts(self, url: str) -> bool:
        """Select filename suffixes and optional URL globs without interpreting local paths."""
        from fnmatch import fnmatchcase

        if not self.extensions and looks_like_html_page_url(unquote(url)):
            return False
        suffix = PurePosixPath(unquote(urlsplit(url).path)).suffix.lower().lstrip(".")
        extensions = {
            value.lower().lstrip(".") for value in (self.extensions or BOOK_EXTENSIONS)
        }
        return suffix in extensions and (
            not self.include
            or any(fnmatchcase(url, pattern) for pattern in self.include)
        )


class WebIngestLimitError(RuntimeError):
    """A crawl, download, capacity or elapsed-time budget forbids further work."""


class WebRunBusyError(RuntimeError):
    """Another worker currently owns this saved run."""


@dataclasses.dataclass
class WebIngestReport:
    """Keep observations, completed acquisitions and failures distinguishable."""

    run_id: str | None
    root_url: str
    mode: str
    status: str = "running"
    registered_files: int = 0
    downloaded_files: int = 0
    already_completed: int = 0
    downloaded_bytes: int = 0
    assets_created: int = 0
    replicas_created: int = 0
    files: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    errors: list[str] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Expose durable IDs and observed outcomes without treating discovery as verification."""
        return dataclasses.asdict(self)
