"""Compose bounded web discovery, durable observations and optional managed acquisition."""

from __future__ import annotations

import dataclasses
import json
import tempfile
import time
from pathlib import Path
from typing import Any
from uuid import UUID

from LiuXin_alpha.ingest.sources.html_common import (
    is_within_root_scope,
    normalize_http_url,
)
from LiuXin_alpha.ingest.sources.native_html import (
    NativeHtmlBackendOptions,
    NativeHtmlDiscoverySource,
)
from LiuXin_alpha.ingest.sources.wget_html import (
    WgetBackendOptions,
    WgetHtmlDiscoverySource,
)
from LiuXin_alpha.ingest.web.acquisition import WebAcquisition
from LiuXin_alpha.ingest.web.budget import WebBudget
from LiuXin_alpha.ingest.web.models import (
    WebIngestLimitError,
    WebIngestOptions,
    WebIngestReport,
    WebRunBusyError,
)
from LiuXin_alpha.ingest.web.repository import WebIngestRepository
from LiuXin_alpha.ingest.web.staging import WebStaging
from LiuXin_alpha.ingest.web.transport import SiteTransport
from LiuXin_alpha.storage.api import StoreAPI


class WebIngestApplication:
    """Own one serial attempt while using the catalogue and storage services supplied by Library."""

    def __init__(
        self, library: Any, options: WebIngestOptions, *, resume: str | None = None
    ) -> None:
        self.library = library
        resume = None if resume is None else str(UUID(resume))
        self.resuming = resume is not None
        self.repository = WebIngestRepository(
            library.database, create=not options.dry_run
        )
        if resume:
            saved = self.repository.run(str(UUID(resume)))
            options = WebIngestOptions.from_mapping(
                json.loads(saved["web_ingest_run_options_json"])
            )
        self.options = options
        self.destination = (
            self._destination()
            if options.mode == "download" and not options.dry_run
            else None
        )
        if self.destination is not None and not resume:
            self.options = dataclasses.replace(
                options, destination=str(self.destination.store_ref)
            )
        self.run_id = (
            None
            if options.dry_run
            else (resume or self.repository.new_run(self.options))
        )
        self.lease_ms = int(max(120.0, options.timeout_s * 3) * 1000)
        self.owner = (
            None
            if self.run_id is None
            else self.repository.claim(self.run_id, lease_ms=self.lease_ms)
        )
        self._last_heartbeat = 0.0
        self.budget = WebBudget(self.options, self._heartbeat, self._charge)
        self.transport = SiteTransport(self.options, self.budget)
        self.report = WebIngestReport(
            self.run_id, self.options.root_url, self.options.mode
        )
        self.candidates: dict[str, dict[str, Any]] = {}

    def _destination(self) -> StoreAPI:
        """Pin destination identity before persisting a new run's replay controls."""
        manager = self.library.storage
        selected = (
            UUID(self.options.destination)
            if self.options.destination
            else manager.get_default_store_ref()
        )
        destination = manager.get_store(selected)
        destination.startup()
        if destination.configuration.read_only:
            raise ValueError("web downloads require a writable managed destination")
        return destination

    def _heartbeat(self) -> None:
        """Amortize lease updates without holding catalogue transactions across network I/O."""
        if self.run_id is not None and time.monotonic() - self._last_heartbeat >= 10:
            assert self.owner is not None
            self.repository.heartbeat(self.run_id, self.owner, lease_ms=self.lease_ms)
            self._last_heartbeat = time.monotonic()

    def _charge(self, count: int) -> int:
        """Keep byte limits durable across resumed attempts, including failed transfers."""
        assert self.run_id is not None and self.owner is not None
        return self.repository.charge_bytes(
            self.run_id, self.owner, count, self.options.max_total_bytes
        )

    def run(self) -> WebIngestReport:
        """Retain partial observations and failures; never label them a complete site inventory."""
        try:
            selection_complete = self._collect_bounded()
            self.report.registered_files = len(self.candidates)
            if self.options.dry_run:
                self.report.files = [
                    {"url": url, "status": "candidate"} for url in self.candidates
                ]
            elif self.options.mode == "download":
                self._acquire()
            else:
                self.report.files = [
                    {
                        "url": url,
                        "remote_file_id": row["remote_file_id"],
                        "status": "registered",
                        "content_verified": False,
                    }
                    for url, row in self.candidates.items()
                ]
            self.report.status = "partial" if self.report.errors else "complete"
            if not selection_complete:
                self.report.status = "limited"
        except WebIngestLimitError as error:
            self.report.status = "limited"
            self.report.errors.append(str(error))
        except WebRunBusyError:
            raise
        except Exception as error:
            self.report.status = "failed"
            self.report.errors.append(str(error))
        except BaseException:
            self.report.status = "interrupted"
            raise
        finally:
            self.report.registered_files = len(self.candidates)
            if self.run_id is not None:
                assert self.owner is not None
                self.report.downloaded_bytes = self.repository.run(self.run_id)[
                    "web_ingest_run_bytes_received"
                ]
                self.repository.finish(self.run_id, self.owner, self.report.to_dict())
        return self.report

    def _collect_bounded(self) -> bool:
        """Keep a full selection usable when a further candidate reaches its ceiling."""
        try:
            self._collect()
        except WebIngestLimitError as error:
            self.report.errors.append(str(error))
            return False
        return True

    def _consider(self, url: str, referrer: str | None = None) -> None:
        """Record only normalized, selected links; leave hashes and byte identities unknown."""
        self.budget.tick()
        normalized = normalize_http_url(url)
        if (
            normalized is None
            or normalized in self.candidates
            or not self.options.accepts(normalized)
        ):
            return
        if not is_within_root_scope(
            self.options.root_url, normalized, span_hosts=False, no_parent=True
        ):
            return
        if len(self.candidates) >= self.options.max_files:
            raise WebIngestLimitError("web ingest reached its selected-file limit")
        if self.options.dry_run:
            self.candidates[normalized] = {"remote_file_url": normalized}
            return
        assert self.run_id is not None and self.owner is not None
        self.repository.require_owner(self.run_id, self.owner)
        candidate = self.repository.observe(self.options.root_url, normalized, referrer)
        self._select(candidate)

    def _select(self, candidate: dict[str, Any]) -> None:
        """Select a stored observation without pretending it was seen again on the website."""
        url = candidate["remote_file_url"]
        if url in self.candidates or not self.options.accepts(url):
            return
        if len(self.candidates) >= self.options.max_files:
            raise WebIngestLimitError("web ingest reached its selected-file limit")
        self.candidates[url] = candidate
        if self.options.dry_run:
            return
        assert self.run_id is not None and self.owner is not None
        if self.repository.item(self.run_id, candidate["remote_file_id"]) is None:
            state = "registered" if self.options.mode == "register" else "pending"
            self.repository.save_item(
                self.run_id,
                self.owner,
                candidate["remote_file_id"],
                state=state,
                receipt={},
            )

    def _observation(self, details: dict[str, object]) -> None:
        """Capture the referring page where the discovery backend supplies one."""
        if details.get("accepted"):
            referrer = details.get("referrer")
            self._consider(
                str(details["url"]), None if referrer is None else str(referrer)
            )

    def _collect(self) -> None:
        """Reuse recorded candidates on request, otherwise crawl with the existing adapters."""
        if self.resuming:
            assert self.run_id is not None
            wanted = {
                row["web_ingest_item_remote_file_id"]
                for row in self.repository.items(self.run_id)
            }
            for candidate in self.repository.files(self.options.root_url):
                if candidate["remote_file_id"] in wanted:
                    self._select(candidate)
            if self.repository.run(self.run_id)["web_ingest_run_discovery_complete"]:
                return
        self._discover()
        if self.run_id is not None and not self.report.errors:
            assert self.owner is not None
            self.repository.discovery_complete(self.run_id, self.owner)

    def _discover(self) -> None:
        """Choose current catalogue observations or a fresh bounded crawl."""
        if self.options.registered_only:
            for candidate in self.repository.files(self.options.root_url):
                self._select(candidate)
            return
        if self.options.accepts(self.options.root_url):
            self._consider(self.options.root_url)
            return
        if self.options.crawler == "native":
            source = NativeHtmlDiscoverySource(
                self.options.root_url,
                options=NativeHtmlBackendOptions(
                    max_depth=self.options.max_depth,
                    max_pages=self.options.max_pages,
                    max_observed_urls=self.options.max_observed_urls,
                    max_html_bytes=self.options.max_html_bytes,
                    timeout_s=self.options.timeout_s,
                    max_http_requests_per_hour=0,
                    respect_robots=False,
                    strict_callbacks=True,
                ),
                request_opener=self.transport.open,
            )
            self._walk(source)
            self.report.errors.extend(source.last_errors)
        else:
            self._walk_wget()

    def _walk(self, source: Any) -> None:
        """Persist observations during enumeration so later crawl failure retains prior progress."""
        for url in source.discover_urls(
            force=True,
            observed_url_callback=self._observation,
            discovered_url_callback=self._consider,
            log_line_callback=lambda line: self.budget.tick(),
        ):
            self._consider(url)

    def _walk_wget(self) -> None:
        """Keep any wget navigation scratch in an owned temporary directory."""
        with tempfile.TemporaryDirectory(prefix="liuxin-web-spider-") as scratch:
            source = WgetHtmlDiscoverySource(
                self.options.root_url,
                options=WgetBackendOptions(
                    max_depth=self.options.max_depth,
                    max_observed_urls=self.options.max_observed_urls,
                    timeout_s=max(1.0, self.budget.deadline - time.monotonic()),
                    max_http_requests_per_hour=self.options.requests_per_hour,
                    respect_robots=self.options.respect_robots,
                    user_agent=self.options.user_agent,
                    strict_callbacks=True,
                    wget_args=(
                        "--no-config",
                        "--directory-prefix=" + scratch,
                        "--timeout=" + str(self.options.timeout_s),
                        "--tries=1",
                    ),
                ),
            )
            self._walk(source)

    def _acquire(self) -> None:
        """Acquire selected objects independently, keeping completed publication retryable."""
        if not self.options.state_directory:
            raise ValueError(
                "download mode requires a state_directory for owned staging"
            )
        assert (
            self.run_id is not None
            and self.owner is not None
            and self.destination is not None
        )
        acquisition = WebAcquisition(
            manager=self.library.storage,
            destination=self.destination,
            repository=self.repository,
            run_id=self.run_id,
            owner=self.owner,
            options=self.options,
            staging=WebStaging(Path(self.options.state_directory), self.run_id),
            budget=self.budget,
            transport=self.transport,
        )
        for candidate in self.candidates.values():
            self.budget.tick()
            try:
                receipt, reused = acquisition.acquire(candidate)
            except (WebIngestLimitError, WebRunBusyError):
                raise
            except Exception as error:
                self._record_failure(candidate, error)
                continue
            self.report.files.append(
                {**receipt, "status": "downloaded", "already_completed": reused}
            )
            if reused:
                self.report.already_completed += 1
            else:
                self.report.downloaded_files += 1
                self.report.assets_created += int(receipt["asset_created"])
                self.report.replicas_created += int(receipt["replica_created"])

    def _record_failure(self, candidate: dict[str, Any], error: Exception) -> None:
        """Preserve complete staging after publication errors, and make transfer failures visible."""
        assert self.run_id is not None and self.owner is not None
        identity = candidate["remote_file_id"]
        previous = self.repository.item(self.run_id, identity)
        state = previous["web_ingest_item_state"] if previous else "failed"
        retained = state in {"staged", "downloaded"}
        receipt = (
            json.loads(previous["web_ingest_item_receipt_json"])
            if retained and previous
            else {}
        )
        self.repository.save_item(
            self.run_id,
            self.owner,
            identity,
            state=state if retained else "failed",
            receipt=receipt,
            error=str(error),
        )
        self.report.errors.append(f"{candidate['remote_file_url']}: {error}")
        self.report.files.append(
            {
                "url": candidate["remote_file_url"],
                "status": "failed",
                "error": str(error),
            }
        )
