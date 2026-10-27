"""Stage complete HTTP objects, then publish through the existing storage manager."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit
from uuid import UUID, uuid5

from LiuXin_alpha.ingest.web.budget import WebBudget
from LiuXin_alpha.ingest.web.models import WebIngestLimitError, WebIngestOptions
from LiuXin_alpha.ingest.web.repository import WebIngestRepository, now_ms
from LiuXin_alpha.ingest.web.staging import WebStaging
from LiuXin_alpha.ingest.web.transport import SiteTransport
from LiuXin_alpha.storage.api import (
    Digest,
    DigitalAssetMetadata,
    StorageManagerAPI,
    StoreAPI,
)
from LiuXin_alpha.storage.drivers.http import HttpStorageDriver


class WebAcquisition:
    """Use catalogue receipts to retry publication without redownloading completed bytes."""

    def __init__(
        self,
        *,
        manager: StorageManagerAPI,
        destination: StoreAPI,
        repository: WebIngestRepository,
        run_id: str,
        owner: str,
        options: WebIngestOptions,
        staging: WebStaging,
        budget: WebBudget,
        transport: SiteTransport,
    ) -> None:
        self.manager, self.destination = manager, destination
        self.repository, self.run_id, self.owner = repository, run_id, owner
        self.options, self.staging, self.budget = options, staging, budget
        self.transport = transport
        # The HTTP driver's root is a directory address space. Keep the origin
        # here so a directly selected book URL is representable too; transport
        # still enforces the operator's narrower path scope before each request.
        root = urlsplit(options.root_url)
        self.driver = HttpStorageDriver(
            urlunsplit((root.scheme, root.netloc, "/", "", "")),
            address_space_uuid=UUID(run_id),
            request_opener=transport.open,
            timeout_s=options.timeout_s,
        )

    def acquire(self, candidate: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """Return the captured receipt and whether it was already complete in this run."""
        identity = candidate["remote_file_id"]
        previous = self.repository.item(self.run_id, identity)
        if previous and previous["web_ingest_item_state"] == "downloaded":
            self.repository.require_owner(self.run_id, self.owner)
            self.staging.discard(identity)
            return json.loads(previous["web_ingest_item_receipt_json"]), True
        if previous and previous["web_ingest_item_state"] == "staged":
            receipt = json.loads(previous["web_ingest_item_receipt_json"])
            self.staging.verify(
                identity, size=receipt["size_bytes"], sha256=receipt["sha256"]
            )
        else:
            receipt = self._download(candidate)
        return self._publish(candidate, receipt), False

    def _download(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Hash observed bytes, reject truncation/limits, and retain only a complete object."""
        identity, url = candidate["remote_file_id"], candidate["remote_file_url"]
        self.budget.capacity(self.staging.root, self.destination, staged_bytes=0)
        self.repository.require_owner(self.run_id, self.owner)
        self.staging.discard(identity, partial=True)
        self.staging.discard(identity)
        path = self.staging.path(identity, partial=True)
        digest = hashlib.sha256()
        size = 0
        consumed = int(
            self.repository.run(self.run_id)["web_ingest_run_bytes_received"]
        )
        if consumed >= self.options.max_total_bytes:
            raise WebIngestLimitError(
                "web ingest reached its total download byte limit"
            )
        try:
            address = self.driver.object_address_from_uri(url)
            with self.driver.open_read(address) as source, path.open("xb") as target:
                evidence = dict(self.transport.last_response)
                self._check_declared_size(evidence)
                # One underlying read preserves short prefixes before a later
                # EOF error, so failed transfers still consume the run budget.
                read_chunk = getattr(source, "read1", source.read)
                while True:
                    self.budget.tick()
                    chunk = read_chunk(
                        min(
                            1024 * 1024,
                            self.options.max_file_bytes - size + 1,
                            self.options.max_total_bytes - consumed + 1,
                        )
                    )
                    if not chunk:
                        break
                    consumed = self.budget.receive(len(chunk), object_bytes=size)
                    self.budget.capacity(
                        self.staging.root,
                        self.destination,
                        staged_bytes=size,
                        incoming=len(chunk),
                    )
                    target.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                target.flush()
                os.fsync(target.fileno())
            receipt = {
                **evidence,
                "remote_file_id": identity,
                "source_url": url,
                "referrer": candidate.get("remote_file_referrer"),
                "root_url": self.options.root_url,
                "retrieved_at": now_ms(),
                "size_bytes": size,
                "sha256": digest.hexdigest(),
            }
            self.repository.require_owner(self.run_id, self.owner)
            os.replace(path, self.staging.path(identity))
            self.repository.save_item(
                self.run_id, self.owner, identity, state="staged", receipt=receipt
            )
            return receipt
        finally:
            self.repository.require_owner(self.run_id, self.owner)
            self.staging.discard(identity, partial=True)

    def _check_declared_size(self, evidence: dict[str, Any]) -> None:
        """Reject oversize declarations before reading a body; streamed limits remain authoritative."""
        length = evidence.get("content_length")
        if length is not None:
            size = int(length)
            if size < 0 or size > self.options.max_file_bytes:
                raise WebIngestLimitError(
                    "declared file size exceeds the per-file download limit"
                )
            consumed = self.repository.run(self.run_id)["web_ingest_run_bytes_received"]
            if size + consumed > self.options.max_total_bytes:
                raise WebIngestLimitError(
                    "declared file size exceeds the remaining download budget"
                )
            self.budget.capacity(self.staging.root, self.destination, staged_bytes=size)

    def _publish(
        self, candidate: dict[str, Any], receipt: dict[str, Any]
    ) -> dict[str, Any]:
        """Keep storage's retry identity stable across crashes between publication and receipt."""
        identity = candidate["remote_file_id"]
        path = self.staging.verify(
            identity, size=receipt["size_bytes"], sha256=receipt["sha256"]
        )
        self.budget.capacity(
            self.staging.root, self.destination, staged_bytes=receipt["size_bytes"]
        )
        self.repository.require_owner(self.run_id, self.owner)
        name = (
            PurePosixPath(unquote(urlsplit(candidate["remote_file_url"]).path)).name
            or identity
        )
        media_type = receipt.get("content_type") or mimetypes.guess_type(name)[0]
        metadata = DigitalAssetMetadata(
            original_name=name,
            media_type=media_type,
            attributes=(("web_provenance", json.dumps(receipt, sort_keys=True)),),
        )
        operation_id = uuid5(UUID(self.run_id), identity + ":" + receipt["sha256"])
        with path.open("rb") as stream:
            result = self.manager.ingest_identified_stream(
                stream,
                size_bytes=receipt["size_bytes"],
                authoritative_digests=(Digest("sha256", receipt["sha256"]),),
                operation_id=operation_id,
                metadata=metadata,
                preferred_store_ref=self.destination.store_ref,
                verify=True,
            )
        if not result.verified:
            raise RuntimeError("storage publication did not produce a verified Replica")
        captured = {
            **receipt,
            "operation_id": str(operation_id),
            "digital_asset_id": int(result.asset_record.digital_asset_id),
            "replica_id": int(result.replica_record.replica_id),
            "asset_created": result.asset_created,
            "replica_created": result.replica_created,
            "verified": result.verified,
        }
        self.repository.save_item(
            self.run_id, self.owner, identity, state="downloaded", receipt=captured
        )
        self.repository.require_owner(self.run_id, self.owner)
        self.staging.discard(identity)
        return captured
