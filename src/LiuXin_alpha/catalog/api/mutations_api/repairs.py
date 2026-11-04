"""Describe previewed Catalog repairs with explicit identity and retention decisions."""

from typing import Protocol


class CatalogRepairsAPI(Protocol):
    """Expose complete previews, stale-state checks and transactional audit receipts.

    Identity and disposal decisions are explicit caller inputs; no background
    process selects equivalent records or infers unwanted data from missing links.
    """

    def merge(
        self,
        *,
        repository,
        source_id,
        target_id,
        resolutions=None,
        relationship_resolutions=None,
        max_rows=10000,
        dry_run=True,
        expected_state=None,
    ) -> dict:
        """Preview a same-repository merge, requiring explicit populated-value conflicts.

        Apply requires the inspected state fingerprint. Unsupported references,
        hierarchy scopes, self-links and incomplete scans block deletion. Every
        accepted change and its audit receipt share one serialized transaction.
        """
        ...

    def cleanup(
        self,
        *,
        repository,
        candidates,
        max_rows=10000,
        dry_run=True,
        expected_state=None,
    ) -> dict:
        """Preview selected disposal with reasons and per-identity preserve decisions.

        Only explicit discard candidates without references or placeholder state
        can be deleted. Apply rechecks a preview fingerprint and records decisions
        atomically; it never infers a global garbage set or changes physical files.
        """
        ...
