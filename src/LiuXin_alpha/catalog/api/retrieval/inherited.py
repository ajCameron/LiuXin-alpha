"""Describe attributed tag inheritance without implying stored metadata changes."""

from typing import Protocol


class InheritedMetadataRetrieverAPI(Protocol):
    """Expose bounded WEMI inheritance with complete relationship provenance.

    Reads retain every parent path, reject truncated results and never schedule
    maintenance or copy inherited values into descendants' stored metadata.
    """

    def tags(self, *, level: str, entity_id: int, max_rows: int = 10000) -> dict:
        """Read direct/ancestor tags and all paths in a read-only snapshot.

        Follow every WEMI parent, preserving distinct Work contributions to shared
        Manifestations. Reject incomplete inspection, missing nodes and nesting in
        another transaction. This neither copies tags nor starts maintenance.
        """
        ...
