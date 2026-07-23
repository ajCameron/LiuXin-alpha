"""Catalog-level semantic projections."""

from __future__ import annotations

from typing import Any

from ..api.common import DatabaseHandle, EntityId, WemiLevel


class ProjectionService:
    """Derived catalog read models.

    Keep this semantic rather than surface-specific. For example: a stable catalog
    display title belongs here; terminal column widths do not.
    """

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def display_title(self, *, level: WemiLevel, entity_id: EntityId) -> str:
        """Return the nearest stable display title for a WEMI entity."""

        candidates: tuple[object, ...]
        if level == "work":
            row = self.repositories.works.require(entity_id)
            candidates = (
                row.get("work_title"),
                row.get("work_canonical_title"),
                row.get("work_sort_title"),
            )
        else:
            from .bundles import BundleRetriever

            retriever = getattr(
                BundleRetriever(self.db, self.repositories),
                f"for_{level}",
            )
            bundle = retriever(entity_id)
            candidates = self._bundle_title_candidates(bundle, level)
        title = next(
            (
                value.strip()
                for value in candidates
                if isinstance(value, str) and value.strip()
            ),
            None,
        )
        return title or f"Untitled {level} {entity_id}"

    def item_summary(self, item_id: EntityId) -> dict[str, object]:
        """Return a compact, storage-independent summary of an Item."""

        bundle = self.repositories.items.get_metadata_bundle(item_id)
        item = bundle.item or {}
        return {
            "item_id": item_id,
            "work_id": None if bundle.work is None else bundle.work.get("work_id"),
            "expression_id": (
                None if bundle.expression is None else bundle.expression.get("expression_id")
            ),
            "manifestation_id": (
                None
                if bundle.manifestation is None
                else bundle.manifestation.get("manifestation_id")
            ),
            "title": self.display_title(level="item", entity_id=item_id),
            "location": item.get("item_location"),
            "lifecycle_status": item.get("item_lifecycle_status"),
            "agents": tuple(
                row.get("agent_canonical_name") for row in bundle.agents
            ),
            "identifiers": tuple(
                (
                    row.get("entity_identifier_scheme"),
                    row.get("entity_identifier_value"),
                )
                for row in bundle.identifiers
            ),
        }

    @staticmethod
    def _bundle_title_candidates(bundle: object, level: WemiLevel) -> tuple[object, ...]:
        work = getattr(bundle, "work", None) or {}
        expression = getattr(bundle, "expression", None) or {}
        manifestation = getattr(bundle, "manifestation", None) or {}
        if level == "expression":
            return (
                expression.get("expression_title_override"),
                expression.get("expression_subtitle"),
                work.get("work_title"),
                work.get("work_canonical_title"),
            )
        return (
            manifestation.get("manifestation_subtitle"),
            expression.get("expression_title_override"),
            expression.get("expression_subtitle"),
            work.get("work_title"),
            work.get("work_canonical_title"),
        )
