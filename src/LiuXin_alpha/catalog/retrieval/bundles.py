"""WEMI bundle retrieval implementation scaffold."""

from __future__ import annotations

from typing import Any

from ..api.common import DatabaseHandle, EntityId, WemiBundle


class BundleRetriever:
    """Read coherent WEMI slices for catalog consumers."""

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def for_item(self, item_id: EntityId) -> WemiBundle:
        raise NotImplementedError("Move item-rooted WEMI bundle retrieval here from databases")

    def for_manifestation(self, manifestation_id: EntityId) -> WemiBundle:
        raise NotImplementedError("Move manifestation-rooted WEMI bundle retrieval here from databases")

    def for_expression(self, expression_id: EntityId) -> WemiBundle:
        raise NotImplementedError("Move expression-rooted WEMI bundle retrieval here from databases")

    def for_work(self, work_id: EntityId) -> WemiBundle:
        raise NotImplementedError("Move work-rooted WEMI bundle retrieval here from databases")
