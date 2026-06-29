from __future__ import annotations

from typing import Any

from LiuXin_alpha.metadata.api import (
    AgentProfileGetterAPI,
    DBMetadataSourceAPI,
    ExpressionMetadataGetterAPI,
    ItemMetadataGetterAPI,
    ManifestationMetadataGetterAPI,
    WorkMetadataGetterAPI,
)


class _WorkSource(WorkMetadataGetterAPI):
    def get_work_identity(self, work_id: int) -> tuple[str, int]:
        return ("work_identity", work_id)

    def get_work_metadata(self, work_id: int) -> tuple[str, int]:
        return ("work_metadata", work_id)


class _ExpressionSource(ExpressionMetadataGetterAPI):
    def get_expression_identity(self, expression_id: int) -> tuple[str, int]:
        return ("expression_identity", expression_id)

    def get_expression_metadata(self, expression_id: int) -> tuple[str, int]:
        return ("expression_metadata", expression_id)


class _ManifestationSource(ManifestationMetadataGetterAPI):
    def get_manifestation_identity(self, manifestation_id: int) -> tuple[str, int]:
        return ("manifestation_identity", manifestation_id)

    def get_manifestation_metadata(self, manifestation_id: int) -> tuple[str, int]:
        return ("manifestation_metadata", manifestation_id)


class _ItemSource(ItemMetadataGetterAPI):
    def get_item_identity(self, item_id: int) -> tuple[str, int]:
        return ("item_identity", item_id)

    def get_item_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, Any] | None = None,
    ) -> tuple[str, int | None, dict[str, Any] | None]:
        return ("item_metadata", item_id, source_row)


class _AgentSource(AgentProfileGetterAPI):
    def get_agent_identity(self, agent_id: int) -> tuple[str, int]:
        return ("agent_identity", agent_id)

    def get_agent_profile(self, agent_id: int) -> tuple[str, int]:
        return ("agent_profile", agent_id)

    def get_agent_participation_snapshot(self, agent_id: int) -> tuple[str, int]:
        return ("agent_participation_snapshot", agent_id)

    def get_work_credit_for_typed_agent(
        self,
        work_id: int,
        agent_id: int,
        type_filter: str,
    ) -> tuple[str, int, int, str]:
        return ("work_credit_for_typed_agent", work_id, agent_id, type_filter)

    def get_work_credits_for_agent(
        self,
        work_id: int,
        agent_id: int,
    ) -> tuple[tuple[str, int, int]]:
        return (("work_credits_for_agent", work_id, agent_id),)

    def get_work_agent_credits(self, work_id: int) -> tuple[tuple[str, int]]:
        return (("work_agent_credits", work_id),)


class _DBMetadataSource(DBMetadataSourceAPI):
    def get_work_identity(self, work_id: int) -> tuple[str, int]:
        return ("work_identity", work_id)

    def get_work_metadata(self, work_id: int) -> tuple[str, int]:
        return ("work_metadata", work_id)

    def get_expression_identity(self, expression_id: int) -> tuple[str, int]:
        return ("expression_identity", expression_id)

    def get_expression_metadata(self, expression_id: int) -> tuple[str, int]:
        return ("expression_metadata", expression_id)

    def get_manifestation_identity(self, manifestation_id: int) -> tuple[str, int]:
        return ("manifestation_identity", manifestation_id)

    def get_manifestation_metadata(self, manifestation_id: int) -> tuple[str, int]:
        return ("manifestation_metadata", manifestation_id)

    def get_item_identity(self, item_id: int) -> tuple[str, int]:
        return ("item_identity", item_id)

    def get_item_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, Any] | None = None,
    ) -> tuple[str, int | None, dict[str, Any] | None]:
        return ("item_metadata", item_id, source_row)

    def get_agent_identity(self, agent_id: int) -> tuple[str, int]:
        return ("agent_identity", agent_id)

    def get_agent_profile(self, agent_id: int) -> tuple[str, int]:
        return ("agent_profile", agent_id)

    def get_agent_participation_snapshot(self, agent_id: int) -> tuple[str, int]:
        return ("agent_participation_snapshot", agent_id)

    def get_work_credit_for_typed_agent(
        self,
        work_id: int,
        agent_id: int,
        type_filter: str,
    ) -> tuple[str, int, int, str]:
        return ("work_credit_for_typed_agent", work_id, agent_id, type_filter)

    def get_work_credits_for_agent(
        self,
        work_id: int,
        agent_id: int,
    ) -> tuple[tuple[str, int, int]]:
        return (("work_credits_for_agent", work_id, agent_id),)

    def get_work_agent_credits(self, work_id: int) -> tuple[tuple[str, int]]:
        return (("work_agent_credits", work_id),)

    def get_liuxin_wemi_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, Any] | None = None,
    ) -> tuple[str, int | None, dict[str, Any] | None]:
        return ("liuxin_wemi_metadata", item_id, source_row)

    def hydrate_metadata(
        self,
        kind: str,
        *,
        work_id: int | None = None,
        expression_id: int | None = None,
        manifestation_id: int | None = None,
        item_id: int | None = None,
        source_row: dict[str, Any] | None = None,
    ) -> tuple[
        str,
        str,
        int | None,
        int | None,
        int | None,
        int | None,
        dict[str, Any] | None,
    ]:
        return (
            "hydrate_metadata",
            kind,
            work_id,
            expression_id,
            manifestation_id,
            item_id,
            source_row,
        )


def test_metadata_source_base_initializers_keep_database_reference() -> None:
    db = object()

    for source_cls in (
        _WorkSource,
        _ExpressionSource,
        _ManifestationSource,
        _ItemSource,
        _AgentSource,
        _DBMetadataSource,
    ):
        assert source_cls(db).db is db
