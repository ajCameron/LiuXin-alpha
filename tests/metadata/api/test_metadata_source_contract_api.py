"""
Verify metadata read/write source protocols and adapter contract enforcement.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata source contract api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py
"""
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
    """
    Provide the WorkSource test fixture or double with explicit deterministic behavior.

    Example:
        Exercise WorkSource through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py
    """
    def get_work_identity(self, work_id: int) -> tuple[str, int]:
        """
        Return work identity from deterministic test state.

        Example:
            Exercise WorkSource.get work identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("work_identity", work_id)

    def get_work_metadata(self, work_id: int) -> tuple[str, int]:
        """
        Return work metadata from deterministic test state.

        Example:
            Exercise WorkSource.get work metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("work_metadata", work_id)


class _ExpressionSource(ExpressionMetadataGetterAPI):
    """
    Provide the ExpressionSource test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ExpressionSource through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py
    """
    def get_expression_identity(self, expression_id: int) -> tuple[str, int]:
        """
        Return expression identity from deterministic test state.

        Example:
            Exercise ExpressionSource.get expression identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param expression_id: Value supplied for expression id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("expression_identity", expression_id)

    def get_expression_metadata(self, expression_id: int) -> tuple[str, int]:
        """
        Return expression metadata from deterministic test state.

        Example:
            Exercise ExpressionSource.get expression metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param expression_id: Value supplied for expression id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("expression_metadata", expression_id)


class _ManifestationSource(ManifestationMetadataGetterAPI):
    """
    Provide the ManifestationSource test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ManifestationSource through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py
    """
    def get_manifestation_identity(self, manifestation_id: int) -> tuple[str, int]:
        """
        Return manifestation identity from deterministic test state.

        Example:
            Exercise ManifestationSource.get manifestation identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param manifestation_id: Value supplied for manifestation id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("manifestation_identity", manifestation_id)

    def get_manifestation_metadata(self, manifestation_id: int) -> tuple[str, int]:
        """
        Return manifestation metadata from deterministic test state.

        Example:
            Exercise ManifestationSource.get manifestation metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param manifestation_id: Value supplied for manifestation id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("manifestation_metadata", manifestation_id)


class _ItemSource(ItemMetadataGetterAPI):
    """
    Provide the ItemSource test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ItemSource through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py
    """
    def get_item_identity(self, item_id: int) -> tuple[str, int]:
        """
        Return item identity from deterministic test state.

        Example:
            Exercise ItemSource.get item identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param item_id: Value supplied for item id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("item_identity", item_id)

    def get_item_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, Any] | None = None,
    ) -> tuple[str, int | None, dict[str, Any] | None]:
        """
        Return item metadata from deterministic test state.

        Example:
            Exercise ItemSource.get item metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param item_id: Value supplied for item id in the focused test operation.
        :param source_row: Value supplied for source row in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("item_metadata", item_id, source_row)


class _AgentSource(AgentProfileGetterAPI):
    """
    Provide the AgentSource test fixture or double with explicit deterministic behavior.

    Example:
        Exercise AgentSource through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py
    """
    def get_agent_identity(self, agent_id: int) -> tuple[str, int]:
        """
        Return agent identity from deterministic test state.

        Example:
            Exercise AgentSource.get agent identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("agent_identity", agent_id)

    def get_agent_profile(self, agent_id: int) -> tuple[str, int]:
        """
        Return agent profile from deterministic test state.

        Example:
            Exercise AgentSource.get agent profile through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("agent_profile", agent_id)

    def get_agent_participation_snapshot(self, agent_id: int) -> tuple[str, int]:
        """
        Return agent participation snapshot from deterministic test state.

        Example:
            Exercise AgentSource.get agent participation snapshot through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("agent_participation_snapshot", agent_id)

    def get_work_credit_for_typed_agent(
        self,
        work_id: int,
        agent_id: int,
        type_filter: str,
    ) -> tuple[str, int, int, str]:
        """
        Return work credit for typed agent from deterministic test state.

        Example:
            Exercise AgentSource.get work credit for typed agent through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :param agent_id: Value supplied for agent id in the focused test operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("work_credit_for_typed_agent", work_id, agent_id, type_filter)

    def get_work_credits_for_agent(
        self,
        work_id: int,
        agent_id: int,
    ) -> tuple[tuple[str, int, int]]:
        """
        Return work credits for agent from deterministic test state.

        Example:
            Exercise AgentSource.get work credits for agent through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return (("work_credits_for_agent", work_id, agent_id),)

    def get_work_agent_credits(self, work_id: int) -> tuple[tuple[str, int]]:
        """
        Return work agent credits from deterministic test state.

        Example:
            Exercise AgentSource.get work agent credits through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return (("work_agent_credits", work_id),)


class _DBMetadataSource(DBMetadataSourceAPI):
    """
    Provide the DBMetadataSource test fixture or double with explicit deterministic behavior.

    Example:
        Exercise DBMetadataSource through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py
    """
    def get_work_identity(self, work_id: int) -> tuple[str, int]:
        """
        Return work identity from deterministic test state.

        Example:
            Exercise DBMetadataSource.get work identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("work_identity", work_id)

    def get_work_metadata(self, work_id: int) -> tuple[str, int]:
        """
        Return work metadata from deterministic test state.

        Example:
            Exercise DBMetadataSource.get work metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("work_metadata", work_id)

    def get_expression_identity(self, expression_id: int) -> tuple[str, int]:
        """
        Return expression identity from deterministic test state.

        Example:
            Exercise DBMetadataSource.get expression identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param expression_id: Value supplied for expression id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("expression_identity", expression_id)

    def get_expression_metadata(self, expression_id: int) -> tuple[str, int]:
        """
        Return expression metadata from deterministic test state.

        Example:
            Exercise DBMetadataSource.get expression metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param expression_id: Value supplied for expression id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("expression_metadata", expression_id)

    def get_manifestation_identity(self, manifestation_id: int) -> tuple[str, int]:
        """
        Return manifestation identity from deterministic test state.

        Example:
            Exercise DBMetadataSource.get manifestation identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param manifestation_id: Value supplied for manifestation id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("manifestation_identity", manifestation_id)

    def get_manifestation_metadata(self, manifestation_id: int) -> tuple[str, int]:
        """
        Return manifestation metadata from deterministic test state.

        Example:
            Exercise DBMetadataSource.get manifestation metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param manifestation_id: Value supplied for manifestation id in the focused test
            operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("manifestation_metadata", manifestation_id)

    def get_item_identity(self, item_id: int) -> tuple[str, int]:
        """
        Return item identity from deterministic test state.

        Example:
            Exercise DBMetadataSource.get item identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param item_id: Value supplied for item id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("item_identity", item_id)

    def get_item_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, Any] | None = None,
    ) -> tuple[str, int | None, dict[str, Any] | None]:
        """
        Return item metadata from deterministic test state.

        Example:
            Exercise DBMetadataSource.get item metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param item_id: Value supplied for item id in the focused test operation.
        :param source_row: Value supplied for source row in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("item_metadata", item_id, source_row)

    def get_agent_identity(self, agent_id: int) -> tuple[str, int]:
        """
        Return agent identity from deterministic test state.

        Example:
            Exercise DBMetadataSource.get agent identity through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("agent_identity", agent_id)

    def get_agent_profile(self, agent_id: int) -> tuple[str, int]:
        """
        Return agent profile from deterministic test state.

        Example:
            Exercise DBMetadataSource.get agent profile through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("agent_profile", agent_id)

    def get_agent_participation_snapshot(self, agent_id: int) -> tuple[str, int]:
        """
        Return agent participation snapshot from deterministic test state.

        Example:
            Exercise DBMetadataSource.get agent participation snapshot through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("agent_participation_snapshot", agent_id)

    def get_work_credit_for_typed_agent(
        self,
        work_id: int,
        agent_id: int,
        type_filter: str,
    ) -> tuple[str, int, int, str]:
        """
        Return work credit for typed agent from deterministic test state.

        Example:
            Exercise DBMetadataSource.get work credit for typed agent through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :param agent_id: Value supplied for agent id in the focused test operation.
        :param type_filter: Value supplied for type filter in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return ("work_credit_for_typed_agent", work_id, agent_id, type_filter)

    def get_work_credits_for_agent(
        self,
        work_id: int,
        agent_id: int,
    ) -> tuple[tuple[str, int, int]]:
        """
        Return work credits for agent from deterministic test state.

        Example:
            Exercise DBMetadataSource.get work credits for agent through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :param agent_id: Value supplied for agent id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return (("work_credits_for_agent", work_id, agent_id),)

    def get_work_agent_credits(self, work_id: int) -> tuple[tuple[str, int]]:
        """
        Return work agent credits from deterministic test state.

        Example:
            Exercise DBMetadataSource.get work agent credits through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param work_id: Value supplied for work id in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return (("work_agent_credits", work_id),)

    def get_liuxin_wemi_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, Any] | None = None,
    ) -> tuple[str, int | None, dict[str, Any] | None]:
        """
        Return liuxin wemi metadata from deterministic test state.

        Example:
            Exercise DBMetadataSource.get liuxin wemi metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param item_id: Value supplied for item id in the focused test operation.
        :param source_row: Value supplied for source row in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Perform the hydrate metadata test-helper operation with deterministic inputs.

        Example:
            Exercise DBMetadataSource.hydrate metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


        :param kind: Value supplied for kind in the focused test operation.
        :param work_id: Value supplied for work id in the focused test operation.
        :param expression_id: Value supplied for expression id in the focused test
            operation.
        :param manifestation_id: Value supplied for manifestation id in the focused test
            operation.
        :param item_id: Value supplied for item id in the focused test operation.
        :param source_row: Value supplied for source row in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify metadata source base initializers keep database reference.

    Example:
        Exercise test metadata source base initializers keep database reference through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_source_contract_api.py


    :return: None; the function records state or raises through its assertions.
    """
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
