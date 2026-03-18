"""
API contracts for Database mixin surfaces.

Modularized, as these mixins are used in multiple different places.
"""

from __future__ import annotations

import abc


class DatabaseRatingMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseRatingMixin``.
    """

    @abc.abstractmethod
    def check_rating_table(self) -> None:
        """
        Ensure canonical rows exist in ``ratings`` and repair malformed entries.

        :return:
        """


class DatabaseNullRowsMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseNullRowsMixin``.
    """

    @abc.abstractmethod
    def ensure_null_rows(self) -> None:
        """
        Ensure required sentinel/null rows exist for schema-specific tables.

        :return:
        """


