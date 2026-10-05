"""
Declare bootstrap repair contracts shared by database facade mixins.

These abstract interfaces specify rating and sentinel-row repair, both potentially writable operations. Their methods perform no repair until implemented by a concrete facade.
"""

from __future__ import annotations

import abc


class DatabaseRatingMixinAPI(abc.ABC):
    """
    Require implementation of the canonical rating-scale repair operation.

    The concrete facade uses IDs 1–11 for values 0.0–5.0 in half steps; it may insert missing records and update incorrect numeric entries.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseRatingMixinAPI)
        True
    """

    @abc.abstractmethod
    def check_rating_table(self) -> None:
        """
        Insert missing standard ratings and synchronize incorrect numeric values.

        Abstract repair hook. The concrete facade repairs IDs 1–11 sequentially, leaves extra IDs intact and can raise when existing rating values are not numeric.

        Example:
            For an open writable database, db.check_rating_table() restores rating ID 1 to 0.0 and ID 11 to 5.0.


        :return: None; ratings outside IDs 1 through 11 are left untouched.
        :raises ValueError: An existing rating cannot be converted to float.
        :raises TypeError: An existing rating value does not support float conversion.
        """


class DatabaseNullRowsMixinAPI(abc.ABC):
    """
    Require implementation of schema-specific sentinel-row repair.

    Concrete FRBR facades prefer the organisation agent sentinel; legacy schemas can use publishers instead.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseNullRowsMixinAPI)
        True
    """

    @abc.abstractmethod
    def ensure_null_rows(self) -> None:
        """
        Insert or repair the series and publishing-entity sentinel records.

        Abstract repair hook. The concrete facade uses schema categories to select series and agent/publisher sentinel records and performs individual inserts or updates.

        Example:
            For an open writable database, db.ensure_null_rows() restores a changed series sentinel display to None and the agent sentinel to its canonical organisation identity.


        :return: None; existing sentinel rows may be updated even when already valid.
        """


