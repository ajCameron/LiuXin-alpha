
"""
Provide metadata SQL operations for series title links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""

from __future__ import annotations

from LiuXin_alpha.errors import DatabaseDriverError

from typing import Any, TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class SeriesTitleLinkMacros:
    """
    Implement the series title links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.get_series_id_from_value("Example")  # doctest: +SKIP
    """

    db: "DatabaseAPI"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SERIES_TITLE_LINK MACROS

    def get_series_id_from_value(self, series: str) -> int:
        """
        Read the first series ID whose stored series value equals the binding.

        Matching follows column collation; no explicit order is supplied.

        Example:
            >>> metadata_sql.get_series_id_from_value("Example")  # doctest: +SKIP


        :param series: Exact stored series value to look up.
        :return: Connection.get(all=False) result, normally series ID or None.
        """
        return self.db.driver.conn.get("SELECT series_id FROM series WHERE series=?;", (series,), all=False)

    # Todo: This typing is more hopeful than true
    def check_for_series_title_link(self, series_id: int, title_id: int) -> bool:
        """
        Read the highest-priority matching series link ID and index.

        Example:
            >>> metadata_sql.check_for_series_title_link(1, 1)  # doctest: +SKIP


        :param series_id: Series row identifier.
        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: Connection.get_row(all=False) result, normally one two-cell row or None,
            not a bool.
        """
        stmt = (
            "SELECT series_title_link_id, series_title_link_index "
            "FROM series_title_links "
            "WHERE series_title_link_series_id = ? AND series_title_link_title_id = ?"
            "ORDER BY series_title_link_priority DESC;"
        )
        return self.db.driver.conn.get_row(stmt, (series_id, title_id), all=False)

    # Todo: What happens if there are no series?
    def get_primary_series_index(self, title_id: int) -> Optional[int]:
        """
        Read the first index after ordering a title's series links by descending priority.

        Example:
            >>> metadata_sql.get_primary_series_index(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: Stored index scalar or the adapter's missing result; ties are unspecified.
        """
        stmt = (
            "SELECT series_title_link_index "
            "FROM series_title_links "
            "WHERE series_title_link_title_id = ?"
            "ORDER BY series_title_link_priority DESC;"
        )
        return self.db.driver.conn.get(stmt, (title_id,), all=False)

    def break_series_title_link(self, title_id: int, series_id: int) -> None:
        """
        Delete all links matching the supplied title and series IDs through the wrapper.

        Example:
            >>> metadata_sql.break_series_title_link(1, 1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param series_id: Series row identifier.
        :return: None.
        """
        del_stmt = (
            "DELETE FROM series_title_links "
            "WHERE series_title_link_series_id = ? AND series_title_link_title_id = ?;"
        )
        self.db.driver_wrapper.execute(
            del_stmt,
            (
                series_id,
                title_id,
            ),
        )

    def link_null_series_to_title(
            self,
            title_id: int,
            series_index: Optional[Union[int, float]]) -> None:
        """
        Insert a series-ID-zero link with the requested index and global maximum priority plus one.

        Empty tables yield NULL priority. Suppresses every DatabaseDriverError; an existing
        link's index is not updated.

        Example:
            >>> metadata_sql.link_null_series_to_title(1, 2)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param series_index: Index stored on the null-series link; None is bound unchanged.
        :return: None.
        """
        stmt = (
            "INSERT INTO series_title_links "
            "(series_title_link_title_id, series_title_link_series_id, "
            "series_title_link_index, series_title_link_priority) "
            "SELECT ?, 0, ?, MAX(series_title_link_priority) + 1 FROM series_title_links;"
        )
        try:
            self.db.driver_wrapper.execute(stmt, (title_id, series_index))
        except DatabaseDriverError:
            # Link has already been set null
            # Todo: Should, if this link exists, update the link with the new index
            pass

    def read_primary_title_series_id_from_meta(self, title_id: int) -> Optional[int]:
        """
        Read the series_id projected by meta for the selected title ID.

        Example:
            >>> metadata_sql.read_primary_title_series_id_from_meta(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: Connection.get(all=False) result, normally a scalar ID or None.
        """
        return self.db.driver.conn.get("SELECT series_id FROM meta WHERE id=?;", (title_id,), all=False)

    def update_index_for_series_title_link(
            self,
            title_id: int,
            series_id: int,
            index: Optional[Union[int, float]]) -> None:
        """
        Float-coerce and write the index on matching series/title links, then commit.

        None and other non-convertible inputs raise before SQL despite the optional
        annotation.

        Example:
            >>> metadata_sql.update_index_for_series_title_link(1, 1, 2)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param series_id: Series row identifier.
        :param index: Index converted with float(); None therefore raises despite its
            annotation.
        :return: None.
        """
        stmt = (
            "UPDATE series_title_links "
            "SET series_title_link_index = ? "
            "WHERE series_title_link_series_id = ?"
            "AND series_title_link_title_id = ?;"
        )
        self.db.driver.conn.execute(stmt, (float(index), series_id, title_id))
        self.db.driver.conn.commit()

    #
    # ------------------------------------------------------------------------------------------------------------------


    def get_title_series_ids_set(self, title_id):
        """
        Collect distinct series IDs from every link belonging to one title.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.get_title_series_ids_set(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: Set of first-cell values, including any null/sentinel values supplied by
            the schema.
        """
        stmt = "SELECT series_title_link_series_id FROM series_title_links WHERE series_title_link_title_id = ?;"
        return set(row[0] for row in self.execute(stmt, (title_id,)))
