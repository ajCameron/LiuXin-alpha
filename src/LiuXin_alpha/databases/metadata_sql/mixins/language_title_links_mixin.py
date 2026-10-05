"""
Provide metadata SQL operations for language title links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""



from LiuXin_alpha.errors import DatabaseIntegrityError


class CMLanguageTitleLinks:
    """
    Implement the language title links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.set_title_primary_language(1, 1)  # doctest: +SKIP
    """



    # Todo: primary_language table
    # Todo: Tests how this responds when you set the land_id to None - should be fine, but check
    def set_title_primary_language(self, title_id, lang_id):
        """
        Remove primary language links, then create a highest-priority primary link.

        Loads title/language rows after deletion. On DatabaseIntegrityError during linking,
        deletes all links for that pair and retries once. Earlier deletions may persist if
        loading or retrying fails; no atomic boundary is added.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.set_title_primary_language(1, 1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param lang_id: Language row ID to link as primary.
        :return: None.
        """
        # There can only be one primary language link between the title and the languages table
        del_stmt = (
            "DELETE FROM language_title_links "
            "WHERE language_title_link_title_id = ? AND language_title_link_type = 'primary';"
        )
        self.execute(del_stmt, (title_id,))

        title_row = self.db.get_row_from_id("titles", row_id=title_id)
        lang_row = self.db.get_row_from_id("languages", row_id=lang_id)

        try:
            self.db.interlink_rows(
                primary_row=title_row,
                secondary_row=lang_row,
                type="primary",
                priority="highest",
            )
        except DatabaseIntegrityError:
            # If there are language title links which are not primary
            del_stmt = (
                "DELETE FROM language_title_links "
                "WHERE language_title_link_title_id = ? AND language_title_link_language_id = ?;"
            )
            self.execute(del_stmt, (title_id, lang_id))

            self.db.interlink_rows(
                primary_row=title_row,
                secondary_row=lang_row,
                type="primary",
                priority="highest",
            )

        # # Add back a link between the title and the new entry
        # insert_stmt = "INSERT INTO language_title_links " \
        #               "(language_title_link_title_id, language_title_link_language_id, language_title_link_type)" \
        #               "VALUES (?, ?, 'primary');"
        # self.execute(insert_stmt, (title_id, lang_id))
