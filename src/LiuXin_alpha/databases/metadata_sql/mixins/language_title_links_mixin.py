

from LiuXin_alpha.errors import DatabaseIntegrityError


class CMLanguageTitleLinks:


    # Todo: primary_language table
    # Todo: Tests how this responds when you set the land_id to None - should be fine, but check
    def set_title_primary_language(self, title_id, lang_id):
        """
        Set the primary title of a work. The primary language of the title is scrubbed and replaced with the given
        language id.
        :param title_id: Id of the title to set the primary language for
        :param lang_id: The id of the language to set primary for the given title
        :return:
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
