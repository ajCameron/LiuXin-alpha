"""Metadata SQL macros for title-to-comment relationships."""




class CMTitleCommentsMacrosMixin:
    """Implement title-to-comment relationship macros."""

    
    def clear_title_comments_from_title_id(self, title_id):
        """
        Remove all the comments linked to a title with the given id.
        :param title_id:
        :return:
        """
        stmt = "DELETE FROM comment_title_links WHERE comment_title_link_title_id = ?;"
        self.db.driver_wrapper.execute(stmt, (title_id,))
