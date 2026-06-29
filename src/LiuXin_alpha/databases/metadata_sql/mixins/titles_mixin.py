



class CMTitlesMacrosMixin:

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TITLES VALUES METHODS

    def update_title(self, title_id, title):
        """
        Preform an update of a title. title row will not be created.
        :param title_id:
        :param title:
        :return:
        """
        if not title:
            self.execute("UPDATE titles SET title=Null WHERE title_id=?;", (title_id,))
        else:
            self.execute("UPDATE titles SET title=? WHERE title_id=?;", (title, title_id))

    # Todo: Add the setting null option
    def update_title_creator_sort(self, title_id, creator_val):
        """
        Update the creator sort for an individual book stored in the title.
        :param title_id:
        :param creator_val:
        :return:
        """
        stmt = "UPDATE titles SET title_creator_sort = ? WHERE title_id = ?;"
        self.execute(stmt, (creator_val, title_id))

    #
    # ------------------------------------------------------------------------------------------------------------------