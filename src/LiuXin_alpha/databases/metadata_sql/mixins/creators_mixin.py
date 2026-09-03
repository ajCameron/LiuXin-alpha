"""Metadata SQL macros for creator catalogue rows."""




class CMCreatorMacrosMixin:
    """Implement creator-row metadata macros."""


    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR VALUES METHODS

    def get_creator_sort(self, creator_id):
        """
        Returns the creator sort value for a given id from the creators table
        :param creator_id:
        :return:
        """
        db_result = self.get("SELECT creator_sort FROM creators WHERE creator_id=?", (creator_id,))
        try:
            return db_result[0][0]
        except IndexError:
            return None

    def get_creator_link(self, creator_id):
        """
        Returns the creator link value for a given id.
        :param creator_id:
        :return:
        """
        db_result = self.get("SELECT creator_link FROM creators WHERE creator_id=?", (creator_id,))
        try:
            return db_result[0][0]
        except IndexError:
            return None

    # Todo: Add the single option - bring into line with the naming scheme
    def update_creator_links(self, values):
        """
        Update the creator links for multiple creators.
        :param values: Iterable of tuples - the first element being the id of the creator and the second element being
                       the new creator links
        :return:
        """
        stmt = "UPDATE creators SET creator_link=? WHERE creator_id=?"
        self.executemany(stmt, values)

    # Todo: Bring into line with the rest by offering a singular and multiple update options
    def update_creator_sorts(self, values):
        """
        Update the creator sorts for multiple individual creators.
        :param values: Iterable of tuples - the first element being the new creator sort and the second element being
                       the creator id to set it for
        :return:
        """
        stmt = "UPDATE creators SET creator_sort=? WHERE creator_id=?"
        self.executemany(stmt, values)

    #
    # ------------------------------------------------------------------------------------------------------------------


    # Todo: Not actually file macros..
    # - FILE MACROS
    def read_creator_with_sort_and_link(self):
        """
        Returns an iterable of types of the form (creator_id, creator, creator_sort, creator_link) from the creators
        table.
        :return:
        """
        stmt = "SELECT creator_id, creator, creator_sort, creator_link FROM creators;"
        return self.execute(stmt)
