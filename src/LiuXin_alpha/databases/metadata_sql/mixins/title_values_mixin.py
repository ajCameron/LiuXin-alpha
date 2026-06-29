

class TileMacrosMixin:


    def set_title_identifier(self, title_id, id_type, id_val):
        """
        Set an identifier linked to a title - the identifier will be set as the primary of that type linked to the
        title.
        :param db: The database to write the changes into
        :param title_id: The id of the title to link the identifier to
        :param id_type: The identifier type (e.g. isbn, e.t.c)
        :param id_val: The identifier will be set to this.
                       No normalization is done - this is supposed to be the name of a valid identifier type which is
                       known to the database.
        :return:
        """
        if id_val:
            # The database has to be updated
            # Todo: Should be using the ensure.identifier method instead of add
            try:
                ident_row = self.db.add.identifier(identifier=id_val, identifier_type=id_type)
            except DatabaseIntegrityError:
                # Identifier already exists - retrieve the row and promote it to the highest priority
                ident_row = self.db.ensure.identifier(identifier=id_val, identifier_type=id_type, error=False)
                ident_id = ident_row["identifier_id"]

                # Check to see if there is already a link between the identifier and the title
                stmt = (
                    "SELECT identifier_title_link_id "
                    "FROM identifier_title_links "
                    "WHERE identifier_title_link_title_id = ? AND identifier_title_link_identifier_id = ?;"
                )
                it_status = self.db.driver.conn.get(stmt, (title_id, ident_id), all=False)

                # The link exists - it just needs to be promoted to the top of the stack
                if it_status:
                    # Retrieve the row
                    it_link = self.db.get_row_from_id("identifier_title_links", it_status)
                    # Maximize the priority
                    it_link["identifier_title_link_priority"] = self.db.get_max("identifier_title_link_priority") + 1
                    it_link.sync()
                else:
                    raise DatabaseIntegrityError("Cannot link to this identifier - it's linked to another title")

            else:
                title_row = self.db.get_row_from_id(table="titles", row_id=title_id)
                self.db.apply.identifier(resource_row=title_row, identifier=ident_row, identifier_type="isbn")

        else:
            # isbn has been passed in as none - wipe all the identifiers of that type linked to the title
            # Foreign keys should also take out the entries on the identifiers table itself
            stmt = (
                "DELETE FROM identifier_title_links "
                "WHERE identifier_title_link_title_id = ? AND identifier_title_link_type = ?;"
            )
            self.db.driver.conn.execute(stmt, (title_id, id_type))
            self.db.driver.conn.commit()

    def set_title_isbn(self, title_id, isbn):
        """
        Set a isbn in the identifiers table for a particular title.
        :param title_id: The id of the book to update.
        :param isbn: The isbn of the book to update.
        :return:
        """
        self.set_title_identifier(title_id=title_id, id_type="isbn", id_val=isbn)

    def set_title_rating(self, title_id, rating):
        """
        Sets the user_rating for the given id - which is the one used in the meta view.
        The rating table should have already been set up by this point - just calculating the appropriate row_id and
        writing it into the ratings table.
        Updates the database - does not update the cache.
        :param db: The database to do the update on
        :param title_id: The id of the book to set the rating for
        :param rating: An integer in the range 0-10.
                       If the integer is 0 - or if the rating evaluates to 0, the rating will be set Null.
        :type rating: int
        :return:
        """
        # Clear the ratings table of any current user_ratings for the title
        self.db.driver.conn.execute(
            "DELETE FROM rating_title_links "
            "WHERE rating_title_link_type = 'user'"
            "AND rating_title_link_title_id = ?;",
            (title_id,),
        )

        if not rating:
            return
        rating = int(rating) + 1

        rat_row_id = rating
        self.db.driver.conn.execute(
            "INSERT INTO rating_title_links "
            "(rating_title_link_title_id, rating_title_link_rating_id, rating_title_link_type) "
            "VALUES (?,?,?);",
            (title_id, rat_row_id, "user"),
        )
        self.db.driver.conn.commit()

    def set_author_sort(self, title_id, sort):
        """
        Set the author sort for a given title id.
        :param title_id:
        :param sort:
        :return:
        """
        self.db.driver.conn.execute("UPDATE titles SET title_creator_sort=? WHERE title_id=?;", (sort, title_id))
        self.db.driver.conn.commit()