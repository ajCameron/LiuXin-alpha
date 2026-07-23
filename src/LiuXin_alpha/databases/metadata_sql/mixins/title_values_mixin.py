from LiuXin_alpha.errors import DatabaseIntegrityError


class TileMacrosMixin:

    def set_title_identifier(self, title_id, id_type, id_val):
        """Set the primary identifier of one scheme for a title.

        This compatibility operation writes the normalized FRBR ownership
        table through the portable database macro contract. It deliberately
        performs no value normalization; catalog-facing callers should use
        :class:`IdentifierRepository` for policy-aware writes. The legacy
        umbrella scheme ``isbn`` is translated to the concrete ISBN-10 or
        ISBN-13 storage scheme.

        :param title_id: Work ID exposed by the compatibility ``titles`` view.
        :param id_type: Non-empty identifier scheme, such as ``isbn``.
        :param id_val: Identifier value, or a false value to clear the scheme.
        :raises DatabaseIntegrityError: If ``title_id`` does not identify a
            Work.
        """

        if not isinstance(id_type, str) or not id_type.strip():
            raise TypeError("id_type must be a non-empty string")
        if id_val and not isinstance(id_val, str):
            raise TypeError("id_val must be a string or a false value")

        compact_scheme = id_type.casefold().replace("-", "").replace("_", "")
        if compact_scheme == "isbn":
            if id_val:
                compact_value = "".join(
                    character
                    for character in id_val
                    if character.isdigit() or character in "Xx"
                )
                if len(compact_value) not in (10, 13):
                    raise ValueError("isbn value must contain 10 or 13 digits")
                schemes = ("isbn{}".format(len(compact_value)),)
            else:
                schemes = ("isbn10", "isbn13", "isbn_10", "isbn_13")
        else:
            schemes = (id_type,)

        macros = self.db.macros
        with macros.transaction():
            if macros.get_row("works", title_id, id_column="work_id") is None:
                raise DatabaseIntegrityError(
                    "Cannot set an identifier for missing Work {!r}".format(title_id)
                )

            rows = tuple(
                row
                for scheme in schemes
                for row in macros.get_rows(
                    "entity_identifiers",
                    where={
                        "entity_identifier_entity_type": "work",
                        "entity_identifier_entity_id": title_id,
                        "entity_identifier_scheme": scheme,
                    },
                    order_by=("entity_identifier_id",),
                )
            )
            if not id_val:
                for row in rows:
                    macros.delete_row(
                        "entity_identifiers",
                        row["entity_identifier_id"],
                        id_column="entity_identifier_id",
                    )
                return

            selected_id = None
            for row in rows:
                identifier_id = row["entity_identifier_id"]
                if (
                    selected_id is None
                    and row["entity_identifier_value"] == id_val
                ):
                    selected_id = identifier_id
                macros.update_row(
                    "entity_identifiers",
                    identifier_id,
                    {"entity_identifier_is_primary": 0},
                    id_column="entity_identifier_id",
                )

            if selected_id is None:
                macros.insert_row(
                    "entity_identifiers",
                    {
                        "entity_identifier_entity_type": "work",
                        "entity_identifier_entity_id": title_id,
                        "entity_identifier_scheme": schemes[0],
                        "entity_identifier_value": id_val,
                        "entity_identifier_is_primary": 1,
                    },
                    id_column="entity_identifier_id",
                )
            else:
                macros.update_row(
                    "entity_identifiers",
                    selected_id,
                    {"entity_identifier_is_primary": 1},
                    id_column="entity_identifier_id",
                )

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
