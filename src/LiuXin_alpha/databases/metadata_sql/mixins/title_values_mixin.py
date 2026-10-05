"""
Provide metadata SQL operations for title values.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""

from LiuXin_alpha.errors import DatabaseIntegrityError


class TileMacrosMixin:
    """
    Implement the title values operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.set_title_identifier(1, "isbn", "9780306406157")  # doctest: +SKIP
    """


    def set_title_identifier(self, title_id, id_type, id_val):
        """
        Set a primary Work identifier through portable macros in a transaction.

        Requires a nonempty string scheme and a string or false value. For the umbrella isbn
        scheme, strips separators only to count 10/13 digit-or-X characters and select
        isbn10/isbn13 storage; the original value is stored without normalization or
        checksum validation. False isbn clears both concrete and underscore alias schemes.

        Requires the Work to exist, otherwise DatabaseIntegrityError. False values delete
        matching rows; nonempty values demote all matches and promote the first exact-value
        match by ascending ID, or insert a new primary row. Other values remain as
        nonprimary identifiers. Catalog policy-aware callers should use
        IdentifierRepository.

        Example:
            >>> metadata_sql.set_title_identifier(1, "isbn", "9780306406157")  # doctest: +SKIP


        :param title_id: Work ID exposed through the compatibility titles surface.
        :param id_type: Nonempty scheme string; casefolded/compacted isbn selects
            compatibility handling, other spelling is stored unchanged.
        :param id_val: Unnormalized identifier string, or a false value to clear matching
            schemes.
        :return: None.
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
        Delegate ISBN setting or clearing to set_title_identifier().

        Retains its scheme selection, transaction, exact-value matching and validation
        behavior.

        Example:
            >>> metadata_sql.set_title_isbn(1, "9780306406157")  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param isbn: ISBN text or a false value to clear both ISBN schemes.
        :return: None.
        """
        self.set_title_identifier(title_id=title_id, id_type="isbn", id_val=isbn)

    def set_title_rating(self, title_id, rating):
        """
        Replace user-type rating links using rating-row ID int(rating)+1.

        Deletes existing user links first. A false rating returns before committing; truthy
        values are converted without checking a 0–10 range, inserted and committed.
        Conversion/insertion errors can leave the deletion pending.

        Example:
            >>> metadata_sql.set_title_rating(1, 4)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param rating: False value to clear user links; otherwise int(rating)+1 selects the
            rating row, without range validation.
        :return: None.
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
        Write title_creator_sort directly on the live connection and commit.

        Example:
            >>> metadata_sql.set_author_sort(1, "Doe, Jane")  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param sort: Replacement title_creator_sort value.
        :return: None.
        """
        self.db.driver.conn.execute("UPDATE titles SET title_creator_sort=? WHERE title_id=?;", (sort, title_id))
        self.db.driver.conn.commit()
