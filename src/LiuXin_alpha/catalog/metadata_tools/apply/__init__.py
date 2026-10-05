
"""
Link legacy metadata Rows to resources with method-specific recovery policies.
"""

from LiuXin_alpha.databases.api import DatabaseAPI

from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.metadata.ebook_metadata_tools import check_issn, check_isbn

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types as string_types

from LiuXin_alpha.catalog.metadata_tools.apply.label_apply_mixin import LabelApplyMixin


class Apply(LabelApplyMixin):
    """
    Group row-link helpers with caller-wired Add and Ensure peers.

    Several methods insert/resolve metadata before validating a resource, and
    some recover integrity errors by deleting/replacing links. No common
    transaction wraps these operations. Return values are usually link Rows.

    Example:
        After Catalog composition, resolve metadata through catalog.apply and
        retain returned link Rows when relationship attributes are needed.
    """

    def __init__(self, database: DatabaseAPI) -> None:
        """
        Retain the database and leave Add/Ensure peers for composition.

        Example:
            A standalone Apply must have its peers wired before methods resolve text.


        :param database: Borrowed database handle; not opened or validated.
        :return: None; assigns db and sets add/ensure to None.
        """

        self.db = database
        self.add = None
        self.ensure = None

    # Todo: This takes a single object - so should be comment
    # Todo: Standardize resource_row to resource
    # Todo: And singular to plural
    def comments(self, comment, resource_row):
        """
        Create or reuse a Comment Row and link it to a resource.

        Require a discoverable link table, then interlink with the resource as primary.
        No enclosing transaction or duplicate-error recovery is provided.

        Example:
            Text creation happens before link-table validation; a later failure can leave the new Row.


        :param comment: Concrete Row or text passed to the wired Add helper.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Input type or resource relationship is unsupported.
        """
        if isinstance(comment, Row):
            synopsis_row = comment
        elif isinstance(comment, string_types):
            synopsis_row = self.add.comment(comment)
        else:
            err_str = "Unable to add comment - type not recognized"
            err_str = default_log.log_variables(err_str, "ERROR", ("comment", comment), ("comment_type", type(comment)))
            raise InputIntegrityError(err_str)

        resource_table = resource_row.table
        interlink_table = self.db.driver_wrapper.get_link_table_name("comments", resource_table)
        if interlink_table is None:
            err_str = (
                "Comment cannot be interlinked with resource - "
                "that resource_row cannot have comments associated with it"
            )
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource_table", resource_table),
                ("resource_row", resource_row),
            )
            raise InputIntegrityError(err_str)

        link_row = self.db.interlink_rows(primary_row=resource_row, secondary_row=synopsis_row)
        return link_row

    def cover(self, cover, resource_row):
        """
        Link an existing concrete Cover Row to a resource.

        Example:
            No cover file is read or created; this operation only links metadata Rows.


        :param cover: Concrete Row accepted without checking its table locally.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: The value is not a Row or no cover link table is found.
        """
        if isinstance(cover, Row):
            cover_row = cover
        else:
            err_str = "Unable to add cover - type not recognized"
            err_str = default_log.log_variables(err_str, "ERROR", ("cover", cover), ("type(cover)", type(cover)))
            raise InputIntegrityError(err_str)

        resource_table = resource_row.table
        interlink_table = self.db.driver_wrapper.get_link_table_name("covers", resource_table)
        if interlink_table is None:
            err_str = (
                "Cover cannot be interlinked with resource - " "that resource_row cannot have covers associated with it"
            )
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource_table", resource_table),
                ("resource_row", resource_row),
            )
            raise InputIntegrityError(err_str)

        link_row = self.db.interlink_rows(primary_row=resource_row, secondary_row=cover_row)
        return link_row

    def creator(
        self,
        resource_row,
        creator_row,
        creator_role="authors",
        creator_priority="highest",
    ):
        """
        Credit a supplied Creator Row on a compatible resource.

        No Creator type or role validation is added by this wrapper.

        Example:
            The legacy creators-to-resource route must exist before linking.


        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param creator_row: Creator Row passed unchanged.
        :param creator_role: Role forwarded as the link type.
        :param creator_priority: Priority forwarded unchanged, default highest.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: The resource has no discoverable Creator link table.
        """
        resource_table = resource_row.table
        interlink_table = self.db.driver_wrapper.get_link_table_name("creators", resource_table)
        if interlink_table is None:
            err_str = (
                "Resource and creator cannot be interlinked - " "that resource cannot have creators associated with it"
            )
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource_table", resource_table),
                ("resource_row", resource_row),
            )
            raise InputIntegrityError(err_str)

        link_row = self.db.interlink_rows(
            primary_row=resource_row,
            secondary_row=creator_row,
            priority=creator_priority,
            type=creator_role,
        )
        return link_row

    def genre(self, resource_row, genre, genre_priority="highest"):
        """
        Resolve a Genre after checking its resource link route.

        Example:
            An unsupported resource fails before text is ensured.


        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param genre: Concrete Row, or text resolved by ensure.genre.
        :param genre_priority: Priority forwarded unchanged.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Link route or Genre input type is unsupported.
        """
        resource_table = resource_row.table
        interlink_table = self.db.driver_wrapper.get_link_table_name("genres", resource_table)
        if interlink_table is None:
            err_str = "Resource and genre cannot be interlinked"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource_table", resource_table),
                ("resource_row", resource_row),
                ("genre", genre),
            )
            raise InputIntegrityError(err_str)

        if isinstance(genre, Row):
            genre_row = genre
        elif isinstance(genre, string_types):
            genre_row = self.ensure.genre(genre)
        else:
            err_str = "Genre cannot be parsed - type not recognized"
            err_str = default_log.log_variables(err_str, "ERROR", ("resource_table", resource_table), ("genre", genre))
            raise InputIntegrityError(err_str)

        link_row = self.db.interlink_rows(primary_row=resource_row, secondary_row=genre_row, priority=genre_priority)
        return link_row

    def identifier(
        self,
        resource_row,
        identifier,
        identifier_type,
        identifier_priority="highest",
        validate_id=True,
    ):
        """
        Validate selected identifier schemes and link a resolved Identifier.

        Local validation lowercases the scheme without stripping and checks only
        isbn/issn. Link-table discovery precedes resolution; a Row bypasses text
        validation. No enclosing transaction is opened.

        Example:
            Setting validate_id=False does not disable Ensure.identifier's scheme validation.


        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param identifier: Concrete Row or text delegated to add.identifier.
        :param identifier_type: Scheme forwarded as link type and to identifier creation.
        :param identifier_priority: Priority forwarded unchanged.
        :param validate_id: Enable the local ISBN/ISSN precheck for text; downstream validation still applies.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Local validation, input type or link route is unsupported.
        """
        if validate_id and isinstance(identifier, string_types):
            if identifier_type.lower() == "issn":
                if check_issn(identifier):
                    pass
                else:
                    raise InputIntegrityError(
                        "Bad identifier passed to apply.identifier\nid_type:{}\nidentifier:{}"
                        "".format(identifier_type, identifier)
                    )
            elif identifier_type.lower() == "isbn":
                if check_isbn(identifier):
                    pass
                else:
                    raise InputIntegrityError(
                        "Bad identifier passed to apply.identifier\nid_type:{}\nidentifier:{}"
                        "".format(identifier_type, identifier)
                    )

        resource_table = resource_row.table
        interlink_table = self.db.driver_wrapper.get_link_table_name("identifiers", resource_table)
        if interlink_table is None:
            err_str = (
                "Resource and identifier cannot be interlinked - "
                "that resource type cannot have identifiers associated with it"
            )
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource_table", resource_table),
                ("identifier", identifier),
            )
            raise InputIntegrityError(err_str)

        if isinstance(identifier, Row):
            identifier_row = identifier
        elif isinstance(identifier, string_types):
            identifier_row = self.add.identifier(identifier=identifier, identifier_type=identifier_type)
        else:
            err_str = "Identifier cannot be parsed - type not recognized"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource_row", resource_row),
                ("identifier", identifier),
                ("identifier_type", identifier_type),
            )
            raise InputIntegrityError(err_str)

        link_row = self.db.interlink_rows(
            primary_row=resource_row,
            secondary_row=identifier_row,
            priority=identifier_priority,
            type=identifier_type,
        )
        return link_row

    # ------------------------------------------------------------------------------------------------------------------
    # - LANGUAGE METHODS
    # Todo: Also want to ship with a language table
    def language(self, language, resource_row, link_type=None):
        """
        Resolve a Language and replace its link after an integrity error.

        The unlink/retry sequence has no enclosing transaction; retry failure may
        leave the original relation removed.

        Example:
            A DatabaseIntegrityError causes unlink_interlink followed by one retry.


        :param language: Concrete Row or name text delegated to ensure.language.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param link_type: Relationship type forwarded unchanged.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Language input is neither a Row nor text.
        """
        if isinstance(language, string_types):
            language_row = self.ensure.language(language_string=language)
        elif isinstance(language, Row):
            language_row = language
        else:
            err_str = "Resource cannot be linked to a language - that resource_row cannot have languages linked to it"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("language", language),
                ("language_type", type(language)),
            )
            raise InputIntegrityError(err_str)

        try:
            link_row = self.db.interlink_rows(primary_row=resource_row, secondary_row=language_row, type=link_type)
        except DatabaseIntegrityError:
            # The title is probably already linked to a language - delete that link and try again
            self.db.unlink_interlink(primary_row=resource_row, secondary_row=language_row)
            link_row = self.db.interlink_rows(primary_row=resource_row, secondary_row=language_row, type=link_type)

        return link_row

    # Todo: It would seem all of these need, at least, a basic function test
    def contained_language(self, language, title_row):
        """
        Link a Language with the contained_in relationship type.

        Example:
            Duplicate/integrity errors are suppressed without checking their cause.


        :param language: Concrete Language Row; enforced with an assertion.
        :param title_row: Title Row used as primary endpoint.
        :return: None; ignores DatabaseIntegrityError from linking.
        :raises AssertionError: Language is not a concrete Row, when assertions are enabled.
        """
        assert isinstance(language, Row), "must pass the language in the form of a row - type(language): {}" "".format(
            type(language)
        )

        try:
            self.db.interlink_rows(primary_row=title_row, secondary_row=language, type="contained_in")
        except DatabaseIntegrityError:
            pass

    def available_language(self, language, title_row):
        """
        Link a Language with the available_language relationship type.

        Example:
            Duplicate/integrity errors are suppressed without checking their cause.


        :param language: Concrete Language Row; enforced with an assertion.
        :param title_row: Title Row used as primary endpoint.
        :return: None; ignores DatabaseIntegrityError from linking.
        :raises AssertionError: Language is not a concrete Row, when assertions are enabled.
        """
        assert isinstance(language, Row), "must pass the language in the form of a row"

        try:
            self.db.interlink_rows(primary_row=title_row, secondary_row=language, type="available_language")
        except DatabaseIntegrityError:
            pass

    def primary_language(self, language, title_row):
        """
        Set a legacy title's primary Language through metadata_sql.

        Example:
            This method does not return an interlink Row or suppress SQL failures.


        :param language: Concrete Language Row, asserted before use.
        :param title_row: Title Row whose row_id identifies the title.
        :return: None; forwards title and Language row IDs.
        :raises AssertionError: Language is not a concrete Row, when assertions are enabled.
        """
        assert isinstance(language, Row), "must pass the language in the form of a row"

        self.db.metadata_sql.set_title_primary_language(title_id=title_row.row_id, lang_id=language.row_id)

    # ------------------------------------------------------------------------------------------------------------------

    # Todo - :param note_type: What type of note is being applied? Options include bio, note & synopsis
    def note(self, note, resource):
        """
        Create or reuse a Note, then link it as the primary endpoint.

        Text insertion can precede a resource type/link-table failure. No enclosing
        transaction protects the aggregate.

        Example:
            The Note is the primary link endpoint; highest priority is requested.


        :param note: Concrete Row or text inserted by add.note.
        :param resource: Concrete resource Row, validated after Note resolution.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Note/resource type or link route is unsupported.
        """
        if isinstance(note, Row):
            note_row = note
        elif isinstance(note, string_types):
            note_row = self.add.note(note=note)
        else:
            err_str = "Note must be a string or row"
            err_str = default_log.log_variables(err_str, "ERROR", ("note", note), ("note_type", type(note)))
            raise InputIntegrityError(err_str)

        if not isinstance(resource, Row):
            err_str = "Resource must be a note"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource", resource),
                ("resource_type", type(resource)),
            )
            raise InputIntegrityError(err_str)

        interlink_table = self.db.driver_wrapper.get_link_table_name("notes", resource.table)
        if not interlink_table:
            err_str = "Resource cannot be noted - no link table exists between them"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource", resource),
                ("note_row", note_row),
                ("note", note),
            )
            raise InputIntegrityError(err_str)

        link_row = self.db.interlink_rows(primary_row=note_row, secondary_row=resource, priority="highest")
        return link_row

    # Todo: Not thread safe - needs upgrading (add autoincrement?)
    # Todo: title_row should be resource row? For consistency.
    def publisher(self, publisher, title_row):
        """
        Resolve a Publisher and link it, promoting an existing legacy link on error.

        Recovery assumes publisher_title_link_priority and raises it above the global
        maximum. A failed lookup or conversion during recovery propagates after
        earlier resolution; no enclosing transaction is opened.

        Example:
            Any DatabaseIntegrityError triggers an existing-link lookup and priority update.


        :param publisher: Concrete Row or text ensured without standardization.
        :param title_row: Title Row used as secondary endpoint.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Publisher input has an unsupported type.
        """
        if isinstance(publisher, Row):
            publisher_row = publisher
        elif isinstance(publisher, string_types):
            publisher_row = self.ensure.publisher(publisher=publisher, standardize=False)
        else:
            raise InputIntegrityError("publisher type not recognized")

        try:
            link_row = self.db.interlink_rows(primary_row=publisher_row, secondary_row=title_row)
        except DatabaseIntegrityError:
            link_row = self.db.get_interlink_row(primary_row=publisher_row, secondary_row=title_row)
            link_priority_max = int(self.db.get_max("publisher_title_link_priority")) + 1
            link_row["publisher_title_link_priority"] = link_priority_max
            link_row.sync()
        return link_row

    def rating(self, rating, rating_type, resource_row):
        """
        Resolve a Rating and replace same-type links after an integrity error.

        Requests priority=not_set. Recovery is not wrapped in a transaction, so a
        retry failure can leave existing same-type links removed.

        Example:
            On DatabaseIntegrityError, unlink all Ratings of rating_type and retry once.


        :param rating: Concrete Row or numeric value; floats truncate to int and bool is accepted.
        :param rating_type: Link type used in creation and recovery deletion.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Rating input or link route is unsupported.
        """
        # Apply a rating to the given resource
        if isinstance(rating, Row):
            rating_row = rating
        elif isinstance(rating, int):
            rating_row = self.ensure.rating(rating)
        elif isinstance(rating, float):
            rating_row = self.ensure.rating(int(rating))
        else:
            err_str = "Unable to add rating - type not recognized"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("rating", rating),
                ("rating_type", rating_type),
                ("resource_row", resource_row),
            )
            raise InputIntegrityError(err_str)

        # Todo: This check should be superfluous - the interlink rows method should throw an error - catch and handle it
        resource_table = resource_row.table
        interlink_table = self.db.driver_wrapper.get_link_table_name("ratings", resource_table)
        if interlink_table is None:
            err_str = "Object cannot be rated - ratings cannot be linked to this resource"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("rating", rating),
                ("rating_type", rating_type),
                ("resource_row", resource_row),
            )
            raise InputIntegrityError(err_str)

        try:
            link_row = self.db.interlink_rows(
                primary_row=resource_row,
                secondary_row=rating_row,
                type=rating_type,
                priority="not_set",
            )
        except DatabaseIntegrityError:
            # Break the old link and make a new one
            self.db.unlink_all(
                primary_row=resource_row,
                secondary_table="ratings",
                type_filter=rating_type,
            )  #
            link_row = self.db.interlink_rows(
                primary_row=resource_row,
                secondary_row=rating_row,
                type=rating_type,
                priority="not_set",
            )

        return link_row

    def series(self, series, series_index, resource_row, stand=True):
        """
        Resolve a Series and attach its index to the resource link.

        Example:
            Unpack ``link_row, series_row``; the first result carries the relationship index.


        :param series: Concrete Row or text resolved by ensure.series_blind.
        :param series_index: Index value forwarded unchanged to interlink_rows.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param stand: Standardization preference passed only when resolving text.
        :return: Tuple (link Row, Series Row), in that order.
        :raises InputIntegrityError: Series input is neither Row nor text.
        """
        # If the series is a string then assume it's a name for a series and ensure it
        if isinstance(series, Row):
            series_row = series
        elif isinstance(series, string_types):
            series_row = self.ensure.series_blind(creator_rows=[], series_name=series, stand=stand)
        else:
            err_str = "Unable to add series - type not recognized"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("series", series),
                ("series_index", series_index),
                ("resource_row", resource_row),
            )
            raise InputIntegrityError(err_str)

        # Link the series to the resource with the given index
        link_row = self.db.interlink_rows(primary_row=resource_row, secondary_row=series_row, index=series_index)

        return link_row, series_row

    # Todo: standarize resource_row to resource
    def subject(self, subject, resource_row, stand=True):
        """
        Resolve a Subject and link it to the resource.

        Example:
            The current Ensure implementation searches exact string text regardless of stand.


        :param subject: Concrete Row or text delegated to ensure.subject.
        :param resource_row: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :param stand: Forwarded to Ensure.subject, whose current implementation ignores it.
        :return: None; the interlink result is discarded.
        :raises InputIntegrityError: Subject input is neither Row nor text.
        """
        # If the subject is a string then assume it's a name for a subject and ensure that it exists - then link it to
        # the given resource row
        if isinstance(subject, Row):
            subject_row = subject
        elif isinstance(subject, string_types):
            subject_row = self.ensure.subject(subject=subject, standardize=stand)
        else:
            err_str = "Unable to add subject - type not recognized"
            err_str = default_log.log_variables(err_str, "ERROR", ("subject", subject), ("resource_row", resource_row))
            raise InputIntegrityError(err_str)

        # Link the subject to the resource
        self.db.interlink_rows(primary_row=resource_row, secondary_row=subject_row)

    def synopsis(self, synopsis, resource):
        """
        Create or reuse a Synopsis Row and link it to a resource.

        Require a discoverable link table, then interlink with the resource as primary.
        No enclosing transaction or duplicate-error recovery is provided.

        Example:
            Text creation happens before link-table validation; a later failure can leave the new Row.


        :param synopsis: Concrete Row or text passed to the wired Add helper.
        :param resource: Resource Row used as a link endpoint; table compatibility is checked by the database.
        :return: Database link Row, not the attached metadata Row.
        :raises InputIntegrityError: Input type or resource relationship is unsupported.
        """
        if isinstance(synopsis, Row):
            synopsis_row = synopsis
        elif isinstance(synopsis, string_types):
            synopsis_row = self.add.synopsis(synopsis)
        else:
            err_str = "Unable to add synopsis - type not recognized"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("synopsis", synopsis),
                ("synopsis_type", type(synopsis)),
            )
            raise InputIntegrityError(err_str)

        resource_table = resource.table
        interlink_table = self.db.driver_wrapper.get_link_table_name("synopses", resource_table)
        if interlink_table is None:
            err_str = (
                "Synopsis cannot be interlinked with resource - "
                "that resource_row cannot have synopses associated with it"
            )
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource_table", resource_table),
                ("resource_row", resource),
            )
            raise InputIntegrityError(err_str)

        link_row = self.db.interlink_rows(primary_row=resource, secondary_row=synopsis_row)
        return link_row
