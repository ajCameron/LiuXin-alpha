from LiuXin.databases.row import Row

from LiuXin.exceptions import DatabaseIntegrityError
from LiuXin.exceptions import InputIntegrityError

from LiuXin.metadata.ebook_metadata_tools import check_issn
from LiuXin.metadata.ebook_metadata_tools import check_isbn

from LiuXin.utils.logger import default_log
from six import string_types


class Apply(object):
    """
    Class to associate resources on the database together by applying one to the other.
    Use methods for this class if you want to link elements from the library together - use the get methods if you want
    to retrieve associated resources - this centralization means that the schema can be more easily changed.
    """

    def __init__(self, database):
        self.db = database
        self.add = None
        self.ensure = None

    # Todo: This takes a single object - so should be comment
    # Todo: Standardize resource_row to resource
    # Todo: And singular to plural
    def comments(self, comment, resource_row):
        """
        Apply a comment to a resource_row.
        :param comment:
        :param resource_row:
        :return:
        """
        if isinstance(comment, Row):
            synopsis_row = comment
        elif isinstance(comment, string_types):
            synopsis_row = self.add.comment(comment)
        else:
            err_str = "Unable to add comment - type not recognized"
            err_str = default_log.log_variables(err_str, "ERROR", ("comment", comment), ("comment_type", type(comment)))
            raise NotImplementedError(err_str)

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
        Apply a cover to a resource row.
        :param cover:
        :param resource_row:
        :return:
        """
        if isinstance(cover, Row):
            cover_row = cover
        else:
            err_str = "Unable to add cover - type not recognized"
            err_str = default_log.log_variables(err_str, "ERROR", ("cover", cover), ("type(cover)", type(cover)))
            raise NotImplementedError(err_str)

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
        Associate a creator with a work, with the role they played in that work's creation.
        :param resource_row: Something which can be associated with a creator
        :param creator_row: The creator row associated with the creator
        :param creator_role: What role did the creator play in the creation of this work?
        :param creator_priority: What priority should the creator have when added to the work?
        :return:
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
        Associate a genre with a resource (currently genres can only be associated with titles).
        :param resource_row: The resource to associate the genre with
        :param genre: The genre to associate with the resource (can be a string or a Row)
        :param genre_priority: The priority to associate the genre with
        :return:
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
            raise NotImplementedError(err_str)

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
        Apply an identifier row to a resource row.
        :param resource_row: The resource to appky the identifier to
        :param identifier: The identifier row
        :param identifier_type: 'isbn' e.t.c.
        :param identifier_priority: The priority to apply the identifier to the resource with (defaults to highest)
        :return:
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
            raise NotImplementedError(err_str)

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
        Apply a language to a given resource.
        Titles can have one and only one language - if the language can't be applied because the title already has one
        then delete that link and insert a new one for the new language.
        :param language: The language to be applied
        :param resource_row: The resource_row to apply the language to.
        :param link_type:
        :return:
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
            raise NotImplementedError(err_str)

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
        Note that a language is contained in the title.
        :param language:
        :param title_row:
        :return:
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
        Note that the language is available for a title (most used for the alternative language tracks of DVDs e.t.c)
        :param language:
        :param title_row:
        :return:
        """
        assert isinstance(language, Row), "must pass the language in the form of a row"

        try:
            self.db.interlink_rows(primary_row=title_row, secondary_row=language, type="available_language")
        except DatabaseIntegrityError:
            pass

    def primary_language(self, language, title_row):
        """
        Set the primary language of a title. If a primary language has already been set the update it to this row.
        :param language: The language to be set as primary
        :param title_row: Set that language row as the primary for the title
        :return:
        """
        assert isinstance(language, Row), "must pass the language in the form of a row"

        self.db.macros.set_title_primary_language(title_id=title_row.row_id, lang_id=language.row_id)

    # ------------------------------------------------------------------------------------------------------------------

    # Todo - :param note_type: What type of note is being applied? Options include bio, note & synopsis
    def note(self, note, resource):
        """
        Apply a note to a given resource.
        :param note: The text of the note to apply to the resource.
        :param resource: The thing to apply the note too.
        :return:
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
        Apply a given publisher row to a given title row.
        If the publisher and title are already linked, then the priority of the link will be increased to maximum.
        :param publisher:
        :param title_row:
        :return:
        """
        if isinstance(publisher, Row):
            publisher_row = publisher
        elif isinstance(publisher, string_types):
            publisher_row = self.ensure.publisher(publisher=publisher, standardize=False)
        else:
            raise NotImplementedError("publisher type not recognized")

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
        Apply a rating to a resource - assumes that the rating is already between 0-10.
        :param rating: The rating to apply to the resource
        :param rating_type: The source of the rating - "amazon", "user", e.t.c
        :param resource_row: The resource to apply the rating to
        :return:
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
            raise NotImplementedError(err_str)

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
        Apply a series to a resource - with the provided series index.
        :param series:
        :param series_index:
        :param resource_row:
        :param stand: Try to standardize the series or not
        :type stand: bool
        :return link_row, series_row: The link row used to connect the series to the resource, the series that the
                                      resource has been linked to
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
            raise NotImplementedError(err_str)

        # Link the series to the resource with the given index
        link_row = self.db.interlink_rows(primary_row=resource_row, secondary_row=series_row, index=series_index)

        return link_row, series_row

    # Todo: standarize resource_row to resource
    def subject(self, subject, resource_row, stand=True):
        """
        Apply a subject to the given resource row - ensure it if required.
        :param subject:
        :param resource_row:
        :param stand:
        :return:
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
            raise NotImplementedError(err_str)

        # Link the subject to the resource
        self.db.interlink_rows(primary_row=resource_row, secondary_row=subject_row)

    def synopsis(self, synopsis, resource):
        """
        Apply a synopsis to a resource_row.
        :param synopsis: The synopsis to associate with the given resource
        :param resource: The resource row to associate a synopsis with - must be a resource that can be linked to
                             a synopsis
        :return link_row:
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
            raise NotImplementedError(err_str)

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

    def tag(self, tag, resource):
        """
        Apply a tag to the given resource.
        If the tag is a row, apply it directly. If the tag is text, then ensure the tag, and then use that row.
        :param tag: A row, string or iterable.
        :param resource: Something which can have a tag applied to it.
        :return:
        """
        if isinstance(tag, Row):
            tag_row = tag
        elif isinstance(tag, (list, set)):
            for tag_str in tag:
                self.tag(tag=tag_str, resource=resource)
            return
        elif isinstance(tag, string_types):
            tag_row = self.ensure.tag(tag_text=tag)
        else:
            err_str = "Tag must be a string or row"
            err_str = default_log.log_variables(err_str, "ERROR", ("tag", tag), ("tag_type", type(tag)))
            raise InputIntegrityError(err_str)

        if not isinstance(resource, Row):
            err_str = "Resource must be a row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource", resource),
                ("resource_type", type(resource)),
            )
            raise InputIntegrityError(err_str)

        interlink_table = self.db.driver_wrapper.get_link_table_name("tags", resource.table)
        if not interlink_table:
            err_str = "Resource cannot be tagged - no link table exists between them"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("resource", resource),
                ("tag_row", tag_row),
                ("tag", tag),
            )
            raise InputIntegrityError(err_str)

        try:
            self.db.interlink_rows(primary_row=tag_row, secondary_row=resource, priority="not_set")
        # Thrown if the tag is already applied to this row
        # Todo: Need to broaden the exception types
        except DatabaseIntegrityError:
            pass
