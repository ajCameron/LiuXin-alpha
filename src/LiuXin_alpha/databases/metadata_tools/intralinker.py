# The corresponding class for interlinked is apply
# Todo: Update a bunch of the doc strings to actually reflect reality


class Intralinker(object):
    """
    Intralink rows on the database.
    """

    def __init__(self, database):
        self.db = database

    def creator_creator(self, primary, secondary, link_type=None):
        """
        Intralink two creators - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def cover_cover(self, primary, secondary, link_type=None):
        """
        Intralink two creators - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def file_file(self, primary, secondary, link_type=None):
        """
        Intralink two title - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def folder_store_folder_store(self, primary, secondary, link_type=None):
        """
        Intralink two title - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def identifier_identifier(self, primary, secondary, link_type=None):
        """
        Intralink two title - returns the intralink row.
        :param primary:
        :param secondary:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def tag_tag(self, primary, secondary, link_type=None):
        """
        Intralink two title - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def title_title(self, primary, secondary, link_type=None):
        """
        Intralink two title - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def publisher_publisher(self, primary, secondary, link_type=None):
        """
        Intralink two title - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row

    def generic(self, primary, secondary, link_type=None):
        """
        Intralink two title - returns the intralink row.
        :param primary:
        :param secondary:
        :param priority:
        :param link_type:
        :return:
        """
        row = self.db.intralink_rows(primary_row=primary, secondary_row=secondary, link_type=link_type)
        return row
