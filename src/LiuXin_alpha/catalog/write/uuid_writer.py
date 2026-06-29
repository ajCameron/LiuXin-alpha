
"""
Writer for writing UUIDs out to the database.
"""



from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from LiuXin_alpha.catalog.write import OneToOneWriter


class UUIDWriter(OneToOneWriter):
    def __init__(self, field):
        super(UUIDWriter, self).__init__(field)
        self.set_books_func = self.set_uuid

    def set_uuid(self, book_id_val_map, db, field, *args):
        """
        Update the uuid for the book.

        :param book_id_val_map: Keyed with the id of the book and valued with the new uuid value
        :param db: The database to preform the update in
        :param field: In memory field representing data from the database
        :param args:
        :return:
        """
        # Todo: This should not have to happen here
        # Update the cache
        field.table.update_uuid_cache(book_id_val_map)

        # Update the database through the uuid field
        return self.one_one_in_books(book_id_val_map, db, field, *args)
