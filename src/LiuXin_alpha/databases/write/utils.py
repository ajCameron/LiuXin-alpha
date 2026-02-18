from __future__ import division, absolute_import, print_function, unicode_literals


class DummyWriter:
    """
    Dummy for when you don't want have to set up an actual functional writer.
    """
    def __init__(self, field) -> None:
        self.field = field
        self.set_books_func = self.dummy

    @staticmethod
    def dummy(book_id_val_map, *args):
        return set()

    def set_books(self, book_id_val_map, db, allow_case_change=True, error=False):
        raise NotImplementedError("writer is not available for this field")

    def set_books_for_enum(self, book_id_val_map, db, field, allow_case_change):
        raise NotImplementedError("writer is not available for this field")


class UpdateDict(dict):
    """
    Designed to hold updates to the database in dictionary form - with some additional attributes (such as have they
    been checked).
    """

    def __init__(self, *args, **kwargs):
        super(UpdateDict, self).__init__(*args, **kwargs)

        self.checked = False
