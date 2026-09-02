"""Genre creation and linking workflows for metadata tools."""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.utils.date import utcnow
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class GenreAdderMixin:
    """
    Add methods for rows in the ``genres`` table.
    """

    def genre(
        self,
        genre,
        genre_sort=None,
        genre_phash=None,
        genre_parent=None,
        genre_position=None,
        genre_full=None,
        genre_datestamp=None,
    ):
        """
        Create a genre row.
        """
        genre_row = Row(database=self.db)

        genre_row["genre"] = genre
        genre_row["genre_sort"] = genre_sort
        genre_row["genre_phash"] = genre_phash

        genre_row["genre_parent"] = six_unicode(genre_parent.row_id) if genre_parent is not None else genre_parent
        genre_row["genre_position"] = genre_position
        genre_row["genre_full"] = genre_full
        genre_row["genre_datestamp"] = genre_datestamp if genre_datestamp is not None else utcnow()

        genre_row.sync()
        return genre_row
