from __future__ import unicode_literals

from six import string_types

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.metadata.standardization import make_series_phash
from LiuXin_alpha.metadata.utils import title_sort as generate_title_sort
from LiuXin_alpha.utils.logging import default_log


class SeriesAdderMixin:
    """
    Add methods for rows in the ``series`` table.
    """

    def series(
        self,
        series,
        series_sort=None,
        series_phash=None,
        series_parent=None,
        series_parent_position=None,
        series_full=None,
        series_creator=None,
        series_note=None,
    ):
        """
        Create a series row.
        """
        series_row = Row(database=self.db)
        series_row["series"] = series
        series_row["series_sort"] = series_sort if series_sort is not None else generate_title_sort(series)
        if series_phash is None:
            if series_creator is not None:
                series_row["series_phash"] = make_series_phash(series_creator["creator"], series)
            else:
                series_row["series_phash"] = make_series_phash("", series)
        else:
            series_row["series_phash"] = series_phash

        if series_parent is None:
            series_row["series_parent"] = None
            series_row["series_parent_position"] = None
        elif isinstance(series_parent, Row):
            series_row["series_parent"] = series_parent["series_id"]
            series_row["series_parent_position"] = series_parent_position
        else:
            err_str = "Can only set the series parent with another series row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("series_parent", series_parent),
                ("series_parent_type", type(series_parent)),
            )
            raise InputIntegrityError(err_str)
        series_row["series_full"] = series_full

        series_row.sync()

        if series_creator is None:
            pass
        elif isinstance(series_creator, Row):
            self.apply.creator(resource_row=series_row, creator_row=series_creator)
        else:
            err_str = "Unable to parse series_creator value - was not a string or row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("series_creator", series_creator),
                ("series_creator_type", type(series_creator)),
            )
            raise InputIntegrityError(err_str)

        if series_note is None:
            pass
        elif isinstance(series_note, Row):
            self.apply.note(note=series_note, resource=series_row)
        elif isinstance(series_note, string_types):
            note_row = self.note(series_note)
            self.apply.note(note=note_row, resource=series_row)
        else:
            err_str = "Unable to parse series_note value - was not a string or row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("series_note", series_note),
                ("series_note_type", type(series_note)),
            )
            raise InputIntegrityError(err_str)

        return series_row
