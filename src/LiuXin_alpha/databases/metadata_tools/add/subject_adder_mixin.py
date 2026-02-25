from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.standardization import make_title_search_term
from LiuXin_alpha.utils.logging import default_log


class SubjectAdderMixin:
    """
    Add methods for rows in the ``subjects`` table.
    """

    def subject(self, subject, subject_sort=None, subject_parent=None):
        """
        Create a subject row.
        """
        subject_row = Row(database=self.db)

        subject_row["subject"] = subject
        subject_row["subject_sort"] = subject_sort if subject_sort is not None else make_title_search_term(subject)

        if subject_parent is None:
            subject_row["subject_parent"] = None
        elif subject_parent is not None and isinstance(subject_parent, Row):
            subject_row["subject_parent"] = subject_parent.row_id
        else:
            err_str = "Unable to parse subject_parent - expected a Row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("subject_parent", subject_parent),
                ("subject_parent_type", type(subject_parent)),
            )
            raise NotImplementedError(err_str)

        subject_row.sync()
        return subject_row
