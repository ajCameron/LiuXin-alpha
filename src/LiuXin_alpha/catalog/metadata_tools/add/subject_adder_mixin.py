"""
Insert a Subject with optional sort text and parent Row.
"""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.metadata.standardization import make_title_search_term
from LiuXin_alpha.utils.logging import default_log


class SubjectAdderMixin:
    """
    Supply subject creation to a legacy Add host.

    The host provides the database and any peers required by the method.
    Validation and synchronization failures propagate to the caller.

    Example:
        A parent Row contributes row_id; no separate intralink is created.
    """

    def subject(self, subject, subject_sort=None, subject_parent=None):
        """
        Insert a Subject with optional sort text and parent Row.

        Example:
            A parent Row contributes row_id; no separate intralink is created.


        :param subject: Subject text stored unchanged.
        :param subject_sort: Sort text; None uses make_title_search_term(subject).
        :param subject_parent: Concrete parent Row, or None.
        :return: Created database Row; synchronization and schema errors propagate.
        :raises InputIntegrityError: A non-None parent is not a concrete Row.
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
            raise InputIntegrityError(err_str)

        subject_row.sync()
        return subject_row
