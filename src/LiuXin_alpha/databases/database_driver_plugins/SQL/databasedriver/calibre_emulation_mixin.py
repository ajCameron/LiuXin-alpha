
"""
Expose filesystem modification time using Calibre-compatible UTC datetime conversion.
"""

import datetime

from LiuXin_alpha.utils.date import utcfromtimestamp


class CalibreEmulationMixin:
    """
    Provide file metadata expected by Calibre-facing driver callers.

    Example:
        A file-backed driver supplies ``database_path`` for this mixin.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CALIBRE EMULATION FUNCTIONS START HERE

    def direct_last_modified(self) -> datetime.datetime:
        """
        Read the database file mtime and convert it to a UTC datetime.

        Filesystem errors propagate; no database connection is opened.

        Example:
            ``driver.direct_last_modified()`` reports the database file modification time.


        :return: The UTC datetime produced by ``utcfromtimestamp``.
        """
        import os

        return utcfromtimestamp(os.stat(self.database_path).st_mtime)


