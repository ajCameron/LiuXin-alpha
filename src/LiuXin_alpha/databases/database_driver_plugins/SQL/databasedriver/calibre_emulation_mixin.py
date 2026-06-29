
"""
calibre emulation add ins.

Some of the calibre emulation methods require direct database access.
"""

import datetime

from LiuXin_alpha.utils.date import utcfromtimestamp


class CalibreEmulationMixin:
    """
    Emulation methods to bring the driver more in line with calibre.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CALIBRE EMULATION FUNCTIONS START HERE

    def direct_last_modified(self) -> datetime.datetime:
        """
        Returns the last modification time for the databases as a utc (unix time code) timestamp.

        :return:
        """
        import os

        return utcfromtimestamp(os.stat(self.database_path).st_mtime)


