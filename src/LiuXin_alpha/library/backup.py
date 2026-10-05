#!/usr/bin/env python
# vim:fileencoding=UTF-8

"""
Create, inspect and restore library backups.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise backup through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from __future__ import unicode_literals, division, absolute_import, print_function

import traceback
import weakref
from threading import Thread, Event

from LiuXin_alpha.file_formats.opf.opf2 import metadata_to_opf

from LiuXin_alpha.utils.logging import prints

# Py2/Py3 compatibility layer


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Abort(Exception):
    """
    Provide the abort contract for validated ebook processing.

    Example:
        Exercise Abort through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    pass


class MetadataBackup(Thread):
    """
    Continuously backup changed metadata into OPF files in the book directory.

    Example:
        Exercise MetadataBackup through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, db, interval=2, scheduling_interval=0.1):
        """
        Initialize and validate the metadatabackup state.

        Example:
            Exercise MetadataBackup.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param interval: Value supplied for interval under the utility contract.
        :param scheduling_interval: Value supplied for scheduling interval under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        Thread.__init__(self)
        self.daemon = True
        self._db = weakref.ref(getattr(db, "new_api", db))
        self.stop_running = Event()
        self.interval = interval
        self.scheduling_interval = scheduling_interval

    @property
    def db(self):
        """
        Holds a weakref to the database

        Example:
            Exercise MetadataBackup.db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = self._db()
        if ans is None:
            raise Abort()
        return ans

    def stop(self):
        """
        Perform the stop operation under explicit file-format and conversion rules.

        Example:
            Exercise MetadataBackup.stop through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.stop_running.set()

    def wait(self, interval):
        """
        Perform the wait operation under explicit file-format and conversion rules.

        Example:
            Exercise MetadataBackup.wait through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param interval: Value supplied for interval under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.stop_running.wait(interval):
            raise Abort()

    def run(self):
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise MetadataBackup.run through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        while not self.stop_running.is_set():
            try:
                self.wait(self.interval)
                self.do_one()
            except Abort:
                break

    def do_one(self):
        """
        Perform the do one operation under explicit file-format and conversion rules.

        Example:
            Exercise MetadataBackup.do one through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            book_id = self.db.get_a_dirtied_book()
            if book_id is None:
                return
        except Abort:
            raise
        except:
            # Happens during interpreter shutdown
            return

        self.wait(0)

        try:
            mi, sequence = self.db.get_metadata_for_dump(book_id)
        except:
            prints("Failed to get backup metadata for id:", book_id, "once")
            traceback.print_exc()
            self.wait(self.interval)
            try:
                mi, sequence = self.db.get_metadata_for_dump(book_id)
            except:
                prints("Failed to get backup metadata for id:", book_id, "again, giving up")
                traceback.print_exc()
                return

        if mi is None:
            self.db.clear_dirtied(book_id, sequence)
            return

        # Give the GUI thread a chance to do something. Python threads don't
        # have priorities, so this thread would naturally keep the processor
        # until some scheduling event happens. The wait makes such an event
        self.wait(self.scheduling_interval)

        try:
            raw = metadata_to_opf(mi)
        except:
            prints("Failed to convert to opf for id:", book_id)
            traceback.print_exc()
            self.db.clear_dirtied(book_id, sequence)
            return

        self.wait(self.scheduling_interval)

        try:
            self.db.write_backup(book_id, raw)
        except:
            prints("Failed to write backup metadata for id:", book_id, "once")
            traceback.print_exc()
            self.wait(self.interval)
            try:
                self.db.write_backup(book_id, raw)
            except:
                prints(
                    "Failed to write backup metadata for id:",
                    book_id,
                    "again, giving up",
                )
                traceback.print_exc()
                return

        self.db.clear_dirtied(book_id, sequence)

    def break_cycles(self):
        # Legacy compatibility
        """
        Perform the break cycles operation under explicit file-format and conversion rules.

        Example:
            Exercise MetadataBackup.break cycles through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


