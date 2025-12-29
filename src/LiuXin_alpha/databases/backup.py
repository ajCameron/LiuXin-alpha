#!/usr/bin/env python2
# vim:fileencoding=UTF-8

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import shutil
import traceback
import weakref
from copy import deepcopy
from threading import Thread, Event

from LiuXin.exceptions import LogicalError
from LiuXin.exceptions import InputIntegrityError

from LiuXin.file_formats.opf.opf2 import metadata_to_opf

from LiuXin.folder_stores.file_manager import path_ok

from LiuXin import prints
from LiuXin.utils.date import file_date
from LiuXin.utils.file_ops.file_properties import get_file_hash
from LiuXin.utils.logger import default_log

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Abort(Exception):
    pass


class MetadataBackup(Thread):
    """
    Continuously backup changed metadata into OPF files in the book directory.
    This class runs in its own thread.
    """

    def __init__(self, db, interval=2, scheduling_interval=0.1):
        Thread.__init__(self)
        self.daemon = True
        self._db = weakref.ref(getattr(db, "new_api", db))
        self.stop_running = Event()
        self.interval = interval
        self.scheduling_interval = scheduling_interval

    @property
    def db(self):
        """
        Holds a weakref to the database - shuts the thread down if it's ever detected that the database has been
        deleted.
        :return:
        """
        ans = self._db()
        if ans is None:
            raise Abort()
        return ans

    def stop(self):
        self.stop_running.set()

    def wait(self, interval):
        if self.stop_running.wait(interval):
            raise Abort()

    def run(self):
        while not self.stop_running.is_set():
            try:
                self.wait(self.interval)
                self.do_one()
            except Abort:
                break

    def do_one(self):
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
        pass


def backup_local_file(file_path, override_path=None):
    """
    Hash backed backup for a local file.
    :param file_path: Path to the file to be backed up
    :param override_path: An override path to back the file up to instead of the automatically generated one
    :return False/new_file_path: False if backup failes, new_file_path if it goes through
    """
    file_path = six_unicode(deepcopy(file_path))
    default_log.info("Backup of file : {}".format(file_path))
    if not path_ok(file_path):
        err_str = "Path failed initial checks.\n"
        err_str += "filepath: {}\n".format(file_path)
        default_log.error(err_str)
        raise InputIntegrityError(err_str)
    if override_path is None:
        new_file_path = make_backup_path(file_path)
    else:
        new_file_path = override_path
    old_hash = get_file_hash(file_path)
    new_hash = None
    shutil.copyfile(src=file_path, dst=new_file_path)
    attempt_count = 1
    while old_hash != new_hash:
        new_hash = get_file_hash(new_file_path)
        if old_hash == new_hash:
            break
        elif 0 < attempt_count <= 2:
            wrn_str = "Attempt to backup local file failed - hashes did not match.\n"
            default_log.log_variables(
                wrn_str,
                "WARN",
                ("filepath", file_path),
                ("new_file_path", new_file_path),
                ("old_hash", old_hash),
                ("new_hash", new_hash),
                ("attempt_count", attempt_count),
            )
        elif attempt_count > 2:
            wrn_str = "Attempt to backup local file has failed three times - aborting.\n"
            default_log.log_variables(
                wrn_str,
                "WARN",
                ("filepath", file_path),
                ("new_file_path", new_file_path),
                ("old_hash", old_hash),
                ("new_hash", new_hash),
                ("attempt_count", attempt_count),
            )
            return False
        os.remove(new_file_path)
        shutil.copyfile(src=file_path, dst=new_file_path)
        attempt_count += 1

    default_log.info("Backup of file : {}\nSuccessfully complete.".format(file_path))
    return new_file_path


def make_backup_path(filepath):
    """
    Name will have the form of [original_file_name] - [datestring]_[version].
    Version starts at 0, and is not printed. Followed by 1 e.t.c.
    :param filepath:
    :return:
    """
    file_name, file_ext = os.path.splitext(filepath)
    file_root = os.path.split(filepath)[0]
    backup_date = file_date()
    used_filenames = [os.path.join(file_root, p) for p in os.listdir(file_root)]
    cand_filepath = file_name + " - " + six_unicode(backup_date) + file_ext
    if cand_filepath not in used_filenames:
        return cand_filepath
    for i in range(1, 100):
        cand_filepath = six_unicode(file_name + " - " + six_unicode(backup_date) + "_{}" + file_ext).format(unicode(i))
        if cand_filepath not in used_filenames:
            return cand_filepath

    err_str = "filepath: " + six_unicode(filepath) + "\n"
    err_str += "appears to have been backed up over a hundred times. Today.\n"
    default_log.error(err_str)
    raise LogicalError(err_str)
