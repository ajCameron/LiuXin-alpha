from __future__ import division, absolute_import, print_function, unicode_literals

import os
import shutil
from copy import deepcopy

from LiuXin_alpha.errors import InputIntegrityError, LogicalError
from LiuXin_alpha.utils.date import file_date
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.paths import path_ok
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash


def backup_local_file(file_path, override_path=None):
    """
    Hash backed backup for a local file.

    :param file_path: Path to the file to be backed up
    :param override_path: An override path to back the file up to instead of the automatically generated one
    :return False/new_file_path: False if backup failes, new_file_path if it goes through
    """
    file_path = deepcopy(file_path)
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


def make_backup_path(filepath: str) -> str:
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
    cand_filepath = file_name + " - " + str(backup_date) + file_ext
    if cand_filepath not in used_filenames:
        return cand_filepath
    for i in range(1, 100):
        cand_filepath = str(file_name + " - " + str(backup_date) + "_{}" + file_ext).format(str(i))
        if cand_filepath not in used_filenames:
            return cand_filepath

    err_str = "filepath: " + str(filepath) + "\n"
    err_str += "appears to have been backed up over a hundred times. Today.\n"
    default_log.error(err_str)
    raise LogicalError(err_str)
