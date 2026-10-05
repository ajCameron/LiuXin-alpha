



# LiuXin style persistent temporary files. By default all create in the scratch folder.
# Some sorta locking database will have to be implemented

"""
Manage temporary files and scratch folders with explicit ownership and cleanup.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise ptempfiles through a consuming regression::

        python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
"""
from __future__ import print_function

# Todo: Tidy up and do some re-writing

import shutil
import os
from copy import deepcopy

# Can use this to generate unique names instead of the uuid time combo
import tempfile
import time
import traceback
import atexit
import subprocess
import pprint

from LiuXin_alpha.constants.paths import LiuXin_scratch_folder
from LiuXin_alpha.utils.which_os import iswindows, isosx
from LiuXin_alpha.constants import get_unicode_windows_env_var, get_windows_temp_path

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.startup_scripts.prefs_folder_manager import create_scratch_folder

from LiuXin_alpha.utils.storage.local.file_ops import file_hasher
from LiuXin_alpha.constants import filesystem_encoding
from LiuXin_alpha.constants import __appname__, __version__
from LiuXin_alpha.utils.python_tools import get_unique_id

from LiuXin_alpha.utils.logging import default_log

__author__ = "Cameron"

_base_dir = LiuXin_scratch_folder


def get_base_scratch_folders():
    """
    Return base scratch folders under the documented compatibility and safety rules.

    Example:
        Exercise get base scratch folders through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return globals()["LiuXin_scratch_folder"]


def set_base_scratch_folders(new_scratch_folder):
    """
    Set base scratch folders under the documented compatibility and safety rules.

    Example:
        Exercise set base scratch folders through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param new_scratch_folder: Value supplied for new scratch folder under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    globals()["LiuXin_scratch_folder"] = new_scratch_folder


class SwitchOutScratchFolder(object):
    """
    Own the SwitchOutScratchFolder resource and clean it according to the selected lifetime policy.

    Example:
        Exercise SwitchOutScratchFolder through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """
    def __init__(self, new_scratch_folder):
        """
        Initialize and validate the SwitchOutScratchFolder state.

        Example:
            Exercise SwitchOutScratchFolder.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param new_scratch_folder: Value supplied for new scratch folder under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.old_scratch_folder = None
        self.new_scratch_folder = new_scratch_folder

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise SwitchOutScratchFolder.  enter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.old_scratch_folder = globals()["LiuXin_scratch_folder"]
        globals()["LiuXin_scratch_folder"] = self.new_scratch_folder
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise SwitchOutScratchFolder.  exit   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc_val: Value supplied for exc val under the utility contract.
        :param exc_tb: Value supplied for exc tb under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        assert exc_type is None
        globals()["LiuXin_scratch_folder"] = self.old_scratch_folder


# def base_dir():
#     global _base_dir
#     if _base_dir is not None and not os.path.exists(_base_dir):
#         # Some people seem to think that running temp file cleaners that delete the temp dirs of running programs is a
#         # good idea!
#         _base_dir = None
#     if _base_dir is None:
#         td = os.environ.get('CALIBRE_WORKER_TEMP_DIR', None)
#         if td is not None:
#             import cPickle, binascii
#             try:
#                 td = cPickle.loads(binascii.unhexlify(td))
#             except:
#                 td = None
#         if td and os.path.exists(td):
#             _base_dir = td
#         else:
#             base = os.environ.get('CALIBRE_TEMP_DIR', None)
#             if base is not None and iswindows:
#                 base = get_unicode_windows_env_var('CALIBRE_TEMP_DIR')
#             prefix = app_prefix(u'tmp_')
#             if base is None:
#                 if iswindows:
#                     # On windows, if the TMP env var points to a path that
#                     # cannot be encoded using the mbcs encoding, then the
#                     # python 2 tempfile algorithm for getting the temporary
#                     # directory breaks. So we use the win32 api to get a
#                     # unicode temp path instead. See
#                     # https://bugs.launchpad.net/bugs/937389
#                     base = get_windows_temp_path()
#                 elif isosx:
#                     # Use the cache dir rather than the temp dir for temp files as Apple
#                     # thinks deleting unused temp files is a good idea. See note under
#                     # _CS_DARWIN_USER_TEMP_DIR here
#                     # https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man3/confstr.3.html
#                     base = osx_cache_dir()
#
#             _base_dir = tempfile.mkdtemp(prefix=prefix, dir=base)
#             atexit.register(determined_remove_dir if iswindows else remove_dir, _base_dir)
#
#         try:
#             tempfile.gettempdir()
#         except:
#             # Widows temp vars set to a path not encodable in mbcs
#             # Use our temp dir
#             tempfile.tempdir = _base_dir
#
#     return _base_dir


_osx_cache_dir = None


def osx_cache_dir():
    """
    Perform the osx cache dir utility operation under explicit compatibility rules.

    Example:
        Exercise osx cache dir through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _osx_cache_dir
    if _osx_cache_dir:
        return _osx_cache_dir
    if _osx_cache_dir is None:
        _osx_cache_dir = False
        import ctypes

        libc = ctypes.CDLL(None)
        buf = ctypes.create_string_buffer(512)
        l = libc.confstr(65538, ctypes.byref(buf), len(buf))  # _CS_DARWIN_USER_CACHE_DIR = 65538
        if 0 < l < len(buf):
            try:
                q = buf.value.decode("utf-8").rstrip("\0")
            except ValueError:
                pass
            if q and os.path.isdir(q) and os.access(q, os.R_OK | os.W_OK | os.X_OK):
                _osx_cache_dir = q
    return q


def reset_base_dir(override_folder=None):
    """
    Perform the reset base dir utility operation under explicit compatibility rules.

    Example:
        Exercise reset base dir through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param override_folder: Value supplied for override folder under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global _base_dir
    _base_dir = override_folder
    base_dir()


def app_prefix(prefix):
    """
    Perform the app prefix utility operation under explicit compatibility rules.

    Example:
        Exercise app prefix through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param prefix: Text prepended to the formatted or selected result.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if iswindows:
        return "%s_" % __appname__
    return "%s_%s_%s" % (__appname__, __version__, prefix)


def determined_remove_dir(x):
    """
    Perform the determined remove dir utility operation under explicit compatibility rules.

    Example:
        Exercise determined remove dir through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param x: Value supplied for x under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for i in range(10):
        try:
            import shutil

            shutil.rmtree(x)
            return
        except:
            import os  # noqa

            if os.path.exists(x):
                # In case some other program has one of the temp files open.
                import time

                time.sleep(0.1)
            else:
                return
    try:
        import shutil

        shutil.rmtree(x, ignore_errors=True)
    except:
        pass


def scrub_LX_scratch_folder():
    """
    Deletes all the files currently in the scratch folder ready for continued use. Only use this command if you're sure that no other process could be using this target destination.

    Example:
        Exercise scrub LX scratch folder through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    shutil.rmtree(LiuXin_scratch_folder, ignore_errors=True)
    create_scratch_folder()


def get_scratch_folder(filename=None, delete_at_exit=False):
    """
    Returns a folder in the default LiuXin_scratch folder (in a parallel dir to the LiuXin install called LiuXin_scratch).

    Example:
        Exercise get scratch folder through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param filename: Filename used for type inference or archive output.
    :param delete_at_exit: Value supplied for delete at exit under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    target_path = get_scratch_folder_path(filename)
    os.mkdir(target_path)
    default_log.dump_to_file(
        file_stuff="".join(traceback.format_stack()),
        file_name=os.path.split(target_path)[1],
        file_ext=".txt",
    )
    if delete_at_exit:
        atexit.register(safe_at_exit_remove_dir, target_path)

    return target_path


def get_scratch_folder_path(filename=None, in_folder=None):
    """
    Gets a valid name for a scratch folder in the LiuXin_scratch_folder.

    Example:
        Exercise get scratch folder path through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param filename: Filename used for type inference or archive output.
    :param in_folder: Value supplied for in folder under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    filename = deepcopy(filename)
    if filename is None:
        filename = ""

    unique_id = get_unique_id()
    unique_id += filename
    if in_folder is None:
        target_path = os.path.join(LiuXin_scratch_folder, unique_id)
    else:
        target_path = os.path.join(in_folder, unique_id)
    return target_path


# Todo: Check that we're removing a folder in the LiuXin scratch folder rather than an arbitary folder
def derez_scratch_folder(folder_path):
    """
    Scrubs a scratch folder from LiuXin_scratch.

    Example:
        Exercise derez scratch folder through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param folder_path: Value supplied for folder path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    folder_path = deepcopy(folder_path)

    # Check that the file is actually in the scratch folder
    directory_name = os.path.basename(folder_path)
    valid_scratch_folder = os.listdir(LiuXin_scratch_folder)

    if directory_name in valid_scratch_folder:
        shutil.rmtree(folder_path)
        if os.path.exists(folder_path):
            raise NotImplementedError("Method just failed.")
        return True
    else:
        raise AssertionError("This method is only to be used to delete scratch folder.")


def force_unicode(x):
    # Cannot use the implementation in calibre.__init__ as it causes a circular
    # dependency
    """
    Perform the force unicode utility operation under explicit compatibility rules.

    Example:
        Exercise force unicode through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(x, bytes):
        x = x.decode(filesystem_encoding)
    return x


def base_dir():
    """
    Perform the base dir utility operation under explicit compatibility rules.

    Example:
        Exercise base dir through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _base_dir
    if _base_dir is None:
        _base_dir = LiuXin_scratch_folder
    os.makedirs(_base_dir, mode=0o700, exist_ok=True)
    if not os.path.isdir(_base_dir):
        raise NotADirectoryError(
            "LiuXin temporary-file root is not a directory: {!r}".format(_base_dir)
        )
    return _base_dir


def _make_file(suffix, prefix, base):
    """
    Perform the make file utility operation under explicit compatibility rules.

    Example:
        Exercise  make file through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param suffix: Text appended to the formatted or selected result.
    :param prefix: Text prepended to the formatted or selected result.
    :param base: Value supplied for base under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    suffix, prefix = map(force_unicode, (suffix, prefix))
    fd, name = tempfile.mkstemp(suffix, prefix, dir=base)
    return fd, name


def _make_dir(suffix, prefix, base):
    """
    Perform the make dir utility operation under explicit compatibility rules.

    Example:
        Exercise  make dir through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param suffix: Text appended to the formatted or selected result.
    :param prefix: Text prepended to the formatted or selected result.
    :param base: Value supplied for base under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    suffix, prefix = map(force_unicode, (suffix, prefix))
    return tempfile.mkdtemp(suffix, prefix, base)


def cleanup(path):
    """
    Perform the cleanup utility operation under explicit compatibility rules.

    Example:
        Exercise cleanup through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        import os as oss

        if oss.path.exists(path):
            oss.remove(path)
    except:
        pass


# # Modified from calibre
# class TemporaryDirectory(object):
#     """
#     A temporary directory intended to be used in a with statement.
#     Intended to replace the calibre object of the same name.
#     """
#     def __init__(self, suffix="", prefix="", dir=None, mode='w+b'):
#         if prefix is None:
#             prefix = ''
#         if suffix is None:
#             suffix = ''
#         if dir is None:
#             dir = base_dir()
#         self.prefix, self.suffix, self.dir, self.mode = prefix, suffix, dir, mode
#         self._file = None
#
#     def __enter__(self):
#         fd, name = _make_file(self.suffix, self.prefix, self.dir)
#         self._file = os.fdopen(fd, self.mode)
#         self._name = name
#         # Ensures that the named file exists to be a target of chdir
#         if not os.path.exists(name):
#             os.mkdir(name)
#         else:
#             print "The file definitely existed here."
#         self._file.close()
#         return name
#
#     def __exit__(self, *args):
#         cleanup(self._name)


def make_path(suffix, prefix, base):
    """
    Constructs a valid name for a temporary file.

    Example:
        Exercise make path through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param suffix: Text appended to the formatted or selected result.
    :param prefix: Text prepended to the formatted or selected result.
    :param base: Value supplied for base under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    suffix, prefix = map(force_unicode, (suffix, prefix))
    unique_id = get_unique_id()
    name = prefix + unique_id + suffix
    return os.path.join(base, name)


class ScratchFolder(object):
    """
    Creates a ScratchFolder - a folder in LiuXin_scratch.

    Example:
        Exercise ScratchFolder through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self):
        """
        Initialize and validate the ScratchFolder state.

        Example:
            Exercise ScratchFolder.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; validated state is stored on the receiving object.
        """
        self.path = None

    def manual_create(self):
        """
        Perform the manual create utility operation under explicit compatibility rules.

        Example:
            Exercise ScratchFolder.manual create through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.path = get_scratch_folder()

    def __enter__(self):
        """
        Creates a scratch folder - stores the path.

        Example:
            Exercise ScratchFolder.  enter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.path = get_scratch_folder()
        return self

    def __exit__(self, *args):
        """
        Cleans up the file - removing the created temporary folder

        Example:
            Exercise ScratchFolder.  exit   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        derez_scratch_folder(self.path)
        self.path = None

    def __del__(self):
        """
        Perform the del utility operation under explicit compatibility rules.

        Example:
            Exercise ScratchFolder.  del   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            derez_scratch_folder(self.path)
        except:
            pass


# TODO: This IS TERRIBLE! REMOVE!
scratchfolder = ScratchFolder()


class ScratchCWDFolder(object):
    """
    Creates a scratch folder - changes the current working dictionary to that folder - changes it back on exit.

    Example:
        Exercise ScratchCWDFolder through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self):
        """
        Initialize and validate the ScratchCWDFolder state.

        Example:
            Exercise ScratchCWDFolder.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; validated state is stored on the receiving object.
        """
        self.path = None
        self.original_cwd = None

    def __enter__(self):
        """
        Creates the scratch folder. Stores the path. Stores the original cwd - changes into the folder.

        Example:
            Exercise ScratchCWDFolder.  enter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.path = get_scratch_folder()
        self.original_cwd = os.getcwd()
        os.chdir(self.path)
        return self

    def __exit__(self, *args):
        """
        Change back to the main directory - then delete the temporary files.

        Example:
            Exercise ScratchCWDFolder.  exit   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        os.chdir(self.original_cwd)
        derez_scratch_folder(self.path)
        self.path = None


scratch_cwd_folder = ScratchCWDFolder()


class ScratchFileCopy(object):
    """
    Creates a scratch copy of a file in a scratch folder.

    Example:
        Exercise ScratchFileCopy through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self, file_path):
        """
        Makes a scratch copy of the file at the given location.

        Example:
            Exercise ScratchFileCopy.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param file_path: Value supplied for file path under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.src_file_path = file_path
        self.file_path = None

    def __enter__(self):
        """
        Copy the file into position.

        Example:
            Exercise ScratchFileCopy.  enter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.src_file_name = os.path.split(self.src_file_path)[1]
        self.src_file_ext = os.path.splitext(self.src_file_path)[1]
        self.dst_folder_path = get_scratch_folder()
        self.dst_file_path = os.path.join(self.dst_folder_path, self.src_file_name)

        # Copy the file - checking the hash before and after
        self.src_file_hash = file_hasher(self.src_file_path)
        shutil.copyfile(src=self.src_file_path, dst=self.dst_file_path)
        dst_file_hash = file_hasher(self.dst_file_path)
        assert self.src_file_hash == dst_file_hash, "Cannot create ScratchFileCopy - hash mismatch"

        # Record with an easy interface
        self.file_path = self.dst_file_path
        return self

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise ScratchFileCopy.  exit   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            derez_scratch_folder(self.dst_folder_path)
        except os.WindowsError:
            shutil.rmtree(self.dst_folder_path)


class TemporaryFile(object):
    """
    Own the TemporaryFile resource and clean it according to the selected lifetime policy.

    Example:
        Exercise TemporaryFile through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """
    def __init__(self, suffix="", prefix="", dir=None, mode="w+b"):
        """
        Initialize and validate the TemporaryFile state.

        Example:
            Exercise TemporaryFile.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param suffix: Text appended to the formatted or selected result.
        :param prefix: Text prepended to the formatted or selected result.
        :param dir: Value supplied for dir under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :return: None; validated state is stored on the receiving object.
        """
        if prefix is None:
            prefix = ""
        if suffix is None:
            suffix = ""
        if dir is None:
            dir = base_dir()
        self.prefix, self.suffix, self.dir, self.mode = prefix, suffix, dir, mode
        self._file = None

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise TemporaryFile.  enter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fd, name = _make_file(self.suffix, self.prefix, self.dir)
        self._file = os.fdopen(fd, self.mode)
        self._name = name
        self._file.close()
        return name

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise TemporaryFile.  exit   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        cleanup(self._name)


class PersistentTemporaryFile(object):
    """
    A file-like object that is a temporary file that is available even after being closed on all platforms. It is automatically deleted on normal program termination.

    Example:
        Exercise PersistentTemporaryFile through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    _file = None

    def __init__(self, suffix="", prefix="", dir=None, mode="w+b"):
        """
        Initialize and validate the PersistentTemporaryFile state.

        Example:
            Exercise PersistentTemporaryFile.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param suffix: Text appended to the formatted or selected result.
        :param prefix: Text prepended to the formatted or selected result.
        :param dir: Value supplied for dir under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :return: None; validated state is stored on the receiving object.
        """
        if prefix is None:
            prefix = ""
        if dir is None:
            dir = base_dir()
        fd, name = _make_file(suffix, prefix, dir)

        self._file = os.fdopen(fd, mode)
        self._name = name
        self._fd = fd
        # atexit.register(cleanup, name)

    def __getattr__(self, name):
        """
        Perform the getattr utility operation under explicit compatibility rules.

        Example:
            Exercise PersistentTemporaryFile.  getattr   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if name == "name":
            return self.__dict__["_name"]
        return getattr(self.__dict__["_file"], name)

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise PersistentTemporaryFile.  enter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise PersistentTemporaryFile.  exit   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.close()

    def __del__(self):
        """
        Perform the del utility operation under explicit compatibility rules.

        Example:
            Exercise PersistentTemporaryFile.  del   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            self.close()
        except:
            pass


class TemporaryDirectory(object):
    """
    A temporary directory intended to be used in a with statement. Creates a temporary dictionary in the LiuXin_scratch folder. Then cleans it up in the exit phase.

    Example:
        Exercise TemporaryDirectory through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self, suffix="", prefix="", dir=None, mode="w+b", keep=False):
        """
        Starts up the temporary directory.

        Example:
            Exercise TemporaryDirectory.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param suffix: Text appended to the formatted or selected result.
        :param prefix: Text prepended to the formatted or selected result.
        :param dir: Value supplied for dir under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param keep: Value supplied for keep under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        # Ensures that an exception won;t be thrown by ensuring the naming is sane.
        if prefix is None:
            prefix = ""
        if suffix is None:
            suffix = ""
        if dir is None:
            dir = LiuXin_scratch_folder
        self.prefix, self.suffix, self.dir, self.mode = prefix, suffix, dir, mode
        self.keep = keep
        self.path = None

    def __enter__(self):
        """
        Entry point for the with statement. Returns the path to the temporary folder.

        Example:
            Exercise TemporaryDirectory.  enter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.path = make_path(self.suffix, self.prefix, self.dir)

        os.mkdir(self.path)
        return self.path

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise TemporaryDirectory.  exit   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            if not self.keep:
                shutil.rmtree(self.path)
        except os.WindowsError:
            shutil.rmtree(self.path)

        atexit.register(safe_at_exit_remove_dir, self.path)


def remove_dir(x):
    """
    Remove dir under the documented compatibility and safety rules.

    Example:
        Exercise remove dir through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param x: Value supplied for x under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        import shutil

        shutil.rmtree(x, ignore_errors=True)
    except:
        pass


def PersistentTemporaryDirectory(suffix="", prefix="", dir=None):
    """
    Return the path to a newly created temporary directory that will be automatically deleted on application exit.

    Example:
        Exercise PersistentTemporaryDirectory through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param suffix: Text appended to the formatted or selected result.
    :param prefix: Text prepended to the formatted or selected result.
    :param dir: Value supplied for dir under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if dir is None:
        dir = base_dir()
    tdir = _make_dir(suffix, prefix, dir)

    atexit.register(remove_dir, tdir)
    return tdir


class SpooledTemporaryFile(tempfile.SpooledTemporaryFile):
    """
    SpooledTemporaryFile from tempfile, with some additional properties to make it more file like.

    Example:
        Exercise SpooledTemporaryFile through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self, max_size=0, suffix="", prefix="", dir=None, mode="w+b", bufsize=-1):
        """
        Initialize and validate the SpooledTemporaryFile state.

        Example:
            Exercise SpooledTemporaryFile.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param max_size: Value supplied for max size under the utility contract.
        :param suffix: Text appended to the formatted or selected result.
        :param prefix: Text prepended to the formatted or selected result.
        :param dir: Value supplied for dir under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param bufsize: Value supplied for bufsize under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if prefix is None:
            prefix = ""
        if suffix is None:
            suffix = ""
        if dir is None:
            dir = base_dir()
        self.__file_name = None
        tempfile.SpooledTemporaryFile.__init__(
            self,
            max_size=max_size,
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            mode=mode,
            bufsize=bufsize,
        )

    def truncate(self, *args):
        # The stdlib SpooledTemporaryFile implementation of truncate() doesn't
        # allow specifying a size.
        """
        Forward the truncate operation while preserving adapter ownership rules.

        Example:
            Exercise SpooledTemporaryFile.truncate through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._file.truncate(*args)

    @property
    def name(self):
        """
        Perform the name utility operation under explicit compatibility rules.

        Example:
            Exercise SpooledTemporaryFile.name through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__file_name

    @name.setter
    def name(self, val):
        """
        Perform the name utility operation under explicit compatibility rules.

        Example:
            Exercise SpooledTemporaryFile.name through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__file_name = val


def better_mktemp(*args, **kwargs):
    """
    Make a temporary directory using the tempfile.mkstemp command.

    Example:
        Exercise better mktemp through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fd, path = tempfile.mkstemp(*args, **kwargs)
    os.close(fd)
    return path


known_ramdisks = set()


# Todo: SERIOUS SECURITY HOLE - REPLACE FORCE MOUNTPOINT WITH FORCE NAME or somthing like that
if not iswindows:

    def get_ramdisk(
        force_mountpoint=False,
        write_creation_traceback=True,
        persist=False,
        size="2048M",
        folder_fallback=True,
        retry=10,
        use_dev_shm=True,
    ):
        """
        Construct a ram disk - of size 512M. May fail if the LiuXin path contains unicode characters.

        Example:
            Exercise get ramdisk through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param force_mountpoint: Value supplied for force mountpoint under the utility
            contract.
        :param write_creation_traceback: Value supplied for write creation traceback under
            the utility contract.
        :param persist: Value supplied for persist under the utility contract.
        :param size: Value supplied for size under the utility contract.
        :param folder_fallback: Value supplied for folder fallback under the utility
            contract.
        :param retry: Value supplied for retry under the utility contract.
        :param use_dev_shm: Value supplied for use dev shm under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert size in ["512M", "2048M"]

        if not use_dev_shm:
            ramdisk_mountpoint = get_scratch_folder_path("RAMDISK") if not force_mountpoint else force_mountpoint

            mount_cmd = 'sudo mkdir -p "{0}" && sudo mount -t tmpfs -o size=2048M tmpfs "{0}"'.format(
                ramdisk_mountpoint
            )
            try:
                sub_output = subprocess.check_output(mount_cmd, shell=True)
            except OSError as e:
                err_str = "Error while trying to mount temporary file system\n"
                err_str += "mount_cmd: {}\n".format(mount_cmd)
                err_str += "OSError message: {}".format(e)
                if retry is not False:
                    time.sleep(retry)
                    try:
                        # Retry - in case it's a temporary blocking issue
                        return get_ramdisk(
                            force_mountpoint=force_mountpoint,
                            write_creation_traceback=write_creation_traceback,
                            persist=persist,
                            size=size,
                            folder_fallback=False,
                            retry=False,
                        )
                    except OSError:
                        pass

                if not folder_fallback:
                    raise OSError(err_str)
                else:
                    return get_scratch_folder()

            assert os.path.exists(ramdisk_mountpoint) and os.path.ismount(ramdisk_mountpoint), ramdisk_mountpoint
            assert not sub_output, "sub_output was, unexpectedly, not null - sub_output: {}".format(sub_output)

        else:
            ramdisk_mountpoint = (
                get_scratch_folder_path("RAMDISK", in_folder="/dev/shm") if not force_mountpoint else force_mountpoint
            )

            # No need to mount - just making a folder in the dir should do
            os.mkdir(ramdisk_mountpoint)

            assert os.path.isdir(ramdisk_mountpoint)

        global known_ramdisks
        known_ramdisks.add(ramdisk_mountpoint)

        # traceback describing the ramdisk creation will be added to the root of the ramdisk
        if write_creation_traceback:
            with open(os.path.join(ramdisk_mountpoint, "creation_traceback.txt"), "w") as tcb_file:
                traceback.print_stack(file=tcb_file)

        assert ramdisk_mountpoint in known_ramdisks

        # Log the creation point of the
        default_log.dump_to_file(
            file_stuff="".join(traceback.format_stack()),
            file_name=os.path.split(ramdisk_mountpoint)[1],
            file_ext=".txt",
        )

        if not persist:
            atexit.register(safe_unmount_ramdisk, ramdisk_mountpoint)

        return ramdisk_mountpoint

else:

    # Todo: This is not going to work. At all
    def get_ramdisk(force_mountpoint=False, write_creation_traceback=True, persist=False):
        """
        Construct a ram disk - of size 512MB. May fail if the LiuXin path contains unicode characters.

        Example:
            Exercise get ramdisk through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param force_mountpoint: Value supplied for force mountpoint under the utility
            contract.
        :param write_creation_traceback: Value supplied for write creation traceback under
            the utility contract.
        :param persist: Value supplied for persist under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ramdisk_mountpoint = get_scratch_folder_path("RAMDISK") if not force_mountpoint else force_mountpoint
        return os.path.join("L:", ramdisk_mountpoint)


# Todo: VERY BADLY NAMED - GIVEN WHAT DEREZ_SCRATCH_FOLDER DOES ABOVE!
def derez_ramdisk(mountpoint):
    """
    Empties a given ramdisk Does not actually remove the disk. All data in the ramdisk will be permanently lost and the folder it was mounted on will be removed.

    Example:
        Exercise derez ramdisk through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param mountpoint: Value supplied for mountpoint under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if mountpoint not in known_ramdisks:
        err_str = "mountpoint was not known to this process. Cannot remove arbitary mountpoints for security reasons\n"
        err_str += "mountpoint: {}".format(mountpoint)
        raise InputIntegrityError(err_str)

    mountpoint_objs = os.listdir(mountpoint)
    for obj in mountpoint_objs:
        shutil.rmtree(os.path.join(mountpoint, obj))


def safe_unmount_ramdisk(mountpoint):

    """
    Perform the safe unmount ramdisk utility operation under explicit compatibility rules.

    Example:
        Exercise safe unmount ramdisk through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param mountpoint: Value supplied for mountpoint under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global known_ramdisks
    if mountpoint not in known_ramdisks:
        return

    if os.path.isdir(mountpoint) and mountpoint.startswith("/dev/shm"):
        # We're dealing with a shm style ramdisk - removing it
        try:
            shutil.rmtree(mountpoint)
        except:
            pass

    # In some cases where we cannot create a ramdisk we might just fall back to a folder. In this case there is nothing
    # to remove - it's already gone
    if not os.path.ismount(mountpoint):
        info_msg = [
            "mountpoit could not be removed - it seems to already be gone",
            "succesful failure?",
            "mountpoint: {}".format(mountpoint),
        ]
        print("\n".join(info_msg))

        # The ramdisk is definitely gone - one way or the other
        known_ramdisks.remove(mountpoint)

        # At the worst we can remove some of the excess files - as they should be removed anyway
        try:
            shutil.rmtree(mountpoint)
        except:
            pass

        return

    unmount_cmd = 'sudo umount -lf "{0}"'.format(mountpoint)
    try:
        sub_output = subprocess.check_output(unmount_cmd, shell=True)
    except Exception:
        pass

    known_ramdisks.remove(mountpoint)

    try:
        shutil.rmtree(mountpoint)
    except:
        pass


def unmount_ramdisk(mountpoint):
    """
    Preforms unmount operations to remove a ramdisk and free up it's memory.

    Example:
        Exercise unmount ramdisk through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param mountpoint: Value supplied for mountpoint under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global known_ramdisks
    if mountpoint not in known_ramdisks:
        err_str = "mountpoint was not known to this process. Cannot remove arbitary mountpoints for security reasons\n"
        err_str += "mountpoint: {}\n".format(mountpoint)
        err_str += "known_ramdisks: \n{}\n".format(pprint.pformat(known_ramdisks))
        raise InputIntegrityError(err_str)

    if os.path.isdir(mountpoint) and mountpoint.startswith("/dev/shm"):
        # We're dealing with a shm style ramdisk - removing it
        shutil.rmtree(mountpoint)

    # In some cases where we cannot create a ramdisk we might just fall back to a folder. In this case there is nothing
    # to remove - it's already gone
    if not os.path.ismount(mountpoint):
        info_msg = [
            "mountpoit could not be removed - it seems to already be gone",
            "succesful failure?",
            "mountpoint: {}".format(mountpoint),
        ]
        print("\n".join(info_msg))

        # The ramdisk is definitely gone - one way or the other
        known_ramdisks.remove(mountpoint)

        # At the worst we can remove some of the excess files - as they should be removed anyway
        try:
            shutil.rmtree(mountpoint)
        except:
            pass

        return

    unmount_cmd = 'sudo umount -lf "{0}"'.format(mountpoint)
    try:
        sub_output = subprocess.check_output(unmount_cmd, shell=True)
    except OSError as e:
        err_msg = [
            "Error while trying to mount temporary file system",
            "unmount_cmd: {}".format(unmount_cmd),
            "OSError message: {}".format(e),
        ]
        raise OSError("\n".join(err_msg))
    except subprocess.CalledProcessError as e:
        err_msg = [
            "subprocess.CalledProcessError while trying to unmount a ramdisk",
            "if you are using Linux Subsystem for Windows this is expected - if not - that's more interesting"
            "unmount_cmd: {}".format(unmount_cmd),
            "exception: {}".format(e),
        ]
        print("\n".join(err_msg))
    else:
        # If subprocess ran at all, check it produced no output
        assert not sub_output, "sub_output was, unexpectedly, not null - sub_output: {}".format(sub_output)

    known_ramdisks.remove(mountpoint)

    try:
        shutil.rmtree(mountpoint)
    except:
        pass


def get_known_ramdisk_count():
    """
    Return the current number of active ramdisks known to the system.

    Example:
        Exercise get known ramdisk count through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global known_ramdisks
    return len(known_ramdisks)


class BaseScratchFolderManager(object):
    """
    Base class from which more sophisticated scratch folder managers can be derived.

    Example:
        Exercise BaseScratchFolderManager through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self, make_in=None, only_derez_own=False):
        """
        Initialize and validate the BaseScratchFolderManager state.

        Example:
            Exercise BaseScratchFolderManager.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param make_in: Value supplied for make in under the utility contract.
        :param only_derez_own: Value supplied for only derez own under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.only_derez_own = only_derez_own

        # Default place to create all the scratch folders
        self.base_scratch_loc = LiuXin_scratch_folder if make_in is None else make_in

        # All the scratch folders created by this class
        self.made_folders = []
        # Folders that shouldn't be deleted when clear is called
        self.pinned_folders = []

    @property
    def pinned(self):
        """
        Return the pinned folders

        Example:
            Exercise BaseScratchFolderManager.pinned through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.pinned_folders

    @property
    def made(self):
        """
        Return all folders which have been created by the folder store manager.

        Example:
            Exercise BaseScratchFolderManager.made through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.made_folders

    def get_scratch_folder(self, filename=None, pinned=False, base_filename=False):
        """
        Returns the path for a new scratch folder in the base_scratch_loc.

        Example:
            Exercise BaseScratchFolderManager.get scratch folder through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param filename: Filename used for type inference or archive output.
        :param pinned: Value supplied for pinned under the utility contract.
        :param base_filename: Value supplied for base filename under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_scratch_folder_name(self, filename=None, base_filename=False):
        """
        Gets a valid name for a scratch folder in the current base folder.

        Example:
            Exercise BaseScratchFolderManager.get scratch folder name through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param filename: Filename used for type inference or archive output.
        :param base_filename: Value supplied for base filename under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def derez_scratch_folder(self, folder_path):
        """
        Removes a scratch folder from the current base_scratch_loc. Will error if the folder isn't in the current base_scratch_loc or if the only_derez_own flag is set to True and the folder was not created by this instance of this class.

        Example:
            Exercise BaseScratchFolderManager.derez scratch folder through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param folder_path: Value supplied for folder path under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def clear(self):
        """
        Delete all the folders that have been created by this folder store.

        Example:
            Exercise BaseScratchFolderManager.clear through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __iter__(self):
        """
        Iterates over all the files currently under management by this class.

        Example:
            Exercise BaseScratchFolderManager.  iter   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: An iterator yielding the normalized values described above.
        """
        for file_path in self.made_folders:
            yield file_path

    def __contains__(self, item):
        """
        Perform the contains utility operation under explicit compatibility rules.

        Example:
            Exercise BaseScratchFolderManager.  contains   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return item in self.made_folders

    def __len__(self):
        """
        Return the number of files currently under management.

        Example:
            Exercise BaseScratchFolderManager.  len   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.made_folders)


class DummyScratchFolderManager(BaseScratchFolderManager):
    """
    Dummy to be used as a default for objects which might be passed a ScratchFolderManager - provides the same interface as the ScratchFolderManager but just provides a passthrough to the main scratch folder methods.

    Example:
        Exercise DummyScratchFolderManager through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self, make_in=None, only_derez_own=False):
        """
        Initialize and validate the DummyScratchFolderManager state.

        Example:
            Exercise DummyScratchFolderManager.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param make_in: Value supplied for make in under the utility contract.
        :param only_derez_own: Value supplied for only derez own under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseScratchFolderManager.__init__(self, make_in=make_in, only_derez_own=only_derez_own)

    def get_scratch_folder(self, filename=None, pinned=False, base_filename=False):
        """
        Return scratch folder under the documented compatibility and safety rules.

        Example:
            Exercise DummyScratchFolderManager.get scratch folder through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param filename: Filename used for type inference or archive output.
        :param pinned: Value supplied for pinned under the utility contract.
        :param base_filename: Value supplied for base filename under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return get_scratch_folder(filename)

    def get_scratch_folder_name(self, filename=None, base_filename=False):
        """
        Return scratch folder name under the documented compatibility and safety rules.

        Example:
            Exercise DummyScratchFolderManager.get scratch folder name through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param filename: Filename used for type inference or archive output.
        :param base_filename: Value supplied for base filename under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return get_scratch_folder_path(filename)

    def derez_scratch_folder(self, folder_path):
        """
        Perform the derez scratch folder utility operation under explicit compatibility rules.

        Example:
            Exercise DummyScratchFolderManager.derez scratch folder through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param folder_path: Value supplied for folder path under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return derez_scratch_folder(folder_path)


scratch_folder_manager = DummyScratchFolderManager()


class ScratchFolderManager(BaseScratchFolderManager):
    """
    Object which produces and manages scratch folders. Useful when you want to taylor the behavior of scratch folders more closely - for example if you want some subprocess to generate scratch folders in a different location to the default. This is useful if you want to speed up test performance by creating all scratch folders in a ramdisk. By default just uses the regular scratch folder system.

    Example:
        Exercise ScratchFolderManager through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py
    """

    def __init__(self, make_in=None, only_derez_own=False):
        """
        Starts up the Manager - by default behaves just like a regular scratch folder.

        Example:
            Exercise ScratchFolderManager.  init   through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param make_in: Value supplied for make in under the utility contract.
        :param only_derez_own: Value supplied for only derez own under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseScratchFolderManager.__init__(self, make_in=make_in, only_derez_own=only_derez_own)

    if iswindows:

        def get_scratch_folder(self, filename=None, pinned=False, base_filename=False):
            """
            Returns the path for a new scratch folder in the base_scratch_loc.

            Example:
                Exercise ScratchFolderManager.get scratch folder through a consuming regression::

                    python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


            :param filename: Filename used for type inference or archive output.
            :param pinned: Value supplied for pinned under the utility contract.
            :param base_filename: Value supplied for base filename under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            target_path = self.get_scratch_folder_name(filename, base_filename=base_filename)
            os.makedirs(target_path)
            assert os.path.exists(target_path)

            self.made_folders.append(target_path)
            if pinned:
                self.pinned_folders.append(target_path)
            return target_path

    else:

        def get_scratch_folder(self, filename=None, pinned=False, base_filename=False):
            """
            Returns the path for a new scratch folder in the base_scratch_loc.

            Example:
                Exercise ScratchFolderManager.get scratch folder through a consuming regression::

                    python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


            :param filename: Filename used for type inference or archive output.
            :param pinned: Value supplied for pinned under the utility contract.
            :param base_filename: Value supplied for base filename under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            target_path = self.get_scratch_folder_name(filename, base_filename=base_filename)
            os.mkdir(target_path)
            assert os.path.exists(target_path)

            self.made_folders.append(target_path)
            if pinned:
                self.pinned_folders.append(target_path)
            return target_path

    def get_scratch_folder_name(self, filename=None, base_filename=False):
        """
        Gets a valid name for a scratch folder in the current base folder.

        Example:
            Exercise ScratchFolderManager.get scratch folder name through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param filename: Filename used for type inference or archive output.
        :param base_filename: Value supplied for base filename under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        filename = deepcopy(filename)
        if filename is None:
            filename = ""

        if not base_filename:
            unique_id = get_unique_id()
            unique_id += filename
            target_path = os.path.join(self.base_scratch_loc, unique_id)
        else:
            target_path = os.path.join(self.base_scratch_loc, filename)
        return target_path

    # Todo: Does not account for their being files in the root of the folder store
    def derez_scratch_folder(self, folder_path):
        """
        Removes a scratch folder from the current base_scratch_loc. Will error if the folder isn't in the current base_scratch_loc or if the only_derez_own flag is set to True and the folder was not created by this instance of this class.

        Example:
            Exercise ScratchFolderManager.derez scratch folder through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param folder_path: Value supplied for folder path under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Ensure that the path is of the same type as stored in the internal creation record
        if self.only_derez_own and folder_path not in self.made_folders:
            raise InputIntegrityError(
                "Cannot delete that folder - it's not under controlled by this manager - " "{}".format(folder_path)
            )

        # Check that the file is actually in the scratch folder
        directory_name = os.path.basename(folder_path)
        valid_scratch_folders = os.listdir(self.base_scratch_loc)

        try:
            self.made_folders.remove(folder_path)
        except ValueError:
            pass
        try:
            self.pinned_folders.remove(folder_path)
        except ValueError:
            pass

        if directory_name in valid_scratch_folders:
            shutil.rmtree(folder_path)
            if os.path.exists(folder_path):
                raise NotImplementedError("Method just failed.")
            return True
        else:
            raise AssertionError(
                "This method is only to be used to delete scratch folder. " "folder_path {}".format(folder_path)
            )

    def make_scratch(self, src_folder_path):
        """
        Copy a folder into a scratch folder. Return the path to the scratch folder.

        Example:
            Exercise ScratchFolderManager.make scratch through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :param src_folder_path: Value supplied for src folder path under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        src_folder_name = os.path.split(src_folder_path)[1]
        scratch_folder = self.get_scratch_folder()
        dst_folder_path = os.path.join(scratch_folder, src_folder_name)
        shutil.copytree(src=src_folder_path, dst=dst_folder_path)
        return dst_folder_path

    def clear(self):
        """
        Delete all the folders that have been created by this folder store.

        Example:
            Exercise ScratchFolderManager.clear through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        folders_for_remove = deepcopy(self.made_folders)
        for folder_path in folders_for_remove:
            if folder_path in self.pinned_folders:
                continue
            self.derez_scratch_folder(folder_path)

    def purge(self):
        """
        Delete ALL the folders that have been created by this folder store.

        Example:
            Exercise ScratchFolderManager.purge through a consuming regression::

                python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        folders_for_remove = deepcopy(self.made_folders)
        for folder_path in folders_for_remove:
            self.derez_scratch_folder(folder_path)


def safe_at_exit_remove_dir(dir_path):
    """
    Remove a directory without throwing an error if the directory does, in fact, not exist

    Example:
        Exercise safe at exit remove dir through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :param dir_path: Value supplied for dir path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        shutil.rmtree(dir_path)
    except OSError:
        pass



def reset_temp_folder_permissions():
    # There are some broken windows installs where the permissions for the temp
    # folder are set to not be executable, which means chdir() into temp
    # folders fails. Try to fix that by resetting the permissions on the temp
    # folder.
    """
    Perform the reset temp folder permissions utility operation under explicit compatibility rules.

    Example:
        Exercise reset temp folder permissions through a consuming regression::

            python -m pytest -q tests/databases/database/test_database_tmpdir_init.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global _base_dir
    if iswindows and _base_dir:
        import subprocess
        from LiuXin_alpha.utils.logging import prints

        parent = os.path.dirname(_base_dir)
        retcode = subprocess.Popen(["icacls.exe", parent, "/reset", "/Q", "/T"]).wait()
        prints(
            "Trying to reset permissions of temp folder",
            parent,
            "return code:",
            retcode,
        )
