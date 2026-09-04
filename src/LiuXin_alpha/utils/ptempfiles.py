



# LiuXin style persistent temporary files. By default all create in the scratch folder.
# Some sorta locking database will have to be implemented

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
    return globals()["LiuXin_scratch_folder"]


def set_base_scratch_folders(new_scratch_folder):
    globals()["LiuXin_scratch_folder"] = new_scratch_folder


class SwitchOutScratchFolder(object):
    def __init__(self, new_scratch_folder):
        self.old_scratch_folder = None
        self.new_scratch_folder = new_scratch_folder

    def __enter__(self):
        self.old_scratch_folder = globals()["LiuXin_scratch_folder"]
        globals()["LiuXin_scratch_folder"] = self.new_scratch_folder
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
    global _base_dir
    _base_dir = override_folder
    base_dir()


def app_prefix(prefix):
    if iswindows:
        return "%s_" % __appname__
    return "%s_%s_%s" % (__appname__, __version__, prefix)


def determined_remove_dir(x):
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
    Deletes all the files currently in the scratch folder ready for continued use.
    Only use this command if you're sure that no other process could be using this target destination.
    """
    shutil.rmtree(LiuXin_scratch_folder, ignore_errors=True)
    create_scratch_folder()


def get_scratch_folder(filename=None, delete_at_exit=False):
    """
    Returns a folder in the default LiuXin_scratch folder (in a parallel dir to the LiuXin install called
    LiuXin_scratch).
    :param filename: Filename to be appended to the unique identifier generated for the scratch folder.
    :return:
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
    :param filename: If present will be appended to the unique identifier to form the full name for the scratch folder.
    :param in_folder:
    :return:
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
    :param folder_path: A folder path to remove
    :return bool:
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
    if isinstance(x, bytes):
        x = x.decode(filesystem_encoding)
    return x


def base_dir():
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
    suffix, prefix = map(force_unicode, (suffix, prefix))
    fd, name = tempfile.mkstemp(suffix, prefix, dir=base)
    return fd, name


def _make_dir(suffix, prefix, base):
    suffix, prefix = map(force_unicode, (suffix, prefix))
    return tempfile.mkdtemp(suffix, prefix, base)


def cleanup(path):
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
    :param suffix:
    :param prefix:
    :param base:
    :return:
    """
    suffix, prefix = map(force_unicode, (suffix, prefix))
    unique_id = get_unique_id()
    name = prefix + unique_id + suffix
    return os.path.join(base, name)


class ScratchFolder(object):
    """
    Creates a ScratchFolder - a folder in LiuXin_scratch.
    """

    def __init__(self):
        self.path = None

    def manual_create(self):
        self.path = get_scratch_folder()

    def __enter__(self):
        """
        Creates a scratch folder - stores the path.
        """
        self.path = get_scratch_folder()
        return self

    def __exit__(self, *args):
        """
        Cleans up the file - removing the created temporary folder
        :param args:
        :return:
        """
        derez_scratch_folder(self.path)
        self.path = None

    def __del__(self):
        try:
            derez_scratch_folder(self.path)
        except:
            pass


# TODO: This IS TERRIBLE! REMOVE!
scratchfolder = ScratchFolder()


class ScratchCWDFolder(object):
    """
    Creates a scratch folder - changes the current working dictionary to that folder - changes it back on exit.
    """

    def __init__(self):
        self.path = None
        self.original_cwd = None

    def __enter__(self):
        """
        Creates the scratch folder. Stores the path.
        Stores the original cwd - changes into the folder.
        :return:
        """
        self.path = get_scratch_folder()
        self.original_cwd = os.getcwd()
        os.chdir(self.path)
        return self

    def __exit__(self, *args):
        """
        Change back to the main directory - then delete the temporary files.
        :param args:
        :return:
        """
        os.chdir(self.original_cwd)
        derez_scratch_folder(self.path)
        self.path = None


scratch_cwd_folder = ScratchCWDFolder()


class ScratchFileCopy(object):
    """
    Creates a scratch copy of a file in a scratch folder.
    """

    def __init__(self, file_path):
        """
        Makes a scratch copy of the file at the given location.
        :param file_path:
        """
        self.src_file_path = file_path
        self.file_path = None

    def __enter__(self):
        """
        Copy the file into position.
        :return:
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
        try:
            derez_scratch_folder(self.dst_folder_path)
        except os.WindowsError:
            shutil.rmtree(self.dst_folder_path)


class TemporaryFile(object):
    def __init__(self, suffix="", prefix="", dir=None, mode="w+b"):
        if prefix is None:
            prefix = ""
        if suffix is None:
            suffix = ""
        if dir is None:
            dir = base_dir()
        self.prefix, self.suffix, self.dir, self.mode = prefix, suffix, dir, mode
        self._file = None

    def __enter__(self):
        fd, name = _make_file(self.suffix, self.prefix, self.dir)
        self._file = os.fdopen(fd, self.mode)
        self._name = name
        self._file.close()
        return name

    def __exit__(self, *args):
        cleanup(self._name)


class PersistentTemporaryFile(object):
    """
    A file-like object that is a temporary file that is available even after being closed on all platforms.
    It is automatically deleted on normal program termination.
    """

    _file = None

    def __init__(self, suffix="", prefix="", dir=None, mode="w+b"):
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
        if name == "name":
            return self.__dict__["_name"]
        return getattr(self.__dict__["_file"], name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass


class TemporaryDirectory(object):
    """
    A temporary directory intended to be used in a with statement.
    Creates a temporary dictionary in the LiuXin_scratch folder.
    Then cleans it up in the exit phase.
    """

    def __init__(self, suffix="", prefix="", dir=None, mode="w+b", keep=False):
        """
        Starts up the temporary directory.
        :param suffix:
        :param prefix:
        :param dir:
        :param mode:
        :param keep:
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
        :return temp_folder_path:
        """
        self.path = make_path(self.suffix, self.prefix, self.dir)

        os.mkdir(self.path)
        return self.path

    def __exit__(self, *args):
        try:
            if not self.keep:
                shutil.rmtree(self.path)
        except os.WindowsError:
            shutil.rmtree(self.path)

        atexit.register(safe_at_exit_remove_dir, self.path)


def remove_dir(x):
    try:
        import shutil

        shutil.rmtree(x, ignore_errors=True)
    except:
        pass


def PersistentTemporaryDirectory(suffix="", prefix="", dir=None):
    """
    Return the path to a newly created temporary directory that will be automatically deleted on application exit.
    :param suffix:
    :param prefix:
    :param dir:
    :return:
    """
    if dir is None:
        dir = base_dir()
    tdir = _make_dir(suffix, prefix, dir)

    atexit.register(remove_dir, tdir)
    return tdir


class SpooledTemporaryFile(tempfile.SpooledTemporaryFile):
    """
    SpooledTemporaryFile from tempfile, with some additional properties to make it more file like.
    """

    def __init__(self, max_size=0, suffix="", prefix="", dir=None, mode="w+b", bufsize=-1):
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
        self._file.truncate(*args)

    @property
    def name(self):
        return self.__file_name

    @name.setter
    def name(self, val):
        self.__file_name = val


def better_mktemp(*args, **kwargs):
    """
    Make a temporary directory using the tempfile.mkstemp command.
    :param args:
    :param kwargs:
    :return:
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
        Construct a ram disk - of size 512M.
        May fail if the LiuXin path contains unicode characters.
        :param force_mountpoint: Either bool (False) or a path to the location you want the mountpoint (for security
                                 reasons should fail unless the path is in the LiuXin scratch folder and the name of the
                                  given mountpoint is ASCII)
        :param write_creation_traceback: If True then will write the traceback of the function which created this ramdisk
                                         into the root of the ramdisk.
                                         Used when debugging when trying to determine where the ramdisk was generated from.
        :param persist: Persist this ramdisk after the process dies.
                        Default is False - the ramdisk will be removed when this process shuts down.
        :param size: The size of the ramdisk to create - defaults to 512M
        :param folder_fallback: If True, then, if we cannot create a ramdisk - will create and return a scratch folder.
                                Which is sub-optimal
        :param retry: Trying to work around "Resource temporarily unavailable" error - on a fail, wait te given number
                      of seconds
        :param use_dev_shm: If True, then we'll try and use the temp file system which
        :return:
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
        Construct a ram disk - of size 512MB.
        May fail if the LiuXin path contains unicode characters.
        :param force_mountpoint: Either bool (False) or a path to the location you want the mountpoint (for security reasons
                                 should fail unless the path is in the LiuXin scratch folder and the name of the given
                                 mountpoint is ASCII)
        :param write_creation_traceback: If True then will write the traceback of the function which created this ramdisk
                                         into the root of the ramdisk.
                                         Used when debugging when trying to determine where the ramdisk was generated from.
        :return:
        """
        ramdisk_mountpoint = get_scratch_folder_path("RAMDISK") if not force_mountpoint else force_mountpoint
        return os.path.join("L:", ramdisk_mountpoint)


# Todo: VERY BADLY NAMED - GIVEN WHAT DEREZ_SCRATCH_FOLDER DOES ABOVE!
def derez_ramdisk(mountpoint):
    """
    Empties a given ramdisk
    Does not actually remove the disk.
    All data in the ramdisk will be permanently lost and the folder it was mounted on will be removed.
    :param mountpoint:
    :return:
    """
    if mountpoint not in known_ramdisks:
        err_str = "mountpoint was not known to this process. Cannot remove arbitary mountpoints for security reasons\n"
        err_str += "mountpoint: {}".format(mountpoint)
        raise InputIntegrityError(err_str)

    mountpoint_objs = os.listdir(mountpoint)
    for obj in mountpoint_objs:
        shutil.rmtree(os.path.join(mountpoint, obj))


def safe_unmount_ramdisk(mountpoint):

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
    :param mountpoint:
    :return:
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
    :return:
    """
    global known_ramdisks
    return len(known_ramdisks)


class BaseScratchFolderManager(object):
    """
    Base class from which more sophisticated scratch folder managers can be derived.
    """

    def __init__(self, make_in=None, only_derez_own=False):
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
        :return:
        """
        return self.pinned_folders

    @property
    def made(self):
        """
        Return all folders which have been created by the folder store manager.
        :return:
        """
        return self.made_folders

    def get_scratch_folder(self, filename=None, pinned=False, base_filename=False):
        """
        Returns the path for a new scratch folder in the base_scratch_loc.
        :param filename: If not None, then this name will be appended to the end of the newly generated folder name.
        :param pinned: If True then the folder will be added to the pinned folders - which will not be removed when
                       clear is called.
        :param base_filename: If True, then will use the raw foldername rather than making a name based on it.
        :return:
        """
        raise NotImplementedError

    def get_scratch_folder_name(self, filename=None, base_filename=False):
        """
        Gets a valid name for a scratch folder in the current base folder.
        :param filename:
        :param base_filename:
        :return:
        """
        raise NotImplementedError

    def derez_scratch_folder(self, folder_path):
        """
        Removes a scratch folder from the current base_scratch_loc. Will error if the folder isn't in the current
        base_scratch_loc or if the only_derez_own flag is set to True and the folder was not created by this instance
        of this class.
        :param folder_path:
        :return bool: Either True or AssertionError
        """
        raise NotImplementedError

    def clear(self):
        """
        Delete all the folders that have been created by this folder store.
        :return:
        """
        raise NotImplementedError

    def __iter__(self):
        """
        Iterates over all the files currently under management by this class.
        :return:
        """
        for file_path in self.made_folders:
            yield file_path

    def __contains__(self, item):
        return item in self.made_folders

    def __len__(self):
        """
        Return the number of files currently under management.
        :return:
        """
        return len(self.made_folders)


class DummyScratchFolderManager(BaseScratchFolderManager):
    """
    Dummy to be used as a default for objects which might be passed a ScratchFolderManager - provides the same interface
    as the ScratchFolderManager but just provides a passthrough to the main scratch folder methods.
    """

    def __init__(self, make_in=None, only_derez_own=False):
        BaseScratchFolderManager.__init__(self, make_in=make_in, only_derez_own=only_derez_own)

    def get_scratch_folder(self, filename=None, pinned=False, base_filename=False):
        return get_scratch_folder(filename)

    def get_scratch_folder_name(self, filename=None, base_filename=False):
        return get_scratch_folder_path(filename)

    def derez_scratch_folder(self, folder_path):
        return derez_scratch_folder(folder_path)


scratch_folder_manager = DummyScratchFolderManager()


class ScratchFolderManager(BaseScratchFolderManager):
    """
    Object which produces and manages scratch folders.
    Useful when you want to taylor the behavior of scratch folders more closely - for example if you want some
    subprocess to generate scratch folders in a different location to the default.
    This is useful if you want to speed up test performance by creating all scratch folders in a ramdisk.
    By default just uses the regular scratch folder system.
    """

    def __init__(self, make_in=None, only_derez_own=False):
        """
        Starts up the Manager - by default behaves just like a regular scratch folder.
        :param make_in: Default location to put all the scratch folders
        :param only_derez_own: If True, then this class will only remove folders that it created
        """
        BaseScratchFolderManager.__init__(self, make_in=make_in, only_derez_own=only_derez_own)

    if iswindows:

        def get_scratch_folder(self, filename=None, pinned=False, base_filename=False):
            """
            Returns the path for a new scratch folder in the base_scratch_loc.
            :param filename: If not None, then this name will be appended to the end of the newly generated folder name.
            :param pinned: If True then the folder will be added to the pinned folders - which will not be removed when
                           clear is called.
            :param base_filename: If True, then will use the raw foldername rather than making a name based on it.
            :return:
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
            :param filename: If not None, then this name will be appended to the end of the newly generated folder name.
            :param pinned: If True then the folder will be added to the pinned folders - which will not be removed when
                           clear is called.
            :param base_filename: If True, then will use the raw foldername rather than making a name based on it.
            :return:
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
        :param filename:
        :param base_filename:
        :return:
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
        Removes a scratch folder from the current base_scratch_loc. Will error if the folder isn't in the current
        base_scratch_loc or if the only_derez_own flag is set to True and the folder was not created by this instance
        of this class.
        :param folder_path:
        :return bool: Either True or AssertionError
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
        :param src_folder_path:
        :return:
        """
        src_folder_name = os.path.split(src_folder_path)[1]
        scratch_folder = self.get_scratch_folder()
        dst_folder_path = os.path.join(scratch_folder, src_folder_name)
        shutil.copytree(src=src_folder_path, dst=dst_folder_path)
        return dst_folder_path

    def clear(self):
        """
        Delete all the folders that have been created by this folder store.
        :return:
        """
        folders_for_remove = deepcopy(self.made_folders)
        for folder_path in folders_for_remove:
            if folder_path in self.pinned_folders:
                continue
            self.derez_scratch_folder(folder_path)

    def purge(self):
        """
        Delete ALL the folders that have been created by this folder store.
        :return:
        """
        folders_for_remove = deepcopy(self.made_folders)
        for folder_path in folders_for_remove:
            self.derez_scratch_folder(folder_path)


def safe_at_exit_remove_dir(dir_path):
    """
    Remove a directory without throwing an error if the directory does, in fact, not exist
    :param dir_path:
    :return:
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
