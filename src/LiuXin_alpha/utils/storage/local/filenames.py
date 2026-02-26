"""
Make strings safe for use as ASCII filenames, while trying to preserve as much meaning as possible.
"""

import os
import errno
import time
import re
from math import ceil

from typing import Union, Optional

from LiuXin_alpha.utils.text import isbytestring

from LiuXin_alpha.constants import force_unicode, preferred_encoding, filesystem_encoding
from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.utils.which_os import iswindows

from LiuXin_alpha.utils.localization import get_udc


_filename_sanitize = re.compile(r'[\xae\0\\|\?\*<":>\+/]')
_filename_sanitize_unicode = frozenset(["\\", "|", "?", "*", "<", '"', ":", ">", "+", "/"] + list(map(chr, range(32))))


def sanitize_file_name(name: str, substitute: str = "_", as_unicode: bool = False) -> str:
    """
    Sanitize the filename `name`. All invalid characters are replaced by `substitute`.

    The set of invalid characters is the union of the invalid characters in Windows, OS X and Linux.
    Also removes leading and trailing whitespace.
    **WARNING:** This function also replaces path separators, so only pass file names and not full paths to it.
    *NOTE:* In this Python 3 port this returns text (`str`), not bytes.
    :param name:
    :param substitute:
    :param as_unicode:
    :return:
    """
    if isinstance(name, bytes):
        name = name.decode(filesystem_encoding, "replace")
    else:
        name = str(name)
    one = _filename_sanitize.sub(substitute, name)
    one = re.sub(r"\s", " ", one).strip()
    bname, ext = os.path.splitext(one)
    one = re.sub(r"^\.+$", "_", bname)
    if as_unicode:
        return one
    one = one.replace("..", substitute)
    one += ext

    # Windows doesn't like path components that end with a period
    if one and one[-1] in (".", " "):
        one = one[:-1] + "_"

    # Names starting with a period are hidden on Unix
    if one.startswith("."):
        one = "_" + one[1:]
    return one


def sanitize_file_name_unicode(name: str, substitute: str="_") -> Union[str, bytes]:
    """
    Sanitize the filename `name`. All invalid characters are replaced by `substitute`.

    The set of invalid characters is the union of the invalid characters in Windows,
    OS X and Linux. Also removes leading and trailing whitespace.
    **WARNING:** This function also replaces path separators, so only pass file names
    and not full paths to it.

    :param name:
    :param substitute:
    :return:
    """
    if isbytestring(name):
        return sanitize_file_name(name, substitute=substitute, as_unicode=True)
    chars = [substitute if c in _filename_sanitize_unicode else c for c in name]
    one = "".join(chars)
    one = re.sub(r"\s", " ", one).strip()
    bname, ext = os.path.splitext(one)
    one = re.sub(r"^\.+$", "_", bname)
    one = one.replace("..", substitute)
    one += ext
    # Windows doesn't like path components that end with a period or space
    if one and one[-1] in (".", " "):
        one = one[:-1] + "_"
    # Names starting with a period are hidden on Unix
    if one.startswith("."):
        one = "_" + one[1:]
    return one


def sanitize_file_name2(name, substitute="_"):
    """
    Sanitize filenames removing invalid chars. Keeps unicode names as unicode
    and bytestrings as bytestrings
    """
    if isbytestring(name):
        return sanitize_file_name(name, substitute=substitute)
    return sanitize_file_name_unicode(name, substitute=substitute)



# Todo: Audit the entire code base for unicode and bytes errors, because I do not understand it right
def ascii_text(orig: str) -> str:
    """
    Force a string to ASCII - if it isn't already.

    :param orig:
    :return:
    """
    udc = get_udc()
    try:
        text = udc.decode(orig)
    except Exception:
        if isinstance(orig, bytes):
            text = orig.decode(preferred_encoding, "replace")
        else:
            text = str(orig)
    return text.encode("ascii", "replace").decode("ascii")


def ascii_filename(orig: str, substitute: str = "_") -> Union[str, bytes]:
    """
    Render a filename to pure ascii.

    :param orig:
    :param substitute:
    :return:
    """
    ans = []
    orig = ascii_text(orig).replace("?", "_")
    for x in orig:
        if ord(x) < 32:
            x = substitute
        ans.append(x)
    return sanitize_file_name("".join(ans), substitute=substitute)


def supports_long_names(path: str) -> bool:
    """
    Checks to see if a drive supports long names.

    :param path:
    :return:
    """
    t = ("a" * 300) + ".txt"
    try:
        p = os.path.join(path, t)
        open(p, "wb").close()
        os.remove(p)
    except:
        return False
    else:
        return True


def shorten_component(s: str, by_what: int) -> str:
    """
    Knock the middle out of a string.

    :param s:
    :param by_what:
    :return:
    """
    l = len(s)
    if l < by_what:
        return s
    l = (l - by_what) // 2
    if l <= 0:
        return s
    return s[:l] + s[-l:]


def shorten_components_to(length: int, components: list[str], more_to_take: int = 0) -> list[str]:
    """
    Shorten each component of a list - so it can be rebuilt later into a shorter path.

    :param length:
    :param components:
    :param more_to_take:
    :return:
    """
    filepath = os.sep.join(components)
    extra = len(filepath) - (length - more_to_take)
    if extra < 1:
        return components
    deltas = []
    for x in components:
        pct = len(x) / float(len(filepath))
        deltas.append(int(ceil(pct * extra)))
    ans = []

    for i, x in enumerate(components):
        delta = deltas[i]
        if delta > len(x):
            r = x[0] if x is components[-1] else ""
        else:
            if x is components[-1]:
                b, e = os.path.splitext(x)
                if e == ".":
                    e = ""
                r = shorten_component(b, delta) + e
                if r.startswith("."):
                    r = x[0] + r
            else:
                r = shorten_component(x, delta)
            r = r.strip()
            if not r:
                r = x.strip()[0] if x.strip() else "x"
        ans.append(r)
    if len(os.sep.join(ans)) > length:
        return shorten_components_to(length, components, more_to_take + 2)
    return ans


def find_executable_in_path(name: str, path: Optional[str] = None) -> Optional[str]:
    """
    Find a given executable in the given path.

    :param name:
    :param path:
    :return:
    """
    if path is None:
        path = os.environ.get("PATH", "")
    sep = ";" if iswindows else ":"
    if iswindows and not name.endswith(".exe"):
        name += ".exe"
    path = path.split(sep)
    for x in path:
        q = os.path.abspath(os.path.join(x, name))
        if os.access(q, os.X_OK):
            return q
    return None


def is_case_sensitive(path: str) -> bool:
    """
    Return True if the filesystem the path terminates in is case-sensitive.

    path must be the path to an existing directory. You must have permission
    to create and delete files in this directory. The results of this test
    apply to the filesystem containing the directory in path.
    Tests by writing two files whose paths are the same up to different cases and checks to see if both exist
    afterwards,
    :param path:
    :return:
    """
    test_is_case_sensitive = False

    if not iswindows:
        name1, name2 = (
            "calibre_test_case_sensitivity.txt",
            "calibre_TesT_CaSe_sensitiVitY.Txt",
        )
        f1, f2 = os.path.join(path, name1), os.path.join(path, name2)
        if os.path.exists(f1):
            os.remove(f1)
        open(f1, "w").close()
        test_is_case_sensitive = not os.path.exists(f2)
        os.remove(f1)

    return test_is_case_sensitive


def case_preserving_open_file(path: Union[str, bytes], mode: str = "wb", mkdir_mode: int = 0o777):
    """
    Open the file pointed to by path with the specified mode.

    If any directories in path do not exist, they are created. Returns the
    opened file object and the path to the opened file object. This path is
    guaranteed to have the same case as the on disk path. For case-insensitive
    filesystems, the returned path may be different from the passed in path.
    The returned path is always unicode and always an absolute path.

    If mode is None, then this function assumes that path points to a directory
    and return the path to the directory as the file object.

    mkdir_mode specifies the mode with which any missing directories in path
    are created.
    """
    if isbytestring(path):
        path = path.decode(filesystem_encoding)

    path = os.path.abspath(path)

    sep = force_unicode(os.sep, "ascii")

    if path.endswith(sep):
        path = path[:-1]
    if not path:
        raise ValueError("Path must not point to root")

    components = path.split(sep)
    if not components:
        raise ValueError("Invalid path: %r" % path)

    cpath = sep
    if iswindows:
        # Always upper case the drive letter and add a trailing slash so that
        # the first os.listdir works correctly
        cpath = components[0].upper() + sep

    bdir = path if mode is None else os.path.dirname(path)
    if not os.path.exists(bdir):
        os.makedirs(bdir, mkdir_mode)

    # Walk all the directories in path, putting the on disk case version of
    # the directory into cpath
    dirs = components[1:] if mode is None else components[1:-1]
    for comp in dirs:
        cdir = os.path.join(cpath, comp)
        cl = comp.lower()
        try:
            candidates = [c for c in os.listdir(cpath) if c.lower() == cl]
        except:
            # Dont have permission to do the listdir, assume the case is
            # correct as we have no way to check it.
            pass
        else:
            if len(candidates) == 1:
                cdir = os.path.join(cpath, candidates[0])
            # else: We are on a case sensitive file system so cdir must already
            # be correct
        cpath = cdir

    if mode is None:
        ans = fpath = cpath
    else:
        fname = components[-1]
        ans = open(os.path.join(cpath, fname), mode)
        # Ensure file and all its metadata is written to disk so that subsequent
        # listdir() has file name in it. I don't know if this is actually
        # necessary, but given the diversity of platforms, best to be safe.
        ans.flush()
        os.fsync(ans.fileno())

        cl = fname.lower()
        try:
            candidates = [c for c in os.listdir(cpath) if c.lower() == cl]
        except EnvironmentError:
            # The containing directory, somehow disappeared?
            candidates = []
        if len(candidates) == 1:
            fpath = os.path.join(cpath, candidates[0])
        else:
            # We are on a case sensitive filesystem
            fpath = os.path.join(cpath, fname)
    return ans, fpath


def windows_get_fileid(path: Union[str, bytes]):
    """
    The fileid uniquely identifies actual file contents (it is the same for all hardlinks to a file).

    Similar to inode number on linux.

    :param path:
    :return:
    """
    import win32file

    # from pywintypes import error

    if isbytestring(path):
        path = path.decode(filesystem_encoding)
    try:
        h = win32file.CreateFileW(
            path,
            0,
            0,
            None,
            win32file.OPEN_EXISTING,
            win32file.FILE_FLAG_BACKUP_SEMANTICS,
            0,
        )
        try:
            data = win32file.GetFileInformationByHandle(h)
        finally:
            win32file.CloseHandle(h)
    except (Exception, EnvironmentError):
        return None
    return data[4], data[8], data[9]


def samefile_windows(src, dst):
    samestring = os.path.normcase(os.path.abspath(src)) == os.path.normcase(os.path.abspath(dst))
    if samestring:
        return True

    a, b = windows_get_fileid(src), windows_get_fileid(dst)
    if a is None and b is None:
        return False
    return a == b


def samefile(src: Union[str, bytes], dst: Union[str, bytes]) -> bool:
    """
    Check if two paths point to the same actual file on the filesystem.

    Handles symlinks, case insensitivity, mapped drives, etc.

    Returns True iff both paths exist and point to the same file on disk.

    Note: On windows will return True if the two string are identical (upto
    case) even if the file does not exist. This is because I have no way of
    knowing how reliable the GetFileInformationByHandle method is.

    :param src:
    :param dst:
    :return:
    """
    if iswindows:
        return samefile_windows(src, dst)

    if hasattr(os.path, "samefile"):
        # Unix
        try:
            return os.path.samefile(src, dst)
        except EnvironmentError:
            return False

    # All other platforms: check for same pathname.
    samestring = os.path.normcase(os.path.abspath(src)) == os.path.normcase(os.path.abspath(dst))
    return samestring


def windows_get_size(path: Union[bytes, str]) -> int:
    """
    On Windows file sizes are only accurately stored in the actual file.

    Not in the directory entry (which could be out of date).

    So we open the file, and get the actual size.
    :param path:
    :return:
    """
    import win32file

    if isbytestring(path):
        path = path.decode(filesystem_encoding)
    h = win32file.CreateFileW(
        path,
        0,
        win32file.FILE_SHARE_READ | win32file.FILE_SHARE_WRITE | win32file.FILE_SHARE_DELETE,
        None,
        win32file.OPEN_EXISTING,
        0,
        None,
    )
    try:
        return win32file.GetFileSize(h)
    finally:
        win32file.CloseHandle(h)


def windows_hardlink(src: Union[str, bytes], dest: Union[str, bytes]) -> None:
    """
    Hardlink two files.

    :param src:
    :param dest:
    :return:
    """
    import win32file, pywintypes

    try:
        win32file.CreateHardLink(dest, src)
    except pywintypes.error as e:
        msg = "Creating hardlink from %s to %s failed: %%s" % (src, dest)
        raise Exception(msg % e)
    src_size = os.path.getsize(src)

    # We open and close dest, to ensure its directory entry is updated
    # see http://blogs.msdn.com/b/oldnewthing/archive/2011/12/26/10251026.aspx
    for i in range(10):
        # If we are on a network filesystem, we have to wait for some indeterminate time, since
        # network file systems are the best thing since sliced bread
        try:
            if windows_get_size(dest) == src_size:
                return
        except EnvironmentError:
            pass
        time.sleep(0.3)

    sz = windows_get_size(dest)
    if sz != src_size:
        msg = "Creating hardlink from %s to %s failed: %%s" % (src, dest)
        raise Exception(msg % ("hardlink size: %d not the same as source size" % sz))


def windows_nlinks(path):
    import win32file

    dwFlagsAndAttributes = win32file.FILE_FLAG_BACKUP_SEMANTICS if os.path.isdir(path) else 0
    if isbytestring(path):
        path = path.decode(filesystem_encoding)
    handle = win32file.CreateFileW(
        path,
        win32file.GENERIC_READ,
        win32file.FILE_SHARE_READ,
        None,
        win32file.OPEN_EXISTING,
        dwFlagsAndAttributes,
        None,
    )
    try:
        return win32file.GetFileInformationByHandle(handle)[7]
    finally:
        handle.Close()


class WindowsAtomicFolderMove:

    """
    Move all the files inside a specified folder in an atomic fashion.

    Preventing any other process from locking a file while the operation is
    incomplete. Raises an IOError if another process has locked a file before
    the operation starts. Note that this only operates on the files in the
    folder, not any sub-folders.
    """

    def __init__(self, path: Union[str, bytes]) -> None:
        """
        Startup the mover class.

        :param path:
        """
        self.handle_map = {}

        import win32file, winerror
        from pywintypes import error
        from collections import defaultdict

        if isbytestring(path):
            path = path.decode(filesystem_encoding)

        if not os.path.exists(path):
            return

        names = os.listdir(path)
        name_to_fileid = {x: windows_get_fileid(os.path.join(path, x)) for x in names}
        fileid_to_names = defaultdict(set)
        for name, fileid in name_to_fileid.iteritems():
            fileid_to_names[fileid].add(name)

        for x in names:
            f = os.path.normcase(os.path.abspath(os.path.join(path, x)))
            if not os.path.isfile(f):
                continue
            try:
                # Ensure the file is not read-only
                win32file.SetFileAttributes(f, win32file.FILE_ATTRIBUTE_NORMAL)
            except:
                pass

            try:
                h = win32file.CreateFileW(
                    f,
                    win32file.GENERIC_READ,
                    win32file.FILE_SHARE_DELETE,
                    None,
                    win32file.OPEN_EXISTING,
                    win32file.FILE_FLAG_SEQUENTIAL_SCAN,
                    0,
                )
            except error as e:
                if getattr(e, "winerror", 0) == winerror.ERROR_SHARING_VIOLATION:
                    # The file could be a hardlink to an already opened file,
                    # in which case we use the same handle for both files
                    fileid = name_to_fileid[x]
                    found = False
                    if fileid is not None:
                        for other in fileid_to_names[fileid]:
                            other = os.path.normcase(os.path.abspath(os.path.join(path, other)))
                            if other in self.handle_map:
                                self.handle_map[f] = self.handle_map[other]
                                found = True
                                break
                    if found:
                        continue

                self.close_handles()
                if getattr(e, "winerror", 0) == winerror.ERROR_SHARING_VIOLATION:

                    from LiuXin_alpha.utils.localization import trans as _

                    err = IOError(errno.EACCES, _("File is open in another process"))
                    err.filename = f
                    raise err
                prints("CreateFile failed for: %r" % f)
                raise
            except:
                self.close_handles()
                prints("CreateFile failed for: %r" % f)
                raise
            self.handle_map[f] = h

    def copy_path_to(self, path, dest):
        """
        Copy a path to the specific destination.

        :param path:
        :param dest:
        :return:
        """

        from LiuXin_alpha.utils.storage.local.file_ops import local_open as lopen
        from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

        import win32file

        handle = None
        for p, h in iteritems(self.handle_map):
            if samefile_windows(path, p):
                handle = h
                break
        if handle is None:
            if os.path.exists(path):
                raise ValueError("The file %r did not exist when this move" " operation was started" % path)
            else:
                raise ValueError("The file %r does not exist" % path)
        try:
            windows_hardlink(path, dest)
            return
        except:
            pass

        win32file.SetFilePointer(handle, 0, win32file.FILE_BEGIN)
        with lopen(dest, "wb") as f:
            while True:
                hr, raw = win32file.ReadFile(handle, 1024 * 1024)
                if hr != 0:
                    raise IOError(hr, "Error while reading from %r" % path)
                if not raw:
                    break
                f.write(raw)

    def release_file(self, path):
        "Release the lock on the file pointed to by path. Will also release the lock on any hardlinks to path"
        key = None
        for p, h in self.handle_map.iteritems():
            if samefile_windows(path, p):
                key = (p, h)
                break
        if key is not None:
            import win32file

            win32file.CloseHandle(key[1])
            remove = [f for f, h in self.handle_map.items() if h is key[1]]
            for x in remove:
                self.handle_map.pop(x)

    def close_handles(self):
        import win32file

        for h in self.handle_map.values():
            win32file.CloseHandle(h)
        self.handle_map = {}

    def delete_originals(self):
        import win32file

        for path in self.handle_map.keys():
            win32file.DeleteFile(path)
        self.close_handles()


def hardlink_file(src: Union[str, bytes], dest: Union[str, bytes]) -> None:
    if iswindows:
        windows_hardlink(src, dest)
        return
    os.link(src, dest)


def nlinks_file(path):
    "Return number of hardlinks to the file"
    if iswindows:
        return windows_nlinks(path)
    return os.stat(path).st_nlink


def atomic_rename(oldpath, newpath):
    """
    Replace the file newpath with the file oldpath. Can fail if the files are on different volumes.

     If succeeds, guaranteed to be atomic. newpath may or may not exist. If it exists, it is replaced.
    :param oldpath:
    :param newpath:
    :return:
    """
    if iswindows:
        import win32file

        for i in range(10):
            try:
                win32file.MoveFileEx(
                    oldpath,
                    newpath,
                    win32file.MOVEFILE_REPLACE_EXISTING | win32file.MOVEFILE_WRITE_THROUGH,
                )
                break
            except Exception as e:
                if i > 8:
                    raise IOError("Atomic rename has failed after 10 attempts") from e

                # Try the rename repeatedly in case something like a virus
                # scanner has opened one of the files (I love windows)
                time.sleep(1)
    else:
        os.rename(oldpath, newpath)


def remove_dir_if_empty(path, ignore_metadata_caches=False):
    """Remove a directory if it is empty or contains only the folder metadata
    caches from different OSes. To delete the folder if it contains only
    metadata caches, set ignore_metadata_caches to True."""
    try:
        os.rmdir(path)
    except OSError as e:
        if e.errno == errno.ENOTEMPTY or len(os.listdir(path)) > 0:
            # Some linux systems appear to raise an EPERM instead of an
            # ENOTEMPTY, see https://bugs.launchpad.net/bugs/1240797
            if ignore_metadata_caches:
                try:
                    found = False
                    for x in os.listdir(path):
                        if x.lower() in {".ds_store", "thumbs.db"}:
                            found = True
                            x = os.path.join(path, x)
                            if os.path.isdir(x):
                                import shutil

                                shutil.rmtree(x)
                            else:
                                os.remove(x)
                except Exception:  # We could get an error, if, for example, windows has locked Thumbs.db
                    found = False
                if found:
                    remove_dir_if_empty(path)
            return
        raise


if iswindows:
    # Python's expanduser is broken for non-ASCII usernames
    def expanduser(path):
        if isinstance(path, bytes):
            path = path.decode(filesystem_encoding)
        if path[:1] != "~":
            return path
        i, n = 1, len(path)
        while i < n and path[i] not in "/\\":
            i += 1
        from win32com.shell import shell, shellcon

        userhome = shell.SHGetFolderPath(0, shellcon.CSIDL_PROFILE, None, 0)
        return userhome + path[i:]

else:
    expanduser = os.path.expanduser


def format_permissions(st_mode):
    import stat

    for func, letter in (x.split(":") for x in "REG:- DIR:d BLK:b CHR:c FIFO:p LNK:l SOCK:s".split()):
        if getattr(stat, "S_IS" + func)(st_mode):
            break
    else:
        letter = "?"
    rwx = ("---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx")
    ans = [letter] + list(rwx[(st_mode >> 6) & 7]) + list(rwx[(st_mode >> 3) & 7]) + list(rwx[(st_mode & 7)])
    if st_mode & stat.S_ISUID:
        ans[3] = "s" if (st_mode & stat.S_IXUSR) else "S"
    if st_mode & stat.S_ISGID:
        ans[6] = "s" if (st_mode & stat.S_IXGRP) else "l"
    if st_mode & stat.S_ISVTX:
        ans[9] = "t" if (st_mode & stat.S_IXUSR) else "T"
    return "".join(ans)
