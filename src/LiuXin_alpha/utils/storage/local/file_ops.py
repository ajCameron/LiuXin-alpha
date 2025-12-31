#!/usr/bin/env python
# -*- coding: latin-1 -*-

from __future__ import print_function


# Enhanced file copy functions
# File copy functionality with added secure hashing sauce
# Liu Xin - 17th July 2014

# Todo: Move this up into the __init__ - file_ops.file_ops is stupid
# Todo: Write an upgraded file hasher that can handle arbitrary file like objects
# Recall the method in databases.backend copy_cover_to to find the size of an arbitary file like object

import re
import os
import time
import hashlib
import shutil
import platform
import ctypes
import zipfile
from os.path import join, getsize
from copy import deepcopy

from io import StringIO, BytesIO

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

# from LiuXin.utils.calibre.constants import iswindows, islinux

from LiuXin_alpha.utils.logging import LiuXin_print

from LiuXin_alpha.utils.which_os import iswindows, isosx, islinux



def local_open(name, mode="r", bufsize=-1):
    """
    Open a file that won't be inherited by child processes.

    Only supports the following modes:
        r, w, a, rb, wb, ab, r+, w+, a+, r+b, w+b, a+b
    :param name:
    :param mode:
    :param bufsize:
    """
    import fcntl
    from LiuXin_alpha.utils.which_os import iswindows, islinux

    if iswindows:

        class fwrapper(object):
            def __init__(self, name, fobject):
                object.__setattr__(self, "fobject", fobject)
                object.__setattr__(self, "name", name)

            def __getattribute__(self, attr):
                if attr in (
                    "name",
                    "__enter__",
                    "__str__",
                    "__unicode__",
                    "__repr__",
                    "__exit__",
                ):
                    return object.__getattribute__(self, attr)
                fobject = object.__getattribute__(self, "fobject")
                return getattr(fobject, attr)

            def __setattr__(self, attr, val):
                fobject = object.__getattribute__(self, "fobject")
                return setattr(fobject, attr, val)

            def __repr__(self):
                fobject = object.__getattribute__(self, "fobject")
                name = object.__getattribute__(self, "name")
                return re.sub(r"""['"]<fdopen>['"]""", repr(name), repr(fobject))

            def __str__(self):
                return repr(self)

            def __unicode__(self):
                return repr(self).decode("utf-8")

            def __enter__(self):
                fobject = object.__getattribute__(self, "fobject")
                fobject.__enter__()
                return self

            def __exit__(self, *args):
                fobject = object.__getattribute__(self, "fobject")
                return fobject.__exit__(*args)

        m = mode[0]
        random = len(mode) > 1 and mode[1] == "+"
        binary = mode[-1] == "b"

        if m == "a":
            flags = os.O_APPEND | os.O_RDWR
            flags |= os.O_RANDOM if random else os.O_SEQUENTIAL
        elif m == "r":
            if random:
                flags = os.O_RDWR | os.O_RANDOM
            else:
                flags = os.O_RDONLY | os.O_SEQUENTIAL
        elif m == "w":
            if random:
                flags = os.O_RDWR | os.O_RANDOM
            else:
                flags = os.O_WRONLY | os.O_SEQUENTIAL
            flags |= os.O_TRUNC | os.O_CREAT
        if binary:
            flags |= os.O_BINARY
        else:
            flags |= os.O_TEXT
        flags |= os.O_NOINHERIT
        fd = os.open(name, flags)
        ans = os.fdopen(fd, mode, bufsize)
        ans = fwrapper(name, ans)
    else:

        try:
            cloexec_flag = fcntl.FD_CLOEXEC
        except AttributeError:
            cloexec_flag = 1
        # Python 2.x uses fopen which on recent glibc/linux kernel at least respects the 'e' mode flag.
        # On OS X the e is ignored.
        # So to try
        # to get atomicity where possible we pass 'e' and then only use
        # fcntl only if CLOEXEC was not set.
        if islinux:
            mode += "e"
        ans = open(name, mode, bufsize)
        old = fcntl.fcntl(ans, fcntl.F_GETFD)
        if not (old & cloexec_flag):
            fcntl.fcntl(ans, fcntl.F_SETFD, old | cloexec_flag)

    return ans


if iswindows:

    def local_open(name, mode='r', bufsize=-1):
        mode += 'N'
        return open(name, mode, bufsize)

elif isosx:
    import fcntl
    FIOCLEX = 0x20006601
    def local_open(name, mode='r', bufsize=-1):
        ans = open(name, mode, bufsize)
        try:
            fcntl.ioctl(ans.fileno(), FIOCLEX)
        except EnvironmentError:
            fcntl.fcntl(ans, fcntl.F_SETFD, fcntl.fcntl(ans, fcntl.F_GETFD) | fcntl.FD_CLOEXEC)
        return ans
else:
    import fcntl
    try:
        cloexec_flag = fcntl.FD_CLOEXEC
    except AttributeError:
        cloexec_flag = 1
    supports_mode_e = False
    def local_open(name, mode='r', bufsize=-1):
        global supports_mode_e
        mode += 'e'
        ans = open(name, mode, bufsize)
        if supports_mode_e:
            return ans
        old = fcntl.fcntl(ans, fcntl.F_GETFD)
        if not (old & cloexec_flag):
            fcntl.fcntl(ans, fcntl.F_SETFD, old | cloexec_flag)
        else:
            supports_mode_e = True
        return ans


def standardize_ext(file_extension: str, dotted: bool = True) -> str:
    """
    Transform an extension into standardized form - ensuring that their either is or is not exactly one leading dot.

    :param file_extension:
    :param dotted:
    :return:
    """
    # Remove the dot
    bare_ext = re.sub(r"^\.*", "", file_extension)
    if dotted:
        return ".{}".format(bare_ext)
    else:
        return bare_ext


def load_file(file_path):
    """
    Load a file into memory and return it as a cStringIO object

    :param file_path:
    :return:
    """
    from LiuXin_alpha.utils.libraries.liuxin_six import six_cStringIO

    with open(file_path, "rb") as image_file:
        image_data = image_file.read()
    return six_cStringIO(image_data)


PROBLEM_NUMBER = 1


# Todo: Upgrade to handle streams as well
def file_hasher(file_in, block_size=64):
    """
    Receives a file path. Returns a hash for that file.
    Now with additional length, due to an observed collision in sha-512.
    :param file_in:
    :param block_size:
    :return file_hash:
    """
    hasher = hashlib.sha512()  # Declaring this as a default causes hash return to be non-deterministic.

    size = file_size(file_in)

    file_in_pointer = open(file_in, "r")

    buf = file_in_pointer.read(block_size)
    while len(buf) > 0:
        hasher.update(buf)
        buf = file_in_pointer.read(block_size)

    file_in_pointer.close()

    # Honestly can't believe this is needed - but I've seen a hash collision, and so it is
    return hasher.hexdigest() + six_unicode(size)


def get_files(folder_path):
    """
    Returns all the files in the folder specified by the folder_path.
    :param folder_path: Path to the folder to probe.
    :return:
    """
    object_list = os.listdir(folder_path)
    file_list = filter(
        bool,
        [n if os.path.isfile(os.path.join(folder_path, n)) else False for n in object_list],
    )
    return file_list


def get_file_paths(folder_path):
    """
    Returns paths to all the files in the folder specified by the folder_path.
    :param folder_path:
    :return:
    """
    object_list = os.listdir(folder_path)
    object_paths = [os.path.join(folder_path, n) for n in object_list]
    return filter(os.path.isfile, object_paths)


def get_folders(folder_path):
    """
    Returns a list of all the folders in the folder specified by the folder path.
    :param folder_path:
    :return:
    """
    object_list = os.listdir(folder_path)
    folder_list = filter(
        bool,
        [n if not os.path.isfile(os.path.join(folder_path, n)) else False for n in object_list],
    )
    return folder_list


def get_folder_paths(folder_path):
    """
    Returns paths to all the files in the folder specified by the folder_path.
    :param folder_path:
    :return:
    """
    object_list = os.listdir(folder_path)
    object_paths = [os.path.join(folder_path, n) for n in object_list]
    return filter(os.path.isdir, object_paths)


# Todo: You've used this when you mean ensured_copy on a number of occasions - merge
def checked_copy(file_in, file_out, blocksize=64):
    """
    Hashes the file before and after copy - checks that the file has copied successfully.
    :param file_in: Existing file to copy
    :param file_out: Non-existing location to copy the file to
    :param blocksize: Blocksize used to compute the hash - has negligible performance implications
    :return file_hash: For later use. Remember to use the same blocksize
    """
    # file_in and file_out should both be in the form of strings. This is to
    # make the syntax more comparable with shutil.copyfile()

    hash_in = file_hasher(file_in, block_size=blocksize)  # Hashing the file before copy
    shutil.copyfile(file_in, file_out)  # Actually copying the file
    hash_out = file_hasher(file_out, block_size=blocksize)

    if hash_in == hash_out:
        return hash_in
    elif hash_in != hash_out:
        return False
    else:
        print("Logical error in file_ops.checked_copy()")
        return False


def ensured_copy(file_in, file_out, blocksize=64, giveup_after=5):
    """
    Copy the file - ensuring that the hash matches - error if it doesn't
    :param file_in:
    :param file_out:
    :param blocksize:
    :param giveup_after: The number of attempts to make to copy the file before giving up and throwing an error.
    :return:
    """
    for i in range(giveup_after):
        copy_status = checked_copy(file_in, file_out, blocksize)
        if copy_status:
            return copy_status
        os.remove(file_out)

    err_str = "Ensured copy has failed more than giveup_after times - file_in: {}".format(file_in)
    raise OSError(err_str)


# Todo: Bug hunt - thought this returned JUST THE NAME not name and ext. Meant get_bare_file_name. Which I just wrote.
def get_file_name(file_path, splitter=os.path.split):
    """
    Returns the name and extension of the file - equivalent to os.path.split(file_path)[1]
    :param file_path:
    :param splitter: Function which splits the last component of the file name out.
    :return:
    """
    return os.path.split(file_path)[1]


def get_bare_file_name(file_path, splitter=os.path.split):
    """
    Return the name of the file without an extension - just the name of the file.
    :param file_path:
    :param splitter: Function to split the final component of the file path out - so that the name can be extracted
                     from it.
    :return:
    """
    file_path_local = six_unicode(file_path)

    # splitting the path down into section
    name = splitter(file_path_local)[1]

    # splitting at . - though there are examples where there are more than one
    file_name_split = name.split(".")
    if len(file_name_split) == 0:
        return ""
    elif len(file_name_split) == 1:
        return file_name_split[0]
    file_name_split = file_name_split[:-1]

    return ".".join(file_name_split)


def file_size(file_in):
    """
    Calculates the file size in bits and returns an integer (not a long).
    :param file_in:
    :return:
    """
    return int(os.path.getsize(file_in))


def get_file_extension(file_in):
    """
    Returns the extension of a file.

    :param file_in:
    :return:
    """
    file_in = six_unicode(file_in)
    file_name, file_ext = os.path.splitext(file_in)
    return file_ext


def is_file_extension_rar(
    extension: str,
) -> bool:
    """
    Analyses file extension to see if its part of a multi-part rar file

    :param extension:
    :return:
    """


    if extension.lower() == ".rar":
        return True

    try:
        test = int(extension[2:])
        if extension[:2] == ".r" and 0 <= test:
            return True
    except ValueError:
        return False
    return False

def is_name_rar_part(file_name):
    """
    Analysing the file name to see if it terminates in a string of the form part--
    :param file_name:
    :return:
    """

    file_name_first_part = get_file_name(file_name)

    file_name_second_part = get_file_extension(file_name)

    if file_name_second_part[:4].lower() == "part":
        return True

    return False


def get_remaining_size_on_disc(file_path):
    """
    Return the folder/drive free space (in bytes).
    :param file_path: Path terminating on the disc.
    :return:
    """
    if platform.system() == "Windows":
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(file_path), None, None, ctypes.pointer(free_bytes))
        return free_bytes.value
    else:
        st = os.statvfs(file_path)
        return st.f_bavail * st.f_frsize


def get_folder_size(file_path):
    """
    Walk the tree and sum the size of all the files to get the total tree size
    :param file_path:
    :return:
    """
    TotalSize = 0

    for item in os.walk(file_path):
        for current_file in item[2]:
            try:
                TotalSize = TotalSize + getsize(join(item[0], current_file))
            except:
                print("Error with file :  " + join(item[0], file))

    return TotalSize


def make_new_folder(folder_path):
    """
    Checks to see if a folder exists. If it doesn't it's created.
    :param folder_path:
    :return:
    """
    d = folder_path
    if not os.path.exists(d):
        os.makedirs(d)


# Used to analyse the structure of folders and files, to determine what should be done with them
def count_file_types(filelist):
    """
    Takes a list of file paths in the form of an index. Returns a dictionary of file extensions and file counts.
    :param filelist:
    :return:
    """
    extension_count = dict()

    for item in filelist:

        extension = get_file_extension(item)
        if extension in extension_count:
            extension_count[extension] += 1
        elif extension != "":
            extension_count[extension] = 1
        else:
            pass

    return extension_count


# Used as part of the pre-processing
def recursive_unrar_unzip(filepath):
    """
    Takes a filepath. Walks down that path, identifying the compressed files we can deal with. Uncompresses them.
    :param filepath:
    :return:
    """
    processed_files = set()  # Storing here files which have been examined, and decompressed if appropriate
    new_files = set()  # Current files, which haven't been uncompressed yet
    all_files = set()  # Storing the position of all files in the tree. Used when calculating the difference

    new_files.add(filepath)  # sp the loop doesn't imediately self terminate
    all_files.add(filepath)

    print("Starting unrar//unzip.")

    while len(new_files) > 0:

        for root, dirs, files in os.walk(filepath):  # Map the directory.

            for item in files:
                all_files.add(os.path.join(root, item))

            print(len(all_files), " found. ", len(processed_files), " processed.", "\r")

        new_files = all_files - processed_files  # Taking the difference to establish which files are new

        for item in new_files:

            if is_file_extension_rar(get_file_extension(item)) and not black_adder.is_rar_archive_book(item):
                unrar_all(item, os.path.dirname(os.path.abspath(item)))

            elif get_file_extension(item) == ".zip":
                unzip_all(item, os.path.dirname(os.path.abspath(item)))

            else:
                pass

            processed_files.add(item)

            print(len(all_files), " found. ", len(processed_files), " processed.", "\r")


def unzip_all(source_filename, destination_directory):
    """
    Takes a sourc filename, unzips everything inside it to the destination_directory.
    :param source_filename:
    :param destination_directory:
    :return:
    """

    try:
        source_filename_local = six_unicode(source_filename)
        destination_directory_local = six_unicode(destination_directory)
    except:
        try:
            print(source_filename)
            shutil.copyfile(
                source_filename,
                "C:\\Python27\\test2\\" + os.path.basename(source_filename),
            )
        except:
            print(source_filename)
            try:
                shutil.copyfile(
                    source_filename,
                    "C:\\Python27\\test2\\" + six_unicode(PROBLEM_NUMBER) + ".rar",
                )
                PROBLEM_NUMBER += 1
            except:
                print("We did everything we could. Calling it. Go in manually.")
    try:
        with zipfile.ZipFile(source_filename_local) as zf:
            zf.extractall(destination_directory_local)
    except:
        with zipfile.ZipFile(source_filename) as zf:
            zf.extractall(destination_directory)


def unrar_all(source_filename, destination_directory):
    """
    Takes a source filename, unrars everything inside it to the destination_directory.
    :param source_filename:
    :param destination_directory:
    :return:
    """

    try:
        source_filename_local = six_unicode(source_filename)
        destination_directory_local = six_unicode(destination_directory)
    except:
        try:
            print(source_filename)
            shutil.copyfile(
                source_filename,
                "C:\\Python27\\test2\\" + os.path.basename(source_filename),
            )
        except:
            print(source_filename)
            try:
                shutil.copyfile(
                    source_filename,
                    "C:\\Python27\\test2\\" + six_unicode(int(time.time() * 1000)) + ".rar",
                )
            except:
                print("We did everything we could. Calling it. Go in manually.")

    try:

        rf = rarfile.RarFile(source_filename_local)
        rarfile.UNRAR_TOOL = "C:\\Program Files (x86)\\Unrar\\UnRAR.exe"  # Feeding it the location of UnRAR.exe
        rf.extractall(path=destination_directory_local)

    except:
        try:

            rf = rarfile.RarFile(source_filename)
            rarfile.UNRAR_TOOL = "C:\\Program Files (x86)\\Unrar\\UnRAR.exe"  # Feeding it the location of UnRAR.exe
            rf.extractall(path=destination_directory)

        except:

            shutil.copyfile(
                source_filename,
                "C:\\Python27\\test2\\" + six_unicode(int(time.time() * 1000)) + ".rar",
            )
            print("Problem file detected. Moving for later examination.")
            print(PROBLEM_NUMBER)


def get_file_name_and_ext(file_path):
    """
    Gives the file name and extension from the file_path.
    :param file_path:
    :return:
    """
    file_path_local = six_unicode(file_path)

    current_letter = None
    position = len(file_path_local)

    while current_letter != "\\":
        position -= 1
        current_letter = file_path_local[position - 1]

    return file_path_local[position:]


def get_tree_size(file_path):
    """
    Walks the tree - accumulates the total size.
    :param file_path:
    :return:
    """
    total_size = 0

    for item in os.walk(file_path):
        for file in item[2]:
            try:
                total_size = total_size + getsize(join(item[0], file))
            except:
                print("Error with file: " + join(item[0], file))

    return total_size


def check_for_LiuXin_format_filename(file_name):
    """Takes a filepath. Looks a LiuXin like start to the file_name."""

    UUID_CHARACTERS = [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
        "n",
        "o",
        "p",
        "q",
        "r",
        "s",
        "t",
        "u",
        "v",
        "w",
        "x",
        "y",
        "z",
    ]

    UNICODE_NUMBERS = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]

    if len(file_name) < 9:
        return False

    for i in range(len(file_name)):  # itterating over the file name
        if i < 9 and file_name[i] not in UNICODE_NUMBERS:
            print("Number check fail.")
            return False

        elif i > 10 and i < 15 and file_name[i].lower() not in UUID_CHARACTERS:
            print("UUID character fail.")
            return False
        elif i == 10 and six_unicode(file_name[i].lower()) != "_":
            print("First underscore fail.")
            return False
        elif i == 15 and six_unicode(file_name[i].lower()) != "_":
            print("Second underscore fail.")
            return False

        else:
            return True


def rebuild_file_path(split_file_path):
    """
    Used when a path has been split down into a tuple or an index of tokens.
    Calls os.path.join repeatably with every element of the iterable.
    :param split_file_path:
    :return:
    """
    split_file_path = deepcopy(split_file_path)

    if len(split_file_path) == 0:
        return False

    current_path = split_file_path[0] + os.sep

    for part in split_file_path[1:]:
        current_path = join(current_path, part)

    return current_path


def make_free_name(original_name, forbidden_names):
    """
    Makes a new name for a file which is not degenerate with any of the existing names in that location.
    :param original_name: The name to be modified
    :param forbidden_names: Names which cannot be used
    :return:
    """
    new_name_template = "{0}_{1}{2}"

    if original_name not in forbidden_names:
        return original_name

    name_base, name_ext = os.path.splitext(original_name)

    for i in range(2, 100):
        new_name = new_name_template.format(name_base, i, name_ext)
        if new_name not in forbidden_names:
            return new_name

    raise NotImplementedError("Couldn't generate a new name - range went over 100 - suggests a faulty loop?")


def ensure_folder(folder_path):
    """
    Make a folder in the given location - if one doesn't exist.
    Will only create the folder if all folders up to it are already present.
    :param folder_path:
    :return:
    """
    if os.path.exists(folder_path):
        return
    else:
        os.makedirs(folder_path)


def tokenize_path(file_path, path_start="", splitter=os.path.split):
    """
    Split a path down into tokens -
    :param file_path: The file path to tokenize
    :param path_start: Ignore this string from the start of a path.
    :param splitter: Function to preform the splitting - defaults to os.path.split
    :return:
    """
    file_path = six_unicode(file_path)
    if path_start:
        assert file_path.startswith(path_start)
        file_path = file_path[len(path_start) :]

    current_split = splitter(file_path)
    path_tokens = []
    while current_split[1]:

        current_path = current_split[0]
        path_tokens.append(current_split[1])
        current_split = splitter(current_path)

    path_tokens.reverse()
    return path_tokens


def compress_dir(root_dir, dst_path):
    """
    Compress a directory into an archive.
    :param root_dir:
    :param dst_path: Path to the archive to write to
    :return:
    """
    from fs.osfs import OSFS
    from fs.zipfs import ZipFS
    from fs.copy import copy_fs

    with OSFS(root_dir) as src_fs:
        with ZipFS(dst_path, write=True) as dst_fs:
            copy_fs(src_fs=src_fs, dst_fs=dst_fs)

    return dst_path
