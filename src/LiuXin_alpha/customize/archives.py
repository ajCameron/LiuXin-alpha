# -*- coding: utf-8 -*-

"""

"""

from __future__ import print_function, unicode_literals

# Compressor drivers allow LiuXin to read/write to compressed file formats

import shutil
import zipfile

from LiuXin.customize import Archive


# Todo: Implement password support
class ZipfileArchive(Archive):
    """
    Interface for the Zipfile compressor object.
    """

    read_formats = {"zip"}
    write_formats = {"zip"}

    def __init__(self, file_path, mode="a", compression_flags=None, write_type="zip"):
        """
        Initialize an object representing the compressed file.
        :param file_path:
        :param mode: (remember that zipfile does not have an rb mode - everything read from the archive will be bytes
                      by default).
        :param compression_flags:
        :param write_type: The type of archive you want to write the files out to - at the moment the only option is
                           zip.
        """
        Archive.__init__(self, file_path, mode, compression_flags, write_type)
        self.arc_file = zipfile.ZipFile(file=self.file_path, mode=self.mode)

    def printdir(self):
        """
        Print the contents of a zipfile.
        :return:
        """
        self.arc_file.printdir()

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GATHER BASIC INFORMATION ABOUT THE FILE
    # ------------------------------------------------------------------------------------------------------------------
    @classmethod
    def is_valid(cls, path):
        """
        Is the file readable and valid.
        :param path:
        :return:
        """
        return zipfile.is_zipfile(filename=path)

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO READ FROM THE ARCHIVE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def getinfo(self, name):
        """
        Returns info on an element in the archive.
        Returns as a dictionary keyed with the name of the element. This is read from the ZipInfo object that is
        returned by ZipFile.
        :param name:
        :return:
        """
        return self.arc_file.getinfo(name)

    def infolist(self):
        """
        Return a list containing a ZipInfo object for each member of the archive. The objects are in the same order as
        their entries in the actual ZIP file on disk if an existing archive was opened.
        :return:
        """
        return self.arc_file.infolist()

    def namelist(self):
        """
        Returns a list of all the archive members by name.
        :return:
        """
        return self.arc_file.namelist()

    @property
    def files(self):
        """
        Returns all the files in the archive.
        :return:
        """
        # If the size of the object is non zero, then assume it's a file
        zip_files = set()
        for zip_obj in self.infolist():
            if zip_obj.file_size != 0:
                zip_files.add(zip_obj.filename)
        return zip_files

    @property
    def folders(self):
        """
        Returns all the folders in the archive.
        :return:
        """
        # If an object in the archive has zero size, and ends with the path separator, assume that it's a folder
        zip_folders = set()
        for zip_obj in self.infolist():
            if zip_obj.filename.endswith("/") and zip_obj.file_size == 0:
                zip_folders.add(zip_obj.filename)
        return zip_folders

    def extract(self, path, pwd, member):
        """
        Extract a member of the archive to the specified path with the specified password.
        :param path:
        :param pwd:
        :param member:
        :return:
        """
        return self.arc_file.extract(member=member, path=path, pwd=pwd)

    def extractall(self, path, members, pwd):
        """
        Extract all members from the archive to the current working directory.
        :param path: specifies a different directory to extract to
        :param members: is optional and must be a subset of the list returned by namelist()
        :param pwd: is the password used for encrypted files.
        :return:
        """
        return self.arc_file.extractall(path=path, members=members, pwd=pwd)

    def get_file(self, path, member, pwd=None):
        """
        Copy a file out to the given path.
        :param path:
        :param member:
        :param pwd:
        :return:
        """
        with open(path, "wb") as dst_file_path:
            with self.arc_file.open(name=member, pwd=pwd) as input_file:
                shutil.copyfileobj(input_file, dst_file_path)

    # ------------------------------------------------------------------------------------------------------------------
    # - WRITE METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------

    def write(self, filename, arcname, compress_type=None):
        """
        (from https://docs.python.org/2/library/zipfile.html#zipfile.ZipFile.write)
        Write the file named filename to the archive, giving it the archive name arcname (by default, this will be the
        same as filename, but without a drive letter and with leading path separators removed). If given, compress_type
        overrides the value given for the compression parameter to the constructor for the new entry. The archive must
        be open with mode ’w’ or ’a’ – calling write() on a ZipFile created with mode ’r’ will raise a RuntimeError.
        Note: There is no official file name encoding for ZIP files. If you have unicode file names, you must convert
        them to byte strings in your desired encoding before passing them to write(). WinZip interprets all file names
        as encoded in CP437, also known as DOS Latin.
        Note: Archive names should be relative to the archive root, that is, they should not start with a path
        separator.
        Note: If arcname (or filename, if arcname is not given) contains a null byte, the name of the file in the
        archive will be truncated at the null byte.
        :param filename:
        :param arcname:
        :param compress_type:
        :return:
        """
        self.arc_file.write(
            filename=filename,
            arcname=arcname,
            compress_type=compress_type,
        )

    def writestr(self, arcname, byte_str, compress_type=None):
        """
        Write a string to the archive. Archive must be opened with mode 'w' or 'a'
        :param arcname: file name it will be given in the archive
        :param bytes_str:
        :param compress_type:
        :return:
        """
        self.arc_file.writestr(zinfo_or_arcname=arcname, bytes=byte_str, compress_type=compress_type)

    # ------------------------------------------------------------------------------------------------------------------
    # - HELPER METHODS START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def testarc(self):
        """
        Calls the ZipFile.testzip() method.
        Read all the files in the archive and check their CRC’s and file headers. Return the name of the first bad file,
        or else return None. Calling testzip() on a closed ZipFile will raise a RuntimeError.
        :return:
        """
        return self.arc_file.testzip()

    def close(self):
        """
        Calls the ZipFile.close() method.
        Close the archive file. You must call close() before exiting your program or essential records will not be
        written.
        :return:
        """
        self.arc_file.close()


def get_compressor_plugins():
    """
    Returns the valid and functional conmpressor plugins.

    :return:
    """
    return [ZipfileArchive]
