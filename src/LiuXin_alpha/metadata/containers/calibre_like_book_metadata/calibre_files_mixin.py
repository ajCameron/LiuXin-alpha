
"""
Mixin for dealing with files with the metadata.
"""

import os



class FileMethodsMixin:
    """
    File methods mixins.
    """

    # Todo: Keep consistent with the cover
    def add_file(self, data, typ="path", file_id=None):
        """
        Takes some cover data - followed by the type of data it is. Adds it to the object.

        Cover tuples are of the form typ (generally the extension of the file) followed by data - the data the file
        is composed of.
        Note - open file handles are fragile and can be closed by return statements - if an open file handle is passed
        into this method it will be read into memory and the data included here. Dump it to disk and pass in a path
        instead?
        If you have open file handlers produced by a read process please include them in register_file_for_cleanup so
        that they can be properly closed after being added to the databases.
        :param data:
        :param typ:
        :param file_id:
        """
        # Open file handles will probabl be closed when return is called with this metadata object - so read them into
        # memory to be safe
        if hasattr(data, "read"):
            data = data.read()

        file_tuple = (typ, data)

        _data = object.__getattribute__(self, "_data")
        _data["files"][file_tuple] = file_id

        self.register_file_for_cleanup(data)

    def record_path_and_file_name(self, file_path):
        """
        For metadata completeness and later analysis the original file path/name of a file is recorded.

        This is a convenience method to add both the name of the file and the path to the file in one method from the
        original path.
        File paths recorded here WILL NOT be added to the system - if you want them to be add them using the add_file
        method - which will record them in the form of a path type file tuple.
        :param file_path: The path to the original file - name will be derieved from this.
        :return:
        """
        object.__getattribute__(self, "_data")["filepath"].append(file_path)

        file_name = os.path.split(file_path)[1]
        object.__getattribute__(self, "_data")["filename"].append(file_name)

    def register_file_for_cleanup(self, file_pointer):
        """
        Register that there might be an open file object.
        :return:
        """
        open_files = object.__getattribute__(self, "_files_for_cleanup")
        open_files.append(file_pointer)

    def close_cleanup_files(self):
        """
        Try and close all the files registered as open in the cleanup files.

        :return:
        """
        open_files = object.__getattribute__(self, "_files_for_cleanup")
        for file_pointer in open_files:
            try:
                file_pointer.close()
            except AttributeError:
                pass