from __future__ import print_function

__author__ = "Cameron"

# The LiuXin decompression system. Used what python modules are available
# As well as making calls out to the command line. When available.
# Dynamically generates a list of the file types and options which can be decompressed.

from copy import deepcopy

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode


class DecompressException(Exception):
    pass


ARCHIVE_BLANK_ATTRIBUTES = {
    "file_path": None,
    "file_name": None,
    "file_extension": None,
    "compression_type": "unknown",
    "block_count": "unknown",
    "physical_size": "unknown",
    "final_size": "unknown",
    "multivolume": "unknown",
    "files": set(),
}


class Archive(object):
    def __init__(self):
        _data = deepcopy(ARCHIVE_BLANK_ATTRIBUTES)
        object.__setattr__(self, "_data", _data)

    def __getattr__(self, item):
        _data = object.__getattribute__(self, "_data")
        if item in _data.keys():
            return _data[item]
        else:
            raise AttributeError("Archive has no property called" + repr(item))

    def __str__(self, full_print=False):
        if not full_print:
            return self.__unicode__().encode("utf-8")

    def __unicode__(self):
        """
        A string representation of a archive object - for output to the console.
        """
        ans = []

        def format(x, y):
            candidate = None
            try:
                candidate = "%-20s: %s" % (six_unicode(x), six_unicode(y))
                # ans.append(u'%-20s: %s'%(unicode(x), unicode(y)))
            except UnicodeDecodeError:
                # Todo: Use the default encoding here
                candidate = "%-20s: %s" % (
                    six_unicode(x, "utf-8"),
                    six_unicode(y, "utf-8"),
                )
                # ans.append(u'%-20s: %s'%(unicode(x,'utf-8'), unicode(y,'utf-8')))
            finally:
                if candidate == None:
                    ans.append("%-20s: %s" % (six_unicode(x), repr(y)))
                else:
                    ans.append(candidate)

        def set_format(x, y):
            assert hasattr(y, "__iter__")
            try:
                candidate = "%-20s: %s" % (six_unicode(x), six_unicode(""))
            except UnicodeDecodeError:
                candidate = "%-20s: %s" % (
                    six_unicode(x, "utf-8"),
                    six_unicode("", "utf-8"),
                )
            ans.append(candidate)
            for item in y:
                try:
                    candidate = "%-20s: %s" % (six_unicode(""), six_unicode(item))
                except UnicodeDecodeError:
                    candidate = "%-20s: %s" % (
                        six_unicode("", "utf-8"),
                        six_unicode(item, "utf-8"),
                    )
                ans.append(candidate)

        format("file_name", self.file_name)
        format("file_extension", self.file_extension)
        format("file_path", self.file_path)
        format("compression_type", self.compression_type)
        format("block_count", self.block_count)
        format("physical_size", self.physical_size)
        set_format("files", self.files)

        return "\n".join(ans)

    def is_null(self, key):
        """
        Checks to see if a key exists (if it doesn't it throws an exception) and if it has value
        :param key: The name of a key in the archive class
        :return key_null: If the value associated to this key
        """
        if key not in ARCHIVE_BLANK_ATTRIBUTES:
            raise ValueError("Unrecognized value in Archive.")

        _data = object.__getattribute__(self, "_data")

        if ARCHIVE_BLANK_ATTRIBUTES[key] == _data[key]:
            return True
        else:
            return False

    def __setattr__(self, key, value):
        value = deepcopy(value)
        _data = object.__getattribute__(self, "_data")

        if value is None:
            print("Nothing!")
            pass

        elif key == "files":
            print("files!")
            if hasattr(value, "__iter__"):
                _data[key] = set(list(_data[key]) + list(value))
            else:
                _data[key].add(value)

        elif key in _data.keys():
            _data[key] = value

        else:
            raise AttributeError("Archive object has no such value.")

    def setattr(self, key, value):
        self.__setattr__(key, value)
