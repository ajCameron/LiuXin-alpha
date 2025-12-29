
"""
Base class for ALL errors that LiuXin should ever throw.
"""

# -------------
# - BASE ERRORS


class LiuXinException(Exception):
    """
    Base LiuXin exception - should be at the root of every exception.
    """


class BadInputException(LiuXinException):
    """
    Are you _sure_ you meant that?
    """
    def __init__(self, argument=None):
        if argument is not None:
            self.argument = argument
            if argument is not None:
                print(self.argument)
        else:
            pass

    def __str__(self):
        return repr(self.argument)

InputIntegrityError = BadInputException


class LogicalError(LiuXinException):
    def __init__(self, argument=None):
        if argument is not None:
            self.argument = argument
            if argument is not None:
                print(self.argument)
        else:
            pass

    def __str__(self):
        return repr(self.argument)



class ImportError(LiuXinException):
    """
    Something has gone wrong while trying to run an import.
    """
    def __init__(self, argument=None):
        if argument is not None:
            self.argument = argument
            if argument is not None:
                print(self.argument)
        else:
            pass

    def __str__(self):
        return repr(self.argument)


class TimedOutError(LiuXinException):
    """
    To be used when a process has timed out - generic replacement which should always be raised instead of whatever
    custom exception the code originally raised.
    """
    pass



class PreferenceError(LiuXinException):
    """
    Something has gone wrong with the preferences class.
    """
    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)


class PlatformError(LiuXinException):
    """
    Error for when something goes wrong due to a method intended for another platform being called on the wrong
    platform.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)




class LXImportError(LiuXinException):
    """
    Error for when something goes wrong when trying to import a file or folder.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)


class LocationError(LiuXinException):
    """
    Error for when something goes wrong when trying to read/write/determine properties of a location .
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)


# -------------


# -----------------
# - DATABASE ERRORS

class LiuXinDatabaseException(LiuXinException):
    """
    Base LiuXin database exception - should be at the root of every database exception.
    """


class InvalidUpdate(LiuXinDatabaseException):
    """
    Generic class for when a database update cannot be processed.
    """

    pass


class InvalidCacheUpdate(InvalidUpdate):
    """
    Called when an update to the cache is invalid for some reason.
    Subclass this error for the specific ways that the update might be invalid.
    """
    pass


class InvalidDBUpdate(InvalidUpdate):
    """
    Called when an update to the database is invalid for some reason.
    Subclass this error for the specific ways that the update might be invalid.
    """
    pass


class NotInDatabaseError(LiuXinDatabaseException):
    """
    Called when the database fails to retrieve a row which is expected to be found.
    """
    pass


class RowReadOnlyError(LiuXinDatabaseException):
    """
    Called when you try and update the database through a row which is read only.
    """
    pass


class DatabaseIntegrityError(LiuXinDatabaseException):
    """
    Something has brought the database into a compromised state.
    """
    def __init__(self, argument):
        if argument is not None:
            self.argument = argument
        else:
            self.argument = None

    def __str__(self):
        return repr(self.argument)  # Todo: Should be unicode


class DatabaseConstraintError(LiuXinDatabaseException):
    def __init__(self, argument):
        if argument is not None:
            self.argument = argument
        else:
            self.argument = None

    def __str__(self):
        return repr(self.argument)  # Todo: Should be unicode


class DatabaseDriverError(LiuXinDatabaseException):
    """
    Attempting to load a database driver has failed.
    """
    def __init__(self, argument=None):
        if argument is not None:
            self.argument = argument
            if argument is not None:
                print(self.argument)
        else:
            pass

    def __str__(self):
        return repr(self.argument)


class RowIntegrityError(LiuXinDatabaseException):
    """
    You've tried to do something with a row which is not supported.
    """
    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)

class SQLParseError(LiuXinDatabaseException):
    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)



class SearchParseError(LiuXinDatabaseException):
    """
    Error for when parsing a search query goes horribly wrong.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)


# -----------------

# -----------------
# - TEST EXCEPTIONS

class CompromisedTestObjectCache(LiuXinException):
    """
    Raised when it's detected that the in memory cache for test objects is in a compromised state.
    """
    pass

# -----------------

# -------------------
# - CONVERSION ERRORS

class LiuXinConversionError(LiuXinException):
    def __init__(self, msg, only_msg=False):
        Exception.__init__(self, msg)
        self.only_msg = only_msg


ConversionError = LiuXinConversionError


class UnknownFormatError(Exception):
    """

    """
    pass


class DRMError(ValueError):
    pass


class ParserError(ValueError):
    pass


# -------------------

# -----------------
# - RESOURCE ERRORS

class ResourceError(LiuXinException):
    """
    Error for when something goes wrong at the Folder level.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)

# -----------------


class NoSuchBook(Exception):
    pass


class NotInCache(Exception):
    pass


class NoSuchFormatInCache(NotInCache):
    pass


class NotInCoverCache(NotInCache):
    pass


class ImportCacheError(Exception):
    pass


class NotInImportCache(NotInCache):
    pass










# ---------------------
# - FOLDER STORE ERRORS


class FolderStoreIntegrityError(LiuXinException):
    def __init__(self, argument=None):
        if argument is not None:
            self.argument = argument
            if argument is not None:
                print(self.argument)
        else:
            pass

    def __str__(self):
        return repr(self.argument)


class InvalidFolderStore(LiuXinException):
    """
    An exception raised if there is some terminal problem importing a folder store driver.
    """

    def __init__(self, argument):
        self.err_str = argument
        if argument is not None:
            print(self.err_str)


class InvalidFolderStoreDriver(LiuXinException):
    """
    An exception raised if there is some terminal problem importing a folder store driver.
    """

    def __init__(self, err_str):
        self.err_str = err_str
        print(self.err_str)



class FSDriverError(LiuXinException):
    """
    Error for when something goes wrong at the FolderStoreDriver level.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)


class FSError(LiuXinException):
    """
    Error for when something goes wrong at the FolderStoreDriver level.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)


class FolderStoreError(LiuXinException):
    """
    Error for when something goes wrong at the FolderStoreDriver level.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)



class FolderError(LiuXinException):
    """
    Error for when something goes wrong at the Folder level.
    """

    def __init__(self, argument):
        self.argument = argument
        if argument is not None:
            print(self.argument)

    def __str__(self):
        return repr(self.argument)



# ---------------------























# ----------------------------------------------------------------------------------------------------------------------
#
# - PLUGIN ERRORS START HERE
#
# ----------------------------------------------------------------------------------------------------------------------


class PluginNotFound(ValueError):
    pass


class InvalidPlugin(ValueError):
    pass


class MetadataReadError(Exception):
    pass


class ArchiveError(Exception):
    """
    Generic exception to be raised if an archive cannot be expanded for some reason.
    """

    pass


# calibre exception
class NoSuchFormat(ValueError):
    pass
