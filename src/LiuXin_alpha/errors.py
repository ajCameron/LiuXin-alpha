
"""
Define application, storage, metadata and conversion error contracts.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise errors through a consuming regression::

        python -m pytest -q tests/surfaces/test_surface_read_errors.py
"""

import logging

logger = logging.getLogger(__name__)


def _log_exception_message(message):
    """
    Perform the log exception message operation under explicit file-format and conversion rules.

    Example:
        Exercise  log exception message through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param message: Value supplied for message under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if message is not None:
        logger.error("%s", message)


# -------------
# - BASE ERRORS


class LiuXinException(Exception):
    """
    Base LiuXin exception - should be at the root of every exception.

    Example:
        Exercise LiuXinException through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """


class BadInputException(LiuXinException):
    """
    Are you _sure_ you meant that?

    Example:
        Exercise BadInputException through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument=None):
        """
        Initialize and validate the badinputexception state.

        Example:
            Exercise BadInputException.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if argument is not None:
            self.argument = argument
            if argument is not None:
                _log_exception_message(self.argument)
        else:
            pass

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise BadInputException.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)

InputIntegrityError = BadInputException


class LogicalError(LiuXinException):
    """
    Report a logicalerror encountered while processing an ebook format.

    Example:
        Exercise LogicalError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument=None):
        """
        Initialize and validate the logicalerror state.

        Example:
            Exercise LogicalError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if argument is not None:
            self.argument = argument
            if argument is not None:
                _log_exception_message(self.argument)
        else:
            pass

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise LogicalError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)



class ImportError(LiuXinException):
    """
    Something has gone wrong while trying to run an import.

    Example:
        Exercise ImportError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument=None):
        """
        Initialize and validate the importerror state.

        Example:
            Exercise ImportError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if argument is not None:
            self.argument = argument
            if argument is not None:
                _log_exception_message(self.argument)
        else:
            pass

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise ImportError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


class TimedOutError(LiuXinException):
    """
    To be used when a process has timed out - generic replacement which should always be raised instead of whatever custom exception the code originally raised.

    Example:
        Exercise TimedOutError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass



class PreferenceError(LiuXinException):
    """
    Something has gone wrong with the preferences class.

    Example:
        Exercise PreferenceError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument):
        """
        Initialize and validate the preferenceerror state.

        Example:
            Exercise PreferenceError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise PreferenceError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


class PlatformError(LiuXinException):
    """
    Error for when something goes wrong due to a method intended for another platform being called on the wrong platform.

    Example:
        Exercise PlatformError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the platformerror state.

        Example:
            Exercise PlatformError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise PlatformError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)




class LXImportError(LiuXinException):
    """
    Error for when something goes wrong when trying to import a file or folder.

    Example:
        Exercise LXImportError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the lximporterror state.

        Example:
            Exercise LXImportError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise LXImportError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


class LocationError(LiuXinException):
    """
    Error for when something goes wrong when trying to read/write/determine properties of a location .

    Example:
        Exercise LocationError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the locationerror state.

        Example:
            Exercise LocationError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise LocationError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


# -------------


# -----------------
# - DATABASE ERRORS

class LiuXinDatabaseException(LiuXinException):
    """
    Base LiuXin database exception - should be at the root of every database exception.

    Example:
        Exercise LiuXinDatabaseException through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class WrongTypedOfTableException(LiuXinDatabaseException):
    """
    You have assumed this table is of the wrong type.

    Example:
        Exercise WrongTypedOfTableException through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class NoSuchPropertyForLinkException(LiuXinDatabaseException):
    """
    You have tried to acces/set a property that does not exist on the link.

    Example:
        Exercise NoSuchPropertyForLinkException through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class InvalidUpdate(LiuXinDatabaseException):
    """
    Generic class for when a database update cannot be processed.

    Example:
        Exercise InvalidUpdate through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    pass


class InvalidCacheUpdate(InvalidUpdate):
    """
    Called when an update to the cache is invalid for some reason. Subclass this error for the specific ways that the update might be invalid.

    Example:
        Exercise InvalidCacheUpdate through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class InvalidDBUpdate(InvalidUpdate):
    """
    Called when an update to the database is invalid for some reason. Subclass this error for the specific ways that the update might be invalid.

    Example:
        Exercise InvalidDBUpdate through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class NotInDatabaseError(LiuXinDatabaseException):
    """
    Called when the database fails to retrieve a row which is expected to be found.

    Example:
        Exercise NotInDatabaseError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class RowReadOnlyError(LiuXinDatabaseException):
    """
    Called when you try and update the database through a row which is read only.

    Example:
        Exercise RowReadOnlyError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class DatabaseIntegrityError(LiuXinDatabaseException):
    """
    Something has brought the database into a compromised state.

    Example:
        Exercise DatabaseIntegrityError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument):
        """
        Initialize and validate the databaseintegrityerror state.

        Example:
            Exercise DatabaseIntegrityError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if argument is not None:
            self.argument = argument
        else:
            self.argument = None

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseIntegrityError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)  # Todo: Should be unicode


class DatabaseConstraintError(LiuXinDatabaseException):
    """
    Report a databaseconstrainterror encountered while processing an ebook format.

    Example:
        Exercise DatabaseConstraintError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument):
        """
        Initialize and validate the databaseconstrainterror state.

        Example:
            Exercise DatabaseConstraintError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if argument is not None:
            self.argument = argument
        else:
            self.argument = None

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseConstraintError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)  # Todo: Should be unicode


class DatabaseDriverError(LiuXinDatabaseException):
    """
    Attempting to load a database driver has failed.

    Example:
        Exercise DatabaseDriverError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument=None):
        """
        Initialize and validate the databasedrivererror state.

        Example:
            Exercise DatabaseDriverError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if argument is not None:
            self.argument = argument
            if argument is not None:
                _log_exception_message(self.argument)
        else:
            pass

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise DatabaseDriverError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


class RowIntegrityError(LiuXinDatabaseException):
    """
    You've tried to do something with a row which is not supported.

    Example:
        Exercise RowIntegrityError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument):
        """
        Initialize and validate the rowintegrityerror state.

        Example:
            Exercise RowIntegrityError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise RowIntegrityError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)

class SQLParseError(LiuXinDatabaseException):
    """
    Report a sqlparseerror encountered while processing an ebook format.

    Example:
        Exercise SQLParseError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument):
        """
        Initialize and validate the sqlparseerror state.

        Example:
            Exercise SQLParseError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise SQLParseError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)



class SearchParseError(LiuXinDatabaseException):
    """
    Error for when parsing a search query goes horribly wrong.

    Example:
        Exercise SearchParseError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the searchparseerror state.

        Example:
            Exercise SearchParseError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise SearchParseError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


# -----------------

# -----------------
# - TEST EXCEPTIONS

class CompromisedTestObjectCache(LiuXinException):
    """
    Raised when it's detected that the in memory cache for test objects is in a compromised state.

    Example:
        Exercise CompromisedTestObjectCache through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass

# -----------------

# -------------------
# - CONVERSION ERRORS

class LiuXinConversionError(LiuXinException):
    """
    Report a liuxinconversionerror encountered while processing an ebook format.

    Example:
        Exercise LiuXinConversionError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, msg, only_msg=False):
        """
        Initialize and validate the liuxinconversionerror state.

        Example:
            Exercise LiuXinConversionError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param msg: Value supplied for msg under the utility contract.
        :param only_msg: Value supplied for only msg under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Exception.__init__(self, msg)
        self.only_msg = only_msg


ConversionError = LiuXinConversionError


class UnknownFormatError(Exception):
    """
    Report a unknownformaterror encountered while processing an ebook format.

    Example:
        Exercise UnknownFormatError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class DRMError(ValueError):
    """
    Report a drmerror encountered while processing an ebook format.

    Example:
        Exercise DRMError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class ParserError(ValueError):
    """
    Report a parsererror encountered while processing an ebook format.

    Example:
        Exercise ParserError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


# -------------------

# -----------------
# - RESOURCE ERRORS

class ResourceError(LiuXinException):
    """
    Error for when something goes wrong at the Folder level.

    Example:
        Exercise ResourceError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the resourceerror state.

        Example:
            Exercise ResourceError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)

# -----------------


class NoSuchBook(Exception):
    """
    Provide the nosuchbook contract for validated ebook processing.

    Example:
        Exercise NoSuchBook through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class CacheError(LiuXinException):
    """
    Report a cacheerror encountered while processing an ebook format.

    Example:
        Exercise CacheError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class CacheLoadError(CacheError):
    """
    Attempting to load a cache off the database fails.

    Example:
        Exercise CacheLoadError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """


class NotInCache(CacheError):
    """
    Provide the notincache contract for validated ebook processing.

    Example:
        Exercise NotInCache through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class WrongTypeOfCacheTable(CacheError):
    """
    Attempting to call a method on a table which makes no sense for it.

    Example:
        Exercise WrongTypeOfCacheTable through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """





class NoSuchFormatInCache(NotInCache):
    """
    Provide the nosuchformatincache contract for validated ebook processing.

    Example:
        Exercise NoSuchFormatInCache through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class NotInCoverCache(NotInCache):
    """
    Provide the notincovercache contract for validated ebook processing.

    Example:
        Exercise NotInCoverCache through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class ImportCacheError(Exception):
    """
    Report a importcacheerror encountered while processing an ebook format.

    Example:
        Exercise ImportCacheError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class NotInImportCache(NotInCache):
    """
    Provide the notinimportcache contract for validated ebook processing.

    Example:
        Exercise NotInImportCache through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass










# ---------------------
# - FOLDER STORE ERRORS


class FolderStoreIntegrityError(LiuXinException):
    """
    Report a folderstoreintegrityerror encountered while processing an ebook format.

    Example:
        Exercise FolderStoreIntegrityError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    def __init__(self, argument=None):
        """
        Initialize and validate the folderstoreintegrityerror state.

        Example:
            Exercise FolderStoreIntegrityError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if argument is not None:
            self.argument = argument
            if argument is not None:
                _log_exception_message(self.argument)
        else:
            pass

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise FolderStoreIntegrityError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


class InvalidFolderStore(LiuXinException):
    """
    An exception raised if there is some terminal problem importing a folder store driver.

    Example:
        Exercise InvalidFolderStore through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the invalidfolderstore state.

        Example:
            Exercise InvalidFolderStore.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.err_str = argument
        if argument is not None:
            _log_exception_message(self.err_str)


class InvalidFolderStoreDriver(LiuXinException):
    """
    An exception raised if there is some terminal problem importing a folder store driver.

    Example:
        Exercise InvalidFolderStoreDriver through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, err_str):
        """
        Initialize and validate the invalidfolderstoredriver state.

        Example:
            Exercise InvalidFolderStoreDriver.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param err_str: Value supplied for err str under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.err_str = err_str
        _log_exception_message(self.err_str)



class FSDriverError(LiuXinException):
    """
    Error for when something goes wrong at the FolderStoreDriver level.

    Example:
        Exercise FSDriverError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the fsdrivererror state.

        Example:
            Exercise FSDriverError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise FSDriverError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


class FSError(LiuXinException):
    """
    Error for when something goes wrong at the FolderStoreDriver level.

    Example:
        Exercise FSError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the fserror state.

        Example:
            Exercise FSError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise FSError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)


class FolderStoreError(LiuXinException):
    """
    Error for when something goes wrong at the FolderStoreDriver level.

    Example:
        Exercise FolderStoreError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the folderstoreerror state.

        Example:
            Exercise FolderStoreError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise FolderStoreError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)



class FolderError(LiuXinException):
    """
    Error for when something goes wrong at the Folder level.

    Example:
        Exercise FolderError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    def __init__(self, argument):
        """
        Initialize and validate the foldererror state.

        Example:
            Exercise FolderError.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :param argument: Value supplied for argument under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.argument = argument
        if argument is not None:
            _log_exception_message(self.argument)

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise FolderError.  str   through a consuming regression::

                python -m pytest -q tests/surfaces/test_surface_read_errors.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self.argument)



# ---------------------























# ----------------------------------------------------------------------------------------------------------------------
#
# - PLUGIN ERRORS START HERE
#
# ----------------------------------------------------------------------------------------------------------------------


class PluginNotFound(ValueError):
    """
    Provide the pluginnotfound contract for validated ebook processing.

    Example:
        Exercise PluginNotFound through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class InvalidPlugin(ValueError):
    """
    Provide the invalidplugin contract for validated ebook processing.

    Example:
        Exercise InvalidPlugin through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class MetadataReadError(Exception):
    """
    Report a metadatareaderror encountered while processing an ebook format.

    Example:
        Exercise MetadataReadError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass


class ArchiveError(Exception):
    """
    Generic exception to be raised if an archive cannot be expanded for some reason.

    Example:
        Exercise ArchiveError through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """

    pass


# calibre exception
class NoSuchFormat(ValueError):
    """
    Provide the nosuchformat contract for validated ebook processing.

    Example:
        Exercise NoSuchFormat through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py
    """
    pass
