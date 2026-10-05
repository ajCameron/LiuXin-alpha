# Provides a base class for the location drivers - these allow LiuXin to manage different types of folder stores in
# different types of locations

# type - Examples of possible locations on which people might want to have LiuXin folders stores are, FTP, Dropbox,
#        GoogleDrive, over a network and On_Disc (on a local disc).
# os_type - What is the current os_type?
# NEEDED_FIELDS - if a folder store of the given type is to be created what fields must be filled out in the
#               - in the corresponding folder store row
# FIELD_DEFAULTS - default values for the needed fields

"""
Expose folder-backed storage through the customization interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise folder store driver through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""
import os
from copy import deepcopy

from typing import Any, TYPE_CHECKING, Union, Optional, BinaryIO

from LiuXin_alpha.constants import get_os_type
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.folder_stores.location import Location
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


# Todo: Merge with the base class from LiuXin.folder_stores.drivers
class LocationDriver:
    """
    A driver allowing LiuXin to interface with a given location.

    Example:
        Exercise LocationDriver through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    # Todo: This needs to be renamed, but last time a straight refactor turned out to be a BAD idea
    type = None
    os_type = get_os_type()

    NEEDED_FIELDS = (
        "folder_store_id",
        "folder_store_identifier",
        "folder_store_path",
        "folder_store_path_os_type",
        "folder_store_type",
        "folder_store_marker_name",
    )
    FIELD_DEFAULTS = {
        "folder_store_type": "on_disk",
        "folder_store_cache": "1",
        "folder_store_storage": "1",
        "folder_store_allocated_size": "0",
        "folder_store_path_os_type": os_type,
    }

    def __init__(self, folder_store_row: Optional[Row] = None) -> None:
        """
        You can start up this class without a folder store row - allowing these classes to be used without a db.

        Example:
            Exercise LocationDriver.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param folder_store_row: Value supplied for folder store row under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        # The row from the folder_stores table corresponding to this store
        self.fs_row = folder_store_row

        # Has the folder_store been created yet?
        self.fs_exists = False
        # Has the folder store been checked to make sure that it still exists?
        self.fs_checked = False

        # Some basic checks sould be done on the store during init - the results are stored here
        self.readable = False
        self.writeable = False

        # Allows details of the database on which this folder store lives to be recorded
        self.db = None

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - OUTPUT METHODS FOR DEBUGGING START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __str__(self) -> str:
        """
        Returns a string representation of the object.

        Example:
            Exercise LocationDriver.  str   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__unicode__()

    def __unicode__(self) -> str:
        """
        Produced a unicode representation of the FolderStoreDriver.

        Example:
            Exercise LocationDriver.  unicode   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []

        def uni_format(x: Any, y: Any) -> None:
            """
            Attempts to safely format an object and add it to the representation.

            Example:
                Exercise LocationDriver.  unicode  .uni format through a consuming regression::

                    python -m pytest -q tests/customize/test_customize_base.py


            :param x: Value supplied for x under the utility contract.
            :param y: Value supplied for y under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
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
                if candidate is None:
                    ans.append("%-20s: %s" % (six_unicode(x), repr(y)))
                else:
                    ans.append(candidate)

        header = "Folder Store Driver.\n"
        uni_format("folder_store_row\n", self.fs_row)
        uni_format("folder_store_checked", self.fs_checked)
        uni_format("folder_store_exists", self.fs_exists)
        uni_format("folder_store_driver_needed_fields", self.NEEDED_FIELDS)
        uni_format("folder_store_driver_field_defaults", self.FIELD_DEFAULTS)

        rtn_str = header + "\n".join(ans)
        return rtn_str

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH THE NEW BOOKS AND COMPRESSED FILES CACHES START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __getitem__(self, item: str) -> Any:
        """
        Provides an access method for the folder store row.

        Example:
            Exercise LocationDriver.  getitem   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        item = deepcopy(item)
        if item in self.fs_row:
            return deepcopy(self.fs_row[item])

        folder_store_item = "folder_store_" + six_unicode(item)
        if folder_store_item in self.fs_row:
            return deepcopy(self.fs_row[folder_store_item])
        raise KeyError(f"{item=} not found.")

    # Todo: Misleading - should be a method which calls the row reload method - and a replacement method like this
    def reload_fs_row(self, new_row: Row) -> None:
        """
        Loads an updated folder store row into the driver.

        Example:
            Exercise LocationDriver.reload fs row through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param new_row: Value supplied for new row under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.fs_row = new_row

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH THE NEW BOOKS AND COMPRESSED FILES CACHES START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_cf_cache(self) -> Union[str, os.PathLike]:
        """
        Returns a path to a compressed files' folder at the top level of the store.

        Example:
            Exercise LocationDriver.direct get cf cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.type != "on_disc":
            raise NotImplementedError
        return ""

    def direct_get_nb_cache(self) -> Union[str, os.PathLike]:
        """
        Returns a path to the new_books folder at the top level of the store.

        Example:
            Exercise LocationDriver.direct get nb cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.type != "on_disc":
            raise NotImplementedError
        return ""

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO MANIPULATE THE ENTIRE FOLDER STORE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # If the folder store has been initialized without a folder_store_row most of these methods will be non-functional

    def direct_create_store(self) -> bool:
        """
        Creates a folder store at the specified location using the metadata stored in the folder store row.

        Example:
            Exercise LocationDriver.direct create store through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.fs_row is None:
            raise TypeError("Cannot create_store without a folder store row.")

        if TYPE_CHECKING:
            return False

        raise NotImplementedError

    def direct_check_store(self) -> bool:
        """
        Checks the folder store at the specified location using the metadata stored in the folder store row.

        Example:
            Exercise LocationDriver.direct check store through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.fs_row is None:
            raise TypeError("Cannot check_store without a folder store row.")

        if TYPE_CHECKING:
            return False

        raise NotImplementedError

    def direct_check_location(self) -> bool:
        """
        Checks that the location of the store is read/write accessible.

        Example:
            Exercise LocationDriver.direct check location through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO ACCESS OBJECTS IN THE STORE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_get_file_stream(self, target_folder_loc: Location, file_name: str) -> BinaryIO:
        """
        Returns a binary file stream rooted at the start of the actual file.

        Example:
            Exercise LocationDriver.direct get file stream through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param file_name: Value supplied for file name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------
    # - METHODS TO GET SCRATCH COPIES OF THE FOLDER STORE CONTENTS START HERE
    # ----------------------------------------------------------------------------

    # Todo: descend Loc from Path - copy and extend the interface
    def direct_get_scratch_file_copy(self, target_folder_loc: Location, target_file_name: str) -> os.PathLike:
        """
        Gets a local copy of a file in the target_folder in the LiuXin_scratch folder.

        Example:
            Exercise LocationDriver.direct get scratch file copy through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param target_file_name: Value supplied for target file name under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_scratch_folder_copy(
        self, target_folder_loc: Location, target_folder_name: str, copy_symlinks: bool = False
    ) -> os.PathLike:
        """
        Makes a local copy of a folder in a scratch folder.

        Example:
            Exercise LocationDriver.direct get scratch folder copy through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param target_folder_name: Value supplied for target folder name under the utility
            contract.
        :param copy_symlinks: Value supplied for copy symlinks under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @staticmethod
    def direct_get_scratch_self_copy(target_folder_loc: Location, copy_symlinks: bool = False) -> os.PathLike:
        """
        Makes a scratch copy of an entire Folder.

        Example:
            Exercise LocationDriver.direct get scratch self copy through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param copy_symlinks: Value supplied for copy symlinks under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO ADD FILES AND FOLDERS TO THE STORE START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def direct_add_local_file(
        self, target_folder_loc: Location, local_file_path: Union[str, os.PathLike]
    ) -> tuple[bool, str]:
        """
        Copies a local file directly into the given :param target_folder_loc:.

        Example:
            Exercise LocationDriver.direct add local file through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param local_file_path: Value supplied for local file path under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_add_local_folder(self, target_folder_loc, local_folder_path: Union[str, os.PathLike]):
        """
        Copies a local folder into the target_folder.

        Example:
            Exercise LocationDriver.direct add local folder through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param local_folder_path: Value supplied for local folder path under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_create_new_folder(self, target_folder_loc: Location, folder_name: str) -> (bool, str):
        """
        Creates a new folder inside the Folder with the given name.

        Example:
            Exercise LocationDriver.direct create new folder through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param folder_name: Value supplied for folder name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO RENAME OBJECTS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Todo: Actually, if target_file_name is None, throw an error - there's a different function for that
    # Todo: Need to sanity check file names - that should be a function in the folder store
    def direct_rename_file(self, target_folder_loc: Location, target_file_name: str, new_file_name: str) -> bool:
        """
        Renames a file in the given Folder specified by a Location.

        Example:
            Exercise LocationDriver.direct rename file through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param target_file_name: Value supplied for target file name under the utility
            contract.
        :param new_file_name: Value supplied for new file name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_rename_folder(self, target_folder_loc: Location, sub_folder_name: str, new_name: str) -> bool:
        """
        Renames a folder in the Folder specified by a Location.

        Example:
            Exercise LocationDriver.direct rename folder through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param sub_folder_name: Value supplied for sub folder name under the utility
            contract.
        :param new_name: Value supplied for new name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Rename this to "direct_rename_folder" and the above to "direct_rename_subfolder"?
    # Todo: Need to sanity check names here as well
    def direct_rename_self(self, target_folder_loc: Location, new_folder_name: str) -> bool:
        """
        Rename the given Folder.

        Example:
            Exercise LocationDriver.direct rename self through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param new_folder_name: Value supplied for new folder name under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO GET OBJECT PROPERTIES START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Todo: There seems to be some redundancy and duplicated names here - check and remove
    # Todo: This might be a set! Check when typing folder store.
    def direct_get_true_sub_folder_list(self, target_folder_loc: Location) -> list[str, ...]:
        """
        Returns the list of files actually in the folder at the target_folder_loc.

        Example:
            Exercise LocationDriver.direct get true sub folder list through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_true_object_lists(self, target_folder_loc: Location) -> tuple[set[str], set[str]]:
        """
        Takes a Folder object - gets the actual contents of this folder from the physical storage medium.

        Example:
            Exercise LocationDriver.direct get true object lists through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_true_sub_folders(self, target_folder_loc: Location) -> list[str, ...]:
        """
        Returns a list of the files actually present in the location.

        Example:
            Exercise LocationDriver.direct get true sub folders through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_true_files(self, target_folder_loc: Location) -> list[str, ...]:
        """
        Returns the list of files at a location.

        Example:
            Exercise LocationDriver.direct get true files through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Impose consistency between "loc" and "location"
    # Todo: Some kind of properties object might well be better here
    def direct_get_all_file_properties(self, target_folder_loc: Location, target_file_name: str):
        """
        Queries the folder store and returns the actual properties of the given file in the Folder.

        Example:
            Exercise LocationDriver.direct get all file properties through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param target_folder_loc: Value supplied for target folder loc under the utility
            contract.
        :param target_file_name: Value supplied for target file name under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_true_folder_size(self, folder_location: Location, sub_folder_name: Optional[str] = None) -> int:
        """
        Takes a location of a folder. Returns the true size of that folder.

        Example:
            Exercise LocationDriver.direct get true folder size through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param folder_location: Value supplied for folder location under the utility
            contract.
        :param sub_folder_name: Value supplied for sub folder name under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_true_file_size(self, folder_location: Location, file_name: str) -> int:
        """
        Takes a folder and a file in that folder. Returns the size of that file.

        Example:
            Exercise LocationDriver.direct get true file size through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param folder_location: Value supplied for folder location under the utility
            contract.
        :param file_name: Value supplied for file name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_folder_true_names_index(self, folder_location: Location) -> list[str, ...]:
        """
        Takes a Folder loc and returns all the names of parents back to the root of the folder store.

        Example:
            Exercise LocationDriver.direct get folder true names index through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param folder_location: Value supplied for folder location under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_exists(self, location: Location) -> bool:
        """
        Directly checks to see if a location exists.

        Example:
            Exercise LocationDriver.direct exists through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param location: Value supplied for location under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH LOCATION OBJECTS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # These methods depend strongly on the type of location - splitting the name of a folder out might work in two very
    # different ways depending if the folder is on an ftp reader_server or on a local disc

    def direct_get_folder_name(self, location: Location) -> str:
        """
        Takes the location - returns the current folder name.

        Example:
            Exercise LocationDriver.direct get folder name through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param location: Value supplied for location under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_file_name(self, location: Location) -> str:
        """
        Takes the location - returns the name of the file it points to.

        Example:
            Exercise LocationDriver.direct get file name through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param location: Value supplied for location under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Should force you to declare a file or a folder
    def direct_make_sub_object_location(self, self_location: Location, object_name: str) -> Location:
        """
        Takes a location and the name of a resource (file or folder) at that location. Returns the path to the sub object.

        Example:
            Exercise LocationDriver.direct make sub object location through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param self_location: Value supplied for self location under the utility contract.
        :param object_name: Value supplied for object name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_parent_location(self, location: Location) -> Location:
        """
        Takes a location object - returns the location object appropriate for the parent of that object

        Example:
            Exercise LocationDriver.direct get parent location through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param location: Value supplied for location under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # Todo: Might be worth splitting the folders and files table down
    #       One table per folder store?
    #       Might be a bit faster and more efficient.
    def direct_get_location_from_rows(
        self, folder_store_row: Row, folder_row_index: Optional[list[Row, ...]] = None, file_row: Optional[Row] = None
    ) -> Location:
        """
        Takes a series of rows and builds a location from them.

        Example:
            Exercise LocationDriver.direct get location from rows through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param folder_store_row: Value supplied for folder store row under the utility
            contract.
        :param folder_row_index: Value supplied for folder row index under the utility
            contract.
        :param file_row: Value supplied for file row under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_object_name(self, object_loc: Location) -> str:
        """
        Takes the location of an object - splits the name out of the location and returns it as a str.

        Example:
            Exercise LocationDriver.direct get object name through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param object_loc: Value supplied for object loc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_seek(
        self, location: Optional[Location] = None, folder_tag: str = None, file_tag: str = None
    ) -> Location:
        """
        Searches the folder store for a file or folder ending with the given tag.

        Example:
            Exercise LocationDriver.direct seek through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param location: Value supplied for location under the utility contract.
        :param folder_tag: Value supplied for folder tag under the utility contract.
        :param file_tag: Value supplied for file tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_get_ext(self, location: Location) -> str:
        """
        Splits the file extension from the file name and returns it.

        Example:
            Exercise LocationDriver.direct get ext through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param location: Value supplied for location under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE FILES AND FOLDERS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    # Todo: Consider renaming some of these parameters
    def direct_delete_file(self, folder_loc: Location, file_name_ext: str) -> bool:
        """
        Method to delete a file from within a folder.

        Example:
            Exercise LocationDriver.direct delete file through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param folder_loc: Value supplied for folder loc under the utility contract.
        :param file_name_ext: Value supplied for file name ext under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def direct_delete_folder(self, folder_loc: Location, folder_name: Optional[str] = None) -> bool:
        """
        Method to delete a subfolder from within a folder - or the folder itself if :param folder_name: is None

        Example:
            Exercise LocationDriver.direct delete folder through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param folder_loc: Value supplied for folder loc under the utility contract.
        :param folder_name: Value supplied for folder name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO MOVE OBJECTS AROUND START HERE

    def direct_move_folder(self, current_location: Location, target_location: Location) -> bool:
        """
        Move a folder around inside the folder store.

        Example:
            Exercise LocationDriver.direct move folder through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param current_location: Value supplied for current location under the utility
            contract.
        :param target_location: Value supplied for target location under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


#
# ----------------------------------------------------------------------------------------------------------------------
