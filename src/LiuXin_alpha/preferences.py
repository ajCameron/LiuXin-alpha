#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Preferneces/tweaks folder which


"""


# Uses the config parser module as a basis for the LIuXin preferences
# This module was developed to replace the calibre tweaks and preferences modules - thus it needed the capability to
# store and retrieve a wider range of data structures
# calibre:tweaks stored python objects which where no easily renderable into JSON form
# calibre:config stored python objects that where.
# They have been merged and this class has been extended to provide support for storing all the objects that should
# be needed
# json was used instead of Pickle - due to the potential for pickle to be exploited for arbitrary code execution
# Needs to be kept as simple as possible to avoid import loops
# Thus the logger is not used - logging is implemented via strings.

try:
    import ConfigParser
except:
    import configparser as ConfigParser

import json
import os
import re
import shutil
import uuid
from copy import deepcopy
from functools import partial

from LiuXin_alpha.constants.paths import LiuXin_prefs_folder

from LiuXin_alpha.utils.libraries.liuxin_json import LiuXinJSON

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class EncodeError(Exception):
    pass


class ParseError(Exception):
    pass


class Preferences(object):
    """
    Stores preferences and tweaks for LiuXin.
    Provides methods to load and retrieve preferences - in the form of certain, limited data structures.
    Stores the preferences in memory for fast access.
    Unlike the standard ConfigParser does not permit adding options with duplicate names - even if they are in
    different sections. Required to prevent confusion when using __setitem__ to update options.
    """

    config_file_name = "LiuXin_prefs_file.ini"
    # dict_64 - all strings are encoded to base64 before being stored - useful if you want to port unicode around
    known_types = {
        "bool",
        "dict",
        "dict_64",
        "int",
        "float",
        "list",
        "list_64",
        "none",
        "set",
        "set_64",
        "str",
        "str_64",
        "tuple",
        "tuple_64",
    }

    def __init__(self, backup_folder=False, cont_backup=True):
        """
        Detects an existing preferences file. Tries to load it.
        If it can't load the file then falls back on the defaults.
        :param backup_folder: If provided then the preferences file will be placed here.
                              If an existing preferences file is found during startup then the file will be moved here
                              before being opened.
        :param cont_backup: If True then the object will be continuously backed up to disc whenever any change is made
                            to any of the options.
        """
        self.continuous_backup = cont_backup

        # Initialize tools
        self.liuxin_json = LiuXinJSON()
        self.val_to_str_plugins = self.build_val_to_str_plugins()
        self.str_to_val_plugins = self.build_str_to_val_plugins()

        # Caches information about the active variables - used for read-write
        self._active_variables = dict()
        self._variable_type = dict()

        # Fields which should not be changes
        self.frozen_options = set()

        # Which field should new variables without a section be stored in by default
        self.misc_section = "calibre_tweaks"

        # See if there is already a preferences file
        self.config_file_path = os.path.join(LiuXin_prefs_folder, Preferences.config_file_name)
        self.config_file_exists = os.path.isfile(self.config_file_path)

        # Check to see if the config file actually has contents
        if self.config_file_exists:
            with open(self.config_file_path, encoding="utf-8") as config_file:
                file_contents = config_file.read()

            if not file_contents:
                self.config_file_exists = False

        # Using the backup_folder status either create the preferences file or copy it.
        if backup_folder:
            new_config_file = os.path.join(backup_folder, Preferences.config_file_name)
            if self.config_file_exists:
                shutil.copy2(self.config_file_path, new_config_file)
            self.config_file_path = new_config_file

        # Stores the config which underlies all stored preferences - allows for easy dumping
        self.config = ConfigParser.RawConfigParser()

        # If the config file doesn't exist reload from defaults and save to the new config file
        if not self.config_file_exists:
            self.load_default_config()
            self.save()
        else:
            self.load()

        # Derive and store some additional information about the comnfig which will enable a more elegant interface
        self.keys_dict = self.load_keys_dict(self.config)

    def load(self):
        """
        Load the file contained at the current config_file_path.

        All variables currently stored will be dropped and reloaded.
        :return:
        """
        self.config = ConfigParser.RawConfigParser()

        with open(self.config_file_path, "r") as cfgfile:
            self.config.read(cfgfile)

        self._active_variables = dict()
        self._variable_type = dict()

        seen_options = set()
        # Read every config from all the sections - validate to make sure that the config file is valid for us (has no
        # degenerate file names)
        for section in self.config.sections():
            for option in self.config.options(section):
                assert option not in seen_options, "config file is not valid - duplicate options"

                val_type, value = self.str_to_val(self.config.get(section, option))
                self._variable_type[option] = val_type
                self._active_variables[option] = value

        # Replace the keys dict with the keys dict from the new config file
        self.keys_dict = self.load_keys_dict(self.config)

    def save(self):
        """
        Saves the current config to the given config dictionary.
        :return:
        """
        with open(self.config_file_path, "w") as cfgfile:
            self.config.write(cfgfile)

    @staticmethod
    def is_64(type_str):
        """
        Is the given type_str of 64 type?
        :param type_str:
        :return:
        """
        return type_str.endswith("64")

    def has_64_version(self, type_str):
        """
        Returns True if the variable has a 64 version - False otherwise.
        E.g. int with return False and tuple will return True
        :param type_str:
        :return:
        """
        new_type_str = type_str + "_64"
        return new_type_str in self.known_types

    def is_json_okay(self, val):
        """
        Try and serialize the object using the standard json function
        :param val:
        :return:
        """
        try:
            self.to_json(val)
        except:
            return False
        return True

    def is_liuxin_json_okay(self, val):
        """
        Try and serialize the object using the the upgraded Liuxin_JSON class.
        :param val:
        :return:
        """
        try:
            self.liuxin_to_json(val)
        except:
            return False
        return True

    def build_val_to_str_plugins(self):
        """
        Returns the val_to_str_plugins dict - keyed with the variable type and valued with the serializer used to render
        it into string format for saving.
        :return:
        """
        return {
            "bool": bool_to_str,
            "dict": self.to_json,
            "dict_64": self.liuxin_to_json,
            "int": str,
            "float": str,
            "list": self.to_json,
            "list_64": self.liuxin_to_json,
            "none": none_to_str,
            "set": partial(set_to_str, handler=self.to_json),
            "set_64": partial(set_to_str, handler=self.liuxin_to_json),
            "str": self.to_json,
            "str_64": self.liuxin_to_json,
            "tuple": self.to_json,
            "tuple_64": self.liuxin_to_json,
        }

    def build_str_to_val_plugins(self):
        """
        Returns the str_to_val_plugins dict - keyed with the variable type and valued with the converter needed to
        make it back into a variable.
        Done here as the behavior of JSON might be set during the class construction.
        :return:
        """
        # Keyed with the name of the type that the function handles, and valued with the function to be called with the
        # raw value string
        return {
            "bool": bool_str_to_bool,
            "dict": self.from_json,
            "dict_64": self.liuxin_from_json,
            "int": int,
            "float": float,
            "list": self.from_json,
            "list_64": self.liuxin_from_json,
            "none": none_str_to_none,
            "set": partial(set_str_to_set, handler=self.from_json),
            "set_64": partial(set_str_to_set, handler=self.liuxin_from_json),
            "str": self.from_json,
            "str_64": self.liuxin_from_json,
            "tuple": partial(tuple_str_to_tuple, handler=self.from_json),
            "tuple_64": partial(tuple_str_to_tuple, handler=self.liuxin_from_json),
        }

    # Todo: Does config support multiple options called the same thing in different sections? Deal with this.
    @staticmethod
    def load_keys_dict(config):
        """
        Load the keys dictionary.
        Keyed with a frozen set containing all the options in that section of the config and valued with the name of
        that section.
        It's used to find the section containing a given option so that option can be retrieved and updated.
        :return:
        """
        sections = config.sections()

        # Producing a dictionary keyed by sections and valued by sets
        sections_dict = dict()
        for section in sections:
            sections_dict[section] = set()
            options = config.options(section)
            for option in options:
                sections_dict[section].add(option)

        # building a keys dict for return
        keys_dict = dict()
        for section in sections_dict:
            keys_dict[frozenset(sections_dict[section])] = section
        return keys_dict

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - ACCESS CONTROLS TO THE UNDERLYING OBJECTS

    def __getitem__(self, item):
        """
        Returns a copy of the active object corresponding to the given name.
        Changes to an active object ARE NOT reflected in the underlying preferences - you need to use __setitem__ to
        update the item with it's changed value to see changes to the underlying data store.
        :param item:
        :return:
        """
        return deepcopy(self._active_variables[item])

    def __setitem__(self, key, value):
        """
        Set the value for the given key.
        If the key corresponds to a preference that already exists in the config then that preference will be updated.
        If there is no corresponding key then it will be added to the Other section of the preferences file.
        NOTE: Setting items has a significant performance hit, as the config file is dumped to disk after every change.
        :param key:
        :param value:
        :return:
        """
        if key in self.frozen_options:
            raise KeyError("cannot update {} - option is designated frozen".format(key))

        # Check to see if the variable is known - if it is then we need to update that value
        if key in self._active_variables.keys():

            # Locate the section the option is in for update
            section = self.get_section(key)

            new_val_type = type(value).__name__
            if new_val_type not in self.known_types:
                raise NotImplementedError("cannot update {} with value {} - unsupported type".format(key, value))

            old_key_type = self._variable_type[key]
            # Detect if the old variable was stored in base 64 form - if it was use that if available - otherwise use
            # the standard form
            if not self.is_64(old_key_type):
                self.type_set(section, key, value, new_val_type)
            elif self.is_64(old_key_type) and self.has_64_version(new_val_type):
                new_val_type += "_64"
                self.type_set(section, key, value, new_val_type)
            elif self.is_64(old_key_type) and not self.has_64_version(new_val_type):
                self.type_set(section, key, value, new_val_type)
            else:
                raise NotImplementedError("this position should never be reached")

        else:

            section = self.misc_section

            new_val_type = type(value).__name__
            if new_val_type not in self.known_types:
                raise NotImplementedError("cannot update {} with value {} - unsupported type".format(key, value))

            # If the standard form of JSON serialization is okay the it can be used - else fall back on base64 encoding.
            if self.is_json_okay(value):
                self.type_set(section, key, value, new_val_type)
            else:
                if self.has_64_version(new_val_type) and self.is_json_okay(value):
                    new_val_type += "_64"
                    self.type_set(section, key, value, new_val_type)
                else:
                    raise NotImplementedError("object cannot be serialized")

        self._active_variables[key] = value
        self._variable_type[key] = new_val_type

        if self.continuous_backup:
            self.save()

    def get_section(self, option):
        """
        Takes a option which exists in a section of the config - returns the section that it's in.
        Raises a KeyError if the option isn't found anywhere in the Config.
        :param option:
        :return:
        """
        for key_set in self.keys_dict:
            if option in key_set:
                return self.keys_dict[key_set]

        raise KeyError("{} not found in config".format(option))

    def get_raw_option_str(self, option):
        """
        Returns the raw string for that option from the underlying config.
        :param option:
        :return:
        """
        return self.config.get(self.get_section(option), option)

    def add_section(self, section):
        """
        Add a section to the underlying config class.
        :param section:
        :return:
        """
        self.config.add_section(section)

    def type_set(self, section, option, value=None, val_type="str"):
        """
        Set a variable in the config - supplying information as to the type of the object so that it can be properly
        pickled and returned.
        If the actual value is set to be None for any val_type then an object of type None will be returned.
        :param section:
        :param option:
        :param value:
        :param val_type:
        :return:
        """
        if val_type not in Preferences.known_types:
            raise NotImplementedError("val_type not recognized")

        # Set the variable in the config and in the _active_variables
        self._active_variables[option] = value
        self._variable_type[option] = val_type

        full_val_str = self.val_to_str(value, val_type)
        self.config.set(section, option, full_val_str)

    def set(self, section, option, value=None):
        """
        Raw set - sets an option in the underlying configuration.
        Sets with the default type - strings.
        :param section:
        :param option:
        :param value:
        :return:
        """
        self.type_set(section, option, value=value, val_type="str")

    # Todo: Remove this function from the codebase - here for legacy reasons
    def parse(self, key, rtn_value_type, default=None):
        """
        Here for legacy compatibility reasons - should be removed as fast as possible.
        Parse and return a value from preferences.
        :param key: The required value from the preferences
        :param rtn_value_type: Should the returned value be bool, str, int e.t.c
        :param default: If the value cannot be rendered to that type then what value should be returned? - default None
        :return:
        """
        try:
            key_value = self.__getitem__(key)
        except KeyError:
            return default

        if rtn_value_type == "str" or rtn_value_type == "string":
            return str(key_value)

        elif rtn_value_type == "bool":
            return bool(key_value)

        elif rtn_value_type == "int":
            return int(key_value)

        elif rtn_value_type == "unicode":
            return six_unicode(key_value)

        else:
            raise NotImplementedError("rtn_value_type {} not recognized".format(rtn_value_type))

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - STARTUP METHODS

    def load_default_config(self):
        """
        Contains all the default values of LiuXin preferences.
        Loads the internal config class with all the individual preferences - with notes as to type of the object.
        :return:
        """
        # Application preferences
        self.add_section("Application")

        self.set("Application", "application_id", str(uuid.uuid4()))
        self.type_set("Application", "dedicated_drive", False, val_type="bool")

        # DatabasePing preferences
        self.add_section("DatabasePing")

        self.set("DatabasePing", "database_id", str(uuid.uuid4()))
        self.type_set("DatabasePing", "run_ta_update_after_each_change", False, val_type="bool")
        self.set("DatabasePing", "library_path", "default")

        # DatabasePing debug preferences
        self.add_section("DatabasePing Debug")

        self.type_set(
            "DatabasePing Debug",
            "include_full_rep_if_row_cant_be_identified",
            False,
            val_type="bool",
        )

        # Folder preferences
        self.add_section("Folders")

        # GUI display preferences
        self.add_section("GUI Display Preferences")

        # Import Preferences
        self.add_section("Import")
        self.type_set("Import", "use_import_cache", True, val_type="bool")
        self.type_set("Import", "use_ic_decompress_files", True, val_type="bool")
        # config.set('Import', 'use_ic_new_books', 'true')
        self.type_set("Import", "import_cache_size", 50000000000, val_type="int")
        self.type_set("Import", "allow_forbidden_stores_override", False, val_type="bool")
        self.type_set("Import", "prompt_forbidden_stores_override", False, val_type="bool")
        # If false will fill from a random store
        self.type_set("Import", "fill_from_first_folder_store", False, val_type="bool")
        self.type_set("Import", "group_creators", True, val_type="bool")
        self.type_set("Import", "retry_limit", 5, val_type="int")
        #: Auto increment series index
        # The algorithm used to assign a book added to an existing series a series number.
        # New series numbers assigned using this tweak are always integer values, except
        # if a constant non-integer is specified.
        # Possible values are:
        # next - First available integer larger than the largest existing number
        # first_free - First available integer larger than 0
        # next_free - First available integer larger than the smallest existing number
        # last_free - First available integer smaller than the largest existing number
        #             Return largest existing + 1 if no free number is found
        # const - Assign the number 1 always
        # no_change - Do not change the series index
        # a number - Assign that number always. The number is not in quotes. Note that
        #            0.0 can be used here.
        # Examples:
        # series_index_auto_increment = 'next'
        # series_index_auto_increment = 'next_free'
        # series_index_auto_increment = 16.5
        #
        # Set the use_series_auto_increment_tweak_when_importing tweak to True to
        # use the above values when importing/adding books. If this tweak is set to
        # False (the default) then the series number will be set to 1 if it is not
        # explicitly set during the import. If set to True, then the
        # series index will be set according to the series_index_auto_increment setting.
        # Note that the use_series_auto_increment_tweak_when_importing tweak is used
        # only when a value is not provided during import. If the importing regular
        # expression produces a value for series_index, or if you are reading metadata
        # from books and the import plugin produces a value, than that value will
        # be used irrespective of the setting of the tweak.
        self.set("Import", "series_index_auto_increment", "next")
        self.type_set(
            "Import",
            "use_series_auto_increment_tweak_when_importing",
            False,
            val_type="bool",
        )

        # Folder Store General Preferences
        # - use_scratch_as_cache -
        self.add_section("Folder_Stores")

        self.type_set("Folder_Stores", "default_cache_folder_store", None, val_type="none")
        self.type_set("Folder_Stores", "leave_space_free", 1000000000, val_type="int")

        # Folder Store Specific Type Preferences - always key with type_of_store,lower()
        self.add_section("on_disk")
        self.type_set("on_disk", "on_disk_use_in_name_tags", False, val_type="bool")

        # Startup Preferences
        self.add_section("Startup")

        # Logging Preferences
        self.add_section("Logging")
        self.type_set("Logging", "output_to_file_width", 120, val_type="int")

        # Operational Preferences
        self.add_section("Operational")
        self.type_set("Operational", "retain_all_original_files", True, val_type="bool")

        # Preferences for the various Data Sources are stored here
        self.add_section("Data_Sources")
        self.type_set("Data_Sources", "ISFDB_sqlite_db_made", False, val_type="bool")

        # Preferences for the tables (allowed values in various columns e.t.c)
        self.add_section("Tables")
        self.type_set(
            "Tables",
            "allowed_titles_intralink_types",
            {"identical", "another_thing", "user_marked_different", "alt_title"},
            val_type="set",
        )
        self.type_set(
            "Tables",
            "allowed_creators_intralink_types",
            {"identical", "user_marked_different", "pseudoname"},
            val_type="set",
        )
        # Todo: Make sure that this is implemented and switchable
        # Todo: Make sure that a tuple of ints is a tuple of ints before and after setting
        self.set("Tables", "book_size_display_mode", "sum")

        # Preferences for output and display
        self.add_section("Display")
        self.type_set("Display", "default_terminal_width", 80, val_type="int")
        self.type_set("Display", "default_terminal_height", 24, val_type="int")

        # Miscellaneous preferences (allowing arbitrary preferences to be set by keyword).
        self.add_section("Misc")
        self.set("Misc", "language", "en")

        # All taken directly from calibre
        self.add_section("calibre_tweaks")

        #: Add separator after completing an author name
        # Should the completion separator be append
        # to the end of the completed text to
        # automatically begin a new completion operation
        # for authors.
        # Can be either True or False
        self.type_set(
            "calibre_tweaks",
            "authors_completer_append_separator",
            False,
            val_type="bool",
        )

        #: Author sort name algorithm
        # The algorithm used to copy author to author_sort
        # Possible values are:
        #  invert: use "fn ln" -> "ln, fn"
        #  copy  : copy author to author_sort without modification
        #  comma : use 'copy' if there is a ',' in the name, otherwise use 'invert'
        #  nocomma : "fn ln" -> "ln fn" (without the comma)
        # When this tweak is changed, the author_sort values stored with each author
        # must be recomputed by right-clicking on an author in the left-hand tags pane,
        # selecting 'manage authors', and pressing 'Recalculate all author sort values'.
        # The author name suffixes are words that are ignored when they occur at the
        # end of an author name. The case of the suffix is ignored and trailing
        # periods are automatically handled. The same is true for prefixes.
        # The author name copy words are a set of words which if they occur in an
        # author name cause the automatically generated author sort string to be
        # identical to the author name. This means that the sort for a string like Acme
        # Inc. will be Acme Inc. instead of Inc., Acme
        self.set("calibre_tweaks", "author_sort_copy_method", "comma")

        author_name_suffixes = (
            "Jr",
            "Sr",
            "Inc",
            "Ph.D",
            "Phd",
            "MD",
            "M.D",
            "I",
            "II",
            "III",
            "IV",
            "Junior",
            "Senior",
        )
        self.type_set(
            "calibre_tweaks",
            "author_name_suffixes",
            author_name_suffixes,
            val_type="tuple",
        )

        author_name_prefixes = ("Mr", "Mrs", "Ms", "Dr", "Prof")
        self.type_set(
            "calibre_tweaks",
            "author_name_prefixes",
            author_name_prefixes,
            val_type="tuple",
        )

        author_name_copywords = (
            "Corporation",
            "Company",
            "Co.",
            "Agency",
            "Council",
            "Committee",
            "Inc.",
            "Institute",
            "Society",
            "Club",
            "Team",
        )
        self.type_set(
            "calibre_tweaks",
            "author_name_copywords",
            author_name_copywords,
            val_type="tuple",
        )

        #: Splitting multiple author names
        # By default, calibre splits a string containing multiple author names on
        # ampersands and the words "and" and "with". You can customize the splitting
        # by changing the regular expression below. Strings are split on whatever the
        # specified regular expression matches, in addition to ampersands.
        # Default: r'(?i),?\s+(and|with)\s+'
        authors_split_regex = r"(?i),?\s+(and|with)\s+"
        self.type_set("calibre_tweaks", "authors_split_regex", authors_split_regex, val_type="str")

        #: Use author sort in Tag Browser
        # Set which author field to display in the tags pane (the list of authors,
        # series, publishers etc on the left hand side). The choices are author and
        # author_sort. This tweak affects only what is displayed under the authors
        # category in the tags pane and content server. Please note that if you set this
        # to author_sort, it is very possible to see duplicate names in the list because
        # although it is guaranteed that author names are unique, there is no such
        # guarantee for author_sort values. Showing duplicates won't break anything, but
        # it could lead to some confusion. When using 'author_sort', the tooltip will
        # show the author's name.
        # Examples:
        #   categories_use_field_for_author_name = 'author'
        #   categories_use_field_for_author_name = 'author_sort'
        categories_use_field_for_author_name = "author"
        self.type_set(
            "calibre_tweaks",
            "categories_use_field_for_author_name",
            categories_use_field_for_author_name,
            val_type="str",
        )

        #: Control partitioning of Tag Browser
        # When partitioning the tags browser, the format of the subcategory label is
        # controlled by a template: categories_collapsed_name_template if sorting by
        # name, categories_collapsed_rating_template if sorting by average rating, and
        # categories_collapsed_popularity_template if sorting by popularity. There are
        # two variables available to the template: first and last. The variable 'first'
        # is the initial item in the subcategory, and the variable 'last' is the final
        # item in the subcategory. Both variables are 'objects'; they each have multiple
        # values that are obtained by using a suffix. For example, first.name for an
        # author category will be the name of the author. The sub-values available are:
        #  name: the printable name of the item
        #  count: the number of books that references this item
        #  avg_rating: the average rating of all the books referencing this item
        #  sort: the sort value. For authors, this is the author_sort for that author
        #  category: the category (e.g., authors, series) that the item is in.
        # Note that the "r'" in front of the { is necessary if there are backslashes
        # (\ characters) in the template. It doesn't hurt anything to leave it there
        # even if there aren't any backslashes.
        categories_collapsed_name_template = r"{first.sort:shorten(4,,0)} - {last.sort:shorten(4,,0)}"
        self.set(
            "calibre_tweaks",
            "categories_collapsed_name_template",
            categories_collapsed_name_template,
        )

        categories_collapsed_rating_template = r"{first.avg_rating:4.2f:ifempty(0)} - {last.avg_rating:4.2f:ifempty(0)}"
        self.set(
            "calibre_tweaks",
            "categories_collapsed_rating_template",
            categories_collapsed_rating_template,
        )

        categories_collapsed_popularity_template = r"{first.count:d} - {last.count:d}"
        self.set(
            "calibre_tweaks",
            "categories_collapsed_popularity_template",
            categories_collapsed_popularity_template,
        )

        #: Control order of categories in the tag browser
        # Change the following dict to change the order that categories are displayed in
        # the tag browser. Items are named using their lookup name, and will be sorted
        # using the number supplied. The lookup name '*' stands for all names that
        # otherwise do not appear. Two names with the same value will be sorted
        # using the default order; the one used when the dict is empty.
        # Example: tag_browser_category_order = {'series':1, 'tags':2, '*':3}
        # resulting in the order series, tags, then everything else in default order.
        tag_browser_category_order = {"*": 1}
        self.type_set(
            "calibre_tweaks",
            "tag_browser_category_order",
            tag_browser_category_order,
            val_type="dict",
        )

        #: Specify columns to sort the booklist by on startup
        # Provide a set of columns to be sorted on when calibre starts
        #  The argument is None if saved sort history is to be used
        #  otherwise it is a list of column,order pairs. Column is the
        #  lookup/search name, found using the tooltip for the column
        #  Order is 0 for ascending, 1 for descending
        # For example, set it to [('authors',0),('title',0)] to sort by
        # title within authors.
        sort_columns_at_startup = None
        self.type_set(
            "calibre_tweaks",
            "sort_columns_at_startup",
            sort_columns_at_startup,
            val_type="none",
        )

        #: Control how dates are displayed
        # Format to be used for publication date and the timestamp (date).
        #  A string controlling how the publication date is displayed in the GUI
        #  d     the day as number without a leading zero (1 to 31)
        #  dd    the day as number with a leading zero (01 to 31)
        #  ddd   the abbreviated localized day name (e.g. 'Mon' to 'Sun').
        #  dddd  the long localized day name (e.g. 'Monday' to 'Sunday').
        #  M     the month as number without a leading zero (1-12)
        #  MM    the month as number with a leading zero (01-12)
        #  MMM   the abbreviated localized month name (e.g. 'Jan' to 'Dec').
        #  MMMM  the long localized month name (e.g. 'January' to 'December').
        #  yy    the year as two digit number (00-99)
        #  yyyy  the year as four digit number
        #  h     the hours without a leading 0 (0 to 11 or 0 to 23, depending on am/pm) '
        #  hh    the hours with a leading 0 (00 to 11 or 00 to 23, depending on am/pm) '
        #  m     the minutes without a leading 0 (0 to 59) '
        #  mm    the minutes with a leading 0 (00 to 59) '
        #  s     the seconds without a leading 0 (0 to 59) '
        #  ss    the seconds with a leading 0 (00 to 59) '
        #  ap    use a 12-hour clock instead of a 24-hour clock, with "ap"
        #        replaced by the localized string for am or pm '
        #  AP    use a 12-hour clock instead of a 24-hour clock, with "AP"
        #        replaced by the localized string for AM or PM '
        #  iso   the date with time and timezone. Must be the only format present
        #  For example, given the date of 9 Jan 2010, the following formats show
        #  MMM yyyy ==> Jan 2010    yyyy ==> 2010       dd MMM yyyy ==> 09 Jan 2010
        #  MM/yyyy ==> 01/2010      d/M/yy ==> 9/1/10   yy ==> 10
        # publication default if not set: MMM yyyy
        # timestamp default if not set: dd MMM yyyy
        # last_modified_display_format if not set: dd MMM yyyy
        gui_pubdate_display_format = "MMM yyyy"
        self.set("calibre_tweaks", "gui_pubdate_display_format", gui_pubdate_display_format)

        gui_timestamp_display_format = "dd MMM yyyy"
        self.set(
            "calibre_tweaks",
            "gui_timestamp_display_format",
            gui_timestamp_display_format,
        )

        gui_last_modified_display_format = "dd MMM yyyy"
        self.set(
            "calibre_tweaks",
            "gui_last_modified_display_format",
            gui_last_modified_display_format,
        )

        #: Control sorting of titles and series in the library display
        # Control title and series sorting in the library view. If set to
        # 'library_order', the title sort field will be used instead of the title.
        # Unless you have manually edited the title sort field, leading articles such as
        # The and A will be ignored. If set to 'strictly_alphabetic', the titles will be
        # sorted as-is (sort by title instead of title sort). For example, with
        # library_order, The Client will sort under 'C'. With strictly_alphabetic, the
        # book will sort under 'T'.
        # This flag affects Calibre's library display. It has no effect on devices. In
        # addition, titles for books added before changing the flag will retain their
        # order until the title is edited. Double-clicking on a title and hitting return
        # without changing anything is sufficient to change the sort.
        title_series_sorting = "library_order"
        self.set("calibre_tweaks", "title_series_sorting", title_series_sorting)

        #: Set the list of words considered to be "articles" for sort strings
        # Set the list of words that are to be considered 'articles' when computing the
        # title sort strings. The articles differ by language. By default, calibre uses
        # a combination of articles from English and whatever language the calibre user
        # interface is set to. In addition, in some contexts where the book language is
        # available, the language of the book is used. You can change the list of
        # articles for a given language or add a new language by editing
        # per_language_title_sort_articles. To tell calibre to use a language other
        # than the user interface language, set, default_language_for_title_sort. For
        # example, to use German, set it to 'deu'. A value of None means the user
        # interface language is used. The setting title_sort_articles is ignored
        # (present only for legacy reasons).
        per_language_title_sort_articles = {
            # English
            "eng": (r"A\s+", r"The\s+", r"An\s+"),
            # Esperanto
            "epo": (r"La\s+", r"L'", "L\xb4"),
            # Spanish
            "spa": (
                r"El\s+",
                r"La\s+",
                r"Lo\s+",
                r"Los\s+",
                r"Las\s+",
                r"Un\s+",
                r"Una\s+",
                r"Unos\s+",
                r"Unas\s+",
            ),
            # French
            "fra": (
                r"Le\s+",
                r"La\s+",
                r"L'",
                "L´",
                "L’",
                r"Les\s+",
                r"Un\s+",
                r"Une\s+",
                r"Des\s+",
                r"De\s+La\s+",
                r"De\s+",
                r"D'",
                "D\xb4",
                "L’",
            ),
            # Italian
            "ita": (
                "Lo\\s+",
                "Il\\s+",
                "L'",
                "L\xb4",
                "La\\s+",
                "Gli\\s+",
                "I\\s+",
                "Le\\s+",
                "Uno\\s+",
                "Un\\s+",
                "Una\\s+",
                "Un'",
                "Un\xb4",
                "Dei\\s+",
                "Degli\\s+",
                "Delle\\s+",
                "Del\\s+",
                "Della\\s+",
                "Dello\\s+",
                "Dell'",
                "Dell\xb4",
            ),
            # Portuguese
            "por": (
                r"A\s+",
                r"O\s+",
                r"Os\s+",
                r"As\s+",
                r"Um\s+",
                r"Uns\s+",
                r"Uma\s+",
                r"Umas\s+",
            ),
            # Romanian
            "ron": (
                r"Un\s+",
                r"O\s+",
                r"Nişte\s+",
            ),
            # German
            "deu": (
                r"Der\s+",
                r"Die\s+",
                r"Das\s+",
                r"Den\s+",
                r"Ein\s+",
                r"Eine\s+",
                r"Einen\s+",
                r"Dem\s+",
                r"Des\s+",
                r"Einem\s+",
                r"Eines\s+",
            ),
            # Dutch
            "nld": (
                r"De\s+",
                r"Het\s+",
                r"Een\s+",
                r"'n\s+",
                r"'s\s+",
                r"Ene\s+",
                r"Ener\s+",
                r"Enes\s+",
                r"Den\s+",
                r"Der\s+",
                r"Des\s+",
                r"'t\s+",
            ),
            # Swedish
            "swe": (
                r"En\s+",
                r"Ett\s+",
                r"Det\s+",
                r"Den\s+",
                r"De\s+",
            ),
            # Turkish
            "tur": (r"Bir\s+",),
            # Afrikaans
            "afr": (
                r"'n\s+",
                r"Die\s+",
            ),
            # Greek
            "ell": (
                r"O\s+",
                r"I\s+",
                r"To\s+",
                r"Ta\s+",
                r"Tus\s+",
                r"Tis\s+",
                r"'Enas\s+",
                r"'Mia\s+",
                r"'Ena\s+",
                r"'Enan\s+",
            ),
            # Hungarian
            "hun": (
                r"A\s+",
                "Az\s+",
                "Egy\s+",
            ),
        }
        self.type_set(
            "calibre_tweaks",
            "per_language_title_sort_articles",
            per_language_title_sort_articles,
            val_type="dict_64",
        )

        default_language_for_title_sort = None
        self.type_set(
            "calibre_tweaks",
            "default_language_for_title_sort",
            default_language_for_title_sort,
            val_type="none",
        )

        title_sort_articles = r"^(A|The|An)\s+"
        self.set("calibre_tweaks", "title_sort_articles", title_sort_articles)

        #: Specify a folder calibre should connect to at startup
        # Specify a folder that calibre should connect to at startup using
        # connect_to_folder. This must be a full path to the folder. If the folder does
        # not exist when calibre starts, it is ignored. If there are '\' characters in
        # the path (such as in Windows paths), you must double them.
        # Examples:
        #     auto_connect_to_folder = 'C:\\Users\\someone\\Desktop\\testlib'
        #     auto_connect_to_folder = '/home/dropbox/My Dropbox/someone/library'
        auto_connect_to_folder = ""
        self.set("calibre_tweaks", "auto_connect_to_folder", auto_connect_to_folder)

        #: Specify renaming rules for SONY collections
        # Specify renaming rules for sony collections. This tweak is only applicable if
        # metadata management is set to automatic. Collections on Sonys are named
        # depending upon whether the field is standard or custom. A collection derived
        # from a standard field is named for the value in that field. For example, if
        # the standard 'series' column contains the value 'Darkover', then the
        # collection name is 'Darkover'. A collection derived from a custom field will
        # have the name of the field added to the value. For example, if a custom series
        # column named 'My Series' contains the name 'Darkover', then the collection
        # will by default be named 'Darkover (My Series)'. For purposes of this
        # documentation, 'Darkover' is called the value and 'My Series' is called the
        # category. If two books have fields that generate the same collection name,
        # then both books will be in that collection.
        # This set of tweaks lets you specify for a standard or custom field how
        # the collections are to be named. You can use it to add a description to a
        # standard field, for example 'Foo (Tag)' instead of the 'Foo'. You can also use
        # it to force multiple fields to end up in the same collection. For example, you
        # could force the values in 'series', '#my_series_1', and '#my_series_2' to
        # appear in collections named 'some_value (Series)', thereby merging all of the
        # fields into one set of collections.
        # There are two related tweaks. The first determines the category name to use
        # for a metadata field.  The second is a template, used to determines how the
        # value and category are combined to create the collection name.
        # The syntax of the first tweak, sony_collection_renaming_rules, is:
        # {'field_lookup_name':'category_name_to_use', 'lookup_name':'name', ...}
        # The second tweak, sony_collection_name_template, is a template. It uses the
        # same template language as plugboards and save templates. This tweak controls
        # how the value and category are combined together to make the collection name.
        # The only two fields available are {category} and {value}. The {value} field is
        # never empty. The {category} field can be empty. The default is to put the
        # value first, then the category enclosed in parentheses, it isn't empty:
        # '{value} {category:|(|)}'
        # Examples: The first three examples assume that the second tweak
        # has not been changed.
        # 1: I want three series columns to be merged into one set of collections. The
        # column lookup names are 'series', '#series_1' and '#series_2'. I want nothing
        # in the parenthesis. The value to use in the tweak value would be:
        #    sony_collection_renaming_rules={'series':'', '#series_1':'', '#series_2':''}
        # 2: I want the word '(Series)' to appear on collections made from series, and
        # the word '(Tag)' to appear on collections made from tags. Use:
        #    sony_collection_renaming_rules={'series':'Series', 'tags':'Tag'}
        # 3: I want 'series' and '#myseries' to be merged, and for the collection name
        # to have '(Series)' appended. The renaming rule is:
        #    sony_collection_renaming_rules={'series':'Series', '#myseries':'Series'}
        # 4: Same as example 2, but instead of having the category name in parentheses
        # and appended to the value, I want it prepended and separated by a colon, such
        # as in Series: Darkover. I must change the template used to format the category name
        # The resulting two tweaks are:
        #    sony_collection_renaming_rules={'series':'Series', 'tags':'Tag'}
        #    sony_collection_name_template='{category:||: }{value}'
        sony_collection_renaming_rules = {}
        self.type_set(
            "calibre_tweaks",
            "sony_collection_renaming_rules",
            sony_collection_renaming_rules,
            val_type="dict",
        )

        sony_collection_name_template = "{value}{category:| (|)}"
        self.set(
            "calibre_tweaks",
            "sony_collection_name_template",
            sony_collection_name_template,
        )

        #: Specify how SONY collections are sorted
        # Specify how sony collections are sorted. This tweak is only applicable if
        # metadata management is set to automatic. You can indicate which metadata is to
        # be used to sort on a collection-by-collection basis. The format of the tweak
        # is a list of metadata fields from which collections are made, followed by the
        # name of the metadata field containing the sort value.
        # Example: The following indicates that collections built from pubdate and tags
        # are to be sorted by the value in the custom column '#mydate', that collections
        # built from 'series' are to be sorted by 'series_index', and that all other
        # collections are to be sorted by title. If a collection metadata field is not
        # named, then if it is a series- based collection it is sorted by series order,
        # otherwise it is sorted by title order.
        # [(['pubdate', 'tags'],'#mydate'), (['series'],'series_index'), (['*'], 'title')]
        # Note that the bracketing and parentheses are required. The syntax is
        # [ ( [list of fields], sort field ) , ( [ list of fields ] , sort field ) ]
        # Default: empty (no rules), so no collection attributes are named.
        sony_collection_sorting_rules = []
        self.type_set(
            "calibre_tweaks",
            "sony_collection_sorting_rules",
            sony_collection_sorting_rules,
            val_type="list",
        )

        #: Control how tags are applied when copying books to another library
        # Set this to True to ensure that tags in 'Tags to add when adding
        # a book' are added when copying books to another library
        add_new_book_tags_when_importing_books = True
        self.type_set(
            "calibre_tweaks",
            "add_new_book_tags_when_importing_books",
            add_new_book_tags_when_importing_books,
            val_type="bool",
        )

        #: Set the maximum number of tags to show per book in the content server
        max_content_server_tags_shown = 5
        self.type_set(
            "calibre_tweaks",
            "max_content_server_tags_shown",
            max_content_server_tags_shown,
            val_type="int",
        )

        #: Set custom metadata fields that the content server will or will not display.
        # content_server_will_display is a list of custom fields to be displayed.
        # content_server_wont_display is a list of custom fields not to be displayed.
        # wont_display has priority over will_display.
        # The special value '*' means all custom fields. The value [] means no entries.
        # Defaults:
        #    content_server_will_display = ['*']
        #    content_server_wont_display = []
        # Examples:
        # To display only the custom fields #mytags and #genre:
        #   content_server_will_display = ['#mytags', '#genre']
        #   content_server_wont_display = []
        # To display all fields except #mycomments:
        #   content_server_will_display = ['*']
        #   content_server_wont_display['#mycomments']
        content_server_will_display = ["*"]
        self.type_set(
            "calibre_tweaks",
            "content_server_will_display",
            content_server_will_display,
            val_type="list",
        )

        content_server_wont_display = []
        self.type_set(
            "calibre_tweaks",
            "content_server_wont_display",
            content_server_wont_display,
            val_type="list",
        )

        #: Set the maximum number of sort 'levels'
        # Set the maximum number of sort 'levels' that calibre will use to resort the
        # library after certain operations such as searches or device insertion. Each
        # sort level adds a performance penalty. If the databases is large (thousands of
        # books) the penalty might be noticeable. If you are not concerned about multi-
        # level sorts, and if you are seeing a slowdown, reduce the value of this tweak.
        maximum_resort_levels = 5
        self.type_set(
            "calibre_tweaks",
            "maximum_resort_levels",
            maximum_resort_levels,
            val_type="int",
        )

        #: Choose whether dates are sorted using visible fields
        # Date values contain both a date and a time. When sorted, all the fields are
        # used, regardless of what is displayed. Set this tweak to True to use only
        # the fields that are being displayed.
        sort_dates_using_visible_fields = False
        self.type_set(
            "calibre_tweaks",
            "sort_dates_using_visible_fields",
            sort_dates_using_visible_fields,
            val_type="bool",
        )

        #: Fuzz value for trimming covers
        # The value used for the fuzz distance when trimming a cover.
        # Colors within this distance are considered equal.
        # The distance is in absolute intensity units.
        cover_trim_fuzz_value = 10
        self.type_set(
            "calibre_tweaks",
            "cover_trim_fuzz_value",
            cover_trim_fuzz_value,
            val_type="int",
        )

        #: Control behavior of the book list
        # You can control the behavior of doubleclicks on the books list.
        # Choices: open_viewer, do_nothing,
        # edit_cell, edit_metadata. Selecting anything other than open_viewer has the
        # side effect of disabling editing a field using a single click.
        # Default: open_viewer.
        # Example: doubleclick_on_library_view = 'do_nothing'
        # You can also control whether the book list scrolls horizontal per column or
        # per pixel. Default is per column.
        doubleclick_on_library_view = "open_viewer"
        self.type_set(
            "calibre_tweaks",
            "doubleclick_on_library_view",
            doubleclick_on_library_view,
            val_type="str",
        )

        horizontal_scrolling_per_column = True
        self.type_set(
            "calibre_tweaks",
            "horizontal_scrolling_per_column",
            horizontal_scrolling_per_column,
            val_type="bool",
        )

        #: Language to use when sorting.
        # Setting this tweak will force sorting to use the
        # collating order for the specified language. This might be useful if you run
        # calibre in English but want sorting to work in the language where you live.
        # Set the tweak to the desired ISO 639-1 language code, in lower case.
        # You can find the list of supported locales at
        # http://publib.boulder.ibm.com/infocenter/iseries/v5r3/topic/nls/rbagsicusortsequencetables.htm
        # Default: locale_for_sorting = '' -- use the language calibre displays in
        # Example: locale_for_sorting = 'fr' -- sort using French rules.
        # Example: locale_for_sorting = 'nb' -- sort using Norwegian rules.
        locale_for_sorting = ""
        self.type_set("calibre_tweaks", "locale_for_sorting", locale_for_sorting, val_type="str")

        #: Number of columns for custom metadata in the edit metadata dialog
        # Set whether to use one or two columns for custom metadata when editing
        # metadata  one book at a time. If True, then the fields are laid out using two
        # columns. If False, one column is used.
        metadata_single_use_2_cols_for_custom_fields = True
        self.type_set(
            "calibre_tweaks",
            "metadata_single_use_2_cols_for_custom_fields",
            metadata_single_use_2_cols_for_custom_fields,
            val_type="bool",
        )

        #: Order of custom column(s) in edit metadata
        # Controls the order that custom columns are listed in edit metadata single
        # and bulk. The columns listed in the tweak are displayed first and in the
        # order provided. Any columns not listed are dislayed after the listed ones,
        # in alphabetical order. Do note that this tweak does not change the size of
        # the edit widgets. Putting comments widgets in this list may result in some
        # odd widget spacing when using two-column mode.
        # Enter a comma-separated list of custom field lookup names, as in
        # metadata_edit_custom_column_order = ['#genre', '#mytags', '#etc']
        metadata_edit_custom_column_order = []
        self.type_set(
            "calibre_tweaks",
            "metadata_edit_custom_column_order",
            metadata_edit_custom_column_order,
            val_type="list",
        )

        #: The number of seconds to wait before sending emails
        # The number of seconds to wait before sending emails when using a
        # public email server like gmx/hotmail/gmail. Default is: 5 minutes
        # Setting it to lower may cause the server's SPAM controls to kick in,
        # making email sending fail. Changes will take effect only after a restart of
        # calibre. You can also change the list of hosts that calibre considers
        # to be public relays here. Any relay host ending with one of the suffixes
        # in the list below will be considered a public email server.
        public_smtp_relay_delay = 301
        self.type_set(
            "calibre_tweaks",
            "public_smtp_relay_delay",
            public_smtp_relay_delay,
            val_type="int",
        )

        public_smtp_relay_host_suffixes = ["gmail.com", "live.com", "gmx.com"]
        self.type_set(
            "calibre_tweaks",
            "public_smtp_relay_host_suffixes",
            public_smtp_relay_host_suffixes,
            val_type="list",
        )

        #: The maximum width and height for covers saved in the calibre library
        # All covers in the calibre library will be resized, preserving aspect ratio,
        # to fit within this size. This is to prevent slowdowns caused by extremely
        # large covers
        maximum_cover_size = (1650, 2200)
        self.type_set("calibre_tweaks", "maximum_cover_size", maximum_cover_size, val_type="tuple")

        #: Where to send downloaded news
        # When automatically sending downloaded news to a connected device, calibre
        # will by default send it to the main memory. By changing this tweak, you can
        # control where it is sent. Valid values are "main", "carda", "cardb". Note
        # that if there isn't enough free space available on the location you choose,
        # the files will be sent to the location with the most free space.
        send_news_to_device_location = "main"
        self.set(
            "calibre_tweaks",
            "send_news_to_device_location",
            send_news_to_device_location,
        )

        #: What interfaces should the content server listen on
        # By default, the calibre content server listens on '0.0.0.0' which means that it
        # accepts IPv4 connections on all interfaces. You can change this to, for
        # example, '127.0.0.1' to only listen for connections from the local machine, or
        # to '::' to listen to all incoming IPv6 and IPv4 connections (this may not
        # work on all operating systems)
        server_listen_on = "0.0.0.0"
        self.set("calibre_tweaks", "server_listen_on", server_listen_on)

        #: Unified toolbar on OS X
        # If you enable this option and restart calibre, the toolbar will be 'unified'
        # with the titlebar as is normal for OS X applications. However, doing this has
        # various bugs, for instance the minimum width of the toolbar becomes twice
        # what it should be and it causes other random bugs on some systems, so turn it
        # on at your own risk!
        unified_title_toolbar_on_osx = False
        self.type_set(
            "calibre_tweaks",
            "unified_title_toolbar_on_osx",
            unified_title_toolbar_on_osx,
            val_type="bool",
        )

        #: Save original file when converting/polishing from same format to same format
        # When calibre does a conversion from the same format to the same format, for
        # example, from EPUB to EPUB, the original file is saved, so that in case the
        # conversion is poor, you can tweak the settings and run it again. By setting
        # this to False you can prevent calibre from saving the original file.
        # Similarly, by setting save_original_format_when_polishing to False you can
        # prevent calibre from saving the original file when polishing.
        save_original_format = True
        self.type_set(
            "calibre_tweaks",
            "save_original_format",
            save_original_format,
            val_type="bool",
        )

        save_original_format_when_polishing = True
        self.type_set(
            "calibre_tweaks",
            "save_original_format_when_polishing",
            save_original_format_when_polishing,
            val_type="bool",
        )

        #: Number of recently viewed books to show
        # Right-clicking the View button shows a list of recently viewed books. Control
        # how many should be shown, here.
        gui_view_history_size = 15
        self.type_set(
            "calibre_tweaks",
            "gui_view_history_size",
            gui_view_history_size,
            val_type="int",
        )

        #: Change the font size of book details in the interface
        # Change the font size at which book details are rendered in the side panel and
        # comments are rendered in the metadata edit dialog. Set it to a positive or
        # negative number to increase or decrease the font size.
        change_book_details_font_size_by = 0
        self.type_set(
            "calibre_tweaks",
            "change_book_details_font_size_by",
            change_book_details_font_size_by,
            val_type="int",
        )

        #: Compile General Program Mode templates to Python
        # Compiled general program mode templates are significantly faster than
        # interpreted templates. Setting this tweak to True causes calibre to compile
        # (in most cases) general program mode templates. Setting it to False causes
        # calibre to use the old behavior -- interpreting the templates. Set the tweak
        # to False if some compiled templates produce incorrect values.
        # Default:    compile_gpm_templates = True
        # No compile: compile_gpm_templates = False
        compile_gpm_templates = True
        self.type_set(
            "calibre_tweaks",
            "compile_gpm_templates",
            compile_gpm_templates,
            val_type="bool",
        )

        #: What format to default to when using the Tweak feature
        # The Tweak feature of calibre allows direct editing of a book format.
        # If multiple formats are available, calibre will offer you a choice
        # of formats, defaulting to your preferred output format if it is available.
        # Set this tweak to a specific value of 'EPUB' or 'AZW3' to always default
        # to that format rather than your output format preference.
        # Set to a value of 'remember' to use whichever format you chose last time you
        # used the Tweak feature.
        # Examples:
        #   default_tweak_format = None       (Use output format)
        #   default_tweak_format = 'EPUB'
        #   default_tweak_format = 'remember'
        default_tweak_format = None
        self.set("calibre_tweaks", "default_tweak_format", default_tweak_format)

        #: Do not preselect a completion when editing authors/tags/series/etc.
        # This means that you can make changes and press Enter and your changes will
        # not be overwritten by a matching completion. However, if you wish to use the
        # completions you will now have to press Tab to select one before pressing
        # Enter. Which technique you prefer will depend on the state of metadata in
        # your library and your personal editing style.
        preselect_first_completion = False
        self.type_set(
            "calibre_tweaks",
            "preselect_first_completion",
            preselect_first_completion,
            val_type="bool",
        )

        #: Completion mode when editing authors/tags/series/etc.
        # By default, when completing items, calibre will show you all the candidates
        # that start with the text you have already typed. You can instead have it show
        # all candidates that contain the text you have already typed. To do this, set
        # completion_mode to 'contains'. For example, if you type asi it will match both
        # Asimov and Quasimodo, whereas the default behavior would match only Asimov.
        completion_mode = "prefix"
        self.set("calibre_tweaks", "completion_mode", completion_mode)

        #: Recognize numbers inside text when sorting
        # This means that when sorting on text fields like title the text "Book 2"
        # will sort before the text "Book 100". If you want this behavior, set
        # numeric_collation = True note that doing so will cause problems with text
        # that starts with numbers and is a little slower.
        numeric_collation = False
        self.type_set("calibre_tweaks", "numeric_collation", numeric_collation, val_type="bool")

        #: Sort the list of libraries alphabetically
        # The list of libraries in the Copy to Library and Quick Switch menus are
        # normally sorted by most used. However, if there are more than a certain
        # number of such libraries, the sorting becomes alphabetic. You can set that
        # number here. The default is ten libraries.
        many_libraries = 10
        self.type_set("calibre_tweaks", "many_libraries", many_libraries, val_type="int")

        #: Highlight the virtual library name when using a Virtual Library
        # The virtual library name next to the Virtual Library button is highlighted in
        # yellow when using a Virtual Library. You can choose the color used for the
        # highlight with this tweak. Set it to 'transparent' to disable highlighting.
        highlight_virtual_library = "yellow"
        self.set("calibre_tweaks", "highlight_virtual_library", highlight_virtual_library)

        #: Choose available output formats for conversion
        # Restrict the list of available output formats in the conversion dialogs.
        # For example, if you only want to convert to EPUB and AZW3, change this to
        # restrict_output_formats = ['EPUB', 'AZW3']. The default value of None causes
        # all available output formats to be present.
        restrict_output_formats = None
        self.type_set(
            "calibre_tweaks",
            "restrict_output_formats",
            restrict_output_formats,
            val_type="list",
        )

        #: Set the thumbnail image quality used by the content server
        # The quality of a thumbnail is largely controlled by the compression quality
        # used when creating it. Set this to a larger number to improve the quality.
        # Note that the thumbnails get much larger with larger compression quality
        # numbers.
        # The value can be between 50 and 99
        content_server_thumbnail_compression_quality = 75
        self.type_set(
            "calibre_tweaks",
            "content_server_thumbnail_compression_quality",
            content_server_thumbnail_compression_quality,
            val_type="int",
        )

        #: Image file types to treat as ebooks when dropping onto the Book Details panel
        # Normally, if you drop any image file in a format known to calibre onto the
        # Book Details panel, it will be used to set the cover. If you want to store
        # some image types as ebooks instead, you can set this tweak.
        # Examples:
        #    cover_drop_exclude = {'tiff', 'webp'}
        cover_drop_exclude = ()
        self.type_set("calibre_tweaks", "cover_drop_exclude", cover_drop_exclude, val_type="tuple")

        self.type_set("calibre_tweaks", "user_categories", None, val_type="none")
        self.type_set("calibre_tweaks", "use_primary_find_in_search", True, val_type="bool")
        self.set("calibre_tweaks", "saved_searches", "")

        self.type_set("calibre_tweaks", "add_formats_to_existing", True, val_type="bool")

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - UTILS

    def val_to_str(self, val, val_type):
        """
        Render the value into a string form suitable for storing in the underlying config.
        :param val:
        :param val_type: What conversion method should be used on this val?
        :return:
        """
        if val is None:
            return "{}:none".format(val_type)

        val_type = val_type.lower()

        handler = self.val_to_str_plugins[val_type]
        return "{}:{}".format(val_type, handler(val))

    def str_to_val(self, val_str):
        """
        Converts a string back to a value.
        :param val_str:
        :return (val_type, val): THe declared valued type, and the actual value itself
        """
        val_type = re.match(r"(^[a-zA-Z0-9_]+):", val_str).group(1).lower()
        if val_type not in Preferences.known_types:
            err_str = "Unable to parse value - value type not recongized for load\n"
            err_str += "val_type: {}\n".format(val_type)
            err_str += "val_str: {}\n".format(val_str)
            raise ParseError(err_str)

        raw_val_str = re.sub(r"(^[a-zA-Z0-9_]+):", "", val_str)

        if raw_val_str.lower() == "none":
            return val_type.lower(), None

        handler = self.str_to_val_plugins[val_type]
        return val_type, handler(raw_val_str)

    @staticmethod
    def __val_parse_err_message(val_type, raw_val_str, val_str):
        """
        Error message for when value parsing fails.
        :return:
        """
        err = list(["Parsing the given raw_val_str into the provided type has failed"])
        err.append("val_type: {}".format(val_type))
        err.append("raw_val_str: {}".format(raw_val_str))
        err.append("val_str: {}".format(val_str))
        return "\n".join(err)

    def liuxin_to_json(self, val):
        """
        Use the LiuXin json encode to dump a file.
        :param val:
        :return:
        """
        return self.liuxin_json.dumps(val)

    def liuxin_from_json(self, val):
        """
        Use the LiuXin json to read back a file.
        :param val:
        :return:
        """
        return self.liuxin_json.loads(val)

    @staticmethod
    def to_json(val, **kwargs):
        """
        Use json to render the value as a string
        :param val:
        :return:
        """
        # Split off as a function to provide for easy customization of sotrage later
        return json.dumps(val, ensure_ascii=False, **kwargs)

    @staticmethod
    def from_json(val):
        """
        Use json to produce the original value from a string.
        :param val:
        :return:
        """
        return json.loads(val)


# ----------------------------------------------------------------------------------------------------------------------
#
# - CONVERTERS TO DESEARLIZE OBJECTS


def bool_to_str(val):
    """
    Convert the given variable into a string for storage - or throw an error
    :param var:
    :return:
    """
    val = str(val).lower()
    if val not in ["true", "false"]:
        raise NotImplementedError("cannot parse val - couldn't convert it to bool - val {}".format(val))
    return val


def none_to_str(val):
    """
    Try and convert the given value to a none string - throws an error if it can't.
    :param val:
    :return:
    """
    val = str(val).lower()
    if val != "none":
        raise NotImplementedError("cannot parse val - couldn't convert it to bool - val {}".format(val))
    return val


def set_to_str(val, handler=json.dumps):
    """
    Try and convert the given set to a string - use the provided handler to try and manage the conversion.
    val will be converted to a list before the handler is called with it.
    :param val:
    :param handler:
    :return:
    """
    val = [v for v in val]
    return handler(val)


def bool_str_to_bool(bool_str):
    if bool_str.lower() == "true":
        return True
    elif bool_str.lower() == "false":
        return False
    else:
        raise NotImplementedError("Cannot convert {} to a bool".format(bool_str))


def none_str_to_none(none_str):
    """
    Try and parse a None string - there is only one correct answer - None
    :param none_str:
    :return:
    """
    if none_str.lower() == "none":
        return None
    else:
        raise NotImplementedError("Cannot convert {} to a None".format(none_str))


def set_str_to_set(set_str, handler=json.loads):
    """
    Try and parse a string into a set (assume that the string is going to give back a iterable - then convert it into
    a set).
    :param set_str:
    :param handler: The function that will be used to convert the given string back into an object
    :return:
    """
    json_rtn = handler(set_str)
    assert isinstance(json_rtn, list)
    return set(json_rtn)


def tuple_str_to_tuple(tuple_str, handler=json.loads):
    """
    Try and parse a string into a tuple (assume that the string is going to give back a iterable - then convert it into
    a tuple).
    :param tuple_str:
    :param handler:
    :return:
    """
    json_rtn = handler(tuple_str)
    assert isinstance(json_rtn, list)
    return tuple(json_rtn)


# TODO: None of these are actually in use - put them somewhere central so a bunch of methods can refer to them
def py_set_adapter(py_set):
    """
    Takes a set - turning it into a string suitable for storing within an SQLite databaase, which can be parsed back out
    by the py_set_converted function.
    :param py_set:
    :return:
    """
    py_set = deepcopy(py_set)
    py_list = []
    for element in py_set:
        # Coerce to unicode, escape any SQL special characters, then add to the list of elements
        element = six_unicode(element)
        element = element.replace("'", "''")
        element = element.replace('"', '\\"')
        py_list.append(element)
    return "'" + "','".join(py_list) + "'"


def set_to_string(pyset):
    """
    Takes a set and converts it into a string suitable for saving in to a preferences file
    :param pyset: A python set
    :return:
    """
    return "PYSET - " + py_set_adapter(pyset)


def py_set_converter(py_set_string):
    """
    Converted intended to be used with set fields from the databases - turns them into sets of unicode strings.
    Takes a string from the databases and returns it as a set.
    :param py_set_string:
    :return py_set:
    """
    py_set_string = deepcopy(py_set_string)
    # Accounting for the way SQL escapes quotes
    py_set_string = py_set_string.replace("''", "'")

    py_set = set()
    last_char = " "
    current_string = ""
    accumulation_mode = False
    for char in py_set_string:
        if char == "'" and last_char != "\\":
            accumulation_mode = not accumulation_mode
            if current_string:
                py_set.add(current_string)
                current_string = ""
        elif char == "'" and last_char == "\\":
            # The SQL \ used to escape a quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise ValueError(err_str)
        elif char == '"' and last_char == "\\":
            # The SQL \ used to escape a double quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
        elif accumulation_mode:
            current_string += char
        else:
            if char != ",":
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise ValueError(err_str)
        last_char = char
    return py_set


def string_to_set(set_string):
    """
    Takes a serialized string and turns it back into a set.
    :param set_string:
    :return:
    """
    pyset_re = r"PYSET - (.*)"
    pyset_pat = re.compile(pyset_re)
    pyset_match = pyset_pat.match(set_string)

    if pyset_match is None:
        return None

    set_string = pyset_match.group(1)

    return py_set_converter(set_string)


#
# ----------------------------------------------------------------------------------------------------------------------

# Setup the default preferences object
preferences = Preferences()
