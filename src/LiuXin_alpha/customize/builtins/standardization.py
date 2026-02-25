"""
These plugins which represent very basic functions of LiuXin - allows for very deep tweaking of behavior.

In this case, how names are standardized and matched.
"""

from __future__ import unicode_literals

import itertools
import re
import uuid
from copy import deepcopy

from LiuXin_alpha.constants import name_prefixes
from LiuXin_alpha.constants import name_suffixes

from LiuXin_alpha.exceptions import InputIntegrityError

from LiuXin_alpha.library.standardization import LITTLE_WORDS
from LiuXin_alpha.library.standardization import ALL_DROP_CHARACTERS

from LiuXin_alpha.metadata import author_to_author_sort

from LiuXin_alpha.utils.icu import lower as icu_lower
from LiuXin_alpha.utils.filenames import ascii_filename
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logger import default_log

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.lx_libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems

from past.builtins import basestring


class TitlePhashHandler:
    """
    When called with a md object, generates a title-author phash which can be used to search the titles table for
    matches.
    Used when adding a title to the library to generate the title_phash - also used when trying to see if there's a
    book corresponding to that metadata in the library.
    This class exists to generate a range of reasonable phashes from the titles and authors - one of which will
    hopefully match so that the title in question can be retrieved.
    Generating the phash, and generating the candidate phashes when searching for a title are similar operations (and
    having the code for both in the same place just makes sense).
    ALWAYS USE THE UI. DO NOT LOAD THIS CLASS DIRECTLY.
    """

    # Drop strings are strings which should be ignored - they're simplified and have to be surrounded with spaces to
    # register
    drop_strings = set([])
    for short, full in iteritems(name_prefixes):
        new_short = icu_lower(short).replace(".", "").replace(",", "")
        new_full = icu_lower(full).replace(".", "").replace(",", "")
        new_short_set = set([s for s in new_short.split(" ")])
        new_full_set = set([s for s in new_full.split(" ")])
        drop_strings.update(new_short_set)
        drop_strings.update(new_full_set)
    for short, full in iteritems(name_suffixes):
        new_short = icu_lower(short).replace(".", "").replace(",", "")
        new_full = icu_lower(full).replace(".", "").replace(",", "")
        new_short_set = set([s for s in new_short.split(" ")])
        new_full_set = set([s for s in new_full.split(" ")])
        drop_strings.update(new_short_set)
        drop_strings.update(new_full_set)

    @classmethod
    def creator_standardize(cls, creator_string):
        """
        Brings a name into standard form.
        This module relies on other standardization modules - the default is the builtin one in
        LiuXin.customize.builtins.standardization_plugins - when loaded through the title_phash_handler() method in the
        UI this should be replaced with the actual standardization plugin.
        In other words - ALWAYS USE THE UI. DO NOT LOAD THIS CLASS DIRECTLY.
        :param creator_string:
        :return:
        """
        return CreatorStandardize.standardize_creator(creator_string)

    @classmethod
    def make_phash(cls, title, authors):
        """
        Takes a title and a list of authors names and generates a phash from them.
        :param title:
        :param authors:
        :return:
        """
        # 1) Standardize the authors list
        authors_list = cls.__do_author_list_standardization(authors)

        # 2) standardize the title string
        title_string = cls.__do_title_standardization(title)
        title_string = title_string.strip()
        title_string = re.sub(r"\s+", "_", title_string)

        # 3) Isolate the last names from the authors list
        author_last_names = sorted([a.split(" ")[-1] for a in authors_list])
        author_string = "_".join(author_last_names)
        return "{}_{}".format(title_string, author_string)

    # Todo: Add name guessing for long author strings
    @classmethod
    def make_cand_phashes(cls, title, authors):
        """
        Takes the title and an authors iterable - tries to generate reasonable phashes from these.
        :param title: A string
        :param authors: An iterable of strings
        :return:
        """
        # Bring the authors list into a normalized form
        authors_list = []
        for author in authors:
            if "&" in author:
                authors_list += author.split("&")
            else:
                authors_list += [author]

        authors_list = cls.__do_author_list_standardization(authors_list)

        # Bring the title string into a normalised form
        title_string = cls.__do_title_standardization(title)

        # Explore ordering to create a range of possible phashes - use all strings available - the number of
        # possibilities increases exponentially - so limiting them to stop disaster (tm)
        phashes = set()
        # For relatively small author counts the full method can be used. This scales horrifically, so the cut off is
        # fairly tight
        if len(authors_list) < 4:
            phashes.add(title_string)
            author_phash_strings = cls.cand_author_strings(authors_list)
            for author_phash in author_phash_strings:
                phashes.add("{}_{}".format(title_string, author_phash))
            return phashes
        # For intermediate counts - try and isolate the surnames - add all combinations to the title to make the phashes
        elif len(authors_list) >= 4 < 6:
            phashes.add(title_string)
            author_surnames = [a.split(" ")[-1] for a in authors_list]
            author_phash_strings = cls._get_all_orderings(author_surname_list=author_surnames)
            for author_phash in author_phash_strings:
                phashes.add("{}_{}".format(title_string, author_phash))
            return phashes
        # Fallback - just arrange everything alphabetically
        else:
            phashes.add(title_string)
            author_surnames = sorted([a.split(" ")[-1] for a in authors_list])
            author_phash = "_".join(author_surnames)
            phashes.add("{}_{}".format(title_string, author_phash))
            return phashes

    @classmethod
    def __do_title_standardization(cls, title_string):
        """
        Standardize the title string.
        :param title_string:
        :return:
        """
        title_string = icu_lower(title_string)
        title_string = cls.normalize_whitespace(title_string)
        title_string = cls.scrub_little_words(title_string)
        title_string = cls.scrub_forbidden_character(title_string)
        title_string = re.sub(r"\s+", "_", title_string)
        return title_string

    @classmethod
    def __do_author_list_standardization(cls, authors_list):
        """
        Tries to bring the given list of authors names into normal form.
        :param authors_list:
        :return:
        """
        authors_list = [icu_lower(a) for a in authors_list]
        authors_list = [cls.swap_first_last(a) for a in authors_list]
        authors_list = [cls.normalize_whitespace(a) for a in authors_list]
        authors_list = [cls.drop_prefixes_suffixes(a) for a in authors_list]
        authors_list = [cls.scrub_little_words(a) for a in authors_list]
        authors_list = [cls.scrub_forbidden_character(a) for a in authors_list]
        return authors_list

    @classmethod
    def swap_first_last(cls, author_string):
        """
        Swamps the first and the last name, if the string is broken with a comma.
        :param author_string:
        :return:
        """
        if "," in author_string:
            author_tokens = author_string.split(",")
            if len(author_tokens) == 2:
                return author_tokens[1].strip() + " " + author_tokens[0].strip()
        return author_string.strip()

    @classmethod
    def normalize_whitespace(cls, author_string):
        """
        Uses a regex replace to normalize all the whitespace down to single spaces.
        :param author_string:
        :return:
        """
        return re.sub(r"\s+", " ", author_string)

    @classmethod
    def drop_prefixes_suffixes(cls, author_string):
        """
        Remove prefixes and suffixes from the given author string.
        :param author_string:
        :return:
        """
        # Prefixes and suffixes need to be removed without killing chunks of words - so tokenize on spaces, drop any
        # strings which are in the drop list
        authors_string_tokens = author_string.split(" ")
        filtered_tokens = []
        for token in authors_string_tokens:
            if icu_lower(token) in cls.drop_strings:
                pass
            else:
                filtered_tokens.append(token)
        return " ".join(filtered_tokens).strip()

    @classmethod
    def scrub_little_words(cls, string):
        """
        Remove little words from the given string.
        :param string:
        :return:
        """
        string_tokens = string.split(" ")
        filtered_tokens = []
        for token in string_tokens:
            if icu_lower(token) not in LITTLE_WORDS:
                filtered_tokens.append(token)
        return " ".join(filtered_tokens)

    @classmethod
    def scrub_forbidden_character(cls, string):
        """
        Remove every forbidden character from the given string - replacing it with whitespace.
        :param string:
        :return:
        """
        allowed_chars = []
        for char in string:
            if char not in ALL_DROP_CHARACTERS:
                allowed_chars.append(char)
        return "".join(allowed_chars)

    @classmethod
    def cand_author_strings(cls, author_name_list):
        """
        Takes an author names list - returns a set of candidate strings.
        :param author_name_list:
        :return:
        """
        all_matches = set()
        for author_orders in itertools.permutations(author_name_list):
            all_matches = all_matches.union(cls._do_one_ordering(author_orders))
        return all_matches

    @classmethod
    def _get_all_orderings(cls, author_surname_list):
        """
        Takes a list of strings - returns a set of them in all possible orders.
        The size of this return scales factorially. So it can become a problem - fast.
        :param author_surname_list: A list of strings to be combined in all possible ways.
        :return:
        """
        author_phashes = set()
        for ordering in itertools.permutations(author_surname_list):
            author_phashes.add("_".join(ordering))
        return author_phashes

    @classmethod
    def _do_one_ordering(cls, ordered_author_name_list):
        """
        Build all author strings for one ordering of author names.
        :param ordered_author_name_list:
        :return:
        """
        match_strings = {""}
        for author_name in ordered_author_name_list:
            new_match_strings = set()
            for author_token in author_name.split(" "):
                for partial_string in deepcopy(match_strings):
                    if partial_string:
                        new_match_strings.add(partial_string + "_" + author_token)
                    else:
                        new_match_strings.add(author_token)
            match_strings = new_match_strings
        return match_strings


class CreatorStandardize:
    """
    Class which presents a single method - standardize_creator. Which does it's best to bring a name into a standard
    form.
    """

    # 0) Replace all white space with single spaces
    # 1) Insert [. ] after every capital followed by another capital
    # 1.5) Insert a [ ] after every full stop
    # 2) Isolated lower case letters should be capitalized and followed with a .as
    # 3) Isolated capital letters should be followed with a full stop
    # 4) The first letter of the string should always become a capital
    # 5) Lower case letters should never be immediately followed by an upper case (except in the case of Mc/Mac
    @classmethod
    def standardize_creator(cls, creator_string):
        """
        Brings a name into a standard form.
        :param creator_string:
        :return:
        """
        creator_string = deepcopy(creator_string)
        input_string_tokenized = creator_string.split(",")
        if len(input_string_tokenized) == 1:
            working_string = input_string_tokenized[0]
        elif len(input_string_tokenized) == 2:
            working_string = input_string_tokenized[1] + " " + input_string_tokenized[0]
        elif len(input_string_tokenized) > 2:
            working_string = ",".join(input_string_tokenized)
        else:
            err_str = "standardize_name has failed. Input could not be parsed."
            default_log.log_variables(err_str, "ERROR", ("creator_string", creator_string))
            raise InputIntegrityError(err_str)

        # 0) Replace all white space with single spaces
        working_string = re.sub(r"\s+", r" ", working_string)

        # 1) Insert [. ] after every capital followed by another capital
        double_caps_re = r"[a-zA-Z0-9. ]*[A-Z][A-Z][a-zA-Z0-9. ]*"
        double_caps_pat = re.compile(double_caps_re)
        while double_caps_pat.match(working_string) is not None:
            working_string = re.sub(r"(?P<I>[A-Z])(?P<II>[A-Z])", r"\g<I>. \g<II>", working_string)

        # 1.5) Insert a [ ] after every full stop
        working_string = re.sub(r"(?P<I>\.)(?P<II>[^\.\s])", r"\g<I> \g<II>", working_string)

        # 2) Isolated lower case letters should be capitalized and followed with a .as
        isolated_lower_re = r"([a-zA-Z0-9.\s]*\s)([a-z])(\s[a-zA-Z0-9.\s]*)"
        isolated_lower_pat = re.compile(isolated_lower_re)
        while isolated_lower_pat.match(working_string) is not None:
            match = isolated_lower_pat.match(working_string)
            working_string = match.group(1) + match.group(2).upper() + match.group(3)

        # 3) Isolated capital letters should be followed with a full stop
        isolated_capital_regex = r"([a-zA-Z0-9. ]*\s)([A-Z])\s([a-zA-Z0-9. ]*)"
        isolated_capital_pat = re.compile(isolated_capital_regex)
        while isolated_capital_pat.match(working_string) is not None:
            match = isolated_capital_pat.match(working_string)
            working_string = match.group(1) + match.group(2).upper() + ". " + match.group(3)

        # 4) The first letter of the string should always be a capital.
        first_letter_regex = r"([a-zA-Z])([a-zA-Z0-9.\s]*)"
        first_letter_pat = re.compile(first_letter_regex)
        working_string_match = first_letter_pat.match(working_string)
        if working_string_match is not None:
            working_string = working_string_match.group(1).upper() + working_string_match.group(2)

        # 5) Unless in the case of Mc/Mac a capital should always be preceded by a space, unless it's Mc/Mac
        # Crude - puts a space in front of every capital
        pre_capital_insert_regex = r"([a-zA-Z0-9.\s]*[a-z])([A-Z])([a-zA-Z0-9.\s]*)"
        pre_capital_insert_pat = re.compile(pre_capital_insert_regex)
        while pre_capital_insert_pat.match(working_string) is not None:
            match = pre_capital_insert_pat.match(working_string)
            working_string = match.group(1) + " " + match.group(2) + match.group(3)

        # The first letter of any word should be a capital
        working_string_tokens = working_string.split()
        new_tokens = []
        for token in working_string_tokens:
            if len(token) == 0:
                current_token = ""
            elif len(token) == 1:
                current_token = token[0].upper() + "."
            else:
                current_token = token[0].upper() + token[1:]
            new_tokens.append(current_token)
        working_string = " ".join(new_tokens)

        # combine any instance of u'Mac' or u'Mc' into the next word.
        post_mc_space_regex = r"([a-zA-Z0-9.\s]*)(Mc|Mac) ([A-Z][a-zA-Z0-9.\s]*)"
        post_mc_space_pat = re.compile(post_mc_space_regex)
        while post_mc_space_pat.match(working_string) is not None:
            match = post_mc_space_pat.match(working_string)
            working_string = match.group(1) + match.group(2) + match.group(3)

        # making sure any white space is reduced to a single space
        working_string = re.sub(r"\s+", r" ", working_string).strip()

        return working_string


class BaseNameGenerator:
    """
    Makes name for Files stored in the FolderStore.
    """

    recognized_restrictions = {
        "total_name_length": "The total length of the name of the file - including extension. Must be an integer",
        "extlen": "The maximum length of the extensions for a file - must be an integer",
    }

    # After the data for make_file_name has been normalised and copied into a dictionary it will be copied into these
    # templates in order until one is found that passes all the restriction tests - this string is then returned - these
    # are present in the order that they're tested in (saved here so that they can be more easily over-ridden)
    template_list = [
        "{0[title]} by {0[creators]}{0[series]}{0[book]}{0[folder]}{0[file]}{0[file_priority]}"
        "{0[tag]}.{0[extension]}",
        "{0[title]} by {0[creators_short]}{0[series]}{0[book]}{0[folder]}{0[file]}{0[file_priority]}"
        "{0[tag]}.{0[extension]}",
        "{0[title]} by {0[creators]}{0[series]}{0[folder]}{0[file]}{0[file_priority]}{0[tag]}." "{0[extension]}",
        "{0[title]} by {0[creators_short]}{0[series]}{0[folder]}{0[file]}{0[file_priority]}{0[tag]}." "{0[extension]}",
        "{0[title]} by {0[creators]}{0[series]}{0[file]}{0[file_priority]}{0[tag]}.{0[extension]}",
        "{0[title]} by {0[creators_short]}{0[series]}{0[file]}{0[file_priority]}{0[tag]}.{0[extension]}",
        "{0[title]} by {0[creators]}{0[series]}{0[file_priority]}{0[tag]}.{0[extension]}",
        "{0[title]} by {0[creators_short]}{0[series]}{0[file_priority]}{0[tag]}.{0[extension]}",
        "{0[title]} by {0[creators]}{0[series]}{0[tag]}.{0[extension]}",
        "{0[title]} by {0[creators_short]}{0[series]}{0[tag]}.{0[extension]}",
    ]

    book_file_template_list = ["{0[title]} by {0[creator_sort]} - cover_id {0[cover_id]}.{0[extension]}"]

    creator_file_template_list = ["{0[creator]} - cover_id {0[cover_id]}.{0[extension]}"]

    series_file_template_list = ["{0[series]} - cover_id {0[cover_id]}.{0[extension]}"]

    # Keyed with the bad character and valued with it's replacement
    # Windows bad characters include ~ # % & * { } \ : ; < > ? / + | " . , `
    bad_character_map = {
        "~": "-",
        "#": "-",
        "%": "-",
        "&": " and ",
        "*": "-",
        "{": "(",
        "}": ")",
        "\\": "-",
        ":": "-",
        ";": "-",
        "<": "(",
        ">": ")",
        "?": "-",
        "/": "-",
        "+": "-",
        "|": "-",
        '"': "-",
        ",": "-",
        ".": "-",
    }

    def __init__(self, restrictions=None):
        """
        Initialize the class
        :param restrictions: A dictionary of restrictions on the final produce - call recognized_restrictions for a dict
                             keyed with the name of the recognized restriction and valued with an explanation
        :return:
        """
        if restrictions is not None:
            self.restrictions = restrictions
        else:
            self.restrictions = dict()

    def __sanitize_name_str(self, name_str):
        """
        Remove and replace any characters in the bad characters map.
        :return:
        """
        name_str = six_unicode(name_str)

        for bad_char in self.bad_character_map:
            bad_char_rep = self.bad_character_map[bad_char]
            name_str = name_str.replace(bad_char, bad_char_rep)
        return name_str

    def __sanitize_ext_str(self, ext_str):
        """
        Remove and replace any characters in the bad character map.  "." are still allowed.
        :param ext_str:
        :return:
        """
        ext_str = six_unicode(ext_str)

        for bad_char in self.bad_character_map:
            if bad_char == ".":
                continue
            bad_char_rep = self.bad_character_map[bad_char]
            ext_str = ext_str.replace(bad_char, bad_char_rep)
        return ext_str

    def make_file_name(
        self,
        title,
        creator_rows,
        extension,
        series=None,
        series_index=None,
        book_id=None,
        folder_id=None,
        file_id=None,
        file_priority=None,
        tag=None,
        forbidden_names=None,
    ):
        """
        Make a name for a file - options to include a lot of information are included - the provided information may or
        may not be included depending on the restrictions imposed by the file length.
        :param creator_rows: A list of the rows - must be a list as the order will be preserved from the iterable.
                            NOTE: The role of the creators will not be recorded - just their names in order
        :param title: Title string
        :param extension: The file extension
        :param series: The series of the object
        :param series_index: Position of the object in the given series
        :param book_id: The id of the book this file is linked to on the database
        :param folder_id: The id of the folder this file should be in
        :param file_id: The id of this file on the database
        :param file_priority: The priority of this file in this book on the database
        :param tag: An optional tag to add at the end of the file name
        :param forbidden_names: A list of names that already exist in the location the object is going to be placed at -
                                makes sure that clashes can't occur (which could silently remove data as one file get's
                                written over the top of another)
        :return:
        """
        # To hard to implement the check for extension length in the proper method - so implementing it here
        if self.restrictions is not None:
            if self.restrictions.get("extlen", None) is not None and extension is not None:
                if self.restrictions["extlen"] < len(extension):
                    err_str = "make_file_name was passed an extension which was too long"
                    default_log.log_variables(
                        err_str,
                        "ERROR",
                        ("extension", extension),
                        ("self.restrictions", self.restrictions),
                    )
                    raise InputIntegrityError(err_str)

        # Stores the components of the file name - keyed with their name and valued as a string - allows all desired
        # information to be put in one place (so some of it can be excluded if the file name goes over.
        fn_components = dict()

        # title
        fn_components["title"] = ascii_filename(title.lstrip()).decode("ascii", "replace").rstrip()

        # Todo: Account for the editor/author problem
        # creators
        if len(creator_rows) == 0:
            fn_components["creators"] = ""
        else:
            creator_strings = []
            creator_short_strings = []
            for creator_row in creator_rows:
                creator_name = self.__sanitize_name_str(
                    ascii_filename(creator_row["creator"].lstrip().decode("ascii", "replace")).rstrip()
                )
                creator_strings.append(creator_name)

                cr_short_name = creator_row["creator_short_name"]

                if isinstance(cr_short_name, basestring):
                    creator_short_name = self.__sanitize_name_str(
                        ascii_filename(cr_short_name.lstrip()).decode("ascii", "replace").rstrip()
                    )

                    creator_short_strings.append(creator_short_name)

            if len(creator_short_strings) > 0:
                fn_components["creators"] = " & ".join(creator_strings)
            else:
                fn_components["creators"] = _("Unknown")
            if len(creator_short_strings) > 0:
                fn_components["creators_short"] = "&".join(creator_short_strings)
            else:
                fn_components["creators_short"] = _("Unknown")

        # extension
        extension = extension.replace(".", "")
        fn_components["extension"] = self.__sanitize_name_str(
            ascii_filename(six_unicode(extension).lstrip()).decode("ascii", "replace").rstrip()
        )

        # series
        if series and six_unicode(series).lower() != "none":
            series_name_str = self.__sanitize_name_str(
                ascii_filename(series.lstrip()).decode("ascii", "replace").rstrip()
            )
            series_pos_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(series_index).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["series"] = " - {0} # {1}".format(series_name_str, series_pos_str)
        else:
            fn_components["series"] = ""

        # book_id
        if book_id:
            book_id_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(book_id).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["book"] = " - lx_book_id # {}".format(book_id_str)
        else:
            fn_components["book"] = ""

        # folder_id
        if folder_id:
            folder_id_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(folder_id).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["folder"] = " - lx_folder # {}".format(folder_id_str)
        else:
            fn_components["folder"] = ""

        # file_id
        if file_id:
            file_id_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(file_id).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["file"] = " - lx_file # {}".format(file_id_str)
        else:
            fn_components["file"] = ""

        # file_priority
        if file_priority:
            file_priority_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(file_priority).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["file_priority"] = " - file_priority # {}".format(file_priority_str)
        else:
            fn_components["file_priority"] = ""

        # tag
        if tag:
            tag_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(tag).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["tag"] = tag_str
        else:
            fn_components["tag"] = ""

        # Work down a list of standard name templates - continue until one is found that fulfills all the restrictions -
        # then return it
        for template in self.template_list:
            file_name_str = template.format(fn_components)
            if self.check_against_restrictions(file_name_str):
                return file_name_str

        raise NotImplementedError("Couldn't make_file_name under the current restrictions")

    def make_file_name_meta(
        self,
        meta_row,
        extension,
        folder_id=None,
        file_id=None,
        file_priority=None,
        tag=None,
    ):
        """
        Takes a meta row retrieve from the database - extracts all the information it needs to make a name for a file
        and returns the new file name.
        :param meta_row: Contains all the metadata which should be needed to make the file name
        :param extension: The extension for the file
        :param folder_id: The id of the folder the file should be in
        :param file_id: The id of the file on the system
        :param file_priority: The priority of the file in the book
        :param tag: Optional tag to add to the end of a file
        :return:
        """
        # Check to see if the given extension conforms to the restrictions
        if self.restrictions is not None:
            if self.restrictions.get("extlen", None) is not None and extension is not None:
                if self.restrictions["extlen"] < len(extension):
                    err_str = "make_file_name was passed an extension which was too long"
                    default_log.log_variables(
                        err_str,
                        "ERROR",
                        ("extension", extension),
                        ("self.restrictions", self.restrictions),
                    )
                    raise InputIntegrityError(err_str)

        # Stores the components of the file name - keyed with their name and valued as a string - allows all desired
        # information to be put in one place (so some of it can be excluded if the file name goes over.
        fn_components = dict()

        # title
        fn_components["title"] = self.__sanitize_name_str(
            ascii_filename(meta_row["title"].lstrip()).decode("ascii", "replace").rstrip()
        )

        # creators
        if not meta_row["authors"]:
            fn_components["creators"] = ""
        else:
            fn_components["creators"] = self.__sanitize_name_str(ascii_filename(meta_row["authors"]))

        # extension
        extension = extension.replace(".", "")
        fn_components["extension"] = self.__sanitize_name_str(
            ascii_filename(six_unicode(extension).lstrip()).decode("ascii", "replace").rstrip()
        )

        # series
        if meta_row["series"] and six_unicode(meta_row["series"]).lower() != "none":
            series = meta_row["series"]

            series_name_str = self.__sanitize_name_str(
                ascii_filename(series.lstrip()).decode("ascii", "replace").rstrip()
            )
            series_pos_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(meta_row["series_index"]).lstrip()).decode("ascii", "replace").rstrip()
            )

            fn_components["series"] = " - {0} # {1}".format(series_name_str, series_pos_str)
        else:
            fn_components["series"] = ""

        book_id_str = self.__sanitize_name_str(
            ascii_filename(six_unicode(meta_row["id"]).lstrip()).decode("ascii", "replace").rstrip()
        )
        fn_components["book"] = " - lx_book_id # {}".format(book_id_str)

        # folder_id
        if folder_id:
            folder_id_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(folder_id).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["folder"] = " - lx_folder # {}".format(folder_id_str)
        else:
            fn_components["folder"] = ""

        # file_id
        if file_id:
            file_id_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(file_id).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["file"] = " - lx_file # {}".format(file_id_str)
        else:
            fn_components["file"] = ""

        # file_priority
        if file_priority:
            file_priority_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(file_priority).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["file_priority"] = " - file_priority # {}".format(file_priority_str)
        else:
            fn_components["file_priority"] = ""

        # tag
        if tag:
            tag_str = self.__sanitize_name_str(
                ascii_filename(six_unicode(tag).lstrip()).decode("ascii", "replace").rstrip()
            )
            fn_components["tag"] = tag_str
        else:
            fn_components["tag"] = ""

        # Work down a list of standard name templates - continue until one is found that fulfills all the restrictions -
        # then return it
        for template in self.template_list:
            file_name_str = template.format(fn_components)
            if self.check_against_restrictions(file_name_str):
                return file_name_str

        raise NotImplementedError("Couldn't make_file_name under the current restrictions")

    def make_book_folder_name_from_row(self, title_row):
        """
        Make the name of a book folder from the books title row
        :param title_row:
        :return:
        """
        return self.__sanitize_name_str(ascii_filename(six_unicode(title_row["title"])))

    def make_book_folder_name(self, meta_row, folder_id):
        """
        Replace with a call to meta at some point.
        :param meta_row: The row corresponding to the book from the meta table
        :param folder_id: The id of the folder that it can be written into the tag
        :return:
        """
        # Build the name
        title_string = self.__sanitize_name_str(ascii_filename(six_unicode(meta_row["title"])))

        if title_string.lower() == "none":
            title_string = None

        if title_string is not None:
            book_name = self.__sanitize_name_str(ascii_filename(six_unicode(title_string)))
        else:
            book_name = _("Unknown")

        series_index = self.__sanitize_name_str(ascii_filename(six_unicode(meta_row["series_index"])))
        if series_index.lower() == "none":
            series_index = None
        if series_index is not None:
            book_name = six_unicode(series_index) + " - " + book_name

        # Build the tag
        book_tag = " - LX_{}_(2_{})_{}".format(folder_id, meta_row["id"], str(uuid.uuid4())[:5])

        return book_name, book_tag

    def make_series_folder_name(self, series_row):
        """
        Make a name for a folder linked to a series.
        :param series_row:
        :return:
        """
        if series_row["series"] is None:
            return "Placeholder - series {}".format(series_row["series_id"])
        return self.__sanitize_name_str(six_unicode(series_row["series"]))

    def make_creators_folder_name(self, creator_list, folder_id):
        """
        Takes a list of creator rows - creates a nice name for them which can be easily parsed by the system.
        Assumes the list is sorted in order of name priority - i.e. first name first.
        :param creator_list:
        :param folder_id:
        :return:
        """
        if len(creator_list) == 0:
            err_str = "make_creators_folder_name has been passed a blank index.\n"
            raise InputIntegrityError(err_str)

        creator_sort_list = []
        for creator in creator_list:
            if creator["creator_sort"] == "None" or creator["creator_sort"] is None:
                creator["creator_sort"] = self.__sanitize_name_str(
                    ascii_filename(author_to_author_sort(creator["creator"]))
                )
                creator.sync()
                creator_sort_list.append(creator["creator_sort"])
            else:
                creator_sort_list.append(creator["creator_sort"])

        creator_id_list = ["4_{}".format(creator["creator_id"]) for creator in creator_list]
        creator_sort_str = " & ".join(creator_sort_list)
        creator_ids_str = "-".join(creator_id_list)
        folder_tag = " - LX_{}_(".format(folder_id) + creator_ids_str + ")_{}".format(str(uuid.uuid4())[:5])
        return creator_sort_str, folder_tag

    def check_against_restrictions(self, candidate_string):
        """
        Checks the string against all the restrictions - returns True if it passes and False otherwise.
        :param candidate_string:
        :return:
        """
        if self.restrictions is None:
            return True

        total_name_length = self.restrictions.get("total_name_length", None)
        if total_name_length is not None and len(candidate_string) > total_name_length:
            return False
        else:
            return True

    def update_file_name_folder(self, file_name, old_folder_id, new_folder_id):
        """
        Files are occasionally moved between folders - as the name of the file can depend on the folder it's in (the
        file name containing the id of the folder) it's then necessary to update the name of the file as well.
        Currently uses a regular expression - remember that files have to be placed inside a folder and cannot be placed
        at the root of a folder store - so the ids in both cases shouldn't be None.
        :param file_name: The name string of the file to update
        :param old_folder_id: The original folder id - to change
        :param new_folder_id: The new folder_id - to change it to.
        :return:
        """
        new_folder_id = self.__sanitize_name_str(six_unicode(new_folder_id))
        folder_id_re = r" - lx_folder # ([0-9]+)"
        folder_new_id_string = " - lx_folder # {}".format(new_folder_id)
        return re.sub(pattern=folder_id_re, repl=folder_new_id_string, string=file_name)

    # ----------------------------------------------------------------------------------------
    #
    # - METHODS TO MAKE THE NAMES OF COVER IMAGES START HERE

    def make_book_cover_name(self, book_row, cover_id, extension):
        """
        Make a name for a book cover.
        :param book_row:
        :param cover_id: The id of the cover (included here to reduce the chances of clashing names).
        :param extension:
        :return:
        """
        book_meta_row = book_row.db.driver_wrapper.get_view_row_from_id("meta", book_row["book_id"])
        return self.make_book_cover_name_meta(book_meta_row, cover_id, extension)

    def make_book_cover_name_meta(self, book_meta_row, cover_id, extension):
        """
        Takes metadata about a book in the form of a row from the meta view - makes a new name for that cover.
        :param book_meta_row: A row from the meta view containing all the information needed to make the name
        :param cover_id: The id of the cover file - used to make sure that two covers aren't accidentally generated
                         in the same place with the same name.
        :param extension: The extension to give the file
        :return:
        """
        bcn_components = dict()
        bcn_components["title"] = self.__sanitize_name_str(book_meta_row["title"])
        bcn_components["creator_sort"] = self.__sanitize_name_str(book_meta_row["authors"])
        if not extension.startswith("."):
            bcn_components["extension"] = self.__sanitize_name_str(extension)
        else:
            bcn_components["extension"] = self.__sanitize_name_str(extension[1:])
        bcn_components["cover_id"] = self.__sanitize_name_str(six_unicode(cover_id))

        for template in self.book_file_template_list:
            book_cover_name = template.format(bcn_components)
            if self.check_against_restrictions(book_cover_name):
                return book_cover_name

        raise NotImplementedError("Couldn't make_book_cover_name under the current restrictions")

    def make_series_image_name(self, series_row, cover_id, extension):
        """
        Make a name suitable for a series cover.

        :param series_row: The series that the cover is associated with
        :param cover_id: The id of the cover file - used to make sure that two covers aren't accidentally generated
                         in the same place with the same name.
        :param extension: The extension of the cover
        :return:
        """
        scn_components = dict()
        scn_components["series"] = self.__sanitize_name_str(series_row["series"])
        if extension.startswith("."):
            scn_components["extension"] = self.__sanitize_name_str(extension[1:])
        else:
            scn_components["extension"] = self.__sanitize_name_str(extension)
        scn_components["cover_id"] = self.__sanitize_name_str(six_unicode(cover_id))

        for template in self.series_file_template_list:
            series_image_name = template.format(scn_components)
            if self.check_against_restrictions(series_image_name):
                return series_image_name

        raise NotImplementedError("Couldn't make_series_image_name under the current restrictions")

    def make_creator_image_name(self, creator_row, cover_id, extension):
        """
        Make a name for a creator image.

        :param creator_row:
        :param cover_id: The id of the cover file - used to make sure that two covers aren't accidentally generated
                         in the same place with the same name.
        :param extension:
        :return:
        """
        cn_components = dict()
        cn_components["creator"] = self.__sanitize_name_str(creator_row["creator"])
        if extension.startswith("."):
            cn_components["extension"] = self.__sanitize_name_str(extension[1:])
        else:
            cn_components["extension"] = self.__sanitize_name_str(extension)
        cn_components["cover_id"] = self.__sanitize_name_str(cover_id)

        for template in self.creator_file_template_list:
            creator_image_name = template.format(cn_components)
            if self.check_against_restrictions(creator_image_name):
                return creator_image_name

        raise NotImplementedError("Couldn't make_creator_image_name under the current restrictions")

    def make_generic_image_name(self, cover_row):
        """
        Make a name from a cover row for a generic image associated with that creator.

        :param cover_row: The cover row to make the name for.
        :return:
        """
        # Build the resources needed to make the name
        if cover_row["cover_name"] is not None:
            raise NotImplementedError("cover_name is not None - cover_name {}".format(cover_row["cover_name"]))

        cover_row_id = self.__sanitize_name_str(cover_row["cover_id"])
        cover_row_ext = self.__sanitize_ext_str(cover_row["cover_extension"])
        if cover_row_ext.startswith("."):
            cover_row_ext = cover_row_ext[1:]

        cover_name = "generic cover - cover_id - {}.{}".format(cover_row_id, cover_row_ext)
        cover_name = self.__sanitize_ext_str(cover_name)
        if self.check_against_restrictions(cover_name):
            return cover_name

        raise NotImplementedError("Couldn't make_generic_image_name under the current restrictions")

    #
    # ----------------------------------------------------------------------------------------


# Todo: The maintenance bot should be on the look out for these all the time
# Todo: Somewhere there is a method to make safe file names - use it here
# Todo: Merge with the above private method
def sanitize_object_names(target_name):
    """
    Makes a name safe for actually writing to all operating systems.

    :param target_name:
    :return:
    """
    target_name = deepcopy(target_name)
    return target_name.replace(":", "-")
