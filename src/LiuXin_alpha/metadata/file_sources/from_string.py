# Does it's best to parse and create reasonable metadata from a file name.
# Might have to create multiple sets of metadata.

import re
import os
from copy import deepcopy
from collections import OrderedDict

from LiuXin.metadata.constants import ISBN_PATTENRS
from LiuXin.utils.general_ops.python_tools import (
    drop_characters_from_string as drop_strip,
)
from LiuXin.metadata.ebook_metadata_tools import check_isbn
from LiuXin.metadata.ebook_metadata_tools import check_name
from LiuXin.metadata.metadata import MetaData


ISBN_DROP_PATTERNS = [r"(\s*ISBN.*)", r"[0-9-/_\\xX\s]+"]

possible_separators = {"-", "/", "_", "\\", ".", " ", "&", ","}
regex_special_characters = {
    ".",
    "\\",
    "+",
    "*",
    "?",
    "[",
    "^",
    "]",
    "$",
    "(",
    ")",
    "{",
    "}",
    "=",
    "!",
    "<",
    ">",
    "|",
    ":",
    "-",
}
possible_parenthesis = {r"(": r")", r"\[": r"\]", r"\{": r"\}", r"\<": r"\>"}
token_trial_regex = {r"(?P<title>.+) by (?P<authors>.+)"}

VALID_FOR = ["FILENAME"]
PRIORITY_FOR = ["NONE"]
RUN_COST = ["LOW"]


def get_metadata(target_string, force_regex=False, full_path_regex=False):
    """
    Attempts to intelligently parse a filename and return all data which can be extracted from it.
    :param target_string:
    :return return_metadata:
    """
    target_string = deepcopy(target_string)
    return_metadata = MetaData()
    # Stripping the extension information and anything from the path except the files actual name
    target_string = os.path.basename(os.path.splitext(target_string)[0])

    # At a minimum we would hope that the filename would contain the title and the author name
    # First checking for an isbn and dropping it if it exists
    # Then trying to cleanly drop the ISBN and all surroundings from the string
    # Todo: Check that this actually works
    return_metadata.isbn = get_isbn_from_string(target_string)
    target_string = drop_isbn_from_string(target_string)
    return_metadata.date, target_string = pop_date(target_string)

    # With (hopefully) any significant chunks of numbers removed, we can now look for author names and titles
    target_tokens = tokenize(target_string)
    matched_tokens = set()
    # Passing through the tokens, matching then eliminating all that are of a known form
    for token in target_tokens:
        # applying the known token patterns to the token, to try and identify the tokens for the tokens
        for regex in token_trial_regex:
            regex_pat = re.compile(regex, re.I)
            regex_match = regex_pat.match(token)
            if regex_match is not None:
                match_dict = regex_match.groupdict()

                if "authors" in match_dict.keys():
                    match_dict["authors"] = match_dict["authors"].split("&")

                return_metadata.dict_add(match_dict)
                matched_tokens.add(token)
            else:
                pass

    # Now considering the tokens that remain.
    # - The first token that is not recognizably a name is taken to be the title.
    # - Any tokens which are names are taken to be authors
    # - Any other tokens are taken to be comments
    target_tokens = [token for token in target_tokens if token not in matched_tokens]
    title_found = False
    for token in target_tokens:
        token_name = check_name(token)
        if token_name:
            return_metadata.authors = token
        elif not token_name and not title_found:
            if return_metadata.is_null("title"):
                return_metadata.title = token
            else:
                return_metadata.comments = token
            title_found = True
        else:
            return_metadata.comments = token

    return return_metadata


# Attempts to bring any string, no matter how weird the layout, into a regular arrangement of tokens for processing
def tokenize(target_string):
    """
    Converts the target string to tokens in a (hopefully) smart way
    :param target_string:
    :return:
    """
    target_string = deepcopy(target_string)
    separator_count_dict = get_separator_count(target_string)
    # Using the separator count dict to determine what needs to be done next
    # If there are no separators (and no parentheses) we are not going to space today - returning the original values
    # as the first and only entry in an index
    if separator_count_dict[next(iter(separator_count_dict))] == 0 and not test_for_parenthesis(target_string):
        if target_string.count("(SPLIT)") != 0:
            return_index = re.split(r"\(SPLIT\)", target_string)
            return [token.strip() for token in return_index]
        else:
            return [target_string]

    # Filtering off the space for special consideration
    space_count = separator_count_dict.pop(" ")
    if separator_count_dict[next(iter(separator_count_dict))] == 0:
        if target_string.count("(SPLIT)") != 0:
            return_index = re.split(r"(\(SPLIT\))", target_string)
            return [token.strip() for token in return_index]
        else:
            return [target_string]

    # If there are no spaces then taking the most common separator and replacing it with spaces
    if (space_count == 0) and separator_count_dict[next(iter(separator_count_dict))] > 0:
        target_separator = next(iter(separator_count_dict))
        if target_separator in regex_special_characters:
            target_separator = "\\" + target_separator
        target_string = re.sub(target_separator, " ", target_string)
        separator_count_dict.pop(next(iter(separator_count_dict)))

    # With spaces either counted or existing, time to split the string down by any pre-marked break points
    split_regex = r"(\(SPLIT\))|"
    for key in separator_count_dict:
        if key in regex_special_characters:
            regex_safe_key = "\\" + key
            split_regex += regex_safe_key + "|"
        else:
            split_regex += key + "|"
    split_regex = split_regex[:-1]
    return_index = re.split(split_regex, target_string)

    # Always be sure to filter the return index for None before applying regex
    return_index = [item.strip() for item in return_index if item is not None]
    # Anything contained within brackets should be filtered out to the end of the list of tokens
    # The index should be split down at occurrences of parenthesises
    return_index = split_out_parenthesized_text(return_index)

    return [item.strip() for item in return_index if item is not None and item.strip() != ""]


def split_out_parenthesized_text(string_index):
    """
    Takes a string index. Splits out the text surrounded by parenthesis.
    :param string_index:
    :return string_index:
    """
    string_index = deepcopy(string_index)

    parenthesis_regex_set = set()
    base_regex = r"{}([^{}]*){}"
    for parenthesis in possible_parenthesis:
        if parenthesis in regex_special_characters:
            l_sub_string = "\\" + parenthesis
            r_sub_string = "\\" + possible_parenthesis[parenthesis]
            parenthesis_regex_set.add(base_regex.format(l_sub_string, r_sub_string, r_sub_string))
        else:
            l_sub_string = parenthesis
            r_sub_string = possible_parenthesis[parenthesis]
            parenthesis_regex_set.add(base_regex.format(l_sub_string, r_sub_string, r_sub_string))

    for regex in parenthesis_regex_set:
        return_index = []
        for string in string_index:
            return_index += extract_by_parenthesis_regex(string, regex)
        string_index = return_index

    return [item.strip() for item in string_index if item is not None and item.strip() != ""]


def extract_by_parenthesis_regex(target_string, regex):
    """
    Takes a regex (with one capture group) - splits a string down by that capture group.
    Inserts the result of that capture into an index containing the parts of the string.
    :param target_string:
    :param regex:
    :return return_index:
    """
    target_string = deepcopy(target_string)
    regex = deepcopy(regex)
    return_index = []
    regex_results = re.findall(regex, target_string, re.I)
    if len(regex_results) == 0:
        return [target_string]
    else:
        split_string = re.split(regex, target_string)
        for string in split_string:
            if string in regex_results:
                return_index.append("(" + string + ")")
            else:
                return_index.append(string)
    return return_index


def test_for_parenthesis(target_string):
    """
    Tests a given string to see if it contains any of the recognized types of parenthesis.
    :param target_string:
    :return True/False:
    """
    target_string = deepcopy(target_string)
    for character in target_string:
        if character in possible_parenthesis.keys():
            return True
    return False


def get_separator_count(target_string, separators=possible_separators):
    """
    Counts the number of separators (from the given separator list in this file).
    Returns a dict keyed by the separator and valued with the count
    :param target_string:
    :param separators: Candidate separators
    :return separator_count_dict - ordered dict by the separator count:
    """
    target_string = deepcopy(target_string)
    # Process the string to find the distribution of separators
    separators = deepcopy(separators)
    separator_count = []
    for separator in separators:
        separator_count.append(target_string.count(separator))
    sep_count_pairs = zip(separators, separator_count)
    sep_count_pairs = sorted(sep_count_pairs, key=lambda count: count[1], reverse=True)
    return OrderedDict([pair for pair in sep_count_pairs])


# (SPLIT) is used as a hard deliminator - a marker that there was content there that has been removed
# Strings will ALWAYS been tokenzied at a (SPLIT) comment
def pop_date(target_string, replacement="(SPLIT)"):
    """
    Tries to extract something formatted like a date from the target string
    :param target_string:
    :param replacement: Replace any detected date string with this string
    :return target_string, date:
    """
    target_string = deepcopy(target_string)
    replacement = deepcopy(replacement)
    return_date = []
    matched_regex = []

    # Starting by trying to extract the date in the format of year-month-day
    regex = r"{}\s*[12][0-9][0-9][0-9]\s*[{}]\s*(1[12]|0[1-9])\s*[{}]\s*(0[1-9]|1[1-9]|2[1-9]|3[01])\s*{}"
    separator_string = "".join(possible_separators)
    regex_set = set()

    for p_type in possible_parenthesis:
        regex_set.add(regex.format(p_type, separator_string, separator_string, possible_parenthesis[p_type]))

    for regex in regex_set:
        for candidate in re.findall(regex, target_string):
            return_date.append(candidate)
            matched_regex.append(regex)

    if len(return_date) == 0:
        pass
    elif len(return_date) == 1:
        target_string = re.sub(matched_regex[0], replacement, target_string)
        return return_date[0], target_string
    elif len(return_date) > 1:
        # Should work properly for this format of date
        return_date.sort()
        for match in matched_regex:
            target_string = re.sub(match, "", target_string)
            return return_date[0], target_string

    # Following up by trying to match to a year
    regex = r"{}\s*[12][0-9][0-9][0-9]\s*{}"
    return_date = []
    matched_regex = []
    regex_set = set()

    for p_type in possible_parenthesis:
        regex_set.add(regex.format(p_type, possible_parenthesis[p_type]))

    for regex in regex_set:
        for candidate in re.findall(regex, target_string):
            return_date.append(candidate)
            matched_regex.append(regex)

    if len(return_date) == 0:
        pass
    elif len(return_date) == 1:
        target_string = re.sub(matched_regex[0], replacement, target_string)
        return return_date[0], target_string
    elif len(return_date) > 1:
        # Should work properly for this format of date
        return_date.sort()
        for match in matched_regex:
            target_string = re.sub(match, replacement, target_string)
            return return_date[0], target_string

    return None, target_string


# Todo - can't find ISBNs embedded in larger blocks of numbers. Might be a good feature. Or not.
def get_isbn_from_string(target_string):
    """
    Takes a string. Tries it's level best to extract useful ISBNs from it. Returns a set of the ones it finds.
    :param target_string: The string to be search
    :return isbn_set: A set of ISBNs found from the string
    """
    target_string = deepcopy(target_string)
    candidate_set = set()

    for regex in ISBN_PATTENRS:
        for candidate in re.findall(regex, target_string):
            candidate_set.add(candidate)

    drop_set = {"-", "/", "_", "\\"}
    candidate_set = {drop_strip(string, drop_set) for string in candidate_set}
    candidate_set = {check_isbn(isbn_candid) for isbn_candid in candidate_set}
    candidate_set = {candidate for candidate in candidate_set if candidate is not None}
    candidate_set = [candidate for candidate in candidate_set if check_isbn(candidate) is not None]

    return candidate_set


def drop_isbn_from_string(target_string, replacement="(SPLIT)"):
    """
    Tries to cleanly drop the isbn and all references to it from the target string
    :param target_string:
    :param replacement: The replacement string to substitute in where an ISBN has been removed
    :return target_string:
    """
    target_string = deepcopy(target_string)

    # Drops instance of the form (ISBN - )
    pre_format_drop_set = {r"{}\s*ISBN[^{}]{}"}
    regex_drop_set = set()
    pos_l_parenthesis = "".join(possible_parenthesis.values())

    # building the formatted drop sets and adding them to the drop set...set
    for regex in pre_format_drop_set:
        for l_bracket in possible_parenthesis.keys():
            r_bracket = possible_parenthesis[l_bracket]
            regex_drop_set.add(regex.format(l_bracket, pos_l_parenthesis, r_bracket))
    for drop_regex in regex_drop_set:
        target_string = re.sub(drop_regex, "", target_string)

    # Dropping isbn of the form of numbers (and xX) separated by the possible separators
    candidate_set = set()
    for regex in ISBN_PATTENRS:
        for candidate in re.findall(regex, target_string):
            candidate_set.add(candidate)

    # keeping the matching strings in their original form and the transformed form
    # These can then be checked to see if they are valid - and, if they are, eliminated
    drop_set = {"-", "/", "_", "\\"}
    matches = dict([(match, match) for match in candidate_set])
    for match in matches.keys():
        matches[match] = drop_strip(matches[match], drop_set)
    matches = dict([(key, check_isbn(matches[key])) for key in matches.keys() if check_isbn(matches[key]) is not None])
    for match in matches:
        target_string = target_string.replace(match, replacement)

    return target_string
