
"""
General extensions to python core functions and classes.
"""


import pprint
import re
import sys
import uuid
from datetime import datetime

from collections import defaultdict, OrderedDict
from copy import deepcopy
from typing import Union, Container, Iterable


from LiuXin_alpha.utils.logging import LiuXin_print
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__author__ = "Cameron"


def uniq(vals, kmap=lambda x: x):
    """
    Remove all duplicates from vals, while preserving order.

    kmap must be a callable that returns a hashable value for every item in vals.

    :param vals:
    :param kmap:
    :return:
    """
    vals = vals or ()
    lvals = (kmap(x) for x in vals)
    seen = set()
    seen_add = seen.add
    return tuple(x for x, k in zip(vals, lvals) if k not in seen and not seen_add(k))


def checked_dictionary_merge(dict_1, dict_2):
    """
    Merges two dictionaries, raising an exception if they have a common keyed element.

    (and thus data would be lost during the merge).
    Should probably raise an exception if this happens.
    """

    dict_1_local = deepcopy(dict_1)
    dict_2_local = deepcopy(dict_2)

    return_dict = dict_1_local

    for item in dict_2_local:
        if item not in return_dict.keys():
            return_dict[item] = dict_2_local[item]
        else:
            LiuXin_print("Error - checked_dictionary_merge failed. Key conflict between the two dictionaries.")
            sys.exit()

    return return_dict


def smart_dictionary_merge(primary_dict, secondary_dict, key_protect: bool = True):
    """
    Takes two dictionaries. One being the primary and one being the secondary.

    Builds a composite dictionary.
    If key_constraint is true it'll raise an exception if an entry from both dictionaries has content.
    If it's false it'll take the entry from the primary dictionary.

    :param primary_dict:
    :param secondary_dict:
    :param key_protect:
    :return:
    """
    p_dict_local = deepcopy(primary_dict)
    p_dict_local = eliminate_whitespace(p_dict_local)

    s_dict_local = deepcopy(secondary_dict)
    s_dict_local = eliminate_whitespace(s_dict_local)

    merged_dict = dict()
    all_keys = set(p_dict_local).union(set(s_dict_local))

    for key in all_keys:

        # if there's a key in the secondary dict but not the primary just copy it across
        if key not in p_dict_local.keys():

            merged_dict[key] = s_dict_local[key]
            continue

        if key not in s_dict_local.keys():

            merged_dict[key] = p_dict_local[key]
            continue

        # in the case of key conflict a little more care must be taken
        p_entry = p_dict_local[key]
        s_entry = s_dict_local[key]

        if p_entry == s_entry:
            merged_dict[key] = p_entry
            continue

        if (p_entry is None) and (s_entry is None):

            merged_dict[key] = None

        elif (p_entry is not None) and (s_entry is None):

            merged_dict[key] = p_entry

        elif (p_entry is None) and (s_entry is not None):

            merged_dict[key] = s_entry

        elif (p_entry is not None) and (s_entry is not None):

            if key_protect:
                raise KeyError("Error - smart_dictionary_merge has encountered a key conflict in key_protect mode.")
        else:
            raise NotImplementedError("Logical error")

    return merged_dict


def eliminate_whitespace(dictionary):
    """
    Scans through a dictionary. Sets the value of any entry with just whitespace to None

    :param dictionary:
    :return:
    """

    l_dict = deepcopy(dictionary)

    null_pattern = re.compile(r"^\s+$")

    for key in l_dict.keys():

        entry = l_dict[key]

        if entry is None:
            continue
        if not isinstance(entry, str):
            continue

        try:
            entry_match = null_pattern.match(entry)
        except TypeError:
            raise TypeError("cannot parse {}".format(key))

        if entry_match is not None:
            l_dict[key] = None

    return l_dict


def append_string_to_keys(old_dict, append_string):
    """
    Takes a dictionary and a string. Appends the string to every key of the dictionary. Returns the new dictionary.

    :param old_dict: The dictionary to be modified
    :param append_string: The string to be appended to every key of the dictionary
    :return:
    """
    old_dict = deepcopy(old_dict)
    append_string = deepcopy(append_string)
    append_string = six_unicode(append_string)

    new_dict = dict()

    for key in old_dict.keys():
        new_key = append_string + six_unicode(key)
        new_dict[new_key] = old_dict[key]

    return new_dict


def get_unique_id() -> str:
    """Returns a unique string for use as a group_id."""

    return six_unicode(uuid.uuid4()) + six_unicode(datetime.now())


def regex_dict_str_rekey(re_key_dict: dict[str, str], start_str: str) -> str:
    """
    Scan every key of the dictionary and return the result of the rekey is in the dictionary - else return the original
    string.

    :param re_key_dict:
    :param start_str:
    :return:
    """
    for rekey_re in re_key_dict.keys():

        rekey_pat = re.compile(rekey_re, re.I)
        if rekey_pat.match(start_str):
            return re_key_dict[rekey_re]

    return start_str


def dict_lower_values(old_dict):
    """
    Apply the string lower method to every value in a dictionary and return the re-valued dictionary.

    If the lower method cannot be applied then just ignore the original value.
    :param old_dict:
    :return:
    """
    new_dict = dict()
    for key in old_dict.keys():
        try:
            new_dict[key] = old_dict[key].lower()
        except AttributeError:
            new_dict[key] = old_dict[key]
    return new_dict


def dict_values_set(old_dict, lower=True):
    """
    Returns a set of all the values in a dict. lower will be called on them if appropriate.

    :param old_dict:
    :param lower:
    :return:
    """
    if not lower:
        return set(v for v in old_dict.values())
    else:
        v_set = set()
        for v in old_dict.values():
            try:
                v_set.add(v.lower())
            except AttributeError:
                v_set.add(v)
        return v_set


def dict_keys_set(old_dict):
    """
    Returns a set of all the keys of a dictionary.

    :param old_dict:
    :return:
    """
    return set(k for k in old_dict.keys())


# Todo: Add collision detection
def regex_dict_rekey(re_key_dict, old_dict, all_rekey=True):
    """
    Use a regex_dict (a dictionary keyed by regex, with values of the new names) to re-key a dictionary.

    This is used to render dictionaries into consistent forms so that they can be compared and examined more easily.
    If all_rekey is true an error will be raised unless EVERY keyed is rekeyed.
    """
    # check tht the requested re-key is consistent
    re_key_dict = deepcopy(re_key_dict)
    old_dict = deepcopy(old_dict)
    if old_dict is None:
        return None
    new_dict = dict()

    original_keys = set(key for key in old_dict.keys())

    re_keys = re_key_dict.keys()
    re_key_pats = [re.compile(key) for key in re_keys]

    assert len(re_key_pats) == len(re_key_dict)

    for i in range(len(re_key_pats)):

        current_pat = re_key_pats[i]
        re_key = re_keys[i]
        new_key = re_key_dict[re_key]

        for key in old_dict.keys():
            key_match = current_pat.match(key)

            if key_match is not None:

                new_dict[new_key] = old_dict[key]
                original_keys.discard(key)

    if all_rekey:
        assert len(new_dict) == len(old_dict), __gen_err_str_regex_dict_rekey(re_key_dict, old_dict, new_dict)
    else:
        for key in original_keys:
            new_dict[key] = old_dict[key]

    return new_dict


# Todo: This should have collision detection - test and replace
def regex_dict_rekey_2(re_key_dict, old_dict, all_rekey=True):
    """
    Uses a regex_dict (a dictionary keyed with an uncompiled regex and valued with the replacement string for a string
    matching that regex) to re-key a dictionary (replace all the keys with the given replacements).
    This is used to standardize a dictionary.
    If all_rekey is True an error will be rasied unless ALL they keys are replaced.
    :param re_key_dict:
    :param old_dict:
    :param all_rekey:
    :return:
    """
    if not old_dict:
        return old_dict

    # Compile the re-key dict (to a list of tuples - the first element being the compiled pattern and the second element
    # being it's replacement if that pattern matches
    comp_rekeys = [(re.compile(key), re_key_dict[key]) for key in re_key_dict.keys()]
    new_dict = dict()
    for key in old_dict.keys():

        match_count = 0
        for rekey_pair in comp_rekeys:

            if rekey_pair[0].match(key):
                new_dict[rekey_pair[1]] = old_dict[key]
                match_count += 1

        if match_count > 1:
            raise KeyError("Degenerate keys")

    if all_rekey:
        assert len(old_dict) == len(new_dict), __gen_err_str_regex_dict_rekey(re_key_dict, old_dict, new_dict)

    return new_dict


def __gen_err_str_regex_dict_rekey(re_key_dict, old_dict, new_dict) -> str:
    """
    Makes an error string for when one of the keys hadn't been transfered properly to the new dict.

    :param re_key_dict:
    :param old_dict:
    :param new_dict:
    :return:
    """
    errs = ["An entry wasn't properly transferred to the new dictionary."]
    errs.extend(["re_key_dict: \n{}\n".format(pprint.pformat(re_key_dict))])
    errs.extend(["old_dict: \n{}\n".format(pprint.pformat(old_dict))])
    errs.extend(["new_dict: \n{}\n".format(pprint.pformat(new_dict))])
    return "\n".join(errs)


# used to render a variable name list into something which can be more easily parsed and understoof
def regex_list_rekey(re_key_dict, old_list, must_rekey=True, null_pad=True):
    """
    Used a regex_dict (a dictionary keyed by a regex, with the values being the new name if that regex matches) to
    rekey every element of a list. This is used to render the list elements into a consistent form so that they can be
    switched,s and appropriate behavior for each adopted more easily.
    :param re_key_dict:
    :param old_list:
    :param must_rekey:
    :param null_pad:
    :return:
    """

    re_key_dict = deepcopy(re_key_dict)
    old_list = deepcopy(old_list)
    new_list = []

    # building a dict keyed by compile regex pattern
    # with value being what it has to be replaced by
    regex_dict = dict()
    for key in re_key_dict.keys():
        key_pat = re.compile(key, re.I)
        regex_dict[key_pat] = re_key_dict[key]

    # using the regex dict to do the replacements
    # checks that each element in the list matches to one and only one pattern
    for element in old_list:

        match_found = False
        match_count = 0
        for pat in regex_dict.keys():
            # attempts to form a match
            try:
                pat_match = pat.match(element)
            except TypeError:
                raise TypeError("Expecting a list of strings. Not a list of strings and things.")
            if pat_match is not None:
                match_count += 1
                new_list.append(regex_dict[pat])
                match_found = True
                break

        if not match_found:
            new_list.append(None)

        if must_rekey:
            assert match_count == 1, repr(match_count)

    return new_list


# searches all the attributes of a given dict for a certain pattern. Returns true if one matches it.
def check_dict_keyes_for_pat(attrib_dict, regex_string):
    """
    Checks the keys of a dictionary to see if at least one matches a regex pattern.
    :param attrib_dict:
    :param regex_string:
    :return True/False:
    """
    assert regex_string is not None
    if attrib_dict is None:
        return False

    regex_pat = re.compile(regex_string)
    attrib_dict = deepcopy(attrib_dict)

    for key in attrib_dict.keys():
        regex_match = regex_pat.match(key)
        if regex_match is not None:
            return True
    else:
        return False


def check_against_regex_set(regex_set, target_string):
    """
    Checks the provided element against every regex in a set. Returns True if it matches one, and False if it does not
    :param regex_set:
    :param target_string:
    :return True or False:
    """
    regex_set = deepcopy(regex_set)
    target_string = deepcopy(target_string)

    for regex in regex_set:
        regex_pat = re.compile(regex)
        regex_match = regex_pat.match(target_string)
        if regex_match is not None:
            return True
    else:
        return False


def scan_index_for_regex(string_index, regex_string, all_return=False):
    """
    Takes an index of strings and a regex string.
    Tries to match the regex to every string in the index.
    Returns any matches.
    :param string_index: An index of strings
    :param regex_string: A regex pattern in the form a string
    :return: Either a set of matches or the first match encountered
    """
    string_index = deepcopy(string_index)
    regex_string = deepcopy(regex_string)
    regex_pat = re.compile(regex_string, re.IGNORECASE)
    if not all_return:
        for string in string_index:
            if regex_pat.match(string) is not None:
                return regex_pat.match(string).group(1)
    else:
        match_strings = []
        for string in string_index:
            if regex_pat.match(string) is not None:
                match_strings.append(regex_pat.match(string).group(1))
        else:
            if match_strings != []:
                return match_strings
            else:
                return None

    return None


def pop_index_by_regex(string_index: list[str], pop_regex: str) -> list[str]:
    """
    Takes an index of strings and a regex.

    Pops any indices which match the regex.
    Returns the shorter regex.
    :param string_index: An index of strings!
    :param pop_regex: The regex that will be applied to every string in the index.
    :return return_index: The index after every matching string has been removed.
    """
    string_index = deepcopy(string_index)
    pop_regex = deepcopy(pop_regex)
    return_index = []
    pop_pat = re.compile(pop_regex, re.IGNORECASE)

    for string in string_index:
        if pop_pat.match(string) is None:
            return_index.append(string)
        else:
            pass

    return return_index


def drop_characters_from_string(
        target_string: str,
        character_set: Union[Container[str], Iterable[str]]
) -> str:
    """
    Iterates through a sequence. Dropping each instance of any characters in the character set from that string.

    :param target_string: Work on this string
    :param character_set: Set of character to drop
    :return new_string:
    """
    for character in character_set:
        assert len(character) == 1

    target_string = deepcopy(target_string)
    character_set = deepcopy(character_set)
    return_string = ""
    for character in target_string:
        if character not in character_set:
            return_string += character
    return return_string


def coerce_row_to_unicode(target_object):
    """
    Takes a row. Iterates through it coercing it to unicode.

    :param target_object: The thing to be converted to unicode
    :return unicode_row:
    """
    if isinstance(target_object, dict):
        row_local = deepcopy(target_object)
        unicode_row = dict()

        for column in row_local.keys():
            unicode_row[six_unicode(column)] = six_unicode(row_local[column])

        return unicode_row

    elif isinstance(target_object, set):
        unicode_set = set()
        for item in target_object:
            unicode_set.add(six_unicode(item))
        return unicode_set

    else:
        return six_unicode(target_object)


def element_to_front(target_list, list_element):
    """
    Promote the given element to the first entry in the list

    :param target_list:
    :return:
    """
    target_list.insert(0, target_list.pop(target_list.index(list_element)))
    return target_list


def nested_DefaultDict_tree_to_dict_tree(default_dict_tree):
    """
    Takes a tree of DefaultDicts and converts it into a tree of dicts - which can be far more easily handled and
    displayed.

    :param default_dict_tree:
    :return:
    """
    # Work down through the levels of the tree
    # If a value is a dictionary, then we need to recurse into it
    # If not, then we need to store that value at that position and move on.
    # Repeat until all dictionaries have been scanned

    # Tree we're building without the problematic stuff
    new_tree = dict()
    # Dictionaries we still need to recurse into
    seen_dicts = dict()

    # Level 1
    for key, value in iteritems(default_dict_tree):
        # Note the dictionary as something we need to recurse into
        if isinstance(value, (defaultdict, OrderedDict, dict)):
            seen_dicts[(key,)] = value
            new_tree[key] = dict(value)
        else:
            new_tree[key] = value

    # If there is no recursion to do, abort
    if not seen_dicts:
        return new_tree

    # Recurse through the rest of the levels
    while seen_dicts:

        new_seen_dicts = dict()
        for pos_tuple, val_dict in iteritems(seen_dicts):
            # We need to examine the dictionary - noting any sub dictionary which need to recurse into
            for new_pos, new_value in iteritems(val_dict):
                new_pos_tuple = tuple(
                    list(pos_tuple)
                    + [
                        new_pos,
                    ]
                )
                if isinstance(new_value, (defaultdict, OrderedDict, dict)):

                    new_seen_dicts[new_pos_tuple] = new_value
                    _add_dict_tree_value(new_tree, new_pos_tuple, dict(new_value))

                else:

                    try:
                        _add_dict_tree_value(new_tree, new_pos_tuple, new_value)
                    except TypeError:
                        err_msg = [
                            "TypeError when trying to _add_dict_tree_value",
                            "new_tree: \n{}".format(pprint.pformat(new_tree)),
                            "pos_tuple: \n{}".format(pos_tuple),
                            "new_value: \n{}".format(new_value),
                        ]
                        raise TypeError("\n".join(err_msg))

        seen_dicts = new_seen_dicts

    return new_tree


def _get_dict_tree_value(dict_tree, pos_list):
    """
    Return the value from a specific place in the tree.
    :param pos_list:
    :return:
    """
    if len(pos_list) == 1:
        return dict_tree[pos_list[0]]
    else:
        new_pos_list = pos_list[1:]
        new_dict_tree = dict_tree[pos_list[0]]
        return _get_dict_tree_value(new_dict_tree, new_pos_list)


def _set_dict_tree_value(dict_tree, pos_list, new_value):
    """
    Replace the value from a specific place in the tree.

    :param dict_tree:
    :param pos_list:
    :param new_value:
    :return:
    """
    next_level = dict_tree
    for position in pos_list[:-1]:
        next_level = next_level[position]
    next_level[pos_list[-1]] = new_value


def _add_dict_tree_value(dict_tree, pos_list, new_value):
    """
    Adding a new value in the designated position - creating new layers for that value if required.

    :param dict_tree:
    :param pos_list:
    :param new_value:
    :return:
    """
    next_level = dict_tree
    for position in pos_list[:-1]:
        if position not in next_level:
            next_level[position] = dict()
        next_level = next_level[position]
    next_level[pos_list[-1]] = new_value
