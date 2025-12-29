"""
Interface for convenient use of 7-zip from within python.

Z-zip needs to be installed seperately before this will work.
"""

from __future__ import print_function

import os
import re
from subprocess import Popen, PIPE
from copy import deepcopy

from LiuXin.constants import iswindows, islinux
from LiuXin.constants import VERBOSE_DEBUG

from LiuXin.utils.ptempfiles import get_scratch_folder
from LiuXin.utils.decompression import DecompressException
from LiuXin.utils.decompression import Archive
from LiuXin.utils.os_ops import split_file_true_name_ext
from LiuXin.utils.general_ops.python_tools import scan_index_for_regex
from LiuXin.utils.logger import default_log

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

__author__ = "Cameron"


# Todo: EVERYTHING IS AWEFUL
def extract_file(source, destination=False, in_memory=False, password=False, recursion=True):
    """
    Tries to use 7-Zip to extract a file.
    :param source: Where the file is.
    :param destination: Where the file is to be extracted to. If False, will be extracted to a ptempdirectory.
    :param in_memory: Should the file be loaded into memory and returned.
    :return:
    """
    source = deepcopy(source)
    source = os.path.abspath(source)
    source = six_unicode(source)
    if destination:
        destination = deepcopy(destination)
        destination = os.path.abspath(destination)
        destination = six_unicode(destination)

    if iswindows:
        command = r'F:\7-Zip\7z.exe x -o"{}" -r -y "{}"'
        if not destination:
            destination = get_scratch_folder()
        command = command.format(destination, source)

        default_log.info("{} is about to be executed on the console.".format(command))

        result = Popen(command, stdout=PIPE)
        result_string = nice_format_return(result)

        if VERBOSE_DEBUG:
            print(result_string)

        if not all_okay(result_string):
            err_string = "Error in attempted file extraction."
            err_string += nice_format_return(result)
            raise DecompressException(err_string)

    elif islinux:

        if not destination:
            destination = get_scratch_folder()
        # sudo is needed because, sometimes, the LiuXin_scratch directory ends up getting locked - and 7z needs
        # elevated privilages to write to it - I'm not wild about it, but it'll do for the moment
        command = r'sudo 7z x -o"{}" -y "{}"'.format(destination, source)

        default_log.info("{} is about to be executed on the console.".format(command))

        try:
            result = Popen(command, stdout=PIPE, shell=True)
        except OSError as e:
            info_str = "Decompression failed - OSErrror"
            info_str = default_log.log_exception(
                info_str,
                e,
                "INFO",
                ("command", command),
                ("source", source),
                ("destination", destination),
            )
            raise DecompressException(info_str)

        result_string = nice_format_return(result)

        if not all_okay(result_string):
            err_string = "Error in attempted file extraction."
            err_string += nice_format_return(result)
            raise DecompressException(err_string)

    return destination


def nice_format_return(result):
    """
    Takes the result of using Popen with stdout=PIPE. Formats it nicely for
    :param result: The result of executing an instruction to the terminal.
    :return result_str: The result in str form to be printer
    """

    first_result_index = []
    second_result_index = []
    for line in result.stdout:
        first_result_index.append(line)

    for line in first_result_index:
        if line.strip() == "":
            pass
        else:
            second_result_index.append(line)

    result_str = """ """
    for line in second_result_index:
        result_str += line

    return result_str


def all_okay(result_str):
    """
    Takes a result string. Parses it to see if the phrase All Okay is in there. Returns True or False
    :param result_str:
    :return:
    """
    result_str = deepcopy(result_str)
    ok_regex = r"Everything is Ok"
    ok_pat = re.compile(ok_regex, re.IGNORECASE)
    if re.search(ok_pat, result_str):
        return True
    else:
        return False


def get_info(source):
    """
    Uses 7-zip to extract information from an archive and either loads it to an Archive object or returns it as a dict.
    :param source:
    :param dict_return:
    :return:
    """
    source = deepcopy(source)
    return_index = []
    return_info = Archive()

    if iswindows:
        command = r'F:\7-Zip\7z.exe l "{}"'
        command = command.format(source)
        result = Popen(command, stdout=PIPE)
        for line in result.stdout:
            return_index.append(line)

        # relies on running regex on the return from running the command on the archive
        # extracting the file_path from the return
        file_path_regex = r"Path = (.*)"
        return_info.file_path = scan_index_for_regex(return_index, file_path_regex)

        # splitting the file_name and extension out of the file_path
        return_info.file_name, return_info.file_extension = split_file_true_name_ext(return_info.file_path)

        # splitting out the type of compression
        comp_type_regex = r"Type = (.*)"
        return_info.compression_type = scan_index_for_regex(return_index, comp_type_regex)

        # splitting out the block count
        block_count_regex = r"Blocks = ([0-9]*)"
        return_info.block_count = scan_index_for_regex(return_index, block_count_regex)

        # trying to determined if the archive is multi-volume
        multi_volume_regex = r"Multivolume = (.*)"
        return_candidate = scan_index_for_regex(return_index, multi_volume_regex)
        if return_candidate == "+":
            return_info.multivolume = True
        elif return_candidate == "-":
            return_info.multivolume = False
        else:
            return_info.multivolume = return_candidate
            # raise AssertionError("Attempt to parse multivolume failed " +  unicode(return_candidate))

        # tries to parse the file names out of the return
        file_regex = r"(?:[0-9-])+\s+(?:[0-9:])+\s+(?:[a-zA-Z0-9.])+\s+(?:[0-9])+\s+(?:[0-9])+\s+(.*)"
        file_returns = scan_index_for_regex(return_index, file_regex, all_return=True)
        return_info.files = set([thing.strip() for thing in file_returns])

        return return_info
