# -*- coding: utf-8 -*-
from __future__ import unicode_literals

__author__ = "root"

# Takes a series of MetaData objects - tries to intelligently combine them in order to synthesize an actually useful
# set of MetaData

import re
from copy import deepcopy

from LiuXin.customize import MDInputTransform

from LiuXin.utils.logger import default_log
from LiuXin.utils.icu import lower as icu_lower

from LiuXin.metadata.metadata import MetaData

from LiuXin.library.standardization import standardize_creator_name

from LiuXin.metadata.ebook_metadata_tools import check_name
from LiuXin.metadata.ebook_metadata_tools import score_title

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

from past.builtins import basestring


# ----------------------------------------------------------------------------------------------------------------------
#
# - HELPER FUNCTIONS FOR THE SYNTHESIZED START HERE
#
# ----------------------------------------------------------------------------------------------------------------------

# Todo: Come back and refine this with actual data


def stnd_token(token_str):
    """
    1) If there is a comma split round the comma and reverse the order
    2) Replace some special characters with whitespace
    3) Standardize whitespace to a single space
    4) Lower case the string
    :param token_str:
    :return:
    """
    token_str = deepcopy(token_str)
    token_tokens = token_str.split(",")
    token_tokens = token_tokens.reverse()
    token_str = " ".join(token_tokens)

    drop_characters = ["_", "-", "'", '"']
    for character in drop_characters:
        token_str = token_str.replace(old=character, new=" ")

    token_str = re.sub(pattern=r"\s+", repl=" ", string=token_str)

    return icu_lower(token_str)


def authors_hash(authors_object):
    """
    Takes an object - either a string or some itterable - converts it into a standardized string for comparison.
    :param authors_object:
    :return:
    """
    # Ensure that the feed in object is an itterable of strings
    if hasattr(authors_object, "__iter__"):
        authors_tokens = authors_object
        new_tokens = []
        for token in authors_tokens:
            new_tokens.append(six_unicode(token))
        authors_tokens = new_tokens
    elif isinstance(authors_object, basestring):
        authors_tokens = authors_object.split("&")
    else:
        err_str = "Unable to hash authors_object.\n"
        err_str += "authors_object: " + six_unicode(authors_object) + "\n"
        err_str += "authors_object_type: " + six_unicode(type(authors_object)) + "\n"
        default_log.critical(err_str)
        raise NotImplementedError(err_str)

    # For each of the tokens
    new_tokens = []
    for token in authors_tokens:
        new_tokens.append(stnd_token(token))

    return " ".join(new_tokens)


class SynthesisMDInputTransform(MDInputTransform):
    """
    Non-functional test.
    """

    def transform_metadata(self, *md_collection):
        """
        Takes a collection of Metadata - tries to intelligently guess a single form for the Metadata and return it.
        :param md_collection:
        :return rtn_md: Always returns a md object
        """
        # Might make sense to have a title and author hash
        # Algorithm for transform metadata
        # 1) Build an index of objects linked to their standardized counterparts
        # 2) Pull all the titles and authors into a list of lists
        #    - Filter for the trivial elements
        # 3) pull them into a list of lists of lists - each element has the form
        # [original_string, transformed_string, author_score, title_score, originally_title/author]
        # 4) The elements with the highest score are set as the author and title respectively
        #    - In the case of tie prefer not swapping title and author
        # 5) All other elements are just added in

        rtn_md = MetaData()

        md_list = [md for md in md_collection]

        if len(md_list) > 1:

            title_author_list = []
            for md in md_list:
                title_author_list.append([md.title, md.title, 0, 0, "title"])
                title_author_list.append(
                    [
                        " & ".join(md.authors.keys()),
                        " ".join(md.authors.keys()),
                        0,
                        0,
                        "author",
                    ]
                )

            # Applying the authors hash to all objects
            for candidate in title_author_list:
                candidate[1] = authors_hash(candidate[1])

            # Eliminate any entries which have the same hash
            hash_set = set()
            new_title_author_list = []
            for candidate in title_author_list:
                if candidate[1] not in hash_set:
                    hash_set.add(candidate[1])
                    new_title_author_list.append(candidate)

            # If the string consists purely of names (and capital letters) then it gets author points
            for candidate in title_author_list:
                if check_name(candidate[1]):
                    candidate[2] += 1

            # If the candidate consists of a mixture of english and non-english words it is probably a title - give it
            # title points
            for candidate in title_author_list:
                candidate[3] = score_title(candidate[1])

            # Having identified things which are probably the title and the author, write them into the metadata
            max_author_score = 0
            best_author = None
            max_title_score = 0
            best_title = None
            for candidate in title_author_list:
                if candidate[2] > max_author_score:
                    best_author = candidate
                if candidate[3] > max_title_score:
                    best_title = candidate

            # If the best author or the best title is None, then taking the first value
            if best_author is None:
                rtn_md.add_creators(md_list[0].creators)
            else:
                rtn_md.add_creators(best_author[0])
            if best_title is None:
                rtn_md.title = md_list[0].title
            else:
                rtn_md.title = best_title[0]

        elif len(md_list) == 1:

            # Try and determine if the title and authors need swapping
            md = md_list[0]
            title_str = md.title
            author_str = " & ".join(md.authors)
            title_name = check_name(title_str)
            author_name = check_name(author_str)
            if title_name and author_name:
                return md
            elif title_name and not author_name:
                rtn_md.title = author_str
                rtn_md.authors = title_str
            elif not title_name and author_name:
                rtn_md.title = title_str
                rtn_md.authors = author_str
            elif not title_name and not author_name:
                return md

        elif len(md_list) == 0:

            wrn_str = "SynthesisMDInputTransform has been called without any input."
            default_log.warn(wrn_str)
            return rtn_md

        else:

            raise NotImplementedError

        # With the author/title fields delt with nullify them and merge all the metadata together
        for metadata in md_list:
            metadata.nullify("authors")
            metadata.nullify("title")
            rtn_md.smart_update(metadata)

        return rtn_md
