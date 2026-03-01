

"""
xml based metadata parser. As such should work on any platform, using just the included python libraries.
"""

from __future__ import unicode_literals

# XML based opf file parser.
# OPF files can come in an exciting range of different configurations. This is a generic parser which should deal with
# most of them (and log it's failures so it can be extended)

import re

from copy import deepcopy

import xml.etree.ElementTree as ET

from LiuXin_alpha.constants import VERBOSE_DEBUG, AUTONOMOUS_MODE, DEV_MODE

# importing the list of internal and external identifiers. To determine where the metadata should flow
from LiuXin_alpha.metadata.constants import INTERNAL_EBOOK_ID_SCHEMA, EXTERNAL_EBOOK_ID_SCHEMA
from LiuXin_alpha.metadata.constants import canonicalize_id_name
from LiuXin_alpha.metadata.metadata import MetaData

from LiuXin.utils.general_ops.io_ops import LiuXin_print
from LiuXin.utils.general_ops.python_tools import dict_keys_set
from LiuXin.utils.general_ops.python_tools import dict_lower_values
from LiuXin.utils.general_ops.python_tools import regex_dict_rekey
from LiuXin.utils.general_ops.python_tools import regex_list_rekey
from LiuXin.utils.logger import default_log

from past.builtins import basestring


class OpfParseError(Exception):
    """
    An error raised when a resource is not found or the OPF file does something unexpected.
    """

    def __init__(self, argument):
        self.argument = argument
        LiuXin_print(self.argument)

    def __str__(self):
        return repr(self.argument)


def get_metadata(
    target_file,
    calibre=False,
    text=False,
    file_is_raw_root=False,
    seek_md_node=True,
    walk=False,
):
    """
    Takes an opf/xml file which is in memory. Parses it and returns the metadata.
    :param target_file:
    :param calibre: Return the metadata as a calibreMetadata object or a LiuXin MetaData object.
    :param text: target_file can be a file location or a string of text. If True tries to parse target_file as a
                 string - if false then tries to load it from disk.
    :param file_is_raw_root: Has the file already been processed into a tree suitable for parsing?
                             If True, target_file is set as root and the parse proceeds.
                             If this is True then the text variable is ignored.
    :param seek_md_node: If True, then looks for a metadata node - then it
    :param walk: walk down from the metadata node adding all nodes
    :return:
    """
    if not file_is_raw_root:
        if not text:
            tree = ET.parse(target_file)
            root = tree.getroot()
        else:
            root = ET.fromstring(target_file)
    else:
        root = target_file

    # Setup the right type of metadata return
    if not calibre:
        md = MetaData()
    else:
        # Todo: Add support for calibre return
        raise NotImplementedError("Still need to handle this")

    if seek_md_node:
        # search a tree for the top level metadata node
        metadata_candidate_pointers = simple_get_metadata_node(root)

        # If no metadata candidate pointers are found, nothing identifiable as metadata was found in this
        if not metadata_candidate_pointers:
            err_str = "Attempt to parse opf file for metadata failed - couldn't find any metadata candidate pointers"
            raise OpfParseError(err_str)

        # Assuming only one Metadata candidate pointer is found
        assert len(metadata_candidate_pointers) == 1, "OPF parse failed - multiple metadata nodes"

        metadata_node = metadata_candidate_pointers[0]

        # If the metadata node has a dc-metadata node under it, then prefer that as the root
        for md_node_child in list(metadata_node):
            if md_node_child.tag.lower() == "dc-metadata":
                metadata_node = md_node_child
                break

    else:

        metadata_node = root

    # Build and then re-key a list of all the node types under the metadata node - rekey cuts the possibilities down
    # and assists with standardization
    node_types_list = []
    node_locations = []
    if not walk:
        for field in metadata_node.iter():
            field_tag = field.tag
            if isinstance(field_tag, basestring):
                node_types_list.append(field.tag)
                node_locations.append(field)
            else:
                if field.keys():
                    info_str = "Unhandled node found while parsing OPF document"
                    default_log.log_variables(info_str, "INFO", ("field.keys()", field.keys()))
    else:
        node_types_list, node_locations = node_walk(
            root=metadata_node,
            node_types_list=node_types_list,
            node_locations=node_locations,
        )

    regex_type_dict = {
        r".*contributor$": "contributor",
        r".*coverage$": "coverage",
        r".*(initial)?(-)?creator$": "creators",
        r".*(?!pub)date$": "date",
        r".*description$": "description",
        r".*identifier$": "identifier",
        r".*keyword$": "keyword",
        r".*language$": "language",
        r".*meta$": "generic_meta_data",
        r".*metadata$": "metadata",
        r".*pubdate": "pubdate",
        r".*publisher$": "publisher",
        r".*rights$": "rights",
        r".*source$": "source",
        r".*subject$": "subject",
        r".*title$": "title",
        r".*user-defined": "user_defined",
        # Known ignored cases start here - still rekeyed, but later ignored
        r"^.*guide$": "known_ignored",
        r"^.*item$": "known_ignored",
        r"^.*itemref$": "known_ignored",
        r"^.*manifest$": "known_ignored",
        r"^.*package$": "known_ignored",
        r"^.*reference$": "known_ignored",
        r"^.*spine$": "known_ignored",
        r"^.*tour$": "known_ignored",
        r"^.*tours$": "known_ignored",
        # Cases found when adapting the method to include parsing the meta.xml file from an odt file
        r".*document-statistic(s)?": "known_ignored",
        r".*generator": "known_ignored",
        r".*editing-cycles": "known_ignored",
        r".*editing-duration": "known_ignored",
    }

    original_types_list = deepcopy(node_types_list)
    try:
        node_types_list = regex_list_rekey(regex_type_dict, node_types_list, must_rekey=False)
    except TypeError as e:
        err_str = "regex_list_rekey failed - probably the list was not a list of strings"
        err_str = default_log.log_exception(err_str, e, "DEBUG", ("node_types_list", node_types_list))
        raise TypeError(err_str)

    # Check to see if all the types have been properly remapped
    if len(original_types_list) != len(node_types_list):
        info_str = (
            "The length of the original types list and the node_types list after regex rekey differed - "
            "might indicate data loss"
        )
        default_log.log_variables(
            info_str,
            "INFO",
            ("original_types_list", original_types_list),
            ("node_types_list", node_types_list),
        )

    nodes = zip(node_types_list, node_locations, original_types_list)

    # using this as a switch to separate out the metadata fields and determine which add function should be called.
    # Quite a lot of the metadata produced by calibre falls under the category of generic_meta_data
    for node in nodes:
        # a switch to try and handle any type of node
        # this could be a plugin architecture - but that might be taking a good thing way too far
        if node[0] == "contributor":
            md = add_contributor(node[1], md)

        elif node[0] == "creators":
            md = add_creator(node[1], md)

        elif node[0] == "date":
            md = add_date(node[1], md)

        elif node[0] == "pubdate":
            md = add_pubdate(node[1], md)

        elif node[0] == "description":
            md = add_description(node[1], md)

        elif node[0] == "identifier":
            md = add_identifier(node[1], md)

        elif node[0] == "keyword":
            # All subjects are stored as tags anyway
            md = add_subject(node[1], md)

        elif node[0] == "language":
            md = add_language(node[1], md)

        elif node[0] == "publisher":
            md = add_publisher(node[1], md)

        elif node[0] == "generic_meta_data":
            md = add_generic_meta_data(node[1], md)

        elif node[0] == "metadata":
            md = add_metadata(node[1], md)

        elif node[0] == "subject":
            md = add_subject(node[1], md)

        elif node[0] == "title":
            md = add_title(node[1], md)

        elif node[0] == "rights":
            md = add_rights(node[1], md)

        elif node[0] == "source":
            md = add_source(node[1], md)

        elif node[0] == "coverage":
            md = add_coverage(node[1], md)

        elif node[0] == "user_defined":
            md = add_user_defined(node[1], md)

        # Known ignored cases
        elif node[0] == "known_ignored":
            pass

        elif node[0] is None:
            err_str = "Case which was not accounted for found when parsing nodes list"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("node", node),
                ("node_attrib", node[1].attrib),
                ("node_text", node[1].text),
                ("node_tail", node[1].tail),
            )
            raise NotImplementedError(err_str)

        else:

            err_str = "Unexpected case in main switch"
            err_str = default_log.log_variables(err_str, "ERROR", ("node", node))
            raise NotImplementedError(err_str)

        assert md is not None, "one of the parse methods has returned md as None - node[0] - {}".format(node[0])

    md.finalize()

    return md


def node_walk(root, node_types_list, node_locations):
    """
    Walk the tree rooted at root - add the field tags to node_types_list and the node to node location.
    :param root:
    :param node_types_list:
    :param node_locations:
    :return:
    """
    for node in root.iter("*"):

        node_types_list.append(node.tag)
        node_locations.append(node)

    return node_types_list, node_locations


def add_contributor(contributor_node, md):
    """
    Takes a contributor node - completely ignores it.
    :param contributor_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(contributor_node)

    re_att_key_dict = {
        r".*role": "role",
        r".*file-as": "creator_sort",
        r".*creator_sort": "creator_sort",
    }

    if attrib is not None:
        attrib = regex_dict_rekey(re_att_key_dict, attrib, all_rekey=False)
        if DEV_MODE and not set(attrib.keys()).issubset(set(re_att_key_dict.values())):
            err_str = "Unexpected field found in contributor attribute dict - could not rekey to known value.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("attrib", attrib),
                ("regex_attrib_key_dict", re_att_key_dict),
                ("set(attrib.keys())", set(attrib.keys())),
                ("set(re_att_key_dict.values())", set(re_att_key_dict.values())),
            )
            raise OpfParseError(err_str)

    if (attrib is not None) and (text is not None) and (tail is None):

        if "role" in attrib.keys():
            md.read_creators({attrib["role"]: text})

        # Currently ignoring file-as - which is the sort for that particular creator
        return md

    info_str = "Unexpected case found during add_contributor"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_coverage(title_node, metadata_return):
    """
    Adds the coverage node - the one example I've been able to find doesn't have any content, but logging it just in
    case for later.
    :param title_node:
    :param metadata_return:
    :return:
    """
    attrib, text, tail = process_node(title_node)

    if (attrib is None) and (text is None) and (tail is None):
        return metadata_return

    info_str = "Unexpected case found during add_coverage"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_creator(creator_node, md):
    """
    Tries to process a creator type node and add it to the metadata return
    :param creator_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(creator_node)

    # dict of known patterns to be applied to the keys (regex attribute key dict)
    re_att_key_dict = {
        r".*role": "role",
        r".*file-as": "creator_sort",
        r".*creator_sort": "creator_sort",
    }

    if attrib is not None:
        # processing the creator attrib dictionary to simplify the key list
        attrib = regex_dict_rekey(re_att_key_dict, attrib, all_rekey=False)
        if DEV_MODE and not set(attrib.keys()).issubset(set(re_att_key_dict.values())):
            err_str = "Unexpected field found in creator attribute dict - could not rekey to known value.\n"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("attrib", attrib),
                ("regex_attrib_key_dict", re_att_key_dict),
                ("set(attrib.keys())", set(attrib.keys())),
                ("set(re_att_key_dict.values())", set(re_att_key_dict.values())),
            )
            raise OpfParseError(err_str)

    if (attrib is not None) and (text is not None) and (tail is None):
        # If role is in the attribute dict keyes then the value will be the role of the person and the text will be
        # their name.
        if "role" in attrib.keys():
            md.read_creators({attrib["role"]: text})

        # In this case, sometimes, the text of the node is the name of the creator
        if "creator_sort" in attrib.keys():
            md.creator_sort = attrib["creator_sort"]
            # Default to author if not actual role is specified
            if "role" not in attrib.keys():
                md.read_creators({"author": text})

        return md

    elif (attrib is None) and (text is not None) and (tail is None):
        # Default to author if not other information is provided
        md.read_creators({"author": text})
        return md

    elif (attrib is None) and (text is None) and (tail is None):
        LiuXin_warning_print("Creator node had no content.")
        return md

    info_str = "Unexpected case found during add_creator"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_date(date_node, md):
    """
    Takes a date node - checks to see if it's a pubdate. If it is, adds it. If not tries to parse it, but will probably
    give up.
    :param date_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(date_node)

    regex_rekey_dict = {r".*event": "event"}
    attrib = regex_dict_rekey(regex_rekey_dict, attrib, all_rekey=False)

    if (attrib is None) and (text is not None) and (tail is None):
        md.pubdate = text
        return md

    elif (attrib is not None) and (text is not None) and (tail is None):
        if set(attrib.keys()) == {"event"} and attrib.get("event", "None").lower() == "publication":
            md.pubdate = text
            return md
        elif set(attrib.keys()) == {"event"} and attrib.get("event", "None").lower() == "original-publication":
            md.note = "original_publication_date - {}".format(text)
            return md
        elif set(attrib.keys()) == {"event"} and attrib.get("event", "None").lower() == "ops-publication":
            md.note = "ops_publication_date - {}".format(text)
            return md
        elif set(attrib.keys()) == {"event"} and attrib.get("event", "None").lower() == "conversion":
            # Not currently handled
            return md
        else:
            err_str = "Unexpected case found while trying to add_date"
            err_str = default_log.log_variables(err_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
            raise NotImplementedError(err_str)

    # With no data to process that's all she wrote
    elif (attrib is None) and (text is None) and (tail is None):
        return md

    err_str = "Unexpected case found in OPF add_date - Node attrib, text, tail as follows"
    err_str = default_log.log_variables(err_str, "ERROR", ("attrib", attrib), ("text", text), ("tail", tail))
    raise OpfParseError(err_str)


# Tries to parse a description node
def add_description(description_node, md):
    """
    Attempts to parse a description_node - currently just assumed it's a comment.
    :param description_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(description_node)

    regex_rekey_dict = {".*content": "content", ".*name": "name"}
    attrib = regex_dict_rekey(regex_rekey_dict, attrib, all_rekey=False)

    if (attrib is None) and (text is not None) and (tail is None):

        md.comment = text
        return md

    elif (attrib is not None) and (text is None) and (tail is None):

        if set(attrib.keys()) == {"content", "name"}:
            if attrib["name"] == "epubcheckdate":
                md.pubdate = attrib["content"]
            else:
                if DEV_MODE:
                    err_str = gen_err_string("description", attrib, text, tail)
                    raise OpfParseError(err_str)
                else:
                    md.tags = attrib.values()
        return md

    err_str = "Unexpected case found in OPF add_meta - Node attrib, text, tail as follows"
    err_str = default_log.log_variables(err_str, "ERROR", ("attrib", attrib), ("text", text), ("tail", tail))
    raise OpfParseError(err_str)


# Tries to deal with metadata in a meta_node - which could be almost anything. Gluck knows.
def add_generic_meta_data(meta_node, md):
    """
    Attempts to parse a generic meta node. Which will (probably) be mostly calibre metadata.
    :param meta_node: meta node to add to the metadata
    :param md: Metadata to add the node to
    :return:
    """
    attrib, text, tail = process_node(meta_node)

    if (attrib is None) and (text is None) and (tail is None):
        # In this case don't really have a node - abort parse
        return md

    if attrib is not None:
        # Todo: Merge all the rekey dicts into a master rekey dict
        rekey_dict = {r".*scheme": "scheme", r".*version": "version"}

        attrib = regex_dict_rekey(rekey_dict, attrib, all_rekey=False)

    if (attrib is not None) and (text is None) and (tail is None):

        # NODES TO BE IGNORED FROM LIT FILES START HERE
        # Check and ignore cover nodes from lit files (they need seperate processing)
        if attrib == {"content": "cover", "name": "cover"}:
            return md

        if attrib == {"content": "chaptertour", "name": "ms-chaptertour"}:
            return md

        # Different types of version nodes set by common programs - all are ignored
        # Ignore nodes indicating that this is the output from a calibre oeb2lit converter.
        # While interesting, there is such a thing as too much metadata
        # NEVER!
        if set(attrib.keys()) == {"content", "name"} and attrib["name"] == "calibre-oeb2lit-version":
            return md

        if set(attrib.keys()) == {"version"}:
            return md
        # ---------------------------------------------

        # check to see if the attribute dictionary is from calibre
        # for the moment any calibre user set metadata fields are ignored. Might want to improve this
        # Todo: (allow users to set behavior for their calibre custom metadata fields.)
        calibre_pat_string = r"calibre:.*"
        calibre_user_pat_string = r"calibre:user_metadata:.*"

        if re.search(calibre_pat_string, attrib["name"], re.I):

            try:
                if attrib["name"] == "calibre:series":
                    md.calibre_series = attrib["content"]

                elif attrib["name"] == "calibre:series_index":
                    md.calibre_series_index = attrib["content"]

                elif attrib["name"] == "calibre:title_sort":
                    md.title_sort = attrib["content"]

                elif attrib["name"] == "calibre:rating":
                    rating = {"calibre": attrib["content"]}
                    md.add_ratings(rating)

                elif attrib["name"] == "calibre:timestamp":
                    md.timestamp = attrib["content"]

                elif attrib["name"] == "cover":
                    pass
                    # Todo: Write a method to extract the referenced cover

                elif attrib["name"] == "calibre:series":
                    md.series = attrib["content"]

                elif attrib["name"] == "calibre:user_categories":
                    pass

                elif attrib["name"] == "calibre:author_link_map":
                    # This attribute stores the author_id link for calibre. Discarding it, for the moment
                    pass

                elif re.search(calibre_user_pat_string, attrib["name"], re.I):
                    pass

                else:
                    err_str = "OPF parser unable to recognize calibre style meta node"
                    err_str = default_log.log_variables(
                        err_str,
                        "ERROR",
                        ("attrib", attrib),
                        ("text", text),
                        ("tail", tail),
                    )
                    raise OpfParseError(err_str)

            except KeyError:
                err_str = "OPF parser found a meta node without either name or content. Code faster!"
                err_str = default_log.log_variables(
                    err_str, "ERROR", ("attrib", attrib), ("text", text), ("tail", tail)
                )
                raise OpfParseError(err_str)

            return md

    elif (attrib is None) and (text is not None) and (tail is None):

        if text.lower()[:9] == "copyright":
            md.rights = text
        else:
            if DEV_MODE:
                err_str = """OPF parser found text of an unrecognized type in a node."""
                err_str += repr(text)
                raise OpfParseError(err_str)
            else:
                md.comments = text
                return md

    err_str = "Unexpected case found in OPF add_generic_meta_data - Node attrib, text, tail as follows"
    err_str = default_log.log_variables(err_str, "ERROR", ("attrib", attrib), ("text", text), ("tail", tail))
    raise OpfParseError(err_str)


def add_identifier(identifier_node, md):
    """
    Attempts to parse an identifiers node. Adds it to the metadata object.
    :param identifier_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(identifier_node)

    rekey_dict = {r".*scheme": "scheme"}

    attrib = regex_dict_rekey(rekey_dict, attrib, all_rekey=False)

    # checking that the observed keyes fall within the set of keys we know how to handle
    attrib_key_set = set(key for key in attrib.keys())
    know_keys = {"id", "scheme"}
    assert attrib_key_set <= know_keys, "Error while trying to add identifier. Unknown key in attribs"

    # Need to distinguish between internal and external identifiers.
    # Because they need to be dealt with differently
    if (attrib is not None) and (text is not None) and (tail is None):

        # Deal with the calibre id case - ignore it
        if attrib.keys() == ["id"] and attrib["id"] == "calibre-uuid":
            return md

        # Deal with the secondary calibre id case - ignore it
        if (
            set(k for k in attrib.keys()) == {"id", "scheme"}
            and attrib["id"] == "calibre_id"
            and attrib["scheme"] == "calibre"
        ):
            return md

        # for the one case thus far observed, scheme is the same as id, so using that
        add_dict = dict()
        try:
            schema_name = canonicalize_id_name(attrib["scheme"])
        except KeyError:
            err_str = "attrib has no scheme\nattrib - {}\ntext - {}".format(attrib, text)
            raise KeyError(err_str)

        # Todo: Add more processing upstream, before database write to deal with strings such as
        # urn:uuid:c609903e-12d0-11e6-a48f-4c72b9252ec6
        add_dict[schema_name] = text
        if schema_name in INTERNAL_EBOOK_ID_SCHEMA:
            md.add_internal_identifiers(add_dict)
            return md
        elif schema_name in EXTERNAL_EBOOK_ID_SCHEMA:
            md.read_identifiers(add_dict)
            return md
        else:
            if DEV_MODE:
                err_str = gen_err_string("identifier", attrib, text, tail)
                err_str += "\nUnable to identify passed identifiers in the internal or external id lists."
                default_log.error(err_str)
                return md
            else:
                md.read_identifiers(add_dict)
                return md

    err_str = "Unexpected data found in OPF add_identifier - node wasn't processed"
    err_str = default_log.log_variables(err_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise OpfParseError(err_str)


# Takes a language node - tries to parse it - assuming this is just the language
# Todo: Add canonicalize language function
def add_language(language_node, metadata_return):
    """
    Takes a language node - tries to process the attribute, text and tail to add it to the metadata.
    :param language_node:
    :param metadata_return:
    :return:
    """
    attrib, text, tail = process_node(language_node)

    if (attrib is None) and (text is not None) and (tail is None):
        metadata_return.language = text
        return metadata_return
    elif (attrib is not None) and (text is not None) and (tail is None):
        if attrib == {"{http://www.w3.org/2001/XMLSchema-instance}type": "dcterms:RFC4646"}:
            metadata_return.language = text
            return metadata_return
        info_str = "Unexpected data found in OPF add_language - node was processed - value set to the text"
        default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
        metadata_return.language = text
        return metadata_return

    info_str = "Unexpected data found in OPF add_language - node wasn't processed"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise OpfParseError(info_str)


# Currently doesn't care about the original metadata node
def add_metadata(metadata_node, md):
    """
    Adds a generic metadata node.
    :param metadata_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(metadata_node)

    if (attrib is None) and (text is None) and (tail is None):
        return md

    info_str = "Unexpected data found in OPF add_metadata - node wasn't processed"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise OpfParseError(info_str)


def add_publisher(publisher_node, metadata_return):
    """
    Takes the attributes, text and tail text of a publisher node. Tries to add it to the metadata object.
    """
    attrib, text, tail = process_node(publisher_node)

    if (attrib is None) and (tail is None) and (text is not None):
        metadata_return.publisher = text
        return metadata_return

    info_str = "Unexpected case found during add_publisher"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_pubdate(date_node, metadata_return):
    """
    Takes a pubdate node - tries to add it to the metadata.
    :param date_node:
    :param metadata_return:
    :return:
    """
    attrib, text, tail = process_node(date_node)

    info_str = "Unexpected structure found when trying to add a pubdate"
    info_str = default_log.log_variables(info_str, "ERROR", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_rights(data_node, md):
    """
    Takes a rights node - tries to add it to the metadata.
    :param data_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(data_node)

    regex_rekey_dict = {r".*type": "type"}
    attrib = regex_dict_rekey(regex_rekey_dict, attrib, all_rekey=False)

    if (attrib is None) and (text is not None) and (tail is None):
        md.rights = text
        return md

    elif (attrib is not None) and (text is not None) and (tail is None):
        if set(attrib.keys()) == {"type"} and attrib.get("type", "None").lower() == "dcterms:uri":
            md.note = "this work has the following liscence - {}".format(text)
            return md

    info_str = "Unexpected structure found when trying to add rights"
    info_str = default_log.log_variables(info_str, "ERROR", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_source(data_node, md):
    """
    Takes a rights node - tries to add it to the metadata.
    :param data_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(data_node)

    # Todo: Add the blank node case to all the nodes
    if (attrib is None) and (text is not None) and (tail is None):
        md.notes = "title_source: {}".format(text)
        return md

    info_str = "Unexpected structure found when trying to add_source"
    info_str = default_log.log_variables(info_str, "ERROR", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


# Takes a subject node - tries to parse it - assuming this is the pubdate
def add_subject(subject_node, metadata_return):
    """
    Takes a subject node - tries to add it to the metadata_return.
    calibre stores tags under the subject heading - so will we.
    :param subject_node: The node to analyze
    :param metadata_return: The metadata object to add it to
    :return metadata_return: The updated metadata object
    """
    attrib, text, tail = process_node(subject_node)

    if (attrib is None) and (text is not None) and (tail is None):
        metadata_return.tags = text
        return metadata_return

    elif (attrib is not None) and (text is not None) and (tail is None):
        if attrib == {"{http://www.idpf.org/2007/opf}event": "original-publication"}:
            metadata_return.note = "orginal_pubdate - {}".format(text)
            return metadata_return
        elif attrib == {"{http://www.idpf.org/2007/opf}event": "ops-publication"}:
            metadata_return.note = "ops-publication - {}".format(text)
            return metadata_return
        else:
            info_str = "Unexpected case found in OPF add_subject"
            info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
            raise NotImplementedError(info_str)

    info_str = "Unexpected case found in OPF add_subject"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_title(title_node, md):
    """
    Takes a title node. Tries to add it to the metadata object.
    :param title_node: Title node to process
    :param md:
    :return:
    """
    attrib, text, tail = process_node(title_node)

    if (attrib is None) and (text is not None) and (tail is None):
        md.title = text
        return md

    info_str = "Unexpected case found during add_title"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def add_user_defined(user_defined_node, md):
    """
    Try and parse user defined metadata nodes.
    :param user_defined_node:
    :param md:
    :return:
    """
    attrib, text, tail = process_node(user_defined_node)

    # Check, and ignore, the blank mode case
    if (attrib is None) and (text is None) and (tail is None):
        return md

    regex_rekey_dict = {r"^.*name$": "name", r"^.*value-type$": "value-type"}

    attrib = regex_dict_rekey(regex_rekey_dict, attrib, all_rekey=True)
    attrib = dict_lower_values(attrib)

    if (attrib is not None) and (text is None) and (tail is None):

        # Ignore the case where the is a custom node called info, but no actual info
        info_re = r"^info\s+[0-9]+$"
        if attrib.keys() == ["name"] and re.match(pattern=info_re, string=attrib["name"], flags=re.I):
            return md

    # One method of custom encoding metadata into odt files is to set all the metadata as standard opf fields with
    # the name set to opf.field_name - this case is intended to handle that
    # Todo: There has to be a better way of handling this
    if (attrib is not None) and (text is not None) and (tail is None):

        # Taken from http://www.idpf.org/epub/20/spec/OPF_2.0.1_draft.htm#Section2.2

        # opf.authors case
        if attrib.keys() == ["name"] and attrib["name"] == "opf.authors":
            md.read_creators({"author": text})
            return md

        if attrib.keys() == ["name"] and attrib["name"] == "opf.authorsort":
            md.creator_sort = text
            return md

        if attrib.keys() == ["name"] and attrib["name"] == "opf.language":
            from LiuXin.utils.localization import canonicalize_lang

            cl = canonicalize_lang(text)
            if cl:
                md.languages = [cl]
            return md

        if attrib.keys() == ["name"] and attrib["name"] == "opf.isbn":
            md.isbn = text
            return md

        series_names = {"series", "opf.series"}
        if attrib.keys() == ["name"] and attrib["name"] in series_names:
            md.calibre_series = text
            return md

        series_index_names = {
            "series_index",
            "seriesindex",
            "opf.series_index",
            "opf.seriesindex",
        }
        if attrib.keys() == ["name"] and attrib["name"] in series_index_names:
            md.calibre_series_index = text
            return md

        # series information can turn up here as well

        # Node that indicates if the opf metadata should be parsed - really should look for this first and use it
        # as a switch to control this function
        # Todo: Implement this - can't be bothered right now
        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] == "opf.metadata":
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] == "opf.pubdate":
            from LiuXin.utils.date import parse_date

            try:
                md.pubdate = parse_date(text, assume_utc=True)
            except ValueError:
                # Something has gone wrong while parsing the string
                md.pubdate = text
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] == "opf.publisher":
            md.publisher = text
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] == "opf.title":
            md.calibre_title = text
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] == "opf.titlesort":
            md.calibre_title_sort = text
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] == "opf.nocover":
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] == "opf.subject":
            md.tags = [t.strip() for t in text.split(",")]
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] in series_names:
            md.calibre_series = text
            return md

        if dict_keys_set(attrib) == {"name", "value-type"} and attrib["name"] in series_index_names:
            try:
                md.calibre_series_index = float(text)
            except ValueError:
                md.calibre_series_index = 1.0
            return md

    info_str = "Unexpected case found during add_user_defined"
    info_str = default_log.log_variables(info_str, "INFO", ("attrib", attrib), ("text", text), ("tail", tail))
    raise NotImplementedError(info_str)


def process_node(xml_node):
    """
    Sets any of the three that are content free to None. Makes the comparison process simpler.
    :param xml_node:
    :return: (attrib, text, tail)
    """
    attrib = xml_node.attrib
    text = xml_node.text
    tail = xml_node.tail

    # If the attributes dictionary is empty, setting it to none
    if attrib == {}:
        attrib = None

    # if text and tail are composed purely of white space, setting them to None
    if text is not None:
        text = text.strip()
    if text == "":
        text = None

    if tail is not None:
        tail = tail.strip()
    if tail == "":
        tail = None

    return attrib, text, tail


def simple_get_metadata_node(root):
    """
    Takes a parsed xml file. Finds the memory location of the metadata node and returns it.
    :param root:
    :return:
    """
    # starts at the top level of the tree. Iterates down.
    children_tags = []
    children_pointers = []
    for child in root:
        children_tags.append(child.tag)
        children_pointers.append(child)
    assert len(children_tags) == len(children_pointers)

    metadata_candidate_tags = []
    metadata_candidate_pointers = []
    metadata_regex = r".*metadata"
    metadata_pat = re.compile(metadata_regex, re.IGNORECASE)

    for i in range(len(children_tags)):

        tag_unicode = children_tags[i]

        if metadata_pat.match(tag_unicode) is not None:
            metadata_candidate_tags.append(children_tags[i])
            metadata_candidate_pointers.append(children_pointers[i])

    if len(metadata_candidate_pointers) == 1:
        return metadata_candidate_pointers
    elif len(metadata_candidate_pointers) == 0:
        # Todo : Add fallback heuristics for AUTONOMOUS_MODE so this doesn't happen
        default_log.warn("Failed to parse OPF file.")
        return False
    else:
        # The first node encountered will be assumed to be the right one
        default_log.warn(
            "Error - multiple metadata candidates found in opf file",
            repr(metadata_candidate_tags),
        )
        return metadata_candidate_pointers


def gen_err_string(node_name, attrib, text, tail):
    """
    Takes all the data needed from a node. Returns the err_str appropriate to print for that node
    :param node_name:
    :param attrib:
    :param text:
    :param tail:
    :return err_str:
    """
    node_name = deepcopy(node_name)
    attrib = deepcopy(attrib)
    text = deepcopy(text)
    tail = deepcopy(tail)

    err_str = """A {} node encountered an unrecognized instance and could not be reliable parsed.
As DEV_MODE is enable this has thrown an error.

    Node attributes - {}
    Node text - {}
    Node tail - {}
    """
    return err_str.format(node_name, repr(attrib), repr(text), repr(tail))
