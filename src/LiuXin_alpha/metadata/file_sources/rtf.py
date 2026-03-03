"""
Read metadata from RTF files - modified and expanded from calibre for LiuXin
"""

# The rtf specification can be found at http://www.biblioscape.com/rtf15_spec.htm

import codecs
import re

from copy import deepcopy

from LiuXin.metadata import string_to_authors
from LiuXin.metadata.metadata import MetaData as MetaInformation

from LiuXin.utils.calibre import force_unicode
from LiuXin.utils.localization import trans as _

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO as StringIO
from LiuXin.utils.lx_libraries.liuxin_six import six_unichar
from LiuXin.utils.lx_libraries.liuxin_six import six_string_types


__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


title_pat = re.compile(r"\{\\info.*?\{\\title(.*?)(?<!\\)\}", re.DOTALL)
subject_pat = re.compile(r"\{\\info.*?\{\\subject(.*?)(?<!\\)\}", re.DOTALL)
author_pat = re.compile(r"\{\\info.*?\{\\author(.*?)(?<!\\)\}", re.DOTALL)
manager_pat = re.compile(r"\{\\info.*?\{\\manager(.*?)(?<!\\)\}", re.DOTALL)
company_pat = re.compile(r"\{\\info.*?\{\\company(.*?)(?<!\\)\}", re.DOTALL)
operator_pat = re.compile(r"\{\\info.*?\{\\operator(.*?)(?<!\\)\}", re.DOTALL)
tags_pat = re.compile(r"\{\\info.*?\{\\category(.*?)(?<!\\)\}", re.DOTALL)
tags_pat_2 = re.compile(r"\{\\info.*?\{\\keywords(.*?)(?<!\\)\}", re.DOTALL)
comment_pat_2 = re.compile(r"\{\\info.*?\{\\comment(.*?)(?<!\\)\}", re.DOTALL)


def get_document_info(stream):
    r"""
    Extract the \info block from an RTF file.
    Return the info block as a string and the position in the file at which it starts.
    :param stream: File like object pointing to the RTF file.
    :return: info_block, start_position
    """
    block_size = 4096
    stream.seek(0)
    found, block = False, ""
    while not found:
        prefix = block[-6:]
        block = prefix + stream.read(block_size)
        actual_block_size = len(block) - len(prefix)
        if len(block) == len(prefix):
            break
        idx = block.find(r"{\info")
        if idx >= 0:
            found = True
            pos = stream.tell() - actual_block_size + idx - len(prefix)
            stream.seek(pos)
        else:
            if block.find(r"\sect") > -1:
                break
    if not found:
        return None, 0
    data, count, = (
        StringIO(),
        0,
    )
    pos = stream.tell()
    while True:
        ch = stream.read(1)
        if ch == "\\":
            data.write(ch + stream.read(1))
            continue
        if ch == "{":
            count += 1
        elif ch == "}":
            count -= 1
        data.write(ch)
        if count == 0:
            break
    return data.getvalue(), pos


def detect_codepage(stream):
    """
    Information needed to convert the ANSII back to unicode.
    :param stream:
    :return: codepage
    :rtype: Will return None if a codepage cannot be found
    """
    pat = re.compile(r"\\ansicpg(\d+)")
    match = pat.search(stream.read(512))

    if match is not None:
        num = match.group(1)
        if num == "0":
            num = "1252"
        codec = "cp" + num
        try:
            codecs.lookup(codec)
            return codec
        except:
            pass

    return


def encode(unistr):
    if not isinstance(unistr, unicode):
        unistr = force_unicode(unistr)
    return "".join([str(c) if ord(c) < 128 else "\\u" + str(ord(c)) + "?" for c in unistr])


def decode(raw, codec):
    if codec is not None:

        def codepage(match):
            return chr(int(match.group(1), 16))

        raw = re.sub(r"\\'([a-fA-F0-9]{2})", codepage, raw)
        raw = raw.decode(codec)

    def uni(match):
        return six_unichar(int(match.group(1)))

    raw = re.sub(r"\\u([0-9]{3,4}).", uni, raw)
    return raw


def get_metadata(target_file):
    """
    Return metadata as a L{MetaInfo} object.
    :param target_file:
    :return: file_md
    :rtype: LiuXin_md
    """
    # Ensuring a stream pointing to the target file
    if isinstance(target_file, six_string_types):
        target_file = deepcopy(target_file)
        with open(target_file, "rb") as stream:
            return rtf_get_metadata_from_stream(stream)
    else:
        stream = target_file
        return rtf_get_metadata_from_stream(stream)


def rtf_get_metadata_from_stream(stream):
    """
    Read metadata from a stream.
    :param stream:
    :return:
    """

    mi = MetaInformation()

    stream.seek(0)
    if stream.read(5) != r"{\rtf":
        return mi
    block = get_document_info(stream)[0]
    if not block:
        return mi

    stream.seek(0)
    cpg = detect_codepage(stream)
    stream.seek(0)

    title_match = title_pat.search(block)
    if title_match is not None:
        title = decode(title_match.group(1).strip(), cpg)
    else:
        title = _("Unknown")
    mi.title = title

    author_match = author_pat.search(block)
    if author_match is not None:
        author = decode(author_match.group(1).strip(), cpg)
    else:
        author = None
    if author:
        mi.authors = string_to_authors(author)

    comment_match = subject_pat.search(block)
    if comment_match is not None:
        comment = decode(comment_match.group(1).strip(), cpg)
        mi.comments = comment

    comment_match_2 = comment_pat_2.search(block)
    if comment_match_2 is not None:
        comment_2 = decode(comment_match_2.group(1).strip(), cpg)
        mi.comments = comment_2

    # Tags serving as a catchall for any extra fields that might be floating around
    # As over tagging is hardly likely to be a serious concern
    tags_match = tags_pat.search(block)
    if tags_match is not None:
        tags = decode(tags_match.group(1).strip(), cpg)
        mi.tags = list(filter(None, (x.strip() for x in tags.split(","))))

    tags_match_2 = tags_pat_2.search(block)
    if tags_match_2 is not None:
        tags_2 = decode(tags_match_2.group(1).strip(), cpg)
        mi.tags = list(filter(None, (x.strip() for x in tags_2.split(","))))

    publisher_match = manager_pat.search(block)
    if publisher_match is not None:
        publisher = decode(publisher_match.group(1).strip(), cpg)
        mi.publisher = publisher

    company_match = company_pat.search(block)
    if company_match is not None:
        company = decode(company_match.group(1).strip(), cpg)
        mi.tags = company

    operator_match = operator_pat.search(block)
    if operator_match is not None:
        operator = decode(operator_match.group(1).strip(), cpg)
        mi.add_creators({"operator": operator})

    return mi


def create_metadata(stream, options):
    """
    Make a metadata packet with the given options.
    :param stream:
    :param options:
    :return:
    """
    md = [r"{\info"]
    if options.title:
        title = encode(options.title)
        md.append(r"{\title %s}" % (title,))

    if options.authors:
        au = options.authors
        if not isinstance(au, six_string_types):
            au = ", ".join(au)
        author = encode(au)
        md.append(r"{\author %s}" % (author,))

    comp = options.comment if hasattr(options, "comment") else options.comments
    if comp:
        comment = encode(comp)
        md.append(r"{\subject %s}" % (comment,))

    if options.publisher:
        publisher = encode(options.publisher)
        md.append(r"{\manager %s}" % (publisher,))

    if options.tags:
        tags = ", ".join(options.tags)
        tags = encode(tags)
        md.append(r"{\category %s}" % (tags,))

    if len(md) > 1:
        md.append("}")
        stream.seek(0)
        src = stream.read()
        ans = src[:6] + "".join(md) + src[6:]
        stream.seek(0)
        stream.write(ans)


def set_metadata(stream, options):
    """
    Modify/add RTF metadata in stream
    :param stream: The stream object to modify
    :param options: Object with metadata attributes title, author, comment, category.
                    Note - if both comment and comments are attributes of this object then the comment attribute will
                    be preferred.
    :type options: For example calibreMetaData object
    :return:
    """

    def add_metadata_item(src, name, val):
        index = src.rindex("}")
        return src[:index] + r"{\ "[:-1] + name + " " + val + "}}"

    src, pos = get_document_info(stream)

    # The metadata packet will have to be created wholesale
    if src is None:
        create_metadata(stream, options)

    # Use the existing metadata packet
    else:
        olen = len(src)

        base_pat = r"\{\\name(.*?)(?<!\\)\}"
        title = options.title
        if title is not None:
            title = encode(title)
            pat = re.compile(base_pat.replace("name", "title"), re.DOTALL)
            if pat.search(src):
                src = pat.sub(r"{\\title " + title + r"}", src)
            else:
                src = add_metadata_item(src, "title", title)

        # Should catch if the comment has been set to the attributes comments or comment
        try:
            comment = options.comment
        except AttributeError:
            comment = options.comments
        if comment is not None:
            comment = encode(comment)
            pat = re.compile(base_pat.replace("name", "subject"), re.DOTALL)
            if pat.search(src):
                src = pat.sub(r"{\\subject " + comment + r"}", src)
            else:
                src = add_metadata_item(src, "subject", comment)

        author = options.authors
        if author is not None:
            author = "& ".join(author)
            author = encode(author)
            pat = re.compile(base_pat.replace("name", "author"), re.DOTALL)
            if pat.search(src):
                src = pat.sub(r"{\\author " + author + r"}", src)
            else:
                src = add_metadata_item(src, "author", author)

        tags = options.tags
        if tags is not None:
            tags = ", ".join(tags)
            tags = encode(tags)
            pat = re.compile(base_pat.replace("name", "category"), re.DOTALL)
            if pat.search(src):
                src = pat.sub(r"{\\category " + tags + r"}", src)
            else:
                src = add_metadata_item(src, "category", tags)

        publisher = options.publisher
        if publisher is not None:
            publisher = encode(publisher)
            pat = re.compile(base_pat.replace("name", "manager"), re.DOTALL)
            if pat.search(src):
                src = pat.sub(r"{\\manager " + publisher + r"}", src)
            else:
                src = add_metadata_item(src, "manager", publisher)

        stream.seek(pos + olen)
        after = stream.read()
        stream.seek(pos)
        stream.truncate()
        stream.write(src)
        stream.write(after)
