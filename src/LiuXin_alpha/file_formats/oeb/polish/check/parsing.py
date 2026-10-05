#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Parse and serialize EPUB/OEB markup under compatibility rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise parsing through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import re
import typing as _typing

try:
    import cssutils
except ModuleNotFoundError:
    cssutils = None
from lxml.etree import XMLParser, XMLSyntaxError, fromstring

from LiuXin_alpha.file_formats.html_entities import html5_entities
from LiuXin_alpha.file_formats.oeb.base import OEB_DOCS, URL_SAFE, XHTML_NS, urlquote
from LiuXin_alpha.file_formats.oeb.polish.check.base import ERROR, INFO, WARN, BaseError
from LiuXin_alpha.file_formats.oeb.polish.pretty import (
    pretty_script_or_style as fix_style_tag,
)
from LiuXin_alpha.file_formats.oeb.polish.utils import PositionFinder, guess_type
from LiuXin_alpha.utils.libraries.calibre_chardet import (
    find_declared_encoding,
    replace_encoding_declarations,
)

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.text import as_unicode as force_unicode
from LiuXin_alpha.utils.text import human_readable
from LiuXin_alpha.utils.text.xml_utils import prepare_string_for_xml

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

HTML_ENTITTIES = frozenset(html5_entities)
XML_ENTITIES = {"lt", "gt", "amp", "apos", "quot"}
ALL_ENTITIES = HTML_ENTITTIES | XML_ENTITIES

replace_pat = re.compile("&(%s);" % "|".join(re.escape(x) for x in sorted((HTML_ENTITTIES - XML_ENTITIES))))
mismatch_pat = re.compile(r"tag mismatch:.+?line (\d+).+?line \d+")


class EmptyFile(BaseError):

    """
    Provide the emptyfile contract for validated ebook processing.

    Example:
        Exercise EmptyFile through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    HELP = _("This file is empty, it contains nothing, you should probably remove it.")
    INDIVIDUAL_FIX = _("Remove this file")

    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        """
        Initialize and validate the emptyfile state.

        Example:
            Exercise EmptyFile.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("The file %s is empty") % name, name)

    def __call__(self: _typing.Self, container: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise EmptyFile.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        container.remove_item(self.name)
        return True


class DecodeError(BaseError):

    """
    Report a decodeerror encountered while processing an ebook format.

    Example:
        Exercise DecodeError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    is_parsing_error = True

    HELP = _(
        "A decoding errors means that the contents of the file could not"
        " be interpreted as text. This usually happens if the file has"
        " an incorrect character encoding declaration or if the file is actually"
        " a binary file, like an image or font that is mislabelled with"
        " an incorrect media type in the OPF."
    )

    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        """
        Initialize and validate the decodeerror state.

        Example:
            Exercise DecodeError.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("Parsing of %s failed, could not decode") % name, name)


class XMLParseError(BaseError):

    """
    Report a xmlparseerror encountered while processing an ebook format.

    Example:
        Exercise XMLParseError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    is_parsing_error = True

    HELP = _(
        "A parsing error in an XML file means that the XML syntax in the file is incorrect."
        " Such a file will most probably not open in an ebook reader. These errors can "
        " usually be fixed automatically, however, automatic fixing can sometimes "
        ' "do the wrong thing".'
    )

    def __init__(self: _typing.Self, msg: _typing.Any, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Initialize and validate the xmlparseerror state.

        Example:
            Exercise XMLParseError.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param msg: Value supplied for msg under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        msg = msg or ""
        BaseError.__init__(self, "Parsing failed: " + msg, *args, **kwargs)
        m = mismatch_pat.search(msg)
        if m is not None:
            self.has_multiple_locations = True
            self.all_locations = [
                (self.name, int(m.group(1)), None),
                (self.name, self.line, self.col),
            ]


class HTMLParseError(XMLParseError):

    """
    Report a htmlparseerror encountered while processing an ebook format.

    Example:
        Exercise HTMLParseError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    HELP = _(
        "A parsing error in an HTML file means that the HTML syntax is incorrect."
        " Most readers will automatically ignore such errors, but they may result in "
        " incorrect display of content. These errors can usually be fixed automatically,"
        ' however, automatic fixing can sometimes "do the wrong thing".'
    )


class NamedEntities(BaseError):

    """
    Provide the namedentities contract for validated ebook processing.

    Example:
        Exercise NamedEntities through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    level = WARN
    INDIVIDUAL_FIX = _("Replace all named entities with their character equivalents in this book")
    HELP = _(
        "Named entities are often only incompletely supported by various book reading software."
        " Therefore, it is best to not use them, replacing them with the actual characters they"
        " represent. This can be done automatically."
    )

    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        """
        Initialize and validate the namedentities state.

        Example:
            Exercise NamedEntities.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("Named entities present"), name)

    def __call__(self: _typing.Self, container: _typing.Any) -> _typing.Any:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise NamedEntities.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        changed = False
        from LiuXin_alpha.file_formats.oeb.polish.check.main import XML_TYPES

        check_types = XML_TYPES | OEB_DOCS
        for name, mt in iteritems(container.mime_map):
            if mt in check_types:
                raw = container.raw_data(name)
                nraw = replace_pat.sub(lambda m: html5_entities[m.group(1)], raw)
                if raw != nraw:
                    changed = True
                    with container.open(name, "wb") as f:
                        f.write(nraw.encode("utf-8"))
        return changed


class EscapedName(BaseError):

    """
    Provide the escapedname contract for validated ebook processing.

    Example:
        Exercise EscapedName through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    level = WARN

    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        """
        Initialize and validate the escapedname state.

        Example:
            Exercise EscapedName.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        from LiuXin_alpha.utils.storage.local.filenames import ascii_filename

        BaseError.__init__(self, _("Filename contains unsafe characters"), name)
        qname = urlquote(name)

        def esc(n: _typing.Any) -> _typing.Any:
            """
            Perform the esc operation under explicit file-format and conversion rules.

            Example:
                Exercise EscapedName.  init  .esc through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


            :param n: Value supplied for n under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "".join(x if x in URL_SAFE else "_" for x in n)

        self.sname = "/".join(esc(ascii_filename(x)) for x in name.split("/"))
        self.HELP = _(
            "The filename {0} contains unsafe characters, that must be escaped, like"
            " this {1}. This can cause problems with some ebook readers. To be"
            " absolutely safe, use only the English alphabet [a-z], the numbers [0-9],"
            " underscores and hyphens in your file names. While many other characters"
            " are allowed, they may cause problems with some software."
        ).format(name, qname)

        self.INDIVIDUAL_FIX = _("Rename the file {0} to {1}").format(name, self.sname)

    def __call__(self: _typing.Self, container: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise EscapedName.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.polish.replace import rename_files

        all_names = set(container.name_path_map)
        bn, ext = self.sname.rpartition(".")[0::2]
        c = 0
        while self.sname in all_names:
            c += 1
            self.sname = "%s_%d.%s" % (bn, c, ext)
        rename_files(container, {self.name: self.sname})
        return True


class TooLarge(BaseError):

    """
    Provide the toolarge contract for validated ebook processing.

    Example:
        Exercise TooLarge through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    level = INFO
    MAX_SIZE = 260 * 1024
    HELP = _(
        "This HTML file is larger than %s. Too large HTML files can cause performance problems"
        " on some ebook readers. Consider splitting this file into smaller sections."
    ) % human_readable(MAX_SIZE)

    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        """
        Initialize and validate the toolarge state.

        Example:
            Exercise TooLarge.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("File too large"), name)


class BadEntity(BaseError):

    """
    Provide the badentity contract for validated ebook processing.

    Example:
        Exercise BadEntity through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    HELP = _(
        "This is an invalid (unrecognized) entity. Replace it with whatever" " text it is supposed to have represented."
    )

    def __init__(self: _typing.Self, ent: _typing.Any, name: _typing.Any, lnum: _typing.Any, col: _typing.Any) -> None:
        """
        Initialize and validate the badentity state.

        Example:
            Exercise BadEntity.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param ent: Value supplied for ent under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param lnum: Value supplied for lnum under the utility contract.
        :param col: Value supplied for col under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("Invalid entity: %s") % ent, name, lnum, col)


class BadNamespace(BaseError):

    """
    Provide the badnamespace contract for validated ebook processing.

    Example:
        Exercise BadNamespace through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    INDIVIDUAL_FIX = _("Run fix HTML on this file, which will automatically insert the correct namespace")

    def __init__(self: _typing.Self, name: _typing.Any, namespace: _typing.Any) -> None:
        """
        Initialize and validate the badnamespace state.

        Example:
            Exercise BadNamespace.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param namespace: Value supplied for namespace under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("Invalid or missing namespace"), name)
        self.HELP = prepare_string_for_xml(
            _(
                "This file has {0}. Its namespace must be {1}. Set the namespace by defining the xmlns"
                ' attribute on the <html> element, like this <html xmlns="{1}">'
            ).format(
                (_("incorrect namespace %s") % namespace) if namespace else _("no namespace"),
                XHTML_NS,
            )
        )

    def __call__(self: _typing.Self, container: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise BadNamespace.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        container.parsed(self.name)
        container.dirty(self.name)
        return True


class NonUTF8(BaseError):

    """
    Provide the nonutf8 contract for validated ebook processing.

    Example:
        Exercise NonUTF8 through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    level = WARN
    INDIVIDUAL_FIX = _("Change this file's encoding to UTF-8")

    def __init__(self: _typing.Self, name: _typing.Any, enc: _typing.Any) -> None:
        """
        Initialize and validate the nonutf8 state.

        Example:
            Exercise NonUTF8.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param enc: Value supplied for enc under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("Non UTF-8 encoding declaration"), name)
        self.HELP = (
            _(
                "This file has its encoding declared as %s. Some"
                " reader software cannot handle non-UTF8 encoded files."
                " You should change the encoding to UTF-8."
            )
            % enc
        )

    def __call__(self: _typing.Self, container: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise NonUTF8.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        raw = container.raw_data(self.name)
        if isinstance(raw, type("")):
            raw, changed = replace_encoding_declarations(raw)
            if changed:
                container.open(self.name, "wb").write(raw.encode("utf-8"))
                return True


class EntitityProcessor(object):
    """
    Provide the entitityprocessor contract for validated ebook processing.

    Example:
        Exercise EntitityProcessor through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, mt: _typing.Any) -> None:
        """
        Initialize and validate the entitityprocessor state.

        Example:
            Exercise EntitityProcessor.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param mt: Value supplied for mt under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.entities = ALL_ENTITIES if mt in OEB_DOCS else XML_ENTITIES
        self.ok_named_entities = []
        self.bad_entities = []

    def __call__(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise EntitityProcessor.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        val = m.group(1).decode("ascii")
        if val in XML_ENTITIES:
            # Leave XML entities alone
            return m.group()

        if val.startswith("#"):
            nval = val[1:]
            try:
                if nval.startswith("x"):
                    int(nval[1:], 16)
                else:
                    int(nval, 10)
            except ValueError:
                # Invalid numerical entity
                self.bad_entities.append((m.start(), m.group()))
                return b" " * len(m.group())
            return m.group()

        if val in self.entities:
            # Known named entity, report it
            self.ok_named_entities.append(m.start())
        else:
            self.bad_entities.append((m.start(), m.group()))
        return b" " * len(m.group())


def check_html_size(name: _typing.Any, mt: _typing.Any, raw: _typing.Any) -> _typing.Any:
    """
    Perform the check html size operation under explicit file-format and conversion rules.

    Example:
        Exercise check html size through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param mt: Value supplied for mt under the utility contract.
    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    errors = []
    if len(raw) > TooLarge.MAX_SIZE:
        errors.append(TooLarge(name))
    return errors


entity_pat = re.compile(rb"&(#{0,1}[a-zA-Z0-9]{1,8});")


def check_encoding_declarations(name: _typing.Any, container: _typing.Any) -> _typing.Any:
    """
    Perform the check encoding declarations operation under explicit file-format and conversion rules.

    Example:
        Exercise check encoding declarations through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param container: Value supplied for container under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    errors = []
    enc = find_declared_encoding(container.raw_data(name))
    if enc is not None and enc.lower() != "utf-8":
        errors.append(NonUTF8(name, enc))
    return errors


def check_xml_parsing(name: _typing.Any, mt: _typing.Any, raw: _typing.Any) -> _typing.Any:
    """
    Perform the check xml parsing operation under explicit file-format and conversion rules.

    Example:
        Exercise check xml parsing through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param mt: Value supplied for mt under the utility contract.
    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not raw:
        return [EmptyFile(name)]
    raw = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    # Get rid of entities as named entities trip up the XML parser
    eproc = EntitityProcessor(mt)
    eraw = entity_pat.sub(eproc, raw)
    parser = XMLParser(recover=False)
    errcls = HTMLParseError if mt in OEB_DOCS else XMLParseError
    errors = []
    if eproc.ok_named_entities:
        errors.append(NamedEntities(name))
    if eproc.bad_entities:
        position = PositionFinder(raw)
        for offset, ent in eproc.bad_entities:
            lnum, col = position(offset)
            errors.append(BadEntity(ent, name, lnum, col))

    try:
        root = fromstring(eraw, parser=parser)
    except UnicodeDecodeError:
        return errors + [DecodeError(name)]
    except XMLSyntaxError as err:
        try:
            line, col = err.position
        except:
            line = col = None
        return errors + [errcls(err.message, name, line, col)]
    except Exception as err:
        return errors + [errcls(err.message, name)]

    if mt in OEB_DOCS:
        if root.nsmap.get(root.prefix, None) != XHTML_NS:
            errors.append(BadNamespace(name, root.nsmap.get(root.prefix, None)))

    return errors


class CSSError(BaseError):

    """
    Report a csserror encountered while processing an ebook format.

    Example:
        Exercise CSSError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    is_parsing_error = True

    def __init__(self: _typing.Self, level: _typing.Any, msg: _typing.Any, name: _typing.Any, line: _typing.Any, col: _typing.Any) -> None:
        """
        Initialize and validate the csserror state.

        Example:
            Exercise CSSError.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param level: Value supplied for level under the utility contract.
        :param msg: Value supplied for msg under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param line: Value supplied for line under the utility contract.
        :param col: Value supplied for col under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.level = level
        prefix = "CSS: "
        BaseError.__init__(self, prefix + msg, name, line, col)
        if level == WARN:
            self.HELP = _(
                "This CSS construct is not recognized. That means that it"
                " most likely will not work on reader devices. Consider"
                " replacing it with something else."
            )
        else:
            self.HELP = _(
                "Some reader programs are very"
                " finicky about CSS stylesheets and will ignore the whole"
                " sheet if there is an error. These errors can often"
                " be fixed automatically, however, automatic fixing will"
                " typically remove unrecognized items, instead of correcting them."
            )
            self.INDIVIDUAL_FIX = _("Try to fix parsing errors in this stylesheet automatically")

    def __call__(self: _typing.Self, container: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise CSSError.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        root = container.parsed(self.name)
        container.dirty(self.name)
        if container.mime_map[self.name] in OEB_DOCS:
            for style in root.xpath('//*[local-name()="style"]'):
                if style.get("type", "text/css") == "text/css" and style.text and style.text.strip():
                    fix_style_tag(container, style)
            for elem in root.xpath("//*[@style]"):
                raw = elem.get("style")
                if raw:
                    elem.set(
                        "style",
                        force_unicode(
                            container.parse_css(raw, is_declaration=True).cssText,
                            "utf-8",
                        ).replace("\n", " "),
                    )
        return True


pos_pats = (re.compile(r"\[(\d+):(\d+)"), re.compile(r"(\d+), (\d+)\)"))


class DuplicateId(BaseError):

    """
    Provide the duplicateid contract for validated ebook processing.

    Example:
        Exercise DuplicateId through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    has_multiple_locations = True

    INDIVIDUAL_FIX = _("Remove the duplicate ids from all but the first element")

    def __init__(self: _typing.Self, name: _typing.Any, eid: _typing.Any, locs: _typing.Any) -> None:
        """
        Initialize and validate the duplicateid state.

        Example:
            Exercise DuplicateId.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param eid: Value supplied for eid under the utility contract.
        :param locs: Value supplied for locs under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseError.__init__(self, _("Duplicate id: %s") % eid, name)
        self.HELP = _(
            "The id {0} is present on more than one element in {1}. This is"
            " not allowed. Remove the id from all but one of the elements"
        ).format(eid, name)
        norm_locs = sorted({lnum for lnum in locs if isinstance(lnum, int) and lnum > 0}) or [1]
        self.all_locations = [(name, lnum, None) for lnum in norm_locs]
        self.duplicate_id = eid

    def __call__(self: _typing.Self, container: _typing.Any) -> bool:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise DuplicateId.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elems = [e for e in container.parsed(self.name).xpath("//*[@id]") if e.get("id") == self.duplicate_id]
        for e in elems[1:]:
            e.attrib.pop("id")
        container.dirty(self.name)
        return True


class ErrorHandler(object):

    """
    Replacement logger to get useful error/warning info out of cssutils during parsing

    Example:
        Exercise ErrorHandler through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """

    def __init__(self: _typing.Self, name: _typing.Any) -> None:
        # may be disabled during setting of known valid items
        """
        Initialize and validate the errorhandler state.

        Example:
            Exercise ErrorHandler.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.errors = []

    def __noop(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Perform the noop operation under explicit file-format and conversion rules.

        Example:
            Exercise ErrorHandler.  noop through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    info = debug = setLevel = getEffectiveLevel = addHandler = removeHandler = __noop

    def __handle(self: _typing.Self, level: _typing.Any, *args: _typing.Any) -> None:
        """
        Perform the handle operation under explicit file-format and conversion rules.

        Example:
            Exercise ErrorHandler.  handle through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param level: Value supplied for level under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        msg = " ".join(map(six_unicode, args))
        line = col = None
        for pat in pos_pats:
            m = pat.search(msg)
            if m is not None:
                line, col = int(m.group(1)), int(m.group(2))
        if msg and line is not None:
            # Ignore error messages with no line numbers as these are usually
            # summary messages for an underlying error with a line number
            if "panose-1" in msg and "unknown property name" in msg.lower():
                return  # panose-1 is allowed in CSS 2.1 and is generated by calibre
            self.errors.append(CSSError(level, msg, self.name, line, col))

    def error(self: _typing.Self, *args: _typing.Any) -> None:
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise ErrorHandler.error through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__handle(ERROR, *args)

    def warn(self: _typing.Self, *args: _typing.Any) -> None:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise ErrorHandler.warn through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__handle(WARN, *args)

    warning = warn


def check_css_parsing(name: _typing.Any, raw: _typing.Any, line_offset: int = 0, is_declaration: bool = False) -> _typing.Any:
    """
    Perform the check css parsing operation under explicit file-format and conversion rules.

    Example:
        Exercise check css parsing through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param raw: Value supplied for raw under the utility contract.
    :param line_offset: Value supplied for line offset under the utility contract.
    :param is_declaration: Value supplied for is declaration under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if cssutils is None:
        return []
    log = ErrorHandler(name)
    parser = cssutils.CSSParser(fetcher=lambda x: (None, None), log=log)
    if is_declaration:
        parser.parseStyle(raw, validate=True)
    else:
        try:
            parser.parseString(raw, validate=True)
        except UnicodeDecodeError:
            return [DecodeError(name)]
    for err in log.errors:
        err.line += line_offset
    return log.errors


def check_filenames(container: _typing.Any) -> _typing.Any:
    """
    Perform the check filenames operation under explicit file-format and conversion rules.

    Example:
        Exercise check filenames through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    errors = []
    all_names = set(container.name_path_map) - container.names_that_must_not_be_changed
    for name in all_names:
        if urlquote(name) != name:
            errors.append(EscapedName(name))
    return errors


def check_ids(container: _typing.Any) -> _typing.Any:
    """
    Perform the check ids operation under explicit file-format and conversion rules.

    Example:
        Exercise check ids through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    errors = []
    mts = set(OEB_DOCS) | {guess_type("a.opf"), guess_type("a.ncx")}
    for name, mt in iteritems(container.mime_map):
        if mt in mts:
            root = container.parsed(name)
            seen_ids = {}
            dups = {}
            for elem in root.xpath("//*[@id]"):
                eid = elem.get("id")
                lnum = getattr(elem, "sourceline", None)
                if not isinstance(lnum, int) or lnum < 1:
                    lnum = 1
                if eid in seen_ids:
                    if eid not in dups:
                        dups[eid] = [seen_ids[eid]]
                    dups[eid].append(lnum)
                else:
                    seen_ids[eid] = lnum
            errors.extend(DuplicateId(name, eid, locs) for eid, locs in iteritems(dups))
    return errors
