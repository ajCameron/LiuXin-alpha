"""
Tokenize and dispatch SGML-like markup through the retained parser interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise sgmllib through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""

# SGML - Standard Generalized Markup Language - an alternative to XML
# XXX This only supports those SGML features used by HTML.

# XXX There should be a way to distinguish between PCDATA (parsed
# character data -- the normal case), RCDATA (replaceable character
# data -- only char and entity references and end tags are special)
# and CDATA (character data -- only end tags are special).  RCDATA is
# not supported at all.

from __future__ import print_function

import _markupbase as markupbase
import re

__all__ = ["SGMLParser", "SGMLParseError"]

# Regular expressions used for parsing

interesting = re.compile("[&<]")
incomplete = re.compile("&([a-zA-Z][a-zA-Z0-9]*|#[0-9]*)?|" "<([a-zA-Z][^<>]*|" "/([a-zA-Z][^<>]*)?|" "![^<>]*)?")

entityref = re.compile("&([a-zA-Z][-.a-zA-Z0-9]*)[^a-zA-Z0-9]")
charref = re.compile("&#(x{0,1}[a-f0-9]+)[^a-f0-9]", re.IGNORECASE)  # Changed by Kovid to handle hex numeric entities

starttagopen = re.compile("<[>a-zA-Z]")
shorttagopen = re.compile("<[a-zA-Z][-.a-zA-Z0-9]*/")
shorttag = re.compile("<([a-zA-Z][-.a-zA-Z0-9]*)/([^/]*)/")
piclose = re.compile(">")
endbracket = re.compile("[<>]")
tagfind = re.compile("[a-zA-Z][-_.a-zA-Z0-9]*")
attrfind = re.compile(
    r"\s*([a-zA-Z_][-:.a-zA-Z_0-9]*)(\s*=\s*" r'(\'[^\']*\'|"[^"]*"|[][\-a-zA-Z0-9./,:;+*%?!&$\(\)_#=~\'"@]*))?'
)


class SGMLParseError(RuntimeError):
    """
    Exception raised for all parse errors.

    Example:
        Exercise SGMLParseError through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    pass


# SGML parser base class -- find tags and call handler functions.
# Usage: p = SGMLParser(); p.feed(data); ...; p.close().
# The dtd is defined by deriving a class which defines methods
# with special names to handle tags: start_foo and end_foo to handle
# <foo> and </foo>, respectively, or do_foo to handle <foo> by itself.
# (Tags are converted to lower case for this purpose.)  The data
# between tags is passed to the parser by calling self.handle_data()
# with some data as argument (the data may be split up in arbitrary
# chunks).  Entity references are passed by calling
# self.handle_entityref() with the entity reference as argument.


class SGMLParser(markupbase.ParserBase):
    # Definition of entities -- derived classes may override
    """
    Parse or evaluate SGMLParser expressions under Calibre template semantics.

    Example:
        Exercise SGMLParser through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    entity_or_charref = re.compile("&(?:" "([a-zA-Z][-.a-zA-Z0-9]*)|#([0-9]+)" ")(;?)")

    def __init__(self, verbose=0):
        """
        Initialize and reset this instance.

        Example:
            Exercise SGMLParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param verbose: Value supplied for verbose under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.verbose = verbose
        self.reset()

    def reset(self):
        """
        Reset this instance. Loses all unprocessed data.

        Example:
            Exercise SGMLParser.reset through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.__starttag_text = None
        self.rawdata = ""
        self.stack = []
        self.lasttag = "???"
        self.nomoretags = 0
        self.literal = 0
        markupbase.ParserBase.reset(self)

    def setnomoretags(self):
        """
        Enter literal mode (CDATA) till EOF.

        Example:
            Exercise SGMLParser.setnomoretags through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.nomoretags = self.literal = 1

    def setliteral(self, *args):
        """
        Enter literal mode (CDATA).

        Example:
            Exercise SGMLParser.setliteral through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.literal = 1

    def feed(self, data):
        """
        Feed some data to the parser.

        Example:
            Exercise SGMLParser.feed through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self.rawdata = self.rawdata + data
        self.goahead(0)

    def close(self):
        """
        Handle the remaining data.

        Example:
            Exercise SGMLParser.close through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.goahead(1)

    def error(self, message):
        """
        Perform the error utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.error through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise SGMLParseError(message)

    # Internal -- handle data as far as reasonable.  May leave state
    # and data to be processed by a subsequent call.  If 'end' is
    # true, force handling all data as if followed by EOF marker.
    def goahead(self, end):
        """
        Perform the goahead utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.goahead through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param end: Value supplied for end under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rawdata = self.rawdata
        i = 0
        n = len(rawdata)
        while i < n:
            if self.nomoretags:
                self.handle_data(rawdata[i:n])
                i = n
                break
            match = interesting.search(rawdata, i)
            if match:
                j = match.start()
            else:
                j = n
            if i < j:
                self.handle_data(rawdata[i:j])
            i = j
            if i == n:
                break
            if rawdata[i] == "<":
                if starttagopen.match(rawdata, i):
                    if self.literal:
                        self.handle_data(rawdata[i])
                        i = i + 1
                        continue
                    k = self.parse_starttag(i)
                    if k < 0:
                        break
                    i = k
                    continue
                if rawdata.startswith("</", i):
                    k = self.parse_endtag(i)
                    if k < 0:
                        break
                    i = k
                    self.literal = 0
                    continue
                if self.literal:
                    if n > (i + 1):
                        self.handle_data("<")
                        i = i + 1
                    else:
                        # incomplete
                        break
                    continue
                if rawdata.startswith("<!--", i):
                    # Strictly speaking, a comment is --.*--
                    # within a declaration tag <!...>.
                    # This should be removed,
                    # and comments handled only in parse_declaration.
                    k = self.parse_comment(i)
                    if k < 0:
                        break
                    i = k
                    continue
                if rawdata.startswith("<?", i):
                    k = self.parse_pi(i)
                    if k < 0:
                        break
                    i = i + k
                    continue
                if rawdata.startswith("<!", i):
                    # This is some sort of declaration; in "HTML as
                    # deployed," this should only be the document type
                    # declaration ("<!DOCTYPE html...>").
                    k = self.parse_declaration(i)
                    if k < 0:
                        break
                    i = k
                    continue
            elif rawdata[i] == "&":
                if self.literal:
                    self.handle_data(rawdata[i])
                    i = i + 1
                    continue
                match = charref.match(rawdata, i)
                if match:
                    name = match.group(1)
                    self.handle_charref(name)
                    i = match.end(0)
                    if rawdata[i - 1] != ";":
                        i = i - 1
                    continue
                match = entityref.match(rawdata, i)
                if match:
                    name = match.group(1)
                    self.handle_entityref(name)
                    i = match.end(0)
                    if rawdata[i - 1] != ";":
                        i = i - 1
                    continue
            else:
                self.error("neither < nor & ??")
            # We get here only if incomplete matches but
            # nothing else
            match = incomplete.match(rawdata, i)
            if not match:
                self.handle_data(rawdata[i])
                i = i + 1
                continue
            j = match.end(0)
            if j == n:
                break  # Really incomplete
            self.handle_data(rawdata[i:j])
            i = j
        # end while
        if end and i < n:
            self.handle_data(rawdata[i:n])
            i = n
        self.rawdata = rawdata[i:]
        # XXX if end: check for empty stack

    # Extensions for the DOCTYPE scanner:
    _decl_otherchars = "="

    # Internal -- parse processing instr, return length or -1 if not terminated
    def parse_pi(self, i):
        """
        Parse pi under the documented compatibility and safety rules.

        Example:
            Exercise SGMLParser.parse pi through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param i: Value supplied for i under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rawdata = self.rawdata
        if rawdata[i : i + 2] != "<?":
            self.error("unexpected call to parse_pi()")
        match = piclose.search(rawdata, i + 2)
        if not match:
            return -1
        j = match.start(0)
        self.handle_pi(rawdata[i + 2 : j])
        j = match.end(0)
        return j - i

    def get_starttag_text(self):
        """
        Return starttag text under the documented compatibility and safety rules.

        Example:
            Exercise SGMLParser.get starttag text through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__starttag_text

    # Internal -- handle starttag, return length or -1 if not terminated
    def parse_starttag(self, i):
        """
        Parse starttag under the documented compatibility and safety rules.

        Example:
            Exercise SGMLParser.parse starttag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param i: Value supplied for i under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.__starttag_text = None
        start_pos = i
        rawdata = self.rawdata
        if shorttagopen.match(rawdata, i):
            # SGML shorthand: <tag/data/ == <tag>data</tag>
            # XXX Can data contain &... (entity or char refs)?
            # XXX Can data contain < or > (tag characters)?
            # XXX Can there be whitespace before the first /?
            match = shorttag.match(rawdata, i)
            if not match:
                return -1
            tag, data = match.group(1, 2)
            self.__starttag_text = "<%s/" % tag
            tag = tag.lower()
            k = match.end(0)
            self.finish_shorttag(tag, data)
            self.__starttag_text = rawdata[start_pos : match.end(1) + 1]
            return k
        # XXX The following should skip matching quotes (' or ")
        # As a shortcut way to exit, this isn't so bad, but shouldn't
        # be used to locate the actual end of the start tag since the
        # < or > characters may be embedded in an attribute value.
        match = endbracket.search(rawdata, i + 1)
        if not match:
            return -1
        j = match.start(0)
        # Now parse the data between i+1 and j into a tag and attrs
        attrs = []
        if rawdata[i : i + 2] == "<>":
            # SGML shorthand: <> == <last open tag seen>
            k = j
            tag = self.lasttag
        else:
            match = tagfind.match(rawdata, i + 1)
            if not match:
                self.error("unexpected call to parse_starttag")
            k = match.end(0)
            tag = rawdata[i + 1 : k].lower()
            self.lasttag = tag
        while k < j:
            match = attrfind.match(rawdata, k)
            if not match:
                break
            attrname, rest, attrvalue = match.group(1, 2, 3)
            if not rest:
                attrvalue = attrname
            else:
                if attrvalue[:1] == "'" == attrvalue[-1:] or attrvalue[:1] == '"' == attrvalue[-1:]:
                    # strip quotes
                    attrvalue = attrvalue[1:-1]
                attrvalue = self.entity_or_charref.sub(self._convert_ref, attrvalue)
            attrs.append((attrname.lower(), attrvalue))
            k = match.end(0)
        if rawdata[j] == ">":
            j = j + 1
        self.__starttag_text = rawdata[start_pos:j]
        self.finish_starttag(tag, attrs)
        return j

    # Internal -- convert entity or character reference
    def _convert_ref(self, match):
        """
        Perform the convert ref utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser. convert ref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param match: Value supplied for match under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if match.group(2):
            return self.convert_charref(match.group(2)) or "&#%s%s" % match.groups()[1:]
        elif match.group(3):
            return self.convert_entityref(match.group(1)) or "&%s;" % match.group(1)
        else:
            return "&%s" % match.group(1)

    # Internal -- parse endtag
    def parse_endtag(self, i):
        """
        Parse endtag under the documented compatibility and safety rules.

        Example:
            Exercise SGMLParser.parse endtag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param i: Value supplied for i under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rawdata = self.rawdata
        match = endbracket.search(rawdata, i + 1)
        if not match:
            return -1
        j = match.start(0)
        tag = rawdata[i + 2 : j].strip().lower()
        if rawdata[j] == ">":
            j = j + 1
        self.finish_endtag(tag)
        return j

    # Internal -- finish parsing of <tag/data/ (same as <tag>data</tag>)
    def finish_shorttag(self, tag, data):
        """
        Perform the finish shorttag utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.finish shorttag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.finish_starttag(tag, [])
        self.handle_data(data)
        self.finish_endtag(tag)

    # Internal -- finish processing of start tag
    # Return -1 for unknown tag, 0 for open-only tag, 1 for balanced tag
    def finish_starttag(self, tag, attrs):
        """
        Perform the finish starttag utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.finish starttag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            method = getattr(self, "start_" + tag)
        except AttributeError:
            try:
                method = getattr(self, "do_" + tag)
            except AttributeError:
                self.unknown_starttag(tag, attrs)
                return -1
            else:
                self.handle_starttag(tag, method, attrs)
                return 0
        else:
            self.stack.append(tag)
            self.handle_starttag(tag, method, attrs)
            return 1

    # Internal -- finish processing of end tag
    def finish_endtag(self, tag):
        """
        Perform the finish endtag utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.finish endtag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not tag:
            found = len(self.stack) - 1
            if found < 0:
                self.unknown_endtag(tag)
                return
        else:
            if tag not in self.stack:
                try:
                    method = getattr(self, "end_" + tag)
                except (AttributeError, ValueError):
                    self.unknown_endtag(tag)
                else:
                    self.report_unbalanced(tag)
                return
            found = len(self.stack)
            for i in range(found):
                if self.stack[i] == tag:
                    found = i
        while len(self.stack) > found:
            tag = self.stack[-1]
            try:
                method = getattr(self, "end_" + tag)
            except AttributeError:
                method = None
            if method:
                self.handle_endtag(tag, method)
            else:
                self.unknown_endtag(tag)
            del self.stack[-1]

    # Overridable -- handle start tag
    def handle_starttag(self, tag, method, attrs):
        """
        Perform the handle starttag utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.handle starttag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param method: Value supplied for method under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        method(attrs)

    # Overridable -- handle end tag
    def handle_endtag(self, tag, method):
        """
        Perform the handle endtag utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.handle endtag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param method: Value supplied for method under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        method()

    # Example -- report an unbalanced </...> tag.
    def report_unbalanced(self, tag):
        """
        Perform the report unbalanced utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.report unbalanced through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.verbose:
            print("*** Unbalanced </" + tag + ">")
            print("*** Stack:", self.stack)

    def convert_charref(self, name):
        """
        Convert character reference, may be overridden.

        Example:
            Exercise SGMLParser.convert charref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            n = int(name)
        except ValueError:
            return
        if not 0 <= n <= 255:
            return
        return self.convert_codepoint(n)

    def convert_codepoint(self, codepoint):
        """
        Perform the convert codepoint utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.convert codepoint through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param codepoint: Value supplied for codepoint under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return unichr(codepoint)

    def handle_charref(self, name):
        """
        Handle character reference, no need to override.

        Example:
            Exercise SGMLParser.handle charref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        replacement = self.convert_charref(name)
        if replacement is None:
            self.unknown_charref(name)
        else:
            self.handle_data(replacement)

    # Definition of entities -- derived classes may override
    entitydefs = {"lt": "<", "gt": ">", "amp": "&", "quot": '"', "apos": "'"}

    def convert_entityref(self, name):
        """
        Convert entity references.

        Example:
            Exercise SGMLParser.convert entityref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        table = self.entitydefs
        if name in table:
            return table[name]
        else:
            return

    def handle_entityref(self, name):
        """
        Handle entity references, no need to override.

        Example:
            Exercise SGMLParser.handle entityref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        replacement = self.convert_entityref(name)
        if replacement is None:
            self.unknown_entityref(name)
        else:
            self.handle_data(self.convert_entityref(name))

    # Example -- handle data, should be overridden
    def handle_data(self, data):
        """
        Perform the handle data utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.handle data through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    # Example -- handle comment, could be overridden
    def handle_comment(self, data):
        """
        Perform the handle comment utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.handle comment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    # Example -- handle declaration, could be overridden
    def handle_decl(self, decl):
        """
        Perform the handle decl utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.handle decl through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param decl: Value supplied for decl under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    # Example -- handle processing instruction, could be overridden
    def handle_pi(self, data):
        """
        Perform the handle pi utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.handle pi through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    # To be overridden -- handlers for unknown objects
    def unknown_starttag(self, tag, attrs):
        """
        Perform the unknown starttag utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.unknown starttag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def unknown_endtag(self, tag):
        """
        Perform the unknown endtag utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.unknown endtag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def unknown_charref(self, ref):
        """
        Perform the unknown charref utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.unknown charref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param ref: Value supplied for ref under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def unknown_entityref(self, ref):
        """
        Perform the unknown entityref utility operation under explicit compatibility rules.

        Example:
            Exercise SGMLParser.unknown entityref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param ref: Value supplied for ref under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


class TestSGMLParser(SGMLParser):
    """
    Parse or evaluate TestSGMLParser expressions under Calibre template semantics.

    Example:
        Exercise TestSGMLParser through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, verbose=0):
        """
        Initialize and validate the TestSGMLParser state.

        Example:
            Exercise TestSGMLParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param verbose: Value supplied for verbose under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.testdata = ""
        SGMLParser.__init__(self, verbose)

    def handle_data(self, data):
        """
        Perform the handle data utility operation under explicit compatibility rules.

        Example:
            Exercise TestSGMLParser.handle data through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.testdata = self.testdata + data
        if len(repr(self.testdata)) >= 70:
            self.flush()

    def flush(self):
        """
        Forward the flush operation while preserving adapter ownership rules.

        Example:
            Exercise TestSGMLParser.flush through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        data = self.testdata
        if data:
            self.testdata = ""
            print("data:", repr(data))

    def handle_comment(self, data):
        """
        Perform the handle comment utility operation under explicit compatibility rules.

        Example:
            Exercise TestSGMLParser.handle comment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.flush()
        r = repr(data)
        if len(r) > 68:
            r = r[:32] + "..." + r[-32:]
        print("comment:", r)

    def unknown_starttag(self, tag, attrs):
        """
        Perform the unknown starttag utility operation under explicit compatibility rules.

        Example:
            Exercise TestSGMLParser.unknown starttag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.flush()
        if not attrs:
            print("start tag: <" + tag + ">")
        else:
            print(
                "start tag: <" + tag,
            )
            for name, value in attrs:
                print(
                    name + "=" + '"' + value + '"',
                )
            print(">")

    def unknown_endtag(self, tag):
        """
        Perform the unknown endtag utility operation under explicit compatibility rules.

        Example:
            Exercise TestSGMLParser.unknown endtag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tag: Value supplied for tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.flush()
        print("end tag: </" + tag + ">")

    def unknown_entityref(self, ref):
        """
        Perform the unknown entityref utility operation under explicit compatibility rules.

        Example:
            Exercise TestSGMLParser.unknown entityref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param ref: Value supplied for ref under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.flush()
        print("*** unknown entity ref: &" + ref + ";")

    def unknown_charref(self, ref):
        """
        Perform the unknown charref utility operation under explicit compatibility rules.

        Example:
            Exercise TestSGMLParser.unknown charref through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param ref: Value supplied for ref under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.flush()
        print("*** unknown char ref: &#" + ref + ";")

    def unknown_decl(self, data):
        """
        Perform the unknown decl utility operation under explicit compatibility rules.

        Example:
            Exercise TestSGMLParser.unknown decl through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.flush()
        print("*** unknown decl: [" + data + "]")

    def close(self):
        """
        Forward the close operation while preserving adapter ownership rules.

        Example:
            Exercise TestSGMLParser.close through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        SGMLParser.close(self)
        self.flush()


def test(args=None):
    """
    Perform the test utility operation under explicit compatibility rules.

    Example:
        Exercise test through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param args: Positional values forwarded to the compatibility implementation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import sys

    if args is None:
        args = sys.argv[1:]

    if args and args[0] == "-s":
        args = args[1:]
        klass = SGMLParser
    else:
        klass = TestSGMLParser

    if args:
        file = args[0]
    else:
        file = "test.html"

    if file == "-":
        f = sys.stdin
    else:
        try:
            f = open(file, "r")
        except IOError as msg:
            print(file, ":", msg)
            sys.exit(1)

    data = f.read()
    if f is not sys.stdin:
        f.close()

    x = klass()
    for c in data:
        x.feed(c)
    x.close()


if __name__ == "__main__":
    test()
