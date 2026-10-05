"""
Parse HTML5 token streams into normalized document trees with specification-compatible recovery.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise html5parser through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

import types
from collections import OrderedDict

from LiuXin_alpha.utils.libraries.liuxin_html5lib import inputstream
from LiuXin_alpha.utils.libraries.liuxin_html5lib import tokenizer

from LiuXin_alpha.utils.libraries.liuxin_html5lib import treebuilders
from LiuXin_alpha.utils.libraries.liuxin_html5lib.treebuilders._base import Marker

from LiuXin_alpha.utils.libraries.liuxin_six import unicode

from LiuXin_alpha.utils.libraries.liuxin_html5lib import utils
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import (
    spaceCharacters,
    asciiUpper2Lower,
    specialElements,
    headingElements,
    E,
    cdataElements,
    rcdataElements,
    tokenTypes,
    tagTokenTypes,
    ReparseException,
    namespaces,
    htmlIntegrationPointElements,
    mathmlTextIntegrationPointElements,
    adjustForeignAttributes as adjustForeignAttributesMap,
    adjustSVGAttributes,
    adjustMathMLAttributes,
)

def with_metaclass(meta, *bases):
    """
    Create a base class with a metaclass.

    Example:
        Exercise with metaclass through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param meta: Value supplied for meta under the utility contract.
    :param bases: Value supplied for bases under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return meta("NewBase", bases, {})


def parse(doc, treebuilder="etree", encoding=None, namespaceHTMLElements=True):
    """
    Parse a string or file-like object into a tree

    Example:
        Exercise parse through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param doc: Value supplied for doc under the utility contract.
    :param treebuilder: Value supplied for treebuilder under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param namespaceHTMLElements: Value supplied for namespaceHTMLElements under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tb = treebuilders.getTreeBuilder(treebuilder)
    p = HTMLParser(tb, namespaceHTMLElements=namespaceHTMLElements)
    return p.parse(doc, encoding=encoding)


def parseFragment(doc, container="div", treebuilder="etree", encoding=None, namespaceHTMLElements=True):
    """
    Perform the parseFragment utility operation under explicit compatibility rules.

    Example:
        Exercise parseFragment through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param doc: Value supplied for doc under the utility contract.
    :param container: Value supplied for container under the utility contract.
    :param treebuilder: Value supplied for treebuilder under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param namespaceHTMLElements: Value supplied for namespaceHTMLElements under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tb = treebuilders.getTreeBuilder(treebuilder)
    p = HTMLParser(tb, namespaceHTMLElements=namespaceHTMLElements)
    return p.parseFragment(doc, container=container, encoding=encoding)


def method_decorator_metaclass(function):
    """
    Perform the method decorator metaclass utility operation under explicit compatibility rules.

    Example:
        Exercise method decorator metaclass through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param function: Value supplied for function under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class Decorated(type):
        """
        Provide the Decorated utility contract with explicit state and cleanup behavior.

        Example:
            Exercise method decorator metaclass.Decorated through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __new__(meta, classname, bases, classDict):
            """
            Perform the new utility operation under explicit compatibility rules.

            Example:
                Exercise method decorator metaclass.Decorated.  new   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param classname: Value supplied for classname under the utility contract.
            :param bases: Value supplied for bases under the utility contract.
            :param classDict: Value supplied for classDict under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            for attributeName, attribute in classDict.items():
                if isinstance(attribute, types.FunctionType):
                    attribute = function(attribute)

                classDict[attributeName] = attribute
            return type.__new__(meta, classname, bases, classDict)

    return Decorated


class HTMLParser(object):

    """
    HTML parser. Generates a tree structure from a stream of (possibly malformed) HTML

    Example:
        Exercise HTMLParser through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    def __init__(
        self,
        tree=None,
        tokenizer=tokenizer.HTMLTokenizer,
        strict=False,
        namespaceHTMLElements=True,
        debug=False,
        track_positions=False,
    ):
        """
        strict - raise an exception when a parse error is encountered

        Example:
            Exercise HTMLParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tree: Value supplied for tree under the utility contract.
        :param tokenizer: Value supplied for tokenizer under the utility contract.
        :param strict: Value supplied for strict under the utility contract.
        :param namespaceHTMLElements: Value supplied for namespaceHTMLElements under the
            utility contract.
        :param debug: Value supplied for debug under the utility contract.
        :param track_positions: Value supplied for track positions under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """

        # Raise an exception on the first error encountered
        self.strict = strict
        self.track_positions = track_positions

        if tree is None:
            tree = treebuilders.getTreeBuilder("etree")
        self.tree = tree(namespaceHTMLElements)
        self.tokenizer_class = tokenizer
        self.errors = []

        self.phases = dict([(name, cls(self, self.tree)) for name, cls in getPhases(debug).items()])

    def _parse(
        self, stream, innerHTML=False, container="div", encoding=None, parseMeta=True, useChardet=True, **kwargs
    ):

        """
        Perform the parse utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser. parse through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param innerHTML: Value supplied for innerHTML under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param parseMeta: Value supplied for parseMeta under the utility contract.
        :param useChardet: Value supplied for useChardet under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.innerHTMLMode = innerHTML
        self.container = container
        self.tokenizer = self.tokenizer_class(
            stream,
            encoding=encoding,
            parseMeta=parseMeta,
            useChardet=useChardet,
            track_positions=self.track_positions,
            parser=self,
            **kwargs
        )
        self.reset()

        while True:
            try:
                self.mainLoop()
                break
            except ReparseException:
                self.reset()

    def reset(self):
        """
        Perform the reset utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.reset through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.tree.reset()
        self.firstStartTag = False
        self.errors = []
        self.log = []  # only used with debug mode
        # "quirks" / "limited quirks" / "no quirks"
        self.compatMode = "no quirks"

        if self.innerHTMLMode:
            self.innerHTML = self.container.lower()

            if self.innerHTML in cdataElements:
                self.tokenizer.state = self.tokenizer.rcdataState
            elif self.innerHTML in rcdataElements:
                self.tokenizer.state = self.tokenizer.rawtextState
            elif self.innerHTML == "plaintext":
                self.tokenizer.state = self.tokenizer.plaintextState
            else:
                # state already is data state
                # self.tokenizer.state = self.tokenizer.dataState
                pass
            self.phase = self.phases["beforeHtml"]
            self.phase.insertHtmlElement()
            self.resetInsertionMode()
        else:
            self.innerHTML = False
            self.phase = self.phases["initial"]

        self.lastPhase = None

        self.beforeRCDataPhase = None

        self.framesetOK = True

    @property
    def documentEncoding(self):
        """
        The name of the character encoding that was used to decode the input stream, or :obj:`None` if that is not determined yet.

        Example:
            Exercise HTMLParser.documentEncoding through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not hasattr(self, "tokenizer"):
            return None
        return self.tokenizer.stream.charEncoding[0]

    def isHTMLIntegrationPoint(self, element):
        """
        Perform the isHTMLIntegrationPoint utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.isHTMLIntegrationPoint through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if element.name == "annotation-xml" and element.namespace == namespaces["mathml"]:
            try:
                return "encoding" in element.attributes and element.attributes["encoding"].translate(
                    asciiUpper2Lower
                ) in ("text/html", "application/xhtml+xml")
            except TypeError:
                # This happens for some documents, for some reason
                # lxml refuses to store a unicode representation of the
                # encoding attribute.
                return element.attributes["encoding"].lower().decode("utf-8", "replace") in (
                    "text/html",
                    "application/xhtml+xml",
                )
        else:
            return (element.namespace, element.name) in htmlIntegrationPointElements

    def isMathMLTextIntegrationPoint(self, element):
        """
        Perform the isMathMLTextIntegrationPoint utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.isMathMLTextIntegrationPoint through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param element: Value supplied for element under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return (element.namespace, element.name) in mathmlTextIntegrationPointElements

    def mainLoop(self):
        """
        Perform the mainLoop utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.mainLoop through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        CharactersToken = tokenTypes["Characters"]
        SpaceCharactersToken = tokenTypes["SpaceCharacters"]
        StartTagToken = tokenTypes["StartTag"]
        EndTagToken = tokenTypes["EndTag"]
        CommentToken = tokenTypes["Comment"]
        DoctypeToken = tokenTypes["Doctype"]
        ParseErrorToken = tokenTypes["ParseError"]

        for token in self.normalizedTokens():
            new_token = token
            while new_token is not None:
                currentNode = self.tree.openElements[-1] if self.tree.openElements else None
                currentNodeNamespace = currentNode.namespace if currentNode is not None else None
                currentNodeName = currentNode.name if currentNode is not None else None

                type = new_token["type"]

                if type == ParseErrorToken:
                    self.parseError(new_token["data"], new_token.get("datavars", {}))
                    new_token = None
                else:
                    if (
                        len(self.tree.openElements) == 0
                        or currentNodeNamespace == self.tree.defaultNamespace
                        or (
                            self.isMathMLTextIntegrationPoint(currentNode)
                            and (
                                (type == StartTagToken and token["name"] not in frozenset(["mglyph", "malignmark"]))
                                or type in (CharactersToken, SpaceCharactersToken)
                            )
                        )
                        or (
                            currentNodeNamespace == namespaces["mathml"]
                            and currentNodeName == "annotation-xml"
                            and token["name"] == "svg"
                        )
                        or (
                            self.isHTMLIntegrationPoint(currentNode)
                            and type in (StartTagToken, CharactersToken, SpaceCharactersToken)
                        )
                    ):
                        phase = self.phase
                    else:
                        phase = self.phases["inForeignContent"]

                    if type == CharactersToken:
                        new_token = phase.processCharacters(new_token)
                    elif type == SpaceCharactersToken:
                        new_token = phase.processSpaceCharacters(new_token)
                    elif type == StartTagToken:
                        new_token = phase.processStartTag(new_token)
                    elif type == EndTagToken:
                        new_token = phase.processEndTag(new_token)
                    elif type == CommentToken:
                        new_token = phase.processComment(new_token)
                    elif type == DoctypeToken:
                        new_token = phase.processDoctype(new_token)

            if type == StartTagToken and token["selfClosing"] and not token["selfClosingAcknowledged"]:
                self.parseError("non-void-element-with-trailing-solidus", {"name": token["name"]})

        # When the loop finishes it's EOF
        reprocess = True
        phases = []
        while reprocess:
            phases.append(self.phase)
            reprocess = self.phase.processEOF()
            if reprocess:
                assert self.phase not in phases

    def normalizedTokens(self):
        """
        Perform the normalizedTokens utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.normalizedTokens through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for token in self.tokenizer:
            yield self.normalizeToken(token)

    def parse(self, stream, encoding=None, parseMeta=True, useChardet=True):
        """
        Parse a HTML document into a well-formed tree

        Example:
            Exercise HTMLParser.parse through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param encoding: Value supplied for encoding under the utility contract.
        :param parseMeta: Value supplied for parseMeta under the utility contract.
        :param useChardet: Value supplied for useChardet under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._parse(
            stream,
            innerHTML=False,
            encoding=encoding,
            parseMeta=parseMeta,
            useChardet=useChardet,
        )
        return self.tree.getDocument()

    def parseFragment(self, stream, container="div", encoding=None, parseMeta=False, useChardet=True):
        """
        Parse a HTML fragment into a well-formed tree fragment

        Example:
            Exercise HTMLParser.parseFragment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param container: Value supplied for container under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param parseMeta: Value supplied for parseMeta under the utility contract.
        :param useChardet: Value supplied for useChardet under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._parse(stream, True, container=container, encoding=encoding)
        return self.tree.getFragment()

    def parseError(self, errorcode="XXX-undefined-error", datavars={}):
        # XXX The idea is to make errorcode mandatory.
        """
        Perform the parseError utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.parseError through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param errorcode: Value supplied for errorcode under the utility contract.
        :param datavars: Value supplied for datavars under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.errors.append((self.tokenizer.stream.position(), errorcode, datavars))
        if self.strict:
            raise ParseError(E[errorcode] % datavars)

    def normalizeToken(self, token):
        """
        HTML5 specific normalizations to the token stream

        Example:
            Exercise HTMLParser.normalizeToken through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if token["type"] == tokenTypes["StartTag"]:
            token["data"] = OrderedDict(token["data"])

        return token

    def adjustMathMLAttributes(self, token):
        """
        Perform the adjustMathMLAttributes utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.adjustMathMLAttributes through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        adjust_attributes(token, adjustMathMLAttributes)

    def adjustSVGAttributes(self, token):
        """
        Perform the adjustSVGAttributes utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.adjustSVGAttributes through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        adjust_attributes(token, adjustSVGAttributes)

    def adjustForeignAttributes(self, token):
        """
        Perform the adjustForeignAttributes utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.adjustForeignAttributes through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        adjust_attributes(token, adjustForeignAttributesMap)

    def reparseTokenNormal(self, token):
        """
        Perform the reparseTokenNormal utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.reparseTokenNormal through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.parser.phase()

    def resetInsertionMode(self):
        # The name of this method is mostly historical. (It's also used in the
        # specification.)
        """
        Perform the resetInsertionMode utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.resetInsertionMode through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        last = False
        newModes = {
            "select": "inSelect",
            "td": "inCell",
            "th": "inCell",
            "tr": "inRow",
            "tbody": "inTableBody",
            "thead": "inTableBody",
            "tfoot": "inTableBody",
            "caption": "inCaption",
            "colgroup": "inColumnGroup",
            "table": "inTable",
            "head": "inBody",
            "body": "inBody",
            "frameset": "inFrameset",
            "html": "beforeHead",
        }
        for node in self.tree.openElements[::-1]:
            nodeName = node.name
            new_phase = None
            if node == self.tree.openElements[0]:
                assert self.innerHTML
                last = True
                nodeName = self.innerHTML
            # Check for conditions that should only happen in the innerHTML
            # case
            if nodeName in ("select", "colgroup", "head", "html"):
                assert self.innerHTML

            if not last and node.namespace != self.tree.defaultNamespace:
                continue

            if nodeName in newModes:
                new_phase = self.phases[newModes[nodeName]]
                break
            elif last:
                new_phase = self.phases["inBody"]
                break

        self.phase = new_phase

    def parseRCDataRawtext(self, token, contentType):
        """
        Generic RCDATA/RAWTEXT Parsing algorithm contentType - RCDATA or RAWTEXT

        Example:
            Exercise HTMLParser.parseRCDataRawtext through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param token: Value supplied for token under the utility contract.
        :param contentType: Value supplied for contentType under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        assert contentType in ("RAWTEXT", "RCDATA")

        self.tree.insertElement(token)

        if contentType == "RAWTEXT":
            self.tokenizer.state = self.tokenizer.rawtextState
        else:
            self.tokenizer.state = self.tokenizer.rcdataState

        self.originalPhase = self.phase

        self.phase = self.phases["text"]

    def impliedTagToken(self, name, type="EndTag", attributes=None, selfClosing=False):
        """
        Perform the impliedTagToken utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLParser.impliedTagToken through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param type: Value supplied for type under the utility contract.
        :param attributes: Value supplied for attributes under the utility contract.
        :param selfClosing: Value supplied for selfClosing under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if attributes is None:
            attributes = {}
        ans = {
            "type": tokenTypes[type],
            "name": name,
            "data": attributes,
            "selfClosing": selfClosing,
        }
        if self.track_positions:
            ans["position"] = (self.tokenizer.stream.position(), True)
        return ans


def getPhases(debug):
    """
    Perform the getPhases utility operation under explicit compatibility rules.

    Example:
        Exercise getPhases through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param debug: Value supplied for debug under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def log(function):
        """
        Logger that records which phase processes each token

        Example:
            Exercise getPhases.log through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param function: Value supplied for function under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        type_names = dict((value, key) for key, value in tokenTypes.items())

        def wrapped(self, *args, **kwargs):
            """
            Perform the wrapped utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.log.wrapped through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param self: Value supplied for self under the utility contract.
            :param args: Positional values forwarded to the compatibility implementation.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if function.__name__.startswith("process") and len(args) > 0:
                token = args[0]
                try:
                    info = {"type": type_names[token["type"]]}
                except:
                    raise
                if token["type"] in tagTokenTypes:
                    info["name"] = token["name"]

                self.parser.log.append(
                    (
                        self.parser.tokenizer.state.__name__,
                        self.parser.phase.__class__.__name__,
                        self.__class__.__name__,
                        function.__name__,
                        info,
                    )
                )
                return function(self, *args, **kwargs)
            else:
                return function(self, *args, **kwargs)

        return wrapped

    def getMetaclass(use_metaclass, metaclass_func):
        """
        Perform the getMetaclass utility operation under explicit compatibility rules.

        Example:
            Exercise getPhases.getMetaclass through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param use_metaclass: Value supplied for use metaclass under the utility contract.
        :param metaclass_func: Value supplied for metaclass func under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if use_metaclass:
            return method_decorator_metaclass(metaclass_func)
        else:
            return type

    class Phase(with_metaclass(getMetaclass(debug, log))):

        """
        Base class for helper object that implements each phase of processing

        Example:
            Exercise getPhases.Phase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """

        def __init__(self, parser, tree):
            """
            Initialize and validate the Phase state.

            Example:
                Exercise getPhases.Phase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.parser = parser
            self.tree = tree
            self.impliedTagToken = parser.impliedTagToken

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise NotImplementedError

        def processComment(self, token):
            # For most phases the following is correct. Where it's not it will be
            # overridden.
            """
            Perform the processComment utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.processComment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertComment(token, self.tree.openElements[-1])

        def processDoctype(self, token):
            """
            Perform the processDoctype utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.processDoctype through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-doctype")

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertText(token["data"])

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertText(token["data"])

        def processStartTag(self, token):
            """
            Perform the processStartTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.processStartTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.startTagHandler[token["name"]](token)

        def startTagHtml(self, token):
            """
            Perform the startTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.startTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not self.parser.firstStartTag and token["name"] == "html":
                self.parser.parseError("non-html-root")
            # XXX Need a check here to see if the first start tag token emitted is
            # this token... If it's not, invoke self.parser.parseError().
            self.tree.apply_html_attributes(token["data"])
            self.parser.firstStartTag = False

        def processEndTag(self, token):
            """
            Perform the processEndTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.Phase.processEndTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.endTagHandler[token["name"]](token)

    class InitialPhase(Phase):
        """
        Provide the InitialPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InitialPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass

        def processComment(self, token):
            """
            Perform the processComment utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.processComment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertComment(token, self.tree.document)

        def processDoctype(self, token):
            """
            Perform the processDoctype utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.processDoctype through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            name = token["name"]
            publicId = token["publicId"]
            systemId = token["systemId"]
            correct = token["correct"]

            if name != "html" or publicId is not None or systemId is not None and systemId != "about:legacy-compat":
                self.parser.parseError("unknown-doctype")

            if publicId is None:
                publicId = ""

            self.tree.insertDoctype(token)

            if publicId != "":
                publicId = publicId.translate(asciiUpper2Lower)

            if (
                not correct
                or token["name"] != "html"
                or publicId.startswith(
                    (
                        "+//silmaril//dtd html pro v0r11 19970101//",
                        "-//advasoft ltd//dtd html 3.0 aswedit + extensions//",
                        "-//as//dtd html 3.0 aswedit + extensions//",
                        "-//ietf//dtd html 2.0 level 1//",
                        "-//ietf//dtd html 2.0 level 2//",
                        "-//ietf//dtd html 2.0 strict level 1//",
                        "-//ietf//dtd html 2.0 strict level 2//",
                        "-//ietf//dtd html 2.0 strict//",
                        "-//ietf//dtd html 2.0//",
                        "-//ietf//dtd html 2.1e//",
                        "-//ietf//dtd html 3.0//",
                        "-//ietf//dtd html 3.2 final//",
                        "-//ietf//dtd html 3.2//",
                        "-//ietf//dtd html 3//",
                        "-//ietf//dtd html level 0//",
                        "-//ietf//dtd html level 1//",
                        "-//ietf//dtd html level 2//",
                        "-//ietf//dtd html level 3//",
                        "-//ietf//dtd html strict level 0//",
                        "-//ietf//dtd html strict level 1//",
                        "-//ietf//dtd html strict level 2//",
                        "-//ietf//dtd html strict level 3//",
                        "-//ietf//dtd html strict//",
                        "-//ietf//dtd html//",
                        "-//metrius//dtd metrius presentational//",
                        "-//microsoft//dtd internet explorer 2.0 html strict//",
                        "-//microsoft//dtd internet explorer 2.0 html//",
                        "-//microsoft//dtd internet explorer 2.0 tables//",
                        "-//microsoft//dtd internet explorer 3.0 html strict//",
                        "-//microsoft//dtd internet explorer 3.0 html//",
                        "-//microsoft//dtd internet explorer 3.0 tables//",
                        "-//netscape comm. corp.//dtd html//",
                        "-//netscape comm. corp.//dtd strict html//",
                        "-//o'reilly and associates//dtd html 2.0//",
                        "-//o'reilly and associates//dtd html extended 1.0//",
                        "-//o'reilly and associates//dtd html extended relaxed 1.0//",
                        "-//softquad software//dtd hotmetal pro 6.0::19990601::extensions to html 4.0//",
                        "-//softquad//dtd hotmetal pro 4.0::19971010::extensions to html 4.0//",
                        "-//spyglass//dtd html 2.0 extended//",
                        "-//sq//dtd html 2.0 hotmetal + extensions//",
                        "-//sun microsystems corp.//dtd hotjava html//",
                        "-//sun microsystems corp.//dtd hotjava strict html//",
                        "-//w3c//dtd html 3 1995-03-24//",
                        "-//w3c//dtd html 3.2 draft//",
                        "-//w3c//dtd html 3.2 final//",
                        "-//w3c//dtd html 3.2//",
                        "-//w3c//dtd html 3.2s draft//",
                        "-//w3c//dtd html 4.0 frameset//",
                        "-//w3c//dtd html 4.0 transitional//",
                        "-//w3c//dtd html experimental 19960712//",
                        "-//w3c//dtd html experimental 970421//",
                        "-//w3c//dtd w3 html//",
                        "-//w3o//dtd w3 html 3.0//",
                        "-//webtechs//dtd mozilla html 2.0//",
                        "-//webtechs//dtd mozilla html//",
                    )
                )
                or publicId
                in (
                    "-//w3o//dtd w3 html strict 3.0//en//",
                    "-/w3c/dtd html 4.0 transitional/en",
                    "html",
                )
                or publicId.startswith(
                    (
                        "-//w3c//dtd html 4.01 frameset//",
                        "-//w3c//dtd html 4.01 transitional//",
                    )
                )
                and systemId is None
                or systemId
                and systemId.lower() == "http://www.ibm.com/data/dtd/v11/ibmxhtml1-transitional.dtd"
            ):
                self.parser.compatMode = "quirks"
            elif (
                publicId.startswith(
                    (
                        "-//w3c//dtd xhtml 1.0 frameset//",
                        "-//w3c//dtd xhtml 1.0 transitional//",
                    )
                )
                or publicId.startswith(
                    (
                        "-//w3c//dtd html 4.01 frameset//",
                        "-//w3c//dtd html 4.01 transitional//",
                    )
                )
                and systemId is not None
            ):
                self.parser.compatMode = "limited quirks"

            self.parser.phase = self.parser.phases["beforeHtml"]

        def anythingElse(self):
            """
            Perform the anythingElse utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.anythingElse through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.compatMode = "quirks"
            self.parser.phase = self.parser.phases["beforeHtml"]

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("expected-doctype-but-got-chars")
            self.anythingElse()
            return token

        def processStartTag(self, token):
            """
            Perform the processStartTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.processStartTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("expected-doctype-but-got-start-tag", {"name": token["name"]})
            self.anythingElse()
            return token

        def processEndTag(self, token):
            """
            Perform the processEndTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.processEndTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("expected-doctype-but-got-end-tag", {"name": token["name"]})
            self.anythingElse()
            return token

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InitialPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("expected-doctype-but-got-eof")
            self.anythingElse()
            return True

    class BeforeHtmlPhase(Phase):
        # helper methods

        """
        Provide the BeforeHtmlPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.BeforeHtmlPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def insertHtmlElement(self):
            """
            Perform the insertHtmlElement utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHtmlPhase.insertHtmlElement through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertRoot(self.impliedTagToken("html", "StartTag"))
            self.parser.phase = self.parser.phases["beforeHead"]

        # other
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHtmlPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.insertHtmlElement()
            return True

        def processComment(self, token):
            """
            Perform the processComment utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHtmlPhase.processComment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertComment(token, self.tree.document)

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHtmlPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHtmlPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.insertHtmlElement()
            return token

        def processStartTag(self, token):
            """
            Perform the processStartTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHtmlPhase.processStartTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if token["name"] == "html":
                self.parser.firstStartTag = True
            self.insertHtmlElement()
            return token

        def processEndTag(self, token):
            """
            Perform the processEndTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHtmlPhase.processEndTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if token["name"] not in ("head", "body", "html", "br"):
                self.parser.parseError("unexpected-end-tag-before-html", {"name": token["name"]})
            else:
                self.insertHtmlElement()
                return token

    class BeforeHeadPhase(Phase):
        """
        Provide the BeforeHeadPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.BeforeHeadPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the BeforeHeadPhase state.

            Example:
                Exercise getPhases.BeforeHeadPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher([("html", self.startTagHtml), ("head", self.startTagHead)])
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher([(("head", "body", "html", "br"), self.endTagImplyHead)])
            self.endTagHandler.default = self.endTagOther

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.startTagHead(self.impliedTagToken("head", "StartTag"))
            return True

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.startTagHead(self.impliedTagToken("head", "StartTag"))
            return token

        def startTagHtml(self, token):
            """
            Perform the startTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.startTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def startTagHead(self, token):
            """
            Perform the startTagHead utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.startTagHead through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.tree.headPointer = self.tree.openElements[-1]
            self.parser.phase = self.parser.phases["inHead"]

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.startTagHead(self.impliedTagToken("head", "StartTag"))
            return token

        def endTagImplyHead(self, token):
            """
            Perform the endTagImplyHead utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.endTagImplyHead through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.startTagHead(self.impliedTagToken("head", "StartTag"))
            return token

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.BeforeHeadPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("end-tag-after-implied-root", {"name": token["name"]})

    class InHeadPhase(Phase):
        """
        Provide the InHeadPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InHeadPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InHeadPhase state.

            Example:
                Exercise getPhases.InHeadPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    ("title", self.startTagTitle),
                    (
                        ("noscript", "noframes", "style"),
                        self.startTagNoScriptNoFramesStyle,
                    ),
                    ("script", self.startTagScript),
                    (
                        ("base", "basefont", "bgsound", "command", "link"),
                        self.startTagBaseLinkCommand,
                    ),
                    ("meta", self.startTagMeta),
                    ("head", self.startTagHead),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    ("head", self.endTagHead),
                    (("br", "html", "body"), self.endTagHtmlBodyBr),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        # the real thing
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return True

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return token

        def startTagHtml(self, token):
            """
            Perform the startTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def startTagHead(self, token):
            """
            Perform the startTagHead utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagHead through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("two-heads-are-not-better-than-one")

        def startTagBaseLinkCommand(self, token):
            """
            Perform the startTagBaseLinkCommand utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagBaseLinkCommand through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.tree.openElements.pop()
            token["selfClosingAcknowledged"] = True

        def startTagMeta(self, token):
            """
            Perform the startTagMeta utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagMeta through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.tree.openElements.pop()
            token["selfClosingAcknowledged"] = True

            attributes = token["data"]
            if self.parser.tokenizer.stream.charEncoding[1] == "tentative":
                if "charset" in attributes:
                    self.parser.tokenizer.stream.changeEncoding(attributes["charset"])
                elif (
                    "content" in attributes
                    and "http-equiv" in attributes
                    and attributes["http-equiv"].lower() == "content-type"
                ):
                    # Encoding it as UTF-8 here is a hack, as really we should pass
                    # the abstract Unicode string, and just use the
                    # ContentAttrParser on that, but using UTF-8 allows all chars
                    # to be encoded and as a ASCII-superset works.
                    data = inputstream.EncodingBytes(attributes["content"].encode("utf-8"))
                    parser = inputstream.ContentAttrParser(data)
                    codec = parser.parse()
                    self.parser.tokenizer.stream.changeEncoding(codec)

        def startTagTitle(self, token):
            """
            Perform the startTagTitle utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagTitle through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseRCDataRawtext(token, "RCDATA")

        def startTagNoScriptNoFramesStyle(self, token):
            # Need to decide whether to implement the scripting-disabled case
            """
            Perform the startTagNoScriptNoFramesStyle utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagNoScriptNoFramesStyle through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseRCDataRawtext(token, "RAWTEXT")

        def startTagScript(self, token):
            """
            Perform the startTagScript utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagScript through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.parser.tokenizer.state = self.parser.tokenizer.scriptDataState
            self.parser.originalPhase = self.parser.phase
            self.parser.phase = self.parser.phases["text"]

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return token

        def endTagHead(self, token):
            """
            Perform the endTagHead utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.endTagHead through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            node = self.parser.tree.openElements.pop()
            assert node.name == "head", "Expected head got %s" % node.name
            self.parser.phase = self.parser.phases["afterHead"]

        def endTagHtmlBodyBr(self, token):
            """
            Perform the endTagHtmlBodyBr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.endTagHtmlBodyBr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return token

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag", {"name": token["name"]})

        def anythingElse(self):
            """
            Perform the anythingElse utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InHeadPhase.anythingElse through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.endTagHead(self.impliedTagToken("head"))

    # XXX If we implement a parser for which scripting is disabled we need to
    # implement this phase.
    #
    # class InHeadNoScriptPhase(Phase):
    class AfterHeadPhase(Phase):
        """
        Provide the AfterHeadPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.AfterHeadPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the AfterHeadPhase state.

            Example:
                Exercise getPhases.AfterHeadPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    ("body", self.startTagBody),
                    ("frameset", self.startTagFrameset),
                    (
                        (
                            "base",
                            "basefont",
                            "bgsound",
                            "link",
                            "meta",
                            "noframes",
                            "script",
                            "style",
                            "title",
                        ),
                        self.startTagFromHead,
                    ),
                    ("head", self.startTagHead),
                ]
            )
            self.startTagHandler.default = self.startTagOther
            self.endTagHandler = utils.MethodDispatcher([(("body", "html", "br"), self.endTagHtmlBodyBr)])
            self.endTagHandler.default = self.endTagOther

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return True

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return token

        def startTagHtml(self, token):
            """
            Perform the startTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.startTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def startTagBody(self, token):
            """
            Perform the startTagBody utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.startTagBody through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.framesetOK = False
            self.tree.insertElement(token)
            self.parser.phase = self.parser.phases["inBody"]

        def startTagFrameset(self, token):
            """
            Perform the startTagFrameset utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.startTagFrameset through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.parser.phase = self.parser.phases["inFrameset"]

        def startTagFromHead(self, token):
            """
            Perform the startTagFromHead utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.startTagFromHead through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag-out-of-my-head", {"name": token["name"]})
            self.tree.openElements.append(self.tree.headPointer)
            self.parser.phases["inHead"].processStartTag(token)
            for node in self.tree.openElements[::-1]:
                if node.name == "head":
                    self.tree.openElements.remove(node)
                    break

        def startTagHead(self, token):
            """
            Perform the startTagHead utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.startTagHead through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag", {"name": token["name"]})

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return token

        def endTagHtmlBodyBr(self, token):
            """
            Perform the endTagHtmlBodyBr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.endTagHtmlBodyBr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.anythingElse()
            return token

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag", {"name": token["name"]})

        def anythingElse(self):
            """
            Perform the anythingElse utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterHeadPhase.anythingElse through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(self.impliedTagToken("body", "StartTag"))
            self.parser.phase = self.parser.phases["inBody"]
            self.parser.framesetOK = True

    class InBodyPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#parsing-main-inbody
        # the really-really-really-very crazy mode

        """
        Provide the InBodyPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InBodyPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InBodyPhase state.

            Example:
                Exercise getPhases.InBodyPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            # Keep a ref to this for special handling of whitespace in <pre>
            self.processSpaceCharactersNonPre = self.processSpaceCharacters

            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    (
                        (
                            "base",
                            "basefont",
                            "bgsound",
                            "command",
                            "link",
                            "meta",
                            "script",
                            "style",
                            "title",
                        ),
                        self.startTagProcessInHead,
                    ),
                    ("body", self.startTagBody),
                    ("frameset", self.startTagFrameset),
                    (
                        (
                            "address",
                            "article",
                            "aside",
                            "blockquote",
                            "center",
                            "details",
                            "details",
                            "dir",
                            "div",
                            "dl",
                            "fieldset",
                            "figcaption",
                            "figure",
                            "footer",
                            "header",
                            "hgroup",
                            "main",
                            "menu",
                            "nav",
                            "ol",
                            "p",
                            "section",
                            "summary",
                            "ul",
                        ),
                        self.startTagCloseP,
                    ),
                    (headingElements, self.startTagHeading),
                    (("pre", "listing"), self.startTagPreListing),
                    ("form", self.startTagForm),
                    (("li", "dd", "dt"), self.startTagListItem),
                    ("plaintext", self.startTagPlaintext),
                    ("a", self.startTagA),
                    (
                        (
                            "b",
                            "big",
                            "code",
                            "em",
                            "font",
                            "i",
                            "s",
                            "small",
                            "strike",
                            "strong",
                            "tt",
                            "u",
                        ),
                        self.startTagFormatting,
                    ),
                    ("nobr", self.startTagNobr),
                    ("button", self.startTagButton),
                    (("applet", "marquee", "object"), self.startTagAppletMarqueeObject),
                    ("xmp", self.startTagXmp),
                    ("table", self.startTagTable),
                    (
                        ("area", "br", "embed", "img", "keygen", "wbr"),
                        self.startTagVoidFormatting,
                    ),
                    (("param", "source", "track"), self.startTagParamSource),
                    ("input", self.startTagInput),
                    ("hr", self.startTagHr),
                    ("image", self.startTagImage),
                    ("isindex", self.startTagIsIndex),
                    ("textarea", self.startTagTextarea),
                    ("iframe", self.startTagIFrame),
                    (("noembed", "noframes", "noscript"), self.startTagRawtext),
                    ("select", self.startTagSelect),
                    (("rp", "rt"), self.startTagRpRt),
                    (("option", "optgroup"), self.startTagOpt),
                    (("math"), self.startTagMath),
                    (("svg"), self.startTagSvg),
                    (
                        (
                            "caption",
                            "col",
                            "colgroup",
                            "frame",
                            "head",
                            "tbody",
                            "td",
                            "tfoot",
                            "th",
                            "thead",
                            "tr",
                        ),
                        self.startTagMisplaced,
                    ),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    ("body", self.endTagBody),
                    ("html", self.endTagHtml),
                    (
                        (
                            "address",
                            "article",
                            "aside",
                            "blockquote",
                            "button",
                            "center",
                            "details",
                            "dialog",
                            "dir",
                            "div",
                            "dl",
                            "fieldset",
                            "figcaption",
                            "figure",
                            "footer",
                            "header",
                            "hgroup",
                            "listing",
                            "main",
                            "menu",
                            "nav",
                            "ol",
                            "pre",
                            "section",
                            "summary",
                            "ul",
                        ),
                        self.endTagBlock,
                    ),
                    ("form", self.endTagForm),
                    ("p", self.endTagP),
                    (("dd", "dt", "li"), self.endTagListItem),
                    (headingElements, self.endTagHeading),
                    (
                        (
                            "a",
                            "b",
                            "big",
                            "code",
                            "em",
                            "font",
                            "i",
                            "nobr",
                            "s",
                            "small",
                            "strike",
                            "strong",
                            "tt",
                            "u",
                        ),
                        self.endTagFormatting,
                    ),
                    (("applet", "marquee", "object"), self.endTagAppletMarqueeObject),
                    ("br", self.endTagBr),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        def isMatchingFormattingElement(self, node1, node2):
            """
            Perform the isMatchingFormattingElement utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.isMatchingFormattingElement through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param node1: Value supplied for node1 under the utility contract.
            :param node2: Value supplied for node2 under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return (
                node1.name == node2.name and node1.namespace == node2.namespace and node1.attributes == node2.attributes
            )

        # helper
        def addFormattingElement(self, token):
            """
            Perform the addFormattingElement utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.addFormattingElement through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            element = self.tree.openElements[-1]

            matchingElements = []
            for node in self.tree.activeFormattingElements[::-1]:
                if node is Marker:
                    break
                elif self.isMatchingFormattingElement(node, element):
                    matchingElements.append(node)

            assert len(matchingElements) <= 3
            if len(matchingElements) == 3:
                self.tree.activeFormattingElements.remove(matchingElements[-1])
            self.tree.activeFormattingElements.append(element)

        # the real deal
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            allowed_elements = frozenset(
                (
                    "dd",
                    "dt",
                    "li",
                    "p",
                    "tbody",
                    "td",
                    "tfoot",
                    "th",
                    "thead",
                    "tr",
                    "body",
                    "html",
                )
            )
            for node in self.tree.openElements[::-1]:
                if node.name not in allowed_elements:
                    self.parser.parseError("expected-closing-tag-but-got-eof")
                    break
            # Stop parsing

        def processSpaceCharactersDropNewline(self, token):
            # Sometimes (start of <pre>, <listing>, and <textarea> blocks) we
            # want to drop leading newlines
            """
            Perform the processSpaceCharactersDropNewline utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.processSpaceCharactersDropNewline through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            data = token["data"]
            self.processSpaceCharacters = self.processSpaceCharactersNonPre
            if (
                data.startswith("\n")
                and self.tree.openElements[-1].name in ("pre", "listing", "textarea")
                and not self.tree.openElements[-1].hasContent()
            ):
                data = data[1:]
            if data:
                self.tree.reconstructActiveFormattingElements()
                self.tree.insertText(data)

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if token["data"] == "\u0000":
                # The tokenizer should always emit null on its own
                return
            self.tree.reconstructActiveFormattingElements()
            self.tree.insertText(token["data"])
            # This must be bad for performance
            if self.parser.framesetOK and any([char not in spaceCharacters for char in token["data"]]):
                self.parser.framesetOK = False

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.tree.insertText(token["data"])

        def startTagProcessInHead(self, token):
            """
            Perform the startTagProcessInHead utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagProcessInHead through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inHead"].processStartTag(token)

        def startTagBody(self, token):
            """
            Perform the startTagBody utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagBody through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag", {"name": "body"})
            if len(self.tree.openElements) == 1 or self.tree.openElements[1].name != "body":
                assert self.parser.innerHTML
            else:
                self.parser.framesetOK = False
                self.tree.apply_body_attributes(token["data"])

        def startTagFrameset(self, token):
            """
            Perform the startTagFrameset utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagFrameset through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag", {"name": "frameset"})
            if len(self.tree.openElements) == 1 or self.tree.openElements[1].name != "body":
                assert self.parser.innerHTML
            elif not self.parser.framesetOK:
                pass
            else:
                if self.tree.openElements[1].parent:
                    self.tree.openElements[1].parent.removeChild(self.tree.openElements[1])
                while self.tree.openElements[-1].name != "html":
                    self.tree.openElements.pop()
                self.tree.insertElement(token)
                self.parser.phase = self.parser.phases["inFrameset"]

        def startTagCloseP(self, token):
            """
            Perform the startTagCloseP utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagCloseP through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("p", variant="button"):
                self.endTagP(self.impliedTagToken("p"))
            self.tree.insertElement(token)

        def startTagPreListing(self, token):
            """
            Perform the startTagPreListing utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagPreListing through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("p", variant="button"):
                self.endTagP(self.impliedTagToken("p"))
            self.tree.insertElement(token)
            self.parser.framesetOK = False
            self.processSpaceCharacters = self.processSpaceCharactersDropNewline

        def startTagForm(self, token):
            """
            Perform the startTagForm utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagForm through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.formPointer:
                self.parser.parseError("unexpected-start-tag", {"name": "form"})
            else:
                if self.tree.elementInScope("p", variant="button"):
                    self.endTagP(self.impliedTagToken("p"))
                self.tree.insertElement(token)
                self.tree.formPointer = self.tree.openElements[-1]

        def startTagListItem(self, token):
            """
            Perform the startTagListItem utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagListItem through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.framesetOK = False

            stopNamesMap = {"li": ["li"], "dt": ["dt", "dd"], "dd": ["dt", "dd"]}
            stopNames = stopNamesMap[token["name"]]
            for node in reversed(self.tree.openElements):
                if node.name in stopNames:
                    self.parser.phase.processEndTag(self.impliedTagToken(node.name, "EndTag"))
                    break
                if node.nameTuple in specialElements and node.name not in (
                    "address",
                    "div",
                    "p",
                ):
                    break

            if self.tree.elementInScope("p", variant="button"):
                self.parser.phase.processEndTag(self.impliedTagToken("p", "EndTag"))

            self.tree.insertElement(token)

        def startTagPlaintext(self, token):
            """
            Perform the startTagPlaintext utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagPlaintext through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("p", variant="button"):
                self.endTagP(self.impliedTagToken("p"))
            self.tree.insertElement(token)
            self.parser.tokenizer.state = self.parser.tokenizer.plaintextState

        def startTagHeading(self, token):
            """
            Perform the startTagHeading utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagHeading through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("p", variant="button"):
                self.endTagP(self.impliedTagToken("p"))
            if self.tree.openElements[-1].name in headingElements:
                self.parser.parseError("unexpected-start-tag", {"name": token["name"]})
                self.tree.openElements.pop()
            self.tree.insertElement(token)

        def startTagA(self, token):
            """
            Perform the startTagA utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagA through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            afeAElement = self.tree.elementInActiveFormattingElements("a")
            if afeAElement is not False:
                self.parser.parseError(
                    "unexpected-start-tag-implies-end-tag",
                    {"startName": "a", "endName": "a"},
                )
                self.endTagFormatting(self.impliedTagToken("a"))
                if afeAElement in self.tree.openElements:
                    self.tree.openElements.remove(afeAElement)
                if afeAElement in self.tree.activeFormattingElements:
                    self.tree.activeFormattingElements.remove(afeAElement)
            self.tree.reconstructActiveFormattingElements()
            self.addFormattingElement(token)

        def startTagFormatting(self, token):
            """
            Perform the startTagFormatting utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagFormatting through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.addFormattingElement(token)

        def startTagNobr(self, token):
            """
            Perform the startTagNobr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagNobr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            if self.tree.elementInScope("nobr"):
                self.parser.parseError(
                    "unexpected-start-tag-implies-end-tag",
                    {"startName": "nobr", "endName": "nobr"},
                )
                self.processEndTag(self.impliedTagToken("nobr"))
                # XXX Need tests that trigger the following
                self.tree.reconstructActiveFormattingElements()
            self.addFormattingElement(token)

        def startTagButton(self, token):
            """
            Perform the startTagButton utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagButton through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.tree.elementInScope("button"):
                self.parser.parseError(
                    "unexpected-start-tag-implies-end-tag",
                    {"startName": "button", "endName": "button"},
                )
                self.processEndTag(self.impliedTagToken("button"))
                return token
            else:
                self.tree.reconstructActiveFormattingElements()
                self.tree.insertElement(token)
                self.parser.framesetOK = False

        def startTagAppletMarqueeObject(self, token):
            """
            Perform the startTagAppletMarqueeObject utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagAppletMarqueeObject through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.tree.insertElement(token)
            self.tree.activeFormattingElements.append(Marker)
            self.parser.framesetOK = False

        def startTagXmp(self, token):
            """
            Perform the startTagXmp utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagXmp through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("p", variant="button"):
                self.endTagP(self.impliedTagToken("p"))
            self.tree.reconstructActiveFormattingElements()
            self.parser.framesetOK = False
            self.parser.parseRCDataRawtext(token, "RAWTEXT")

        def startTagTable(self, token):
            """
            Perform the startTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.parser.compatMode != "quirks":
                if self.tree.elementInScope("p", variant="button"):
                    self.processEndTag(self.impliedTagToken("p"))
            self.tree.insertElement(token)
            self.parser.framesetOK = False
            self.parser.phase = self.parser.phases["inTable"]

        def startTagVoidFormatting(self, token):
            """
            Perform the startTagVoidFormatting utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagVoidFormatting through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.tree.insertElement(token)
            self.tree.openElements.pop()
            token["selfClosingAcknowledged"] = True
            self.parser.framesetOK = False

        def startTagInput(self, token):
            """
            Perform the startTagInput utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagInput through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            framesetOK = self.parser.framesetOK
            self.startTagVoidFormatting(token)
            if "type" in token["data"] and token["data"]["type"].translate(asciiUpper2Lower) == "hidden":
                # input type=hidden doesn't change framesetOK
                self.parser.framesetOK = framesetOK

        def startTagParamSource(self, token):
            """
            Perform the startTagParamSource utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagParamSource through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.tree.openElements.pop()
            token["selfClosingAcknowledged"] = True

        def startTagHr(self, token):
            """
            Perform the startTagHr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagHr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("p", variant="button"):
                self.endTagP(self.impliedTagToken("p"))
            self.tree.insertElement(token)
            self.tree.openElements.pop()
            token["selfClosingAcknowledged"] = True
            self.parser.framesetOK = False

        def startTagImage(self, token):
            # No really...
            """
            Perform the startTagImage utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagImage through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError(
                "unexpected-start-tag-treated-as",
                {"originalName": "image", "newName": "img"},
            )
            self.processStartTag(
                self.impliedTagToken(
                    "img",
                    "StartTag",
                    attributes=token["data"],
                    selfClosing=token["selfClosing"],
                )
            )

        def startTagIsIndex(self, token):
            """
            Perform the startTagIsIndex utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagIsIndex through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("deprecated-tag", {"name": "isindex"})
            if self.tree.formPointer:
                return
            form_attrs = {}
            if "action" in token["data"]:
                form_attrs["action"] = token["data"]["action"]
            self.processStartTag(self.impliedTagToken("form", "StartTag", attributes=form_attrs))
            self.processStartTag(self.impliedTagToken("hr", "StartTag"))
            self.processStartTag(self.impliedTagToken("label", "StartTag"))
            # XXX Localization ...
            if "prompt" in token["data"]:
                prompt = token["data"]["prompt"]
            else:
                prompt = "This is a searchable index. Enter search keywords: "
            self.processCharacters({"type": tokenTypes["Characters"], "data": prompt})
            attributes = token["data"].copy()
            if "action" in attributes:
                del attributes["action"]
            if "prompt" in attributes:
                del attributes["prompt"]
            attributes["name"] = "isindex"
            self.processStartTag(
                self.impliedTagToken(
                    "input",
                    "StartTag",
                    attributes=attributes,
                    selfClosing=token["selfClosing"],
                )
            )
            self.processEndTag(self.impliedTagToken("label"))
            self.processStartTag(self.impliedTagToken("hr", "StartTag"))
            self.processEndTag(self.impliedTagToken("form"))

        def startTagTextarea(self, token):
            """
            Perform the startTagTextarea utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagTextarea through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.parser.tokenizer.state = self.parser.tokenizer.rcdataState
            self.processSpaceCharacters = self.processSpaceCharactersDropNewline
            self.parser.framesetOK = False

        def startTagIFrame(self, token):
            """
            Perform the startTagIFrame utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagIFrame through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.framesetOK = False
            self.startTagRawtext(token)

        def startTagRawtext(self, token):
            """
            iframe, noembed noframes, noscript(if scripting enabled)

            Example:
                Exercise getPhases.InBodyPhase.startTagRawtext through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseRCDataRawtext(token, "RAWTEXT")

        def startTagOpt(self, token):
            """
            Perform the startTagOpt utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagOpt through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name == "option":
                self.parser.phase.processEndTag(self.impliedTagToken("option"))
            self.tree.reconstructActiveFormattingElements()
            self.parser.tree.insertElement(token)

        def startTagSelect(self, token):
            """
            Perform the startTagSelect utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagSelect through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.tree.insertElement(token)
            self.parser.framesetOK = False
            if self.parser.phase in (
                self.parser.phases["inTable"],
                self.parser.phases["inCaption"],
                self.parser.phases["inColumnGroup"],
                self.parser.phases["inTableBody"],
                self.parser.phases["inRow"],
                self.parser.phases["inCell"],
            ):
                self.parser.phase = self.parser.phases["inSelectInTable"]
            else:
                self.parser.phase = self.parser.phases["inSelect"]

        def startTagRpRt(self, token):
            """
            Perform the startTagRpRt utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagRpRt through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("ruby"):
                self.tree.generateImpliedEndTags()
                if self.tree.openElements[-1].name != "ruby":
                    self.parser.parseError()
            self.tree.insertElement(token)

        def startTagMath(self, token):
            """
            Perform the startTagMath utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagMath through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.parser.adjustMathMLAttributes(token)
            self.parser.adjustForeignAttributes(token)
            token["namespace"] = namespaces["mathml"]
            self.tree.insertElement(token)
            # Need to get the parse error right for the case where the token
            # has a namespace not equal to the xmlns attribute
            if token["selfClosing"]:
                self.tree.openElements.pop()
                token["selfClosingAcknowledged"] = True

        def startTagSvg(self, token):
            """
            Perform the startTagSvg utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagSvg through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.parser.adjustSVGAttributes(token)
            self.parser.adjustForeignAttributes(token)
            token["namespace"] = namespaces["svg"]
            self.tree.insertElement(token)
            # Need to get the parse error right for the case where the token
            # has a namespace not equal to the xmlns attribute
            if token["selfClosing"]:
                self.tree.openElements.pop()
                token["selfClosingAcknowledged"] = True

        def startTagMisplaced(self, token):
            """
            Elements that should be children of other elements that have a different insertion mode; here they are ignored "caption", "col", "colgroup", "frame", "frameset", "head", "option", "optgroup", "tbody", "td", "tfoot", "th", "thead", "tr", "noscript"

            Example:
                Exercise getPhases.InBodyPhase.startTagMisplaced through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag-ignored", {"name": token["name"]})

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.reconstructActiveFormattingElements()
            self.tree.insertElement(token)

        def endTagP(self, token):
            """
            Perform the endTagP utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagP through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not self.tree.elementInScope("p", variant="button"):
                self.startTagCloseP(self.impliedTagToken("p", "StartTag"))
                self.parser.parseError("unexpected-end-tag", {"name": "p"})
                self.endTagP(self.impliedTagToken("p", "EndTag"))
            else:
                self.tree.generateImpliedEndTags("p")
                if self.tree.openElements[-1].name != "p":
                    self.parser.parseError("unexpected-end-tag", {"name": "p"})
                node = self.tree.openElements.pop()
                while node.name != "p":
                    node = self.tree.openElements.pop()

        def endTagBody(self, token):
            """
            Perform the endTagBody utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagBody through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not self.tree.elementInScope("body"):
                self.parser.parseError()
                return
            elif self.tree.openElements[-1].name != "body":
                for node in self.tree.openElements[2:]:
                    if node.name not in frozenset(
                        (
                            "dd",
                            "dt",
                            "li",
                            "optgroup",
                            "option",
                            "p",
                            "rp",
                            "rt",
                            "tbody",
                            "td",
                            "tfoot",
                            "th",
                            "thead",
                            "tr",
                            "body",
                            "html",
                        )
                    ):
                        # Not sure this is the correct name for the parse error
                        self.parser.parseError(
                            "expected-one-end-tag-but-got-another",
                            {"expectedName": "body", "gotName": node.name},
                        )
                        break
            self.parser.phase = self.parser.phases["afterBody"]

        def endTagHtml(self, token):
            # We repeat the test for the body end tag token being ignored here
            """
            Perform the endTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.tree.elementInScope("body"):
                self.endTagBody(self.impliedTagToken("body"))
                return token

        def endTagBlock(self, token):
            # Put us back in the right whitespace handling mode
            """
            Perform the endTagBlock utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagBlock through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if token["name"] == "pre":
                self.processSpaceCharacters = self.processSpaceCharactersNonPre
            inScope = self.tree.elementInScope(token["name"])
            if inScope:
                self.tree.generateImpliedEndTags()
            if self.tree.openElements[-1].name != token["name"]:
                self.parser.parseError("end-tag-too-early", {"name": token["name"]})
            if inScope:
                node = self.tree.openElements.pop()
                while node.name != token["name"]:
                    node = self.tree.openElements.pop()

        def endTagForm(self, token):
            """
            Perform the endTagForm utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagForm through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            node = self.tree.formPointer
            self.tree.formPointer = None
            if node is None or not self.tree.elementInScope(node):
                self.parser.parseError("unexpected-end-tag", {"name": "form"})
            else:
                self.tree.generateImpliedEndTags()
                if self.tree.openElements[-1] != node:
                    self.parser.parseError("end-tag-too-early-ignored", {"name": "form"})
                self.tree.openElements.remove(node)

        def endTagListItem(self, token):
            """
            Perform the endTagListItem utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagListItem through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if token["name"] == "li":
                variant = "list"
            else:
                variant = None
            if not self.tree.elementInScope(token["name"], variant=variant):
                self.parser.parseError("unexpected-end-tag", {"name": token["name"]})
            else:
                self.tree.generateImpliedEndTags(exclude=token["name"])
                if self.tree.openElements[-1].name != token["name"]:
                    self.parser.parseError("end-tag-too-early", {"name": token["name"]})
                node = self.tree.openElements.pop()
                while node.name != token["name"]:
                    node = self.tree.openElements.pop()

        def endTagHeading(self, token):
            """
            Perform the endTagHeading utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagHeading through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            for item in headingElements:
                if self.tree.elementInScope(item):
                    self.tree.generateImpliedEndTags()
                    break
            if self.tree.openElements[-1].name != token["name"]:
                self.parser.parseError("end-tag-too-early", {"name": token["name"]})

            for item in headingElements:
                if self.tree.elementInScope(item):
                    item = self.tree.openElements.pop()
                    while item.name not in headingElements:
                        item = self.tree.openElements.pop()
                    break

        def endTagFormatting(self, token):
            """
            The much-feared adoption agency algorithm

            Example:
                Exercise getPhases.InBodyPhase.endTagFormatting through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            # http://svn.whatwg.org/webapps/complete.html#adoptionAgency revision 7867
            # XXX Better parseError messages appreciated.

            # Step 1
            outerLoopCounter = 0

            # Step 2
            while outerLoopCounter < 8:

                # Step 3
                outerLoopCounter += 1

                # Step 4:

                # Let the formatting element be the last element in
                # the list of active formatting elements that:
                # - is between the end of the list and the last scope
                # marker in the list, if any, or the start of the list
                # otherwise, and
                # - has the same tag name as the token.
                formattingElement = self.tree.elementInActiveFormattingElements(token["name"])
                if formattingElement is False or (
                    formattingElement in self.tree.openElements and not self.tree.elementInScope(formattingElement.name)
                ):
                    # If there is no such node, then abort these steps
                    # and instead act as described in the "any other
                    # end tag" entry below.
                    self.endTagOther(token)
                    return

                # Otherwise, if there is such a node, but that node is
                # not in the stack of open elements, then this is a
                # parse error; remove the element from the list, and
                # abort these steps.
                elif formattingElement not in self.tree.openElements:
                    self.parser.parseError("adoption-agency-1.2", {"name": token["name"]})
                    self.tree.activeFormattingElements.remove(formattingElement)
                    return

                # Otherwise, if there is such a node, and that node is
                # also in the stack of open elements, but the element
                # is not in scope, then this is a parse error; ignore
                # the token, and abort these steps.
                elif not self.tree.elementInScope(formattingElement.name):
                    self.parser.parseError("adoption-agency-4.4", {"name": token["name"]})
                    return

                # Otherwise, there is a formatting element and that
                # element is in the stack and is in scope. If the
                # element is not the current node, this is a parse
                # error. In any case, proceed with the algorithm as
                # written in the following steps.
                else:
                    if formattingElement != self.tree.openElements[-1]:
                        self.parser.parseError("adoption-agency-1.3", {"name": token["name"]})

                # Step 5:

                # Let the furthest block be the topmost node in the
                # stack of open elements that is lower in the stack
                # than the formatting element, and is an element in
                # the special category. There might not be one.
                afeIndex = self.tree.openElements.index(formattingElement)
                furthestBlock = None
                for element in self.tree.openElements[afeIndex:]:
                    if element.nameTuple in specialElements:
                        furthestBlock = element
                        break

                # Step 6:

                # If there is no furthest block, then the UA must
                # first pop all the nodes from the bottom of the stack
                # of open elements, from the current node up to and
                # including the formatting element, then remove the
                # formatting element from the list of active
                # formatting elements, and finally abort these steps.
                if furthestBlock is None:
                    element = self.tree.openElements.pop()
                    while element != formattingElement:
                        element = self.tree.openElements.pop()
                    self.tree.activeFormattingElements.remove(element)
                    return

                # Step 7
                commonAncestor = self.tree.openElements[afeIndex - 1]

                # Step 8:
                # The bookmark is supposed to help us identify where to reinsert
                # nodes in step 15. We have to ensure that we reinsert nodes after
                # the node before the active formatting element. Note the bookmark
                # can move in step 9.7
                bookmark = self.tree.activeFormattingElements.index(formattingElement)

                # Step 9
                lastNode = node = furthestBlock
                innerLoopCounter = 0

                index = self.tree.openElements.index(node)
                while innerLoopCounter < 3:
                    innerLoopCounter += 1
                    # Node is element before node in open elements
                    index -= 1
                    node = self.tree.openElements[index]
                    if node not in self.tree.activeFormattingElements:
                        self.tree.openElements.remove(node)
                        continue
                    # Step 9.6
                    if node == formattingElement:
                        break
                    # Step 9.7
                    if lastNode == furthestBlock:
                        bookmark = self.tree.activeFormattingElements.index(node) + 1
                    # Step 9.8
                    clone = node.cloneNode()
                    # Replace node with clone
                    self.tree.activeFormattingElements[self.tree.activeFormattingElements.index(node)] = clone
                    self.tree.openElements[self.tree.openElements.index(node)] = clone
                    node = clone
                    # Step 9.9
                    # Remove lastNode from its parents, if any
                    if lastNode.parent is not None:
                        lastNode.parent.removeChild(lastNode)
                    node.appendChild(lastNode)
                    # Step 9.10
                    lastNode = node

                # Step 10
                # Foster parent lastNode if commonAncestor is a
                # table, tbody, tfoot, thead, or tr we need to foster
                # parent the lastNode
                if lastNode.parent is not None:
                    lastNode.parent.removeChild(lastNode)

                if commonAncestor.name in frozenset(("table", "tbody", "tfoot", "thead", "tr")):
                    parent, insertBefore = self.tree.getTableMisnestedNodePosition()
                    parent.insertBefore(lastNode, insertBefore)
                else:
                    commonAncestor.appendChild(lastNode)

                # Step 11
                clone = formattingElement.cloneNode()

                # Step 12
                furthestBlock.reparentChildren(clone)

                # Step 13
                furthestBlock.appendChild(clone)

                # Step 14
                self.tree.activeFormattingElements.remove(formattingElement)
                self.tree.activeFormattingElements.insert(bookmark, clone)

                # Step 15
                self.tree.openElements.remove(formattingElement)
                self.tree.openElements.insert(self.tree.openElements.index(furthestBlock) + 1, clone)

        def endTagAppletMarqueeObject(self, token):
            """
            Perform the endTagAppletMarqueeObject utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagAppletMarqueeObject through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope(token["name"]):
                self.tree.generateImpliedEndTags()
            if self.tree.openElements[-1].name != token["name"]:
                self.parser.parseError("end-tag-too-early", {"name": token["name"]})

            if self.tree.elementInScope(token["name"]):
                element = self.tree.openElements.pop()
                while element.name != token["name"]:
                    element = self.tree.openElements.pop()
                self.tree.clearActiveFormattingElements()

        def endTagBr(self, token):
            """
            Perform the endTagBr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagBr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError(
                "unexpected-end-tag-treated-as",
                {"originalName": "br", "newName": "br element"},
            )
            self.tree.reconstructActiveFormattingElements()
            self.tree.insertElement(self.impliedTagToken("br", "StartTag"))
            self.tree.openElements.pop()

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InBodyPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            for node in self.tree.openElements[::-1]:
                if node.name == token["name"]:
                    self.tree.generateImpliedEndTags(exclude=token["name"])
                    if self.tree.openElements[-1].name != token["name"]:
                        self.parser.parseError("unexpected-end-tag", {"name": token["name"]})
                    while self.tree.openElements.pop() != node:
                        pass
                    break
                else:
                    if node.nameTuple in specialElements:
                        self.parser.parseError("unexpected-end-tag", {"name": token["name"]})
                        break

    class TextPhase(Phase):
        """
        Provide the TextPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.TextPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the TextPhase state.

            Example:
                Exercise getPhases.TextPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)
            self.startTagHandler = utils.MethodDispatcher([])
            self.startTagHandler.default = self.startTagOther
            self.endTagHandler = utils.MethodDispatcher([("script", self.endTagScript)])
            self.endTagHandler.default = self.endTagOther

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.TextPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertText(token["data"])

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.TextPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError(
                "expected-named-closing-tag-but-got-eof",
                {"name": self.tree.openElements[-1].name},
            )
            self.tree.openElements.pop()
            self.parser.phase = self.parser.originalPhase
            return True

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.TextPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            assert False, "Tried to process start tag %s in RCDATA/RAWTEXT mode" % token["name"]

        def endTagScript(self, token):
            """
            Perform the endTagScript utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.TextPhase.endTagScript through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            node = self.tree.openElements.pop()
            assert node.name == "script"
            self.parser.phase = self.parser.originalPhase
            # The rest of this method is all stuff that only happens if
            # document.write works

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.TextPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.openElements.pop()
            self.parser.phase = self.parser.originalPhase

    class InTablePhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#in-table

        """
        Provide the InTablePhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InTablePhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InTablePhase state.

            Example:
                Exercise getPhases.InTablePhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)
            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    ("caption", self.startTagCaption),
                    ("colgroup", self.startTagColgroup),
                    ("col", self.startTagCol),
                    (("tbody", "tfoot", "thead"), self.startTagRowGroup),
                    (("td", "th", "tr"), self.startTagImplyTbody),
                    ("table", self.startTagTable),
                    (("style", "script"), self.startTagStyleScript),
                    ("input", self.startTagInput),
                    ("form", self.startTagForm),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    ("table", self.endTagTable),
                    (
                        (
                            "body",
                            "caption",
                            "col",
                            "colgroup",
                            "html",
                            "tbody",
                            "td",
                            "tfoot",
                            "th",
                            "thead",
                            "tr",
                        ),
                        self.endTagIgnore,
                    ),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        # helper methods
        def clearStackToTableContext(self):
            # "clear the stack back to a table context"
            """
            Perform the clearStackToTableContext utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.clearStackToTableContext through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            while self.tree.openElements[-1].name not in ("table", "html"):
                # self.parser.parseError("unexpected-implied-end-tag-in-table",
                #  {"name":  self.tree.openElements[-1].name})
                self.tree.openElements.pop()
            # When the current node is <html> it's an innerHTML case

        # processing methods
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name != "html":
                self.parser.parseError("eof-in-table")
            else:
                assert self.parser.innerHTML
            # Stop parsing

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            originalPhase = self.parser.phase
            self.parser.phase = self.parser.phases["inTableText"]
            self.parser.phase.originalPhase = originalPhase
            self.parser.phase.processSpaceCharacters(token)

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            originalPhase = self.parser.phase
            self.parser.phase = self.parser.phases["inTableText"]
            self.parser.phase.originalPhase = originalPhase
            self.parser.phase.processCharacters(token)

        def insertText(self, token):
            # If we get here there must be at least one non-whitespace character
            # Do the table magic!
            """
            Perform the insertText utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.insertText through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertFromTable = True
            self.parser.phases["inBody"].processCharacters(token)
            self.tree.insertFromTable = False

        def startTagCaption(self, token):
            """
            Perform the startTagCaption utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagCaption through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.clearStackToTableContext()
            self.tree.activeFormattingElements.append(Marker)
            self.tree.insertElement(token)
            self.parser.phase = self.parser.phases["inCaption"]

        def startTagColgroup(self, token):
            """
            Perform the startTagColgroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagColgroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.clearStackToTableContext()
            self.tree.insertElement(token)
            self.parser.phase = self.parser.phases["inColumnGroup"]

        def startTagCol(self, token):
            """
            Perform the startTagCol utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagCol through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.startTagColgroup(self.impliedTagToken("colgroup", "StartTag"))
            return token

        def startTagRowGroup(self, token):
            """
            Perform the startTagRowGroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagRowGroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.clearStackToTableContext()
            self.tree.insertElement(token)
            self.parser.phase = self.parser.phases["inTableBody"]

        def startTagImplyTbody(self, token):
            """
            Perform the startTagImplyTbody utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagImplyTbody through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.startTagRowGroup(self.impliedTagToken("tbody", "StartTag"))
            return token

        def startTagTable(self, token):
            """
            Perform the startTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError(
                "unexpected-start-tag-implies-end-tag",
                {"startName": "table", "endName": "table"},
            )
            self.parser.phase.processEndTag(self.impliedTagToken("table"))
            if not self.parser.innerHTML:
                return token

        def startTagStyleScript(self, token):
            """
            Perform the startTagStyleScript utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagStyleScript through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inHead"].processStartTag(token)

        def startTagInput(self, token):
            """
            Perform the startTagInput utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagInput through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if "type" in token["data"] and token["data"]["type"].translate(asciiUpper2Lower) == "hidden":
                self.parser.parseError("unexpected-hidden-input-in-table")
                self.tree.insertElement(token)
                # XXX associate with form
                self.tree.openElements.pop()
            else:
                self.startTagOther(token)

        def startTagForm(self, token):
            """
            Perform the startTagForm utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagForm through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-form-in-table")
            if self.tree.formPointer is None:
                self.tree.insertElement(token)
                self.tree.formPointer = self.tree.openElements[-1]
                self.tree.openElements.pop()

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag-implies-table-voodoo", {"name": token["name"]})
            # Do the table magic!
            self.tree.insertFromTable = True
            self.parser.phases["inBody"].processStartTag(token)
            self.tree.insertFromTable = False

        def endTagTable(self, token):
            """
            Perform the endTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.endTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("table", variant="table"):
                self.tree.generateImpliedEndTags()
                if self.tree.openElements[-1].name != "table":
                    self.parser.parseError(
                        "end-tag-too-early-named",
                        {
                            "gotName": "table",
                            "expectedName": self.tree.openElements[-1].name,
                        },
                    )
                while self.tree.openElements[-1].name != "table":
                    self.tree.openElements.pop()
                self.tree.openElements.pop()
                self.parser.resetInsertionMode()
            else:
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()

        def endTagIgnore(self, token):
            """
            Perform the endTagIgnore utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.endTagIgnore through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag", {"name": token["name"]})

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTablePhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag-implies-table-voodoo", {"name": token["name"]})
            # Do the table magic!
            self.tree.insertFromTable = True
            self.parser.phases["inBody"].processEndTag(token)
            self.tree.insertFromTable = False

    class InTableTextPhase(Phase):
        """
        Provide the InTableTextPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InTableTextPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InTableTextPhase state.

            Example:
                Exercise getPhases.InTableTextPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)
            self.originalPhase = None
            self.characterTokens = []

        def flushCharacters(self):
            """
            Perform the flushCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableTextPhase.flushCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            data = "".join([item["data"] for item in self.characterTokens])
            if any([item not in spaceCharacters for item in data]):
                token = {"type": tokenTypes["Characters"], "data": data}
                self.parser.phases["inTable"].insertText(token)
            elif data:
                self.tree.insertText(data)
            self.characterTokens = []

        def processComment(self, token):
            """
            Perform the processComment utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableTextPhase.processComment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.flushCharacters()
            self.parser.phase = self.originalPhase
            return token

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableTextPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.flushCharacters()
            self.parser.phase = self.originalPhase
            return True

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableTextPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if token["data"] == "\u0000":
                return
            self.characterTokens.append(token)

        def processSpaceCharacters(self, token):
            # pretty sure we should never reach here
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableTextPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.characterTokens.append(token)

        #        assert False

        def processStartTag(self, token):
            """
            Perform the processStartTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableTextPhase.processStartTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.flushCharacters()
            self.parser.phase = self.originalPhase
            return token

        def processEndTag(self, token):
            """
            Perform the processEndTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableTextPhase.processEndTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.flushCharacters()
            self.parser.phase = self.originalPhase
            return token

    class InCaptionPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#in-caption

        """
        Provide the InCaptionPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InCaptionPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InCaptionPhase state.

            Example:
                Exercise getPhases.InCaptionPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    (
                        (
                            "caption",
                            "col",
                            "colgroup",
                            "tbody",
                            "td",
                            "tfoot",
                            "th",
                            "thead",
                            "tr",
                        ),
                        self.startTagTableElement,
                    ),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    ("caption", self.endTagCaption),
                    ("table", self.endTagTable),
                    (
                        (
                            "body",
                            "col",
                            "colgroup",
                            "html",
                            "tbody",
                            "td",
                            "tfoot",
                            "th",
                            "thead",
                            "tr",
                        ),
                        self.endTagIgnore,
                    ),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        def ignoreEndTagCaption(self):
            """
            Perform the ignoreEndTagCaption utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.ignoreEndTagCaption through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return not self.tree.elementInScope("caption", variant="table")

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.phases["inBody"].processEOF()

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processCharacters(token)

        def startTagTableElement(self, token):
            """
            Perform the startTagTableElement utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.startTagTableElement through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError()
            # XXX Have to duplicate logic here to find out if the tag is ignored
            ignoreEndTag = self.ignoreEndTagCaption()
            self.parser.phase.processEndTag(self.impliedTagToken("caption"))
            if not ignoreEndTag:
                return token

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def endTagCaption(self, token):
            """
            Perform the endTagCaption utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.endTagCaption through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not self.ignoreEndTagCaption():
                # AT this code is quite similar to endTagTable in "InTable"
                self.tree.generateImpliedEndTags()
                if self.tree.openElements[-1].name != "caption":
                    self.parser.parseError(
                        "expected-one-end-tag-but-got-another",
                        {
                            "gotName": "caption",
                            "expectedName": self.tree.openElements[-1].name,
                        },
                    )
                while self.tree.openElements[-1].name != "caption":
                    self.tree.openElements.pop()
                self.tree.openElements.pop()
                self.tree.clearActiveFormattingElements()
                self.parser.phase = self.parser.phases["inTable"]
            else:
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()

        def endTagTable(self, token):
            """
            Perform the endTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.endTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError()
            ignoreEndTag = self.ignoreEndTagCaption()
            self.parser.phase.processEndTag(self.impliedTagToken("caption"))
            if not ignoreEndTag:
                return token

        def endTagIgnore(self, token):
            """
            Perform the endTagIgnore utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.endTagIgnore through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag", {"name": token["name"]})

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCaptionPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processEndTag(token)

    class InColumnGroupPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#in-column

        """
        Provide the InColumnGroupPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InColumnGroupPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InColumnGroupPhase state.

            Example:
                Exercise getPhases.InColumnGroupPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher([("html", self.startTagHtml), ("col", self.startTagCol)])
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher([("colgroup", self.endTagColgroup), ("col", self.endTagCol)])
            self.endTagHandler.default = self.endTagOther

        def ignoreEndTagColgroup(self):
            """
            Perform the ignoreEndTagColgroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.ignoreEndTagColgroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.tree.openElements[-1].name == "html"

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.tree.openElements[-1].name == "html":
                assert self.parser.innerHTML
                return
            else:
                ignoreEndTag = self.ignoreEndTagColgroup()
                self.endTagColgroup(self.impliedTagToken("colgroup"))
                if not ignoreEndTag:
                    return True

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            ignoreEndTag = self.ignoreEndTagColgroup()
            self.endTagColgroup(self.impliedTagToken("colgroup"))
            if not ignoreEndTag:
                return token

        def startTagCol(self, token):
            """
            Perform the startTagCol utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.startTagCol through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.tree.openElements.pop()

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            ignoreEndTag = self.ignoreEndTagColgroup()
            self.endTagColgroup(self.impliedTagToken("colgroup"))
            if not ignoreEndTag:
                return token

        def endTagColgroup(self, token):
            """
            Perform the endTagColgroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.endTagColgroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.ignoreEndTagColgroup():
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()
            else:
                self.tree.openElements.pop()
                self.parser.phase = self.parser.phases["inTable"]

        def endTagCol(self, token):
            """
            Perform the endTagCol utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.endTagCol through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("no-end-tag", {"name": "col"})

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InColumnGroupPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            ignoreEndTag = self.ignoreEndTagColgroup()
            self.endTagColgroup(self.impliedTagToken("colgroup"))
            if not ignoreEndTag:
                return token

    class InTableBodyPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#in-table0

        """
        Provide the InTableBodyPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InTableBodyPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InTableBodyPhase state.

            Example:
                Exercise getPhases.InTableBodyPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)
            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    ("tr", self.startTagTr),
                    (("td", "th"), self.startTagTableCell),
                    (
                        ("caption", "col", "colgroup", "tbody", "tfoot", "thead"),
                        self.startTagTableOther,
                    ),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    (("tbody", "tfoot", "thead"), self.endTagTableRowGroup),
                    ("table", self.endTagTable),
                    (
                        (
                            "body",
                            "caption",
                            "col",
                            "colgroup",
                            "html",
                            "td",
                            "th",
                            "tr",
                        ),
                        self.endTagIgnore,
                    ),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        # helper methods
        def clearStackToTableBodyContext(self):
            """
            Perform the clearStackToTableBodyContext utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.clearStackToTableBodyContext through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            while self.tree.openElements[-1].name not in (
                "tbody",
                "tfoot",
                "thead",
                "html",
            ):
                # self.parser.parseError("unexpected-implied-end-tag-in-table",
                #  {"name": self.tree.openElements[-1].name})
                self.tree.openElements.pop()
            if self.tree.openElements[-1].name == "html":
                assert self.parser.innerHTML

        # the rest
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.phases["inTable"].processEOF()

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processSpaceCharacters(token)

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processCharacters(token)

        def startTagTr(self, token):
            """
            Perform the startTagTr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.startTagTr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.clearStackToTableBodyContext()
            self.tree.insertElement(token)
            self.parser.phase = self.parser.phases["inRow"]

        def startTagTableCell(self, token):
            """
            Perform the startTagTableCell utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.startTagTableCell through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("unexpected-cell-in-table-body", {"name": token["name"]})
            self.startTagTr(self.impliedTagToken("tr", "StartTag"))
            return token

        def startTagTableOther(self, token):
            # XXX AT Any ideas on how to share this with endTagTable?
            """
            Perform the startTagTableOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.startTagTableOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if (
                self.tree.elementInScope("tbody", variant="table")
                or self.tree.elementInScope("thead", variant="table")
                or self.tree.elementInScope("tfoot", variant="table")
            ):
                self.clearStackToTableBodyContext()
                self.endTagTableRowGroup(self.impliedTagToken(self.tree.openElements[-1].name))
                return token
            else:
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processStartTag(token)

        def endTagTableRowGroup(self, token):
            """
            Perform the endTagTableRowGroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.endTagTableRowGroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope(token["name"], variant="table"):
                self.clearStackToTableBodyContext()
                self.tree.openElements.pop()
                self.parser.phase = self.parser.phases["inTable"]
            else:
                self.parser.parseError("unexpected-end-tag-in-table-body", {"name": token["name"]})

        def endTagTable(self, token):
            """
            Perform the endTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.endTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if (
                self.tree.elementInScope("tbody", variant="table")
                or self.tree.elementInScope("thead", variant="table")
                or self.tree.elementInScope("tfoot", variant="table")
            ):
                self.clearStackToTableBodyContext()
                self.endTagTableRowGroup(self.impliedTagToken(self.tree.openElements[-1].name))
                return token
            else:
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()

        def endTagIgnore(self, token):
            """
            Perform the endTagIgnore utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.endTagIgnore through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag-in-table-body", {"name": token["name"]})

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InTableBodyPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processEndTag(token)

    class InRowPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#in-row

        """
        Provide the InRowPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InRowPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InRowPhase state.

            Example:
                Exercise getPhases.InRowPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)
            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    (("td", "th"), self.startTagTableCell),
                    (
                        ("caption", "col", "colgroup", "tbody", "tfoot", "thead", "tr"),
                        self.startTagTableOther,
                    ),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    ("tr", self.endTagTr),
                    ("table", self.endTagTable),
                    (("tbody", "tfoot", "thead"), self.endTagTableRowGroup),
                    (
                        ("body", "caption", "col", "colgroup", "html", "td", "th"),
                        self.endTagIgnore,
                    ),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        # helper methods (XXX unify this with other table helper methods)
        def clearStackToTableRowContext(self):
            """
            Perform the clearStackToTableRowContext utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.clearStackToTableRowContext through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            while self.tree.openElements[-1].name not in ("tr", "html"):
                self.parser.parseError(
                    "unexpected-implied-end-tag-in-table-row",
                    {"name": self.tree.openElements[-1].name},
                )
                self.tree.openElements.pop()

        def ignoreEndTagTr(self):
            """
            Perform the ignoreEndTagTr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.ignoreEndTagTr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return not self.tree.elementInScope("tr", variant="table")

        # the rest
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.phases["inTable"].processEOF()

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processSpaceCharacters(token)

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processCharacters(token)

        def startTagTableCell(self, token):
            """
            Perform the startTagTableCell utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.startTagTableCell through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.clearStackToTableRowContext()
            self.tree.insertElement(token)
            self.parser.phase = self.parser.phases["inCell"]
            self.tree.activeFormattingElements.append(Marker)

        def startTagTableOther(self, token):
            """
            Perform the startTagTableOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.startTagTableOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            ignoreEndTag = self.ignoreEndTagTr()
            self.endTagTr(self.impliedTagToken("tr"))
            # XXX how are we sure it's always ignored in the innerHTML case?
            if not ignoreEndTag:
                return token

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processStartTag(token)

        def endTagTr(self, token):
            """
            Perform the endTagTr utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.endTagTr through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if not self.ignoreEndTagTr():
                self.clearStackToTableRowContext()
                self.tree.openElements.pop()
                self.parser.phase = self.parser.phases["inTableBody"]
            else:
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()

        def endTagTable(self, token):
            """
            Perform the endTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.endTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            ignoreEndTag = self.ignoreEndTagTr()
            self.endTagTr(self.impliedTagToken("tr"))
            # Reprocess the current tag if the tr end tag was not ignored
            # XXX how are we sure it's always ignored in the innerHTML case?
            if not ignoreEndTag:
                return token

        def endTagTableRowGroup(self, token):
            """
            Perform the endTagTableRowGroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.endTagTableRowGroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.tree.elementInScope(token["name"], variant="table"):
                self.endTagTr(self.impliedTagToken("tr"))
                return token
            else:
                self.parser.parseError()

        def endTagIgnore(self, token):
            """
            Perform the endTagIgnore utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.endTagIgnore through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag-in-table-row", {"name": token["name"]})

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InRowPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inTable"].processEndTag(token)

    class InCellPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#in-cell

        """
        Provide the InCellPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InCellPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InCellPhase state.

            Example:
                Exercise getPhases.InCellPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)
            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    (
                        (
                            "caption",
                            "col",
                            "colgroup",
                            "tbody",
                            "td",
                            "tfoot",
                            "th",
                            "thead",
                            "tr",
                        ),
                        self.startTagTableOther,
                    ),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    (("td", "th"), self.endTagTableCell),
                    (("body", "caption", "col", "colgroup", "html"), self.endTagIgnore),
                    (("table", "tbody", "tfoot", "thead", "tr"), self.endTagImply),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        # helper
        def closeCell(self):
            """
            Perform the closeCell utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.closeCell through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("td", variant="table"):
                self.endTagTableCell(self.impliedTagToken("td"))
            elif self.tree.elementInScope("th", variant="table"):
                self.endTagTableCell(self.impliedTagToken("th"))

        # the rest
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.phases["inBody"].processEOF()

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processCharacters(token)

        def startTagTableOther(self, token):
            """
            Perform the startTagTableOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.startTagTableOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.tree.elementInScope("td", variant="table") or self.tree.elementInScope("th", variant="table"):
                self.closeCell()
                return token
            else:
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def endTagTableCell(self, token):
            """
            Perform the endTagTableCell utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.endTagTableCell through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope(token["name"], variant="table"):
                self.tree.generateImpliedEndTags(token["name"])
                if self.tree.openElements[-1].name != token["name"]:
                    self.parser.parseError("unexpected-cell-end-tag", {"name": token["name"]})
                    while True:
                        node = self.tree.openElements.pop()
                        if node.name == token["name"]:
                            break
                else:
                    self.tree.openElements.pop()
                self.tree.clearActiveFormattingElements()
                self.parser.phase = self.parser.phases["inRow"]
            else:
                self.parser.parseError("unexpected-end-tag", {"name": token["name"]})

        def endTagIgnore(self, token):
            """
            Perform the endTagIgnore utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.endTagIgnore through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag", {"name": token["name"]})

        def endTagImply(self, token):
            """
            Perform the endTagImply utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.endTagImply through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.tree.elementInScope(token["name"], variant="table"):
                self.closeCell()
                return token
            else:
                # sometimes innerHTML case
                self.parser.parseError()

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InCellPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processEndTag(token)

    class InSelectPhase(Phase):
        """
        Provide the InSelectPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InSelectPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InSelectPhase state.

            Example:
                Exercise getPhases.InSelectPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    ("option", self.startTagOption),
                    ("optgroup", self.startTagOptgroup),
                    ("select", self.startTagSelect),
                    (("input", "keygen", "textarea"), self.startTagInput),
                    ("script", self.startTagScript),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    ("option", self.endTagOption),
                    ("optgroup", self.endTagOptgroup),
                    ("select", self.endTagSelect),
                ]
            )
            self.endTagHandler.default = self.endTagOther

        # http://www.whatwg.org/specs/web-apps/current-work/#in-select
        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name != "html":
                self.parser.parseError("eof-in-select")
            else:
                assert self.parser.innerHTML

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if token["data"] == "\u0000":
                return
            self.tree.insertText(token["data"])

        def startTagOption(self, token):
            # We need to imply </option> if <option> is the current node.
            """
            Perform the startTagOption utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.startTagOption through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name == "option":
                self.tree.openElements.pop()
            self.tree.insertElement(token)

        def startTagOptgroup(self, token):
            """
            Perform the startTagOptgroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.startTagOptgroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name == "option":
                self.tree.openElements.pop()
            if self.tree.openElements[-1].name == "optgroup":
                self.tree.openElements.pop()
            self.tree.insertElement(token)

        def startTagSelect(self, token):
            """
            Perform the startTagSelect utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.startTagSelect through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-select-in-select")
            self.endTagSelect(self.impliedTagToken("select"))

        def startTagInput(self, token):
            """
            Perform the startTagInput utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.startTagInput through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("unexpected-input-in-select")
            if self.tree.elementInScope("select", variant="select"):
                self.endTagSelect(self.impliedTagToken("select"))
                return token
            else:
                assert self.parser.innerHTML

        def startTagScript(self, token):
            """
            Perform the startTagScript utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.startTagScript through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inHead"].processStartTag(token)

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag-in-select", {"name": token["name"]})

        def endTagOption(self, token):
            """
            Perform the endTagOption utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.endTagOption through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name == "option":
                self.tree.openElements.pop()
            else:
                self.parser.parseError("unexpected-end-tag-in-select", {"name": "option"})

        def endTagOptgroup(self, token):
            # </optgroup> implicitly closes <option>
            """
            Perform the endTagOptgroup utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.endTagOptgroup through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name == "option" and self.tree.openElements[-2].name == "optgroup":
                self.tree.openElements.pop()
            # It also closes </optgroup>
            if self.tree.openElements[-1].name == "optgroup":
                self.tree.openElements.pop()
            # But nothing else
            else:
                self.parser.parseError("unexpected-end-tag-in-select", {"name": "optgroup"})

        def endTagSelect(self, token):
            """
            Perform the endTagSelect utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.endTagSelect through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.elementInScope("select", variant="select"):
                node = self.tree.openElements.pop()
                while node.name != "select":
                    node = self.tree.openElements.pop()
                self.parser.resetInsertionMode()
            else:
                # innerHTML case
                assert self.parser.innerHTML
                self.parser.parseError()

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag-in-select", {"name": token["name"]})

    class InSelectInTablePhase(Phase):
        """
        Provide the InSelectInTablePhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InSelectInTablePhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InSelectInTablePhase state.

            Example:
                Exercise getPhases.InSelectInTablePhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [
                    (
                        (
                            "caption",
                            "table",
                            "tbody",
                            "tfoot",
                            "thead",
                            "tr",
                            "td",
                            "th",
                        ),
                        self.startTagTable,
                    )
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher(
                [
                    (
                        (
                            "caption",
                            "table",
                            "tbody",
                            "tfoot",
                            "thead",
                            "tr",
                            "td",
                            "th",
                        ),
                        self.endTagTable,
                    )
                ]
            )
            self.endTagHandler.default = self.endTagOther

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectInTablePhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.phases["inSelect"].processEOF()

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectInTablePhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inSelect"].processCharacters(token)

        def startTagTable(self, token):
            """
            Perform the startTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectInTablePhase.startTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError(
                "unexpected-table-element-start-tag-in-select-in-table",
                {"name": token["name"]},
            )
            self.endTagOther(self.impliedTagToken("select"))
            return token

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectInTablePhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inSelect"].processStartTag(token)

        def endTagTable(self, token):
            """
            Perform the endTagTable utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectInTablePhase.endTagTable through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError(
                "unexpected-table-element-end-tag-in-select-in-table",
                {"name": token["name"]},
            )
            if self.tree.elementInScope(token["name"], variant="table"):
                self.endTagOther(self.impliedTagToken("select"))
                return token

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InSelectInTablePhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inSelect"].processEndTag(token)

    class InForeignContentPhase(Phase):
        """
        Provide the InForeignContentPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InForeignContentPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        breakoutElements = frozenset(
            [
                "b",
                "big",
                "blockquote",
                "body",
                "br",
                "center",
                "code",
                "dd",
                "div",
                "dl",
                "dt",
                "em",
                "embed",
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
                "head",
                "hr",
                "i",
                "img",
                "li",
                "listing",
                "menu",
                "meta",
                "nobr",
                "ol",
                "p",
                "pre",
                "ruby",
                "s",
                "small",
                "span",
                "strong",
                "strike",
                "sub",
                "sup",
                "table",
                "tt",
                "u",
                "ul",
                "var",
            ]
        )

        def __init__(self, parser, tree):
            """
            Initialize and validate the InForeignContentPhase state.

            Example:
                Exercise getPhases.InForeignContentPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

        def adjustSVGTagNames(self, token):
            """
            Perform the adjustSVGTagNames utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InForeignContentPhase.adjustSVGTagNames through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            replacements = {
                "altglyph": "altGlyph",
                "altglyphdef": "altGlyphDef",
                "altglyphitem": "altGlyphItem",
                "animatecolor": "animateColor",
                "animatemotion": "animateMotion",
                "animatetransform": "animateTransform",
                "clippath": "clipPath",
                "feblend": "feBlend",
                "fecolormatrix": "feColorMatrix",
                "fecomponenttransfer": "feComponentTransfer",
                "fecomposite": "feComposite",
                "feconvolvematrix": "feConvolveMatrix",
                "fediffuselighting": "feDiffuseLighting",
                "fedisplacementmap": "feDisplacementMap",
                "fedistantlight": "feDistantLight",
                "feflood": "feFlood",
                "fefunca": "feFuncA",
                "fefuncb": "feFuncB",
                "fefuncg": "feFuncG",
                "fefuncr": "feFuncR",
                "fegaussianblur": "feGaussianBlur",
                "feimage": "feImage",
                "femerge": "feMerge",
                "femergenode": "feMergeNode",
                "femorphology": "feMorphology",
                "feoffset": "feOffset",
                "fepointlight": "fePointLight",
                "fespecularlighting": "feSpecularLighting",
                "fespotlight": "feSpotLight",
                "fetile": "feTile",
                "feturbulence": "feTurbulence",
                "foreignobject": "foreignObject",
                "glyphref": "glyphRef",
                "lineargradient": "linearGradient",
                "radialgradient": "radialGradient",
                "textpath": "textPath",
            }

            if token["name"] in replacements:
                token["name"] = replacements[token["name"]]

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InForeignContentPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if token["data"] == "\u0000":
                token["data"] = "\uFFFD"
            elif self.parser.framesetOK and any(char not in spaceCharacters for char in token["data"]):
                self.parser.framesetOK = False
            Phase.processCharacters(self, token)

        def processStartTag(self, token):
            """
            Perform the processStartTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InForeignContentPhase.processStartTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            currentNode = self.tree.openElements[-1]
            if token["name"] in self.breakoutElements or (
                token["name"] == "font" and set(token["data"].keys()) & set(["color", "face", "size"])
            ):
                self.parser.parseError(
                    "unexpected-html-element-in-foreign-content",
                    {"name": token["name"]},
                )
                while (
                    self.tree.openElements[-1].namespace != self.tree.defaultNamespace
                    and not self.parser.isHTMLIntegrationPoint(self.tree.openElements[-1])
                    and not self.parser.isMathMLTextIntegrationPoint(self.tree.openElements[-1])
                ):
                    self.tree.openElements.pop()
                return token

            else:
                if currentNode.namespace == namespaces["mathml"]:
                    self.parser.adjustMathMLAttributes(token)
                elif currentNode.namespace == namespaces["svg"]:
                    self.adjustSVGTagNames(token)
                    self.parser.adjustSVGAttributes(token)
                self.parser.adjustForeignAttributes(token)
                token["namespace"] = currentNode.namespace
                self.tree.insertElement(token)
                if token["selfClosing"]:
                    self.tree.openElements.pop()
                    token["selfClosingAcknowledged"] = True

        def processEndTag(self, token):
            """
            Perform the processEndTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InForeignContentPhase.processEndTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            nodeIndex = len(self.tree.openElements) - 1
            node = self.tree.openElements[-1]
            if node.name != token["name"]:
                self.parser.parseError("unexpected-end-tag", {"name": token["name"]})

            while True:
                if node.name.translate(asciiUpper2Lower) == token["name"]:
                    # XXX this isn't in the spec but it seems necessary
                    if self.parser.phase == self.parser.phases["inTableText"]:
                        self.parser.phase.flushCharacters()
                        self.parser.phase = self.parser.phase.originalPhase
                    while self.tree.openElements.pop() != node:
                        assert self.tree.openElements
                    new_token = None
                    break
                nodeIndex -= 1

                node = self.tree.openElements[nodeIndex]
                if node.namespace != self.tree.defaultNamespace:
                    continue
                else:
                    new_token = self.parser.phase.processEndTag(token)
                    break
            return new_token

    class AfterBodyPhase(Phase):
        """
        Provide the AfterBodyPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.AfterBodyPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the AfterBodyPhase state.

            Example:
                Exercise getPhases.AfterBodyPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher([("html", self.startTagHtml)])
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher([("html", self.endTagHtml)])
            self.endTagHandler.default = self.endTagOther

        def processEOF(self):
            # Stop parsing
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterBodyPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass

        def processComment(self, token):
            # This is needed because data is to be appended to the <html> element
            # here and not to whatever is currently open.
            """
            Perform the processComment utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterBodyPhase.processComment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertComment(token, self.tree.openElements[0])

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterBodyPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("unexpected-char-after-body")
            self.parser.phase = self.parser.phases["inBody"]
            return token

        def startTagHtml(self, token):
            """
            Perform the startTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterBodyPhase.startTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterBodyPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("unexpected-start-tag-after-body", {"name": token["name"]})
            self.parser.phase = self.parser.phases["inBody"]
            return token

        def endTagHtml(self, name):
            """
            Perform the endTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterBodyPhase.endTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.parser.innerHTML:
                self.parser.parseError("unexpected-end-tag-after-body-innerhtml")
            else:
                self.parser.phase = self.parser.phases["afterAfterBody"]

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterBodyPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("unexpected-end-tag-after-body", {"name": token["name"]})
            self.parser.phase = self.parser.phases["inBody"]
            return token

    class InFramesetPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#in-frameset

        """
        Provide the InFramesetPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.InFramesetPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the InFramesetPhase state.

            Example:
                Exercise getPhases.InFramesetPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [
                    ("html", self.startTagHtml),
                    ("frameset", self.startTagFrameset),
                    ("frame", self.startTagFrame),
                    ("noframes", self.startTagNoframes),
                ]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher([("frameset", self.endTagFrameset)])
            self.endTagHandler.default = self.endTagOther

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name != "html":
                self.parser.parseError("eof-in-frameset")
            else:
                assert self.parser.innerHTML

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-char-in-frameset")

        def startTagFrameset(self, token):
            """
            Perform the startTagFrameset utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.startTagFrameset through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)

        def startTagFrame(self, token):
            """
            Perform the startTagFrame utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.startTagFrame through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertElement(token)
            self.tree.openElements.pop()

        def startTagNoframes(self, token):
            """
            Perform the startTagNoframes utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.startTagNoframes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag-in-frameset", {"name": token["name"]})

        def endTagFrameset(self, token):
            """
            Perform the endTagFrameset utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.endTagFrameset through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.tree.openElements[-1].name == "html":
                # innerHTML case
                self.parser.parseError("unexpected-frameset-in-frameset-innerhtml")
            else:
                self.tree.openElements.pop()
            if not self.parser.innerHTML and self.tree.openElements[-1].name != "frameset":
                # If we're not in innerHTML mode and the the current node is not a
                # "frameset" element (anymore) then switch.
                self.parser.phase = self.parser.phases["afterFrameset"]

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.InFramesetPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag-in-frameset", {"name": token["name"]})

    class AfterFramesetPhase(Phase):
        # http://www.whatwg.org/specs/web-apps/current-work/#after3

        """
        Provide the AfterFramesetPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.AfterFramesetPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the AfterFramesetPhase state.

            Example:
                Exercise getPhases.AfterFramesetPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [("html", self.startTagHtml), ("noframes", self.startTagNoframes)]
            )
            self.startTagHandler.default = self.startTagOther

            self.endTagHandler = utils.MethodDispatcher([("html", self.endTagHtml)])
            self.endTagHandler.default = self.endTagOther

        def processEOF(self):
            # Stop parsing
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterFramesetPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterFramesetPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-char-after-frameset")

        def startTagNoframes(self, token):
            """
            Perform the startTagNoframes utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterFramesetPhase.startTagNoframes through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inHead"].processStartTag(token)

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterFramesetPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-start-tag-after-frameset", {"name": token["name"]})

        def endTagHtml(self, token):
            """
            Perform the endTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterFramesetPhase.endTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.phase = self.parser.phases["afterAfterFrameset"]

        def endTagOther(self, token):
            """
            Perform the endTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterFramesetPhase.endTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("unexpected-end-tag-after-frameset", {"name": token["name"]})

    class AfterAfterBodyPhase(Phase):
        """
        Provide the AfterAfterBodyPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.AfterAfterBodyPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the AfterAfterBodyPhase state.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher([("html", self.startTagHtml)])
            self.startTagHandler.default = self.startTagOther

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass

        def processComment(self, token):
            """
            Perform the processComment utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.processComment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertComment(token, self.tree.document)

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processSpaceCharacters(token)

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("expected-eof-but-got-char")
            self.parser.phase = self.parser.phases["inBody"]
            return token

        def startTagHtml(self, token):
            """
            Perform the startTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.startTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("expected-eof-but-got-start-tag", {"name": token["name"]})
            self.parser.phase = self.parser.phases["inBody"]
            return token

        def processEndTag(self, token):
            """
            Perform the processEndTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterBodyPhase.processEndTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.parser.parseError("expected-eof-but-got-end-tag", {"name": token["name"]})
            self.parser.phase = self.parser.phases["inBody"]
            return token

    class AfterAfterFramesetPhase(Phase):
        """
        Provide the AfterAfterFramesetPhase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise getPhases.AfterAfterFramesetPhase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, parser, tree):
            """
            Initialize and validate the AfterAfterFramesetPhase state.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param parser: Value supplied for parser under the utility contract.
            :param tree: Value supplied for tree under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            Phase.__init__(self, parser, tree)

            self.startTagHandler = utils.MethodDispatcher(
                [("html", self.startTagHtml), ("noframes", self.startTagNoFrames)]
            )
            self.startTagHandler.default = self.startTagOther

        def processEOF(self):
            """
            Perform the processEOF utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.processEOF through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            pass

        def processComment(self, token):
            """
            Perform the processComment utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.processComment through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.tree.insertComment(token, self.tree.document)

        def processSpaceCharacters(self, token):
            """
            Perform the processSpaceCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.processSpaceCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processSpaceCharacters(token)

        def processCharacters(self, token):
            """
            Perform the processCharacters utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.processCharacters through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("expected-eof-but-got-char")

        def startTagHtml(self, token):
            """
            Perform the startTagHtml utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.startTagHtml through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inBody"].processStartTag(token)

        def startTagNoFrames(self, token):
            """
            Perform the startTagNoFrames utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.startTagNoFrames through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.parser.phases["inHead"].processStartTag(token)

        def startTagOther(self, token):
            """
            Perform the startTagOther utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.startTagOther through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("expected-eof-but-got-start-tag", {"name": token["name"]})

        def processEndTag(self, token):
            """
            Perform the processEndTag utility operation under explicit compatibility rules.

            Example:
                Exercise getPhases.AfterAfterFramesetPhase.processEndTag through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param token: Value supplied for token under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.parser.parseError("expected-eof-but-got-end-tag", {"name": token["name"]})

    return {
        "initial": InitialPhase,
        "beforeHtml": BeforeHtmlPhase,
        "beforeHead": BeforeHeadPhase,
        "inHead": InHeadPhase,
        # XXX "inHeadNoscript": InHeadNoScriptPhase,
        "afterHead": AfterHeadPhase,
        "inBody": InBodyPhase,
        "text": TextPhase,
        "inTable": InTablePhase,
        "inTableText": InTableTextPhase,
        "inCaption": InCaptionPhase,
        "inColumnGroup": InColumnGroupPhase,
        "inTableBody": InTableBodyPhase,
        "inRow": InRowPhase,
        "inCell": InCellPhase,
        "inSelect": InSelectPhase,
        "inSelectInTable": InSelectInTablePhase,
        "inForeignContent": InForeignContentPhase,
        "afterBody": AfterBodyPhase,
        "inFrameset": InFramesetPhase,
        "afterFrameset": AfterFramesetPhase,
        "afterAfterBody": AfterAfterBodyPhase,
        "afterAfterFrameset": AfterAfterFramesetPhase,
        # XXX after after frameset
    }


def adjust_attributes(token, replacements):
    """
    Perform the adjust attributes utility operation under explicit compatibility rules.

    Example:
        Exercise adjust attributes through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param token: Value supplied for token under the utility contract.
    :param replacements: Value supplied for replacements under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if set(token["data"]) & set(replacements):
        token["data"] = OrderedDict((replacements.get(k, k), v) for k, v in token["data"].items())


class ParseError(Exception):

    """
    Error in parsed document

    Example:
        Exercise ParseError through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    pass
