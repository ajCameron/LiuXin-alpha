"""
Transform the parsed Markdown element tree before serialization.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise treeprocessors through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import annotations

import typing as _typing
from . import util
from . import odict
from . import inlinepatterns


def build_treeprocessors(md_instance: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
    """
    Build the default treeprocessors for Markdown.

    Example:
        Exercise build treeprocessors through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param md_instance: Value supplied for md instance under the utility contract.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    treeprocessors = odict.OrderedDict()
    treeprocessors["inline"] = InlineProcessor(md_instance)
    treeprocessors["prettify"] = PrettifyTreeprocessor(md_instance)
    return treeprocessors


def isString(s: _typing.Any) -> _typing.Any:
    """
    Check if it's string

    Example:
        Exercise isString through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not isinstance(s, util.AtomicString):
        return isinstance(s, util.string_type)
    return False


class Treeprocessor(util.Processor):
    """
    Treeprocessors are run on the ElementTree object before serialization.

    Example:
        Exercise Treeprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def run(self: _typing.Self, root: _typing.Any) -> None:
        """
        Subclasses of Treeprocessor should implement a `run` method, which takes a root ElementTree. This method can return another ElementTree object, and the existing root ElementTree will be replaced, or it can modify the current tree and return None.

        Example:
            Exercise Treeprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


class InlineProcessor(Treeprocessor):
    """
    A Treeprocessor that traverses a tree, applying inline patterns.

    Example:
        Exercise InlineProcessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, md: _typing.Any) -> None:
        """
        Initialize and validate the inlineprocessor state.

        Example:
            Exercise InlineProcessor.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.__placeholder_prefix = util.INLINE_PLACEHOLDER_PREFIX
        self.__placeholder_suffix = util.ETX
        self.__placeholder_length = 4 + len(self.__placeholder_prefix) + len(self.__placeholder_suffix)
        self.__placeholder_re = util.INLINE_PLACEHOLDER_RE
        self.markdown = md

    def __makePlaceholder(self: _typing.Self, type: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Generate a placeholder

        Example:
            Exercise InlineProcessor.  makePlaceholder through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        id = "%04d" % len(self.stashed_nodes)
        hash = util.INLINE_PLACEHOLDER % id
        return hash, id

    def __findPlaceholder(self: _typing.Self, data: _typing.Any, index: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Extract id from data string, start from index

        Example:
            Exercise InlineProcessor.  findPlaceholder through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        m = self.__placeholder_re.search(data, index)
        if m:
            return m.group(1), m.end()
        else:
            return None, index + 1

    def __stashNode(self: _typing.Self, node: _typing.Any, type: _typing.Any) -> _typing.Any:
        """
        Add node to stash

        Example:
            Exercise InlineProcessor.  stashNode through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param node: Value supplied for node under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        placeholder, id = self.__makePlaceholder(type)
        self.stashed_nodes[id] = node
        return placeholder

    def __handleInline(self: _typing.Self, data: _typing.Any, patternIndex: int = 0) -> _typing.Any:
        """
        Process string with inline patterns and replace it with placeholders

        Example:
            Exercise InlineProcessor.  handleInline through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param patternIndex: Value supplied for patternIndex under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(data, util.AtomicString):
            startIndex = 0
            while patternIndex < len(self.markdown.inlinePatterns):
                data, matched, startIndex = self.__applyPattern(
                    self.markdown.inlinePatterns.value_for_index(patternIndex),
                    data,
                    patternIndex,
                    startIndex,
                )
                if not matched:
                    patternIndex += 1
        return data

    def __processElementText(self: _typing.Self, node: _typing.Any, subnode: _typing.Any, isText: bool = True) -> None:
        """
        Process placeholders in Element.text or Element.tail of Elements popped from self.stashed_nodes.

        Example:
            Exercise InlineProcessor.  processElementText through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param node: Value supplied for node under the utility contract.
        :param subnode: Value supplied for subnode under the utility contract.
        :param isText: Value supplied for isText under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if isText:
            text = subnode.text
            subnode.text = None
        else:
            text = subnode.tail
            subnode.tail = None

        childResult = self.__processPlaceholders(text, subnode)

        if not isText and node is not subnode:
            pos = list(node).index(subnode)
            node.remove(subnode)
        else:
            pos = 0

        childResult.reverse()
        for newChild in childResult:
            node.insert(pos, newChild)

    def __processPlaceholders(self: _typing.Self, data: _typing.Any, parent: _typing.Any) -> _typing.Any:
        """
        Process string with placeholders and generate ElementTree tree.

        Example:
            Exercise InlineProcessor.  processPlaceholders through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        def linkText(text: _typing.Any) -> None:
            """
            Perform the linkText operation under explicit file-format and conversion rules.

            Example:
                Exercise InlineProcessor.  processPlaceholders.linkText through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :param text: Text parsed, normalized or rendered.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if text:
                if result:
                    if result[-1].tail:
                        result[-1].tail += text
                    else:
                        result[-1].tail = text
                else:
                    if parent.text:
                        parent.text += text
                    else:
                        parent.text = text

        result = []
        strartIndex = 0
        while data:
            index = data.find(self.__placeholder_prefix, strartIndex)
            if index != -1:
                id, phEndIndex = self.__findPlaceholder(data, index)

                if id in self.stashed_nodes:
                    node = self.stashed_nodes.get(id)

                    if index > 0:
                        text = data[strartIndex:index]
                        linkText(text)

                    if not isString(node):  # it's Element
                        for child in [node] + list(node):
                            if child.tail:
                                if child.tail.strip():
                                    self.__processElementText(node, child, False)
                            if child.text:
                                if child.text.strip():
                                    self.__processElementText(child, child)
                    else:  # it's just a string
                        linkText(node)
                        strartIndex = phEndIndex
                        continue

                    strartIndex = phEndIndex
                    result.append(node)

                else:  # wrong placeholder
                    end = index + len(self.__placeholder_prefix)
                    linkText(data[strartIndex:end])
                    strartIndex = end
            else:
                text = data[strartIndex:]
                if isinstance(data, util.AtomicString):
                    # We don't want to loose the AtomicString
                    text = util.AtomicString(text)
                linkText(text)
                data = ""

        return result

    def __applyPattern(self: _typing.Self, pattern: _typing.Any, data: _typing.Any, patternIndex: _typing.Any, startIndex: int = 0) -> tuple[_typing.Any, ...]:
        """
        Check if the line fits the pattern, create the necessary elements, add it to stashed_nodes.

        Example:
            Exercise InlineProcessor.  applyPattern through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param pattern: Value supplied for pattern under the utility contract.
        :param data: Value supplied for data under the utility contract.
        :param patternIndex: Value supplied for patternIndex under the utility contract.
        :param startIndex: Value supplied for startIndex under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        match = pattern.getCompiledRegExp().match(data[startIndex:])
        leftData = data[:startIndex]

        if not match:
            return data, False, 0

        node = pattern.handleMatch(match)

        if node is None:
            return data, True, len(leftData) + match.span(len(match.groups()))[0]

        if not isString(node):
            if not isinstance(node.text, util.AtomicString):
                # We need to process current node too
                for child in [node] + list(node):
                    if not isString(node):
                        if child.text:
                            child.text = self.__handleInline(child.text, patternIndex + 1)
                        if child.tail:
                            child.tail = self.__handleInline(child.tail, patternIndex)

        placeholder = self.__stashNode(node, pattern.type())

        return (
            "%s%s%s%s" % (leftData, match.group(1), placeholder, match.groups()[-1]),
            True,
            0,
        )

    def run(self: _typing.Self, tree: _typing.Any) -> _typing.Any:
        """
        Apply inline patterns to a parsed Markdown tree.

        Example:
            Exercise InlineProcessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param tree: Value supplied for tree under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.stashed_nodes = {}

        stack = [tree]

        while stack:
            currElement = stack.pop()
            insertQueue = []
            for child in list(currElement):
                if child.text and not isinstance(child.text, util.AtomicString):
                    text = child.text
                    child.text = None
                    lst = self.__processPlaceholders(self.__handleInline(text), child)
                    stack += lst
                    insertQueue.append((child, lst))
                if child.tail:
                    tail = self.__handleInline(child.tail)
                    dumby = util.etree.Element("d")
                    tailResult = self.__processPlaceholders(tail, dumby)
                    if dumby.text:
                        child.tail = dumby.text
                    else:
                        child.tail = None
                    pos = list(currElement).index(child) + 1
                    tailResult.reverse()
                    for newChild in tailResult:
                        currElement.insert(pos, newChild)
                if len(child):
                    stack.append(child)

            for element, lst in insertQueue:
                if self.markdown.enable_attributes:
                    if element.text and isString(element.text):
                        element.text = inlinepatterns.handleAttributes(element.text, element)
                i = 0
                for newChild in lst:
                    if self.markdown.enable_attributes:
                        # Processing attributes
                        if newChild.tail and isString(newChild.tail):
                            newChild.tail = inlinepatterns.handleAttributes(newChild.tail, element)
                        if newChild.text and isString(newChild.text):
                            newChild.text = inlinepatterns.handleAttributes(newChild.text, newChild)
                    element.insert(i, newChild)
                    i += 1
        return tree


class PrettifyTreeprocessor(Treeprocessor):
    """
    Add linebreaks to the html document.

    Example:
        Exercise PrettifyTreeprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def _prettifyETree(self: _typing.Self, elem: _typing.Any) -> None:
        """
        Recursively add linebreaks to ElementTree children.

        Example:
            Exercise PrettifyTreeprocessor. prettifyETree through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        i = "\n"
        if util.isBlockLevel(elem.tag) and elem.tag not in ["code", "pre"]:
            if (not elem.text or not elem.text.strip()) and len(elem) and util.isBlockLevel(elem[0].tag):
                elem.text = i
            for e in elem:
                if util.isBlockLevel(e.tag):
                    self._prettifyETree(e)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        if not elem.tail or not elem.tail.strip():
            elem.tail = i

    def run(self: _typing.Self, root: _typing.Any) -> None:
        """
        Add linebreaks to ElementTree root object.

        Example:
            Exercise PrettifyTreeprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self._prettifyETree(root)
        # Do <br />'s seperately as they are often in the middle of
        # inline content and missed by _prettifyETree.
        brs = root.iter("br")
        for br in brs:
            if not br.tail or not br.tail.strip():
                br.tail = "\n"
            else:
                br.tail = "\n%s" % br.tail
        # Clean up extra empty lines at end of code blocks.
        pres = root.iter("pre")
        for pre in pres:
            if len(pre) and pre[0].tag == "code":
                pre[0].text = (pre[0].text or "").rstrip() + "\n"
