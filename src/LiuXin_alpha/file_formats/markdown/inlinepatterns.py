"""
Recognize and transform Markdown inline syntax, links and emphasis.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise inlinepatterns through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
INLINE PATTERNS
=============================================================================

Inline patterns such as *emphasis* are handled by means of auxiliary
objects, one per pattern.  Pattern objects must be instances of classes
that extend markdown.Pattern.  Each pattern object uses a single regular
expression and needs support the following methods:

    pattern.getCompiledRegExp() # returns a regular expression

    pattern.handleMatch(m) # takes a match object and returns
                           # an ElementTree element or just plain text

All of python markdown's built-in patterns subclass from Pattern,
but you can add additional patterns that don't.

Also note that all the regular expressions used by inline must
capture the whole block.  For this reason, they all start with
'^(.*)' and end with '(.*)!'.  In case with built-in expression
Pattern takes care of adding the "^(.*)" and "(.*)!".

Finally, the order in which regular expressions are applied is very
important - e.g. if we first replace http://.../ links with <a> tags
and _then_ try to replace inline html, we would end up with a mess.
So, we apply the expressions in the following order:

* escape and backticks have to go before everything else, so
  that we can preempt any markdown patterns by escaping them.

* then we handle auto-links (must be done before inline html)

* then we handle inline HTML.  At this point we will simply
  replace all inline HTML strings with a placeholder and add
  the actual HTML to a hash.

* then inline images (must be done before links)

* then bracketed links, first regular then reference-style

* finally we apply strong and emphasis
"""

from . import util
from . import odict

import re

# Py2/Py3 compatibility stuff
try:
    from urllib.parse import urlparse, urlunparse
except ImportError:
    from urlparse import urlparse, urlunparse
try:
    from html import entities
except ImportError:
    import htmlentitydefs as entities


def build_inlinepatterns(md_instance: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
    """
    Build the default set of inline patterns for Markdown.

    Example:
        Exercise build inlinepatterns through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param md_instance: Value supplied for md instance under the utility contract.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    inlinePatterns = odict.OrderedDict()
    inlinePatterns["backtick"] = BacktickPattern(BACKTICK_RE)
    inlinePatterns["escape"] = EscapePattern(ESCAPE_RE, md_instance)
    inlinePatterns["reference"] = ReferencePattern(REFERENCE_RE, md_instance)
    inlinePatterns["link"] = LinkPattern(LINK_RE, md_instance)
    inlinePatterns["image_link"] = ImagePattern(IMAGE_LINK_RE, md_instance)
    inlinePatterns["image_reference"] = ImageReferencePattern(IMAGE_REFERENCE_RE, md_instance)
    inlinePatterns["short_reference"] = ReferencePattern(SHORT_REF_RE, md_instance)
    inlinePatterns["autolink"] = AutolinkPattern(AUTOLINK_RE, md_instance)
    inlinePatterns["automail"] = AutomailPattern(AUTOMAIL_RE, md_instance)
    inlinePatterns["linebreak"] = SubstituteTagPattern(LINE_BREAK_RE, "br")
    if md_instance.safeMode != "escape":
        inlinePatterns["html"] = HtmlPattern(HTML_RE, md_instance)
    inlinePatterns["entity"] = HtmlPattern(ENTITY_RE, md_instance)
    inlinePatterns["not_strong"] = SimpleTextPattern(NOT_STRONG_RE)
    inlinePatterns["strong_em"] = DoubleTagPattern(STRONG_EM_RE, "strong,em")
    inlinePatterns["strong"] = SimpleTagPattern(STRONG_RE, "strong")
    inlinePatterns["emphasis"] = SimpleTagPattern(EMPHASIS_RE, "em")
    if md_instance.smart_emphasis:
        inlinePatterns["emphasis2"] = SimpleTagPattern(SMART_EMPHASIS_RE, "em")
    else:
        inlinePatterns["emphasis2"] = SimpleTagPattern(EMPHASIS_2_RE, "em")
    return inlinePatterns


"""
The actual regular expressions for patterns
-----------------------------------------------------------------------------
"""

NOBRACKET = r"[^\]\[]*"
BRK = r"\[(" + (NOBRACKET + r"(\[") * 6 + (NOBRACKET + r"\])*") * 6 + NOBRACKET + r")\]"
NOIMG = r"(?<!\!)"

BACKTICK_RE = r"(?<!\\)(`+)(.+?)(?<!`)\2(?!`)"  # `e=f()` or ``e=f("`")``
ESCAPE_RE = r"\\(.)"  # \<
EMPHASIS_RE = r"(\*)([^\*]+)\2"  # *emphasis*
STRONG_RE = r"(\*{2}|_{2})(.+?)\2"  # **strong**
STRONG_EM_RE = r"(\*{3}|_{3})(.+?)\2"  # ***strong***
SMART_EMPHASIS_RE = r"(?<!\w)(_)(?!_)(.+?)(?<!_)\2(?!\w)"  # _smart_emphasis_
EMPHASIS_2_RE = r"(_)(.+?)\2"  # _emphasis_
LINK_RE = NOIMG + BRK + r"""\(\s*(<.*?>|((?:(?:\(.*?\))|[^\(\)]))*?)\s*((['"])(.*?)\12\s*)?\)"""
# [text](url) or [text](<url>) or [text](url "title")

IMAGE_LINK_RE = r"\!" + BRK + r"\s*\((<.*?>|([^\)]*))\)"
# ![alttxt](http://x.com/) or ![alttxt](<http://x.com/>)
REFERENCE_RE = NOIMG + BRK + r"\s?\[([^\]]*)\]"  # [Google][3]
SHORT_REF_RE = NOIMG + r"\[([^\]]+)\]"  # [Google]
IMAGE_REFERENCE_RE = r"\!" + BRK + r"\s?\[([^\]]*)\]"  # ![alt text][2]
NOT_STRONG_RE = r"((^| )(\*|_)( |$))"  # stand-alone * or _
AUTOLINK_RE = r"<((?:[Ff]|[Hh][Tt])[Tt][Pp][Ss]?://[^>]*)>"  # <http://www.123.com>
AUTOMAIL_RE = r"<([^> \!]*@[^> ]*)>"  # <me@example.com>

HTML_RE = r"(\<([a-zA-Z/][^\>]*?|\!--.*?--)\>)"  # <...>
ENTITY_RE = r"(&[\#a-zA-Z0-9]*;)"  # &amp;
LINE_BREAK_RE = r"  \n"  # two spaces at end of line


def dequote(string: _typing.Any) -> _typing.Any:
    """
    Remove quotes from around a string.

    Example:
        Exercise dequote through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param string: Value supplied for string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if (string.startswith('"') and string.endswith('"')) or (string.startswith("'") and string.endswith("'")):
        return string[1:-1]
    else:
        return string


ATTR_RE = re.compile(r"\{@([^\}]*)=([^\}]*)}")  # {@id=123}


def handleAttributes(text: _typing.Any, parent: _typing.Any) -> _typing.Any:
    """
    Set values of an element based on attribute definitions ({@id=123}).

    Example:
        Exercise handleAttributes through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param text: Text parsed, normalized or rendered.
    :param parent: Value supplied for parent under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    def attributeCallback(match: _typing.Any) -> None:
        """
        Perform the attributeCallback operation under explicit file-format and conversion rules.

        Example:
            Exercise handleAttributes.attributeCallback through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param match: Value supplied for match under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        parent.set(match.group(1), match.group(2).replace("\n", " "))

    return ATTR_RE.sub(attributeCallback, text)


"""
The pattern classes
-----------------------------------------------------------------------------
"""


class Pattern(object):
    """
    Base class that inline patterns subclass.

    Example:
        Exercise Pattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, pattern: _typing.Any, markdown_instance: _typing.Any = None) -> None:
        """
        Create an instant of an inline pattern.

        Example:
            Exercise Pattern.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param pattern: Value supplied for pattern under the utility contract.
        :param markdown_instance: Value supplied for markdown instance under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.pattern = pattern
        self.compiled_re = re.compile("^(.*?)%s(.*?)$" % pattern, re.DOTALL | re.UNICODE)

        # Api for Markdown to pass safe_mode into instance
        self.safe_mode = False
        if markdown_instance:
            self.markdown = markdown_instance

    def getCompiledRegExp(self: _typing.Self) -> _typing.Any:
        """
        Return a compiled regular expression.

        Example:
            Exercise Pattern.getCompiledRegExp through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.compiled_re

    def handleMatch(self: _typing.Self, m: _typing.Any) -> None:
        """
        Return a ElementTree element from the given match.

        Example:
            Exercise Pattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def type(self: _typing.Self) -> _typing.Any:
        """
        Return class name, to define pattern type

        Example:
            Exercise Pattern.type through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__class__.__name__

    def unescape(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Return unescaped text given text with an inline placeholder.

        Example:
            Exercise Pattern.unescape through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: An iterator yielding the normalized values described above.
        """
        try:
            stash = self.markdown.treeprocessors["inline"].stashed_nodes
        except KeyError:
            return text

        def itertext(el: _typing.Any) -> _typing.Iterator[_typing.Any]:
            """
            Reimplement Element.itertext for older python versions

            Example:
                Exercise Pattern.unescape.itertext through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :param el: Value supplied for el under the utility contract.
            :return: An iterator yielding the normalized values described above.
            """
            tag = el.tag
            if not isinstance(tag, util.string_type) and tag is not None:
                return
            if el.text:
                yield el.text
            for e in el:
                for s in itertext(e):
                    yield s
                if e.tail:
                    yield e.tail

        def get_stash(m: _typing.Any) -> _typing.Any:
            """
            Return stash under the format's safety and compatibility rules.

            Example:
                Exercise Pattern.unescape.get stash through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :param m: Value supplied for m under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            id = m.group(1)
            if id in stash:
                value = stash.get(id)
                if isinstance(value, util.string_type):
                    return value
                else:
                    # An etree Element - return text content only
                    return "".join(itertext(value))

        return util.INLINE_PLACEHOLDER_RE.sub(get_stash, text)


class SimpleTextPattern(Pattern):
    """
    Return a simple text of group(2) of a Pattern.

    Example:
        Exercise SimpleTextPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise SimpleTextPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = m.group(2)
        if text == util.INLINE_PLACEHOLDER_PREFIX:
            return None
        return text


class EscapePattern(Pattern):
    """
    Return an escaped character.

    Example:
        Exercise EscapePattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise EscapePattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        char = m.group(2)
        if char in self.markdown.ESCAPED_CHARS:
            return "%s%s%s" % (util.STX, ord(char), util.ETX)
        else:
            return "\\%s" % char


class SimpleTagPattern(Pattern):
    """
    Return element of type `tag` with a text attribute of group(3) of a Pattern.

    Example:
        Exercise SimpleTagPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, pattern: _typing.Any, tag: _typing.Any) -> None:
        """
        Initialize and validate the simpletagpattern state.

        Example:
            Exercise SimpleTagPattern.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param pattern: Value supplied for pattern under the utility contract.
        :param tag: Value supplied for tag under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Pattern.__init__(self, pattern)
        self.tag = tag

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise SimpleTagPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element(self.tag)
        el.text = m.group(3)
        return el


class SubstituteTagPattern(SimpleTagPattern):
    """
    Return an element of type `tag` with no children.

    Example:
        Exercise SubstituteTagPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise SubstituteTagPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return util.etree.Element(self.tag)


class BacktickPattern(Pattern):
    """
    Return a `<code>` element containing the matching text.

    Example:
        Exercise BacktickPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __init__(self: _typing.Self, pattern: _typing.Any) -> None:
        """
        Initialize and validate the backtickpattern state.

        Example:
            Exercise BacktickPattern.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param pattern: Value supplied for pattern under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Pattern.__init__(self, pattern)
        self.tag = "code"

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise BacktickPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element(self.tag)
        el.text = util.AtomicString(m.group(3).strip())
        return el


class DoubleTagPattern(SimpleTagPattern):
    """
    Return a ElementTree element nested in tag2 nested in tag1.

    Example:
        Exercise DoubleTagPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise DoubleTagPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tag1, tag2 = self.tag.split(",")
        el1 = util.etree.Element(tag1)
        el2 = util.etree.SubElement(el1, tag2)
        el2.text = m.group(3)
        return el1


class HtmlPattern(Pattern):
    """
    Store raw inline html and return a placeholder.

    Example:
        Exercise HtmlPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise HtmlPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rawhtml = self.unescape(m.group(2))
        place_holder = self.markdown.htmlStash.store(rawhtml)
        return place_holder

    def unescape(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Return unescaped text given text with an inline placeholder.

        Example:
            Exercise HtmlPattern.unescape through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            stash = self.markdown.treeprocessors["inline"].stashed_nodes
        except KeyError:
            return text

        def get_stash(m: _typing.Any) -> _typing.Any:
            """
            Return stash under the format's safety and compatibility rules.

            Example:
                Exercise HtmlPattern.unescape.get stash through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :param m: Value supplied for m under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            id = m.group(1)
            value = stash.get(id)
            if value is not None:
                try:
                    return self.markdown.serializer(value)
                except:
                    return "\\%s" % value

        return util.INLINE_PLACEHOLDER_RE.sub(get_stash, text)


class LinkPattern(Pattern):
    """
    Return a link element from the given match.

    Example:
        Exercise LinkPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise LinkPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element("a")
        el.text = m.group(2)
        title = m.group(13)
        href = m.group(9)

        if href:
            if href[0] == "<":
                href = href[1:-1]
            el.set("href", self.sanitize_url(self.unescape(href.strip())))
        else:
            el.set("href", "")

        if title:
            title = dequote(self.unescape(title))
            el.set("title", title)
        return el

    def sanitize_url(self: _typing.Self, url: _typing.Any) -> _typing.Any:
        """
        Sanitize a url against xss attacks in "safe_mode".

        Example:
            Exercise LinkPattern.sanitize url through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param url: Value supplied for url under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        url = url.replace(" ", "%20")
        if not self.markdown.safeMode:
            # Return immediately bipassing parsing.
            return url

        try:
            scheme, netloc, path, params, query, fragment = url = urlparse(url)
        except ValueError:
            # Bad url - so bad it couldn't be parsed.
            return ""

        locless_schemes = ["", "mailto", "news"]
        allowed_schemes = locless_schemes + ["http", "https", "ftp", "ftps"]
        if scheme not in allowed_schemes:
            # Not a known (allowed) scheme. Not safe.
            return ""

        if netloc == "" and scheme not in locless_schemes:
            # This should not happen. Treat as suspect.
            return ""

        for part in url[2:]:
            if ":" in part:
                # A colon in "path", "parameters", "query" or "fragment" is suspect.
                return ""

        # Url passes all tests. Return url as-is.
        return urlunparse(url)


class ImagePattern(LinkPattern):
    """
    Return a img element from the given match.

    Example:
        Exercise ImagePattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise ImagePattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element("img")
        src_parts = m.group(9).split()
        if src_parts:
            src = src_parts[0]
            if src[0] == "<" and src[-1] == ">":
                src = src[1:-1]
            el.set("src", self.sanitize_url(self.unescape(src)))
        else:
            el.set("src", "")
        if len(src_parts) > 1:
            el.set("title", dequote(self.unescape(" ".join(src_parts[1:]))))

        if self.markdown.enable_attributes:
            truealt = handleAttributes(m.group(2), el)
        else:
            truealt = m.group(2)

        el.set("alt", self.unescape(truealt))
        return el


class ReferencePattern(LinkPattern):
    """
    Match to a stored reference and return link element.

    Example:
        Exercise ReferencePattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    NEWLINE_CLEANUP_RE = re.compile(r"[ ]?\n", re.MULTILINE)

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise ReferencePattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            id = m.group(9).lower()
        except IndexError:
            id = None
        if not id:
            # if we got something like "[Google][]" or "[Goggle]"
            # we'll use "google" as the id
            id = m.group(2).lower()

        # Clean up linebreaks in id
        id = self.NEWLINE_CLEANUP_RE.sub(" ", id)
        if not id in self.markdown.references:  # ignore undefined refs
            return None
        href, title = self.markdown.references[id]

        text = m.group(2)
        return self.makeTag(href, title, text)

    def makeTag(self: _typing.Self, href: _typing.Any, title: _typing.Any, text: _typing.Any) -> _typing.Any:
        """
        Perform the makeTag operation under explicit file-format and conversion rules.

        Example:
            Exercise ReferencePattern.makeTag through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param href: Value supplied for href under the utility contract.
        :param title: Value supplied for title under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element("a")

        el.set("href", self.sanitize_url(href))
        if title:
            el.set("title", title)

        el.text = text
        return el


class ImageReferencePattern(ReferencePattern):
    """
    Match to a stored reference and return img element.

    Example:
        Exercise ImageReferencePattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def makeTag(self: _typing.Self, href: _typing.Any, title: _typing.Any, text: _typing.Any) -> _typing.Any:
        """
        Perform the makeTag operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageReferencePattern.makeTag through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param href: Value supplied for href under the utility contract.
        :param title: Value supplied for title under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element("img")
        el.set("src", self.sanitize_url(href))
        if title:
            el.set("title", title)

        if self.markdown.enable_attributes:
            text = handleAttributes(text, el)

        el.set("alt", self.unescape(text))
        return el


class AutolinkPattern(Pattern):
    """
    Return a link Element given an autolink (`<http://example/com>`).

    Example:
        Exercise AutolinkPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise AutolinkPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element("a")
        el.set("href", self.unescape(m.group(2)))
        el.text = util.AtomicString(m.group(2))
        return el


class AutomailPattern(Pattern):
    """
    Return a mailto link Element given an automail link (`<foo@example.com>`).

    Example:
        Exercise AutomailPattern through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def handleMatch(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the handleMatch operation under explicit file-format and conversion rules.

        Example:
            Exercise AutomailPattern.handleMatch through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = util.etree.Element("a")
        email = self.unescape(m.group(2))
        if email.startswith("mailto:"):
            email = email[len("mailto:") :]

        def codepoint2name(code: _typing.Any) -> _typing.Any:
            """
            Return entity definition by code, or the code if not defined.

            Example:
                Exercise AutomailPattern.handleMatch.codepoint2name through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :param code: Value supplied for code under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            entity = entities.codepoint2name.get(code)
            if entity:
                return "%s%s;" % (util.AMP_SUBSTITUTE, entity)
            else:
                return "%s#%d;" % (util.AMP_SUBSTITUTE, code)

        letters = [codepoint2name(ord(letter)) for letter in email]
        el.text = util.AtomicString("".join(letters))

        mailto = "mailto:" + email
        mailto = "".join([util.AMP_SUBSTITUTE + "#%d;" % ord(letter) for letter in mailto])
        el.set("href", mailto)
        return el
