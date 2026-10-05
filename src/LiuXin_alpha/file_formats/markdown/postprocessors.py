"""
Apply final text substitutions after Markdown serialization.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise postprocessors through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
POST-PROCESSORS
=============================================================================

Markdown also allows post-processors, which are similar to preprocessors in
that they need to implement a "run" method. However, they are run after core
processing.

"""

from . import util
from . import odict
import re


def build_postprocessors(md_instance: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
    """
    Build the default postprocessors for Markdown.

    Example:
        Exercise build postprocessors through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param md_instance: Value supplied for md instance under the utility contract.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    postprocessors = odict.OrderedDict()
    postprocessors["raw_html"] = RawHtmlPostprocessor(md_instance)
    postprocessors["amp_substitute"] = AndSubstitutePostprocessor()
    postprocessors["unescape"] = UnescapePostprocessor()
    return postprocessors


class Postprocessor(util.Processor):
    """
    Postprocessors are run after the ElementTree it converted back into text.

    Example:
        Exercise Postprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def run(self: _typing.Self, text: _typing.Any) -> None:
        """
        Subclasses of Postprocessor should implement a `run` method, which takes the html document as a single text string and returns a (possibly modified) string.

        Example:
            Exercise Postprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


class RawHtmlPostprocessor(Postprocessor):
    """
    Restore raw html to the document.

    Example:
        Exercise RawHtmlPostprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def run(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Iterate over html stash and restore "safe" html.

        Example:
            Exercise RawHtmlPostprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for i in range(self.markdown.htmlStash.html_counter):
            html, safe = self.markdown.htmlStash.rawHtmlBlocks[i]
            if self.markdown.safeMode and not safe:
                if str(self.markdown.safeMode).lower() == "escape":
                    html = self.escape(html)
                elif str(self.markdown.safeMode).lower() == "remove":
                    html = ""
                else:
                    html = self.markdown.html_replacement_text
            if self.isblocklevel(html) and (safe or not self.markdown.safeMode):
                text = text.replace(
                    "<p>%s</p>" % (self.markdown.htmlStash.get_placeholder(i)),
                    html + "\n",
                )
            text = text.replace(self.markdown.htmlStash.get_placeholder(i), html)
        return text

    def escape(self: _typing.Self, html: _typing.Any) -> _typing.Any:
        """
        Basic html escaping

        Example:
            Exercise RawHtmlPostprocessor.escape through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param html: Value supplied for html under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        html = html.replace("&", "&amp;")
        html = html.replace("<", "&lt;")
        html = html.replace(">", "&gt;")
        return html.replace('"', "&quot;")

    def isblocklevel(self: _typing.Self, html: _typing.Any) -> _typing.Any:
        """
        Perform the isblocklevel operation under explicit file-format and conversion rules.

        Example:
            Exercise RawHtmlPostprocessor.isblocklevel through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param html: Value supplied for html under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        m = re.match(r"^\<\/?([^ >]+)", html)
        if m:
            if m.group(1)[0] in ("!", "?", "@", "%"):
                # Comment, php etc...
                return True
            return util.isBlockLevel(m.group(1))
        return False


class AndSubstitutePostprocessor(Postprocessor):
    """
    Restore valid entities

    Example:
        Exercise AndSubstitutePostprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def run(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise AndSubstitutePostprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        text = text.replace(util.AMP_SUBSTITUTE, "&")
        return text


class UnescapePostprocessor(Postprocessor):
    """
    Restore escaped chars

    Example:
        Exercise UnescapePostprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    RE = re.compile(r"%s(\d+)%s" % (util.STX, util.ETX))

    def unescape(self: _typing.Self, m: _typing.Any) -> _typing.Any:
        """
        Perform the unescape operation under explicit file-format and conversion rules.

        Example:
            Exercise UnescapePostprocessor.unescape through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return util.int2str(int(m.group(1)))

    def run(self: _typing.Self, text: _typing.Any) -> _typing.Any:
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise UnescapePostprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.RE.sub(self.unescape, text)
