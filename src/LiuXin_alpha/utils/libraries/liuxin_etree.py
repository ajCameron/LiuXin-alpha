"""
Provide stable XML element helpers over the available etree backend.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise liuxin etree through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""

from __future__ import annotations

from typing import Any, Callable

import xml.etree.ElementTree as _stdlib_etree

try:
    from lxml import etree as _lxml_etree  # type: ignore
    from lxml.builder import ElementMaker as _LxmlElementMaker  # type: ignore
except Exception:  # pragma: no cover - exercised in no-lxml runtimes
    _lxml_etree = None
    _LxmlElementMaker = None


LXML_AVAILABLE = _lxml_etree is not None


def _backend() -> Any:
    """
    Perform the backend utility operation under explicit compatibility rules.

    Example:
        Exercise  backend through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _lxml_etree if LXML_AVAILABLE else _stdlib_etree


class _EtreeFacade:
    """
    Minimal facade that emulates the subset of `lxml.etree` used by LiuXin.

    Example:
        Exercise  EtreeFacade through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """

    _Element = getattr(_backend(), "_Element", _stdlib_etree.Element)
    XMLSyntaxError = getattr(_backend(), "XMLSyntaxError", _stdlib_etree.ParseError)
    ParseError = getattr(_backend(), "ParseError", _stdlib_etree.ParseError)
    Comment = getattr(_backend(), "Comment", _stdlib_etree.Comment)
    ProcessingInstruction = getattr(_backend(), "ProcessingInstruction", _stdlib_etree.ProcessingInstruction)
    Entity = getattr(_backend(), "Entity", str)
    XSLTExtension = getattr(_backend(), "XSLTExtension", object)
    XSLT = getattr(_backend(), "XSLT", None)

    def __getattr__(self, name: str) -> Any:
        """
        Perform the getattr utility operation under explicit compatibility rules.

        Example:
            Exercise  EtreeFacade.  getattr   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return getattr(_backend(), name)

    def XMLParser(self, *args: Any, **kwargs: Any) -> Any:
        """
        Perform the XMLParser utility operation under explicit compatibility rules.

        Example:
            Exercise  EtreeFacade.XMLParser through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if LXML_AVAILABLE:
            return _lxml_etree.XMLParser(*args, **kwargs)
        allowed: dict[str, Any] = {}
        for key in ("target", "encoding"):
            if key in kwargs:
                allowed[key] = kwargs[key]
        return _stdlib_etree.XMLParser(**allowed)

    def Element(self, tag: str, attrib: dict[str, Any] | None = None, **extra: Any) -> Any:
        """
        Perform the Element utility operation under explicit compatibility rules.

        Example:
            Exercise  EtreeFacade.Element through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param tag: Value supplied for tag under the utility contract.
        :param attrib: Value supplied for attrib under the utility contract.
        :param extra: Value supplied for extra under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if LXML_AVAILABLE:
            return _lxml_etree.Element(tag, attrib=attrib, **extra)
        attrib_out = dict(attrib or {})
        extra.pop("nsmap", None)
        attrib_out.update(extra)
        return _stdlib_etree.Element(tag, attrib_out)

    def SubElement(self, parent: Any, tag: str, attrib: dict[str, Any] | None = None, **extra: Any) -> Any:
        """
        Perform the SubElement utility operation under explicit compatibility rules.

        Example:
            Exercise  EtreeFacade.SubElement through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param parent: Value supplied for parent under the utility contract.
        :param tag: Value supplied for tag under the utility contract.
        :param attrib: Value supplied for attrib under the utility contract.
        :param extra: Value supplied for extra under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if LXML_AVAILABLE:
            return _lxml_etree.SubElement(parent, tag, attrib=attrib, **extra)
        attrib_out = dict(attrib or {})
        extra.pop("nsmap", None)
        attrib_out.update(extra)
        return _stdlib_etree.SubElement(parent, tag, attrib_out)

    def fromstring(self, text: Any, parser: Any | None = None, **kwargs: Any) -> Any:
        """
        Perform the fromstring utility operation under explicit compatibility rules.

        Example:
            Exercise  EtreeFacade.fromstring through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param text: Text parsed, normalized or rendered.
        :param parser: Value supplied for parser under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if LXML_AVAILABLE:
            return _lxml_etree.fromstring(text, parser=parser, **kwargs)
        if parser is None:
            return _stdlib_etree.fromstring(text)
        return _stdlib_etree.fromstring(text, parser=parser)

    def parse(self, source: Any, parser: Any | None = None, **kwargs: Any) -> Any:
        """
        Parse the supplied date text and return its normalized datetime value.

        Example:
            Exercise  EtreeFacade.parse through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param source: Value supplied for source under the utility contract.
        :param parser: Value supplied for parser under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if LXML_AVAILABLE:
            return _lxml_etree.parse(source, parser=parser, **kwargs)
        if parser is None:
            return _stdlib_etree.parse(source)
        return _stdlib_etree.parse(source, parser=parser)

    def tostring(self, element: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Perform the tostring utility operation under explicit compatibility rules.

        Example:
            Exercise  EtreeFacade.tostring through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param element: Value supplied for element under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if LXML_AVAILABLE:
            return _lxml_etree.tostring(element, *args, **kwargs)
        kwargs = dict(kwargs)
        kwargs.pop("pretty_print", None)
        kwargs.pop("with_tail", None)
        kwargs.pop("inclusive_ns_prefixes", None)
        kwargs.pop("with_comments", None)
        return _stdlib_etree.tostring(element, *args, **kwargs)

    def XPath(self, expression: str, namespaces: dict[str, str] | None = None) -> Callable[..., Any]:
        """
        Perform the XPath utility operation under explicit compatibility rules.

        Example:
            Exercise  EtreeFacade.XPath through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param expression: Value supplied for expression under the utility contract.
        :param namespaces: Value supplied for namespaces under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if LXML_AVAILABLE:
            return _lxml_etree.XPath(expression, namespaces=namespaces)

        ns = namespaces or {}

        def _xpath(node: Any, *args: Any, **kwargs: Any) -> Any:
            """
            Perform the xpath utility operation under explicit compatibility rules.

            Example:
                Exercise  EtreeFacade.XPath. xpath through a consuming regression::

                    python -m pytest -q tests/scripts/test_docstring_migration.py


            :param node: Value supplied for node under the utility contract.
            :param args: Positional values forwarded to the compatibility implementation.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if expression == "string()":
                return "".join(node.itertext())
            if expression.startswith("@"):
                attr = expression[1:]
                value = node.get(attr)
                return [] if value is None else [value]
            try:
                return node.findall(expression, ns)
            except Exception as exc:
                raise NotImplementedError(
                    "XPath expression requires lxml: {!r}".format(expression)
                ) from exc

        return _xpath


etree = _EtreeFacade()


class _StdlibElementMaker:
    """
    Tiny stdlib-compatible replacement for lxml.builder.ElementMaker.

    Example:
        Exercise  StdlibElementMaker through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """

    def __init__(self, namespace: str | None = None, nsmap: dict[str | None, str] | None = None) -> None:
        """
        Initialize and validate the StdlibElementMaker state.

        Example:
            Exercise  StdlibElementMaker.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param nsmap: Value supplied for nsmap under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        self.nsmap = nsmap or {}

    def _qualify(self, tag: str) -> str:
        """
        Perform the qualify utility operation under explicit compatibility rules.

        Example:
            Exercise  StdlibElementMaker. qualify through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param tag: Value supplied for tag under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.namespace and not tag.startswith("{"):
            return "{%s}%s" % (self.namespace, tag)
        return tag

    def __getattr__(self, tag: str) -> Callable[..., Any]:
        """
        Perform the getattr utility operation under explicit compatibility rules.

        Example:
            Exercise  StdlibElementMaker.  getattr   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param tag: Value supplied for tag under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return lambda *children, **attrib: self._make(tag, *children, **attrib)

    def __call__(self, tag: str, *children: Any, **attrib: Any) -> Any:
        """
        Perform the call utility operation under explicit compatibility rules.

        Example:
            Exercise  StdlibElementMaker.  call   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param tag: Value supplied for tag under the utility contract.
        :param children: Value supplied for children under the utility contract.
        :param attrib: Value supplied for attrib under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._make(tag, *children, **attrib)

    def _append_child(self, elem: Any, child: Any) -> None:
        """
        Perform the append child utility operation under explicit compatibility rules.

        Example:
            Exercise  StdlibElementMaker. append child through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param elem: Value supplied for elem under the utility contract.
        :param child: Value supplied for child under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if child is None:
            return
        if isinstance(child, (list, tuple)):
            for item in child:
                self._append_child(elem, item)
            return
        if isinstance(child, (str, bytes)):
            txt = child.decode("utf-8", "replace") if isinstance(child, bytes) else child
            if elem.text:
                elem.text += txt
            else:
                elem.text = txt
            return
        elem.append(child)

    def _make(self, tag: str, *children: Any, **attrib: Any) -> Any:
        """
        Perform the make utility operation under explicit compatibility rules.

        Example:
            Exercise  StdlibElementMaker. make through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param tag: Value supplied for tag under the utility contract.
        :param children: Value supplied for children under the utility contract.
        :param attrib: Value supplied for attrib under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = etree.Element(self._qualify(tag), attrib=attrib, nsmap=self.nsmap)
        for child in children:
            self._append_child(elem, child)
        return elem


ElementMaker = _LxmlElementMaker if _LxmlElementMaker is not None else _StdlibElementMaker

