"""
XML backend compatibility layer.

Prefer `lxml` when available; otherwise fall back to stdlib
`xml.etree.ElementTree` with a small adapter surface so core imports keep
working in constrained environments.
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
    return _lxml_etree if LXML_AVAILABLE else _stdlib_etree


class _EtreeFacade:
    """
    Minimal facade that emulates the subset of `lxml.etree` used by LiuXin.
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
        return getattr(_backend(), name)

    def XMLParser(self, *args: Any, **kwargs: Any) -> Any:
        if LXML_AVAILABLE:
            return _lxml_etree.XMLParser(*args, **kwargs)
        allowed: dict[str, Any] = {}
        for key in ("target", "encoding"):
            if key in kwargs:
                allowed[key] = kwargs[key]
        return _stdlib_etree.XMLParser(**allowed)

    def Element(self, tag: str, attrib: dict[str, Any] | None = None, **extra: Any) -> Any:
        if LXML_AVAILABLE:
            return _lxml_etree.Element(tag, attrib=attrib, **extra)
        attrib_out = dict(attrib or {})
        extra.pop("nsmap", None)
        attrib_out.update(extra)
        return _stdlib_etree.Element(tag, attrib_out)

    def SubElement(self, parent: Any, tag: str, attrib: dict[str, Any] | None = None, **extra: Any) -> Any:
        if LXML_AVAILABLE:
            return _lxml_etree.SubElement(parent, tag, attrib=attrib, **extra)
        attrib_out = dict(attrib or {})
        extra.pop("nsmap", None)
        attrib_out.update(extra)
        return _stdlib_etree.SubElement(parent, tag, attrib_out)

    def fromstring(self, text: Any, parser: Any | None = None, **kwargs: Any) -> Any:
        if LXML_AVAILABLE:
            return _lxml_etree.fromstring(text, parser=parser, **kwargs)
        if parser is None:
            return _stdlib_etree.fromstring(text)
        return _stdlib_etree.fromstring(text, parser=parser)

    def parse(self, source: Any, parser: Any | None = None, **kwargs: Any) -> Any:
        if LXML_AVAILABLE:
            return _lxml_etree.parse(source, parser=parser, **kwargs)
        if parser is None:
            return _stdlib_etree.parse(source)
        return _stdlib_etree.parse(source, parser=parser)

    def tostring(self, element: Any, *args: Any, **kwargs: Any) -> Any:
        if LXML_AVAILABLE:
            return _lxml_etree.tostring(element, *args, **kwargs)
        kwargs = dict(kwargs)
        kwargs.pop("pretty_print", None)
        kwargs.pop("with_tail", None)
        kwargs.pop("inclusive_ns_prefixes", None)
        kwargs.pop("with_comments", None)
        return _stdlib_etree.tostring(element, *args, **kwargs)

    def XPath(self, expression: str, namespaces: dict[str, str] | None = None) -> Callable[..., Any]:
        if LXML_AVAILABLE:
            return _lxml_etree.XPath(expression, namespaces=namespaces)

        ns = namespaces or {}

        def _xpath(node: Any, *args: Any, **kwargs: Any) -> Any:
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
    """

    def __init__(self, namespace: str | None = None, nsmap: dict[str | None, str] | None = None) -> None:
        self.namespace = namespace
        self.nsmap = nsmap or {}

    def _qualify(self, tag: str) -> str:
        if self.namespace and not tag.startswith("{"):
            return "{%s}%s" % (self.namespace, tag)
        return tag

    def __getattr__(self, tag: str) -> Callable[..., Any]:
        return lambda *children, **attrib: self._make(tag, *children, **attrib)

    def __call__(self, tag: str, *children: Any, **attrib: Any) -> Any:
        return self._make(tag, *children, **attrib)

    def _append_child(self, elem: Any, child: Any) -> None:
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
        elem = etree.Element(self._qualify(tag), attrib=attrib, nsmap=self.nsmap)
        for child in children:
            self._append_child(elem, child)
        return elem


ElementMaker = _LxmlElementMaker if _LxmlElementMaker is not None else _StdlibElementMaker

