"""
Walk lxml element trees through normalized HTML5 token events.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lxmletree through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

try:
    text_type = unicode
except NameError:
    text_type = str

from lxml import etree
from ..treebuilders.etree import tag_regexp

from . import _base

from .. import ihatexml


def ensure_str(s):
    """
    Perform the ensure str utility operation under explicit compatibility rules.

    Example:
        Exercise ensure str through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if s is None:
        return None
    elif isinstance(s, text_type):
        return s
    else:
        return s.decode("utf-8", "strict")


class Root(object):
    """
    Provide the Root utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Root through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, et):
        """
        Initialize and validate the Root state.

        Example:
            Exercise Root.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param et: Value supplied for et under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.elementtree = et
        self.children = []
        if et.docinfo.internalDTD:
            self.children.append(
                Doctype(
                    self,
                    ensure_str(et.docinfo.root_name),
                    ensure_str(et.docinfo.public_id),
                    ensure_str(et.docinfo.system_url),
                )
            )
        root = et.getroot()
        node = root

        while node.getprevious() is not None:
            node = node.getprevious()
        while node is not None:
            self.children.append(node)
            node = node.getnext()

        self.text = None
        self.tail = None

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise Root.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.children[key]

    def getnext(self):
        """
        Perform the getnext utility operation under explicit compatibility rules.

        Example:
            Exercise Root.getnext through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def __len__(self):
        """
        Perform the len utility operation under explicit compatibility rules.

        Example:
            Exercise Root.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return 1


class Doctype(object):
    """
    Provide the Doctype utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Doctype through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, root_node, name, public_id, system_id):
        """
        Initialize and validate the Doctype state.

        Example:
            Exercise Doctype.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param root_node: Value supplied for root node under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param public_id: Value supplied for public id under the utility contract.
        :param system_id: Value supplied for system id under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.root_node = root_node
        self.name = name
        self.public_id = public_id
        self.system_id = system_id

        self.text = None
        self.tail = None

    def getnext(self):
        """
        Perform the getnext utility operation under explicit compatibility rules.

        Example:
            Exercise Doctype.getnext through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.root_node.children[1]


class FragmentRoot(Root):
    """
    Provide the FragmentRoot utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FragmentRoot through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, children):
        """
        Initialize and validate the FragmentRoot state.

        Example:
            Exercise FragmentRoot.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param children: Value supplied for children under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.children = [FragmentWrapper(self, child) for child in children]
        self.text = self.tail = None

    def getnext(self):
        """
        Perform the getnext utility operation under explicit compatibility rules.

        Example:
            Exercise FragmentRoot.getnext through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None


class FragmentWrapper(object):
    """
    Provide the FragmentWrapper utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FragmentWrapper through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, fragment_root, obj):
        """
        Initialize and validate the FragmentWrapper state.

        Example:
            Exercise FragmentWrapper.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param fragment_root: Value supplied for fragment root under the utility contract.
        :param obj: Value supplied for obj under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.root_node = fragment_root
        self.obj = obj
        if hasattr(self.obj, "text"):
            self.text = ensure_str(self.obj.text)
        else:
            self.text = None
        if hasattr(self.obj, "tail"):
            self.tail = ensure_str(self.obj.tail)
        else:
            self.tail = None

    def __getattr__(self, name):
        """
        Perform the getattr utility operation under explicit compatibility rules.

        Example:
            Exercise FragmentWrapper.  getattr   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return getattr(self.obj, name)

    def getnext(self):
        """
        Perform the getnext utility operation under explicit compatibility rules.

        Example:
            Exercise FragmentWrapper.getnext through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        siblings = self.root_node.children
        idx = siblings.index(self)
        if idx < len(siblings) - 1:
            return siblings[idx + 1]
        else:
            return None

    def __getitem__(self, key):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise FragmentWrapper.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.obj[key]

    def __bool__(self):
        """
        Expose bool behavior for the compatibility container.

        Example:
            Exercise FragmentWrapper.  bool   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: True when the documented condition holds; otherwise False.
        """
        return bool(self.obj)

    def getparent(self):
        """
        Perform the getparent utility operation under explicit compatibility rules.

        Example:
            Exercise FragmentWrapper.getparent through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def __str__(self):
        """
        Perform the str utility operation under explicit compatibility rules.

        Example:
            Exercise FragmentWrapper.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(self.obj)

    def __unicode__(self):
        """
        Perform the unicode utility operation under explicit compatibility rules.

        Example:
            Exercise FragmentWrapper.  unicode   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(self.obj)

    def __len__(self):
        """
        Perform the len utility operation under explicit compatibility rules.

        Example:
            Exercise FragmentWrapper.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.obj)


class TreeWalker(_base.NonRecursiveTreeWalker):
    """
    Provide the TreeWalker utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TreeWalker through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, tree):
        """
        Initialize and validate the TreeWalker state.

        Example:
            Exercise TreeWalker.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param tree: Value supplied for tree under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if hasattr(tree, "getroot"):
            tree = Root(tree)
        elif isinstance(tree, list):
            tree = FragmentRoot(tree)
        _base.NonRecursiveTreeWalker.__init__(self, tree)
        self.filter = ihatexml.InfosetFilter()

    def getNodeDetails(self, node):
        """
        Perform the getNodeDetails utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getNodeDetails through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(node, tuple):  # Text node
            node, key = node
            assert key in ("text", "tail"), "Text nodes are text or tail, found %s" % key
            return _base.TEXT, ensure_str(getattr(node, key))

        elif isinstance(node, Root):
            return (_base.DOCUMENT,)

        elif isinstance(node, Doctype):
            return _base.DOCTYPE, node.name, node.public_id, node.system_id

        elif isinstance(node, FragmentWrapper) and not hasattr(node, "tag"):
            return _base.TEXT, node.obj

        elif node.tag == etree.Comment:
            return _base.COMMENT, ensure_str(node.text)

        elif node.tag == etree.Entity:
            return _base.ENTITY, ensure_str(node.text)[1:-1]  # strip &;

        else:
            # This is assumed to be an ordinary element
            match = tag_regexp.match(ensure_str(node.tag))
            if match:
                namespace, tag = match.groups()
            else:
                namespace = None
                tag = ensure_str(node.tag)
            attrs = {}
            for name, value in list(node.attrib.items()):
                name = ensure_str(name)
                value = ensure_str(value)
                match = tag_regexp.match(name)
                if match:
                    attrs[(match.group(1), match.group(2))] = value
                else:
                    attrs[(None, name)] = value
            return (
                _base.ELEMENT,
                namespace,
                self.filter.fromXmlName(tag),
                attrs,
                len(node) > 0 or node.text,
            )

    def getFirstChild(self, node):
        """
        Perform the getFirstChild utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getFirstChild through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert not isinstance(node, tuple), "Text nodes have no children"

        assert len(node) or node.text, "Node has no children"
        if node.text:
            return (node, "text")
        else:
            return node[0]

    def getNextSibling(self, node):
        """
        Perform the getNextSibling utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getNextSibling through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(node, tuple):  # Text node
            node, key = node
            assert key in ("text", "tail"), "Text nodes are text or tail, found %s" % key
            if key == "text":
                # XXX: we cannot use a "bool(node) and node[0] or None" construct here
                # because node[0] might evaluate to False if it has no child element
                if len(node):
                    return node[0]
                else:
                    return None
            else:  # tail
                return node.getnext()

        return (node, "tail") if node.tail else node.getnext()

    def getParentNode(self, node):
        """
        Perform the getParentNode utility operation under explicit compatibility rules.

        Example:
            Exercise TreeWalker.getParentNode through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(node, tuple):  # Text node
            node, key = node
            assert key in ("text", "tail"), "Text nodes are text or tail, found %s" % key
            if key == "text":
                return node
            # else: fallback to "normal" processing

        return node.getparent()
