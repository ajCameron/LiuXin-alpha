"""
Provide the ordered mapping behavior required by the Markdown processor registry.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise odict through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import annotations

import typing as _typing
from . import util


from copy import deepcopy


def iteritems_compat(d: _typing.Any) -> _typing.Any:
    """
    Return an iterator over the (key, value) pairs of a dictionary. Copied from `six` module.

    Example:
        Exercise iteritems compat through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param d: Value supplied for d under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if hasattr(d, "items"):
        return iter(d.items())
    return iter(d)


class OrderedDict(dict):
    """
    A dictionary that keeps its keys in the order in which they're inserted.

    Example:
        Exercise OrderedDict through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """

    def __new__(cls: type[_typing.Self], *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the new operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.  new   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        instance = super(OrderedDict, cls).__new__(cls, *args, **kwargs)
        instance.keyOrder = []
        return instance

    def __init__(self: _typing.Self, data: _typing.Any = None) -> None:
        """
        Initialize and validate the ordereddict state.

        Example:
            Exercise OrderedDict.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if data is None or isinstance(data, dict):
            data = data or []
            super(OrderedDict, self).__init__(data)
            self.keyOrder = list(data) if data else []
        else:
            super(OrderedDict, self).__init__()
            super_set = super(OrderedDict, self).__setitem__
            for key, value in data:
                # Take the ordering from first key
                if key not in self:
                    self.keyOrder.append(key)
                # But override with last value in data (dict() does this)
                super_set(key, value)

    def __deepcopy__(self: _typing.Self, memo: _typing.Any) -> _typing.Any:
        """
        Perform the deepcopy operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.  deepcopy   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param memo: Value supplied for memo under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__class__([(key, deepcopy(value, memo)) for key, value in self.items()])

    def __copy__(self: _typing.Self) -> _typing.Any:
        # The Python's default copy implementation will alter the state
        # of self. The reason for this seems complex but is likely related to
        # subclassing dict.
        """
        Perform the copy operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.  copy   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.copy()

    def __setitem__(self: _typing.Self, key: _typing.Any, value: _typing.Any) -> None:
        """
        Perform the setitem operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.  setitem   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if key not in self:
            self.keyOrder.append(key)
        super(OrderedDict, self).__setitem__(key, value)

    def __delitem__(self: _typing.Self, key: _typing.Any) -> None:
        """
        Perform the delitem operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.  delitem   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        super(OrderedDict, self).__delitem__(key)
        self.keyOrder.remove(key)

    def __iter__(self: _typing.Self) -> _typing.Any:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(self.keyOrder)

    def __reversed__(self: _typing.Self) -> _typing.Any:
        """
        Perform the reversed operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.  reversed   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return reversed(self.keyOrder)

    def pop(self: _typing.Self, k: _typing.Any, *args: _typing.Any) -> _typing.Any:
        """
        Perform the pop operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.pop through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param k: Value supplied for k under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        result = super(OrderedDict, self).pop(k, *args)
        try:
            self.keyOrder.remove(k)
        except ValueError:
            # Key wasn't in the dictionary in the first place. No problem.
            pass
        return result

    def popitem(self: _typing.Self) -> _typing.Any:
        """
        Perform the popitem operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.popitem through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        result = super(OrderedDict, self).popitem()
        self.keyOrder.remove(result[0])
        return result

    def _iteritems(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iteritems operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict. iteritems through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for key in self.keyOrder:
            yield key, self[key]

    def _iterkeys(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iterkeys operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict. iterkeys through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for key in self.keyOrder:
            yield key

    def _itervalues(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the itervalues operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict. itervalues through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for key in self.keyOrder:
            yield self[key]

    if util.PY3:
        items = _iteritems
        keys = _iterkeys
        values = _itervalues
    else:
        iteritems = _iteritems
        iterkeys = _iterkeys
        itervalues = _itervalues

        def items(self: _typing.Self) -> _typing.Any:
            """
            Perform the items operation under explicit file-format and conversion rules.

            Example:
                Exercise OrderedDict.items through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return [(k, self[k]) for k in self.keyOrder]

        def keys(self: _typing.Self) -> _typing.Any:
            """
            Perform the keys operation under explicit file-format and conversion rules.

            Example:
                Exercise OrderedDict.keys through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.keyOrder[:]

        def values(self: _typing.Self) -> _typing.Any:
            """
            Perform the values operation under explicit file-format and conversion rules.

            Example:
                Exercise OrderedDict.values through a consuming regression::

                    python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return [self[k] for k in self.keyOrder]

    def update(self: _typing.Self, dict_: _typing.Any) -> None:
        """
        Perform the update operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.update through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param dict_: Value supplied for dict under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for k, v in iteritems_compat(dict_):
            self[k] = v

    def setdefault(self: _typing.Self, key: _typing.Any, default: _typing.Any) -> _typing.Any:
        """
        Perform the setdefault operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.setdefault through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if key not in self:
            self.keyOrder.append(key)
        return super(OrderedDict, self).setdefault(key, default)

    def value_for_index(self: _typing.Self, index: _typing.Any) -> _typing.Any:
        """
        Returns the value of the item at the given zero-based index.

        Example:
            Exercise OrderedDict.value for index through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self[self.keyOrder[index]]

    def insert(self: _typing.Self, index: _typing.Any, key: _typing.Any, value: _typing.Any) -> None:
        """
        Inserts the key, value pair before the item with the given index.

        Example:
            Exercise OrderedDict.insert through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param index: Value supplied for index under the utility contract.
        :param key: Metadata, identifier or local-variable key.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if key in self.keyOrder:
            n = self.keyOrder.index(key)
            del self.keyOrder[n]
            if n < index:
                index -= 1
        self.keyOrder.insert(index, key)
        super(OrderedDict, self).__setitem__(key, value)

    def copy(self: _typing.Self) -> _typing.Any:
        """
        Returns a copy of this object.

        Example:
            Exercise OrderedDict.copy through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # This way of initializing the copy means it works for subclasses, too.
        return self.__class__(self)

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Replaces the normal dict.__repr__ with a version that returns the keys in their Ordered order.

        Example:
            Exercise OrderedDict.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "{%s}" % ", ".join(["%r: %r" % (k, v) for k, v in iteritems_compat(self)])

    def clear(self: _typing.Self) -> None:
        """
        Perform the clear operation under explicit file-format and conversion rules.

        Example:
            Exercise OrderedDict.clear through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        super(OrderedDict, self).clear()
        self.keyOrder = []

    def index(self: _typing.Self, key: _typing.Any) -> _typing.Any:
        """
        Return the index of a given key.

        Example:
            Exercise OrderedDict.index through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.keyOrder.index(key)
        except ValueError:
            raise ValueError("Element '%s' was not found in OrderedDict" % key)

    def index_for_location(self: _typing.Self, location: _typing.Any) -> _typing.Any:
        """
        Return index or None for a given location.

        Example:
            Exercise OrderedDict.index for location through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param location: Value supplied for location under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if location == "_begin":
            i = 0
        elif location == "_end":
            i = None
        elif location.startswith("<") or location.startswith(">"):
            i = self.index(location[1:])
            if location.startswith(">"):
                if i >= len(self):
                    # last item
                    i = None
                else:
                    i += 1
        else:
            raise ValueError('Not a valid location: "%s". Location key ' 'must start with a ">" or "<".' % location)
        return i

    def add(self: _typing.Self, key: _typing.Any, value: _typing.Any, location: _typing.Any) -> None:
        """
        Insert by key location.

        Example:
            Exercise OrderedDict.add through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :param value: Value normalized, stored, formatted or returned.
        :param location: Value supplied for location under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        i = self.index_for_location(location)
        if i is not None:
            self.insert(i, key, value)
        else:
            self.__setitem__(key, value)

    def link(self: _typing.Self, key: _typing.Any, location: _typing.Any) -> None:
        """
        Change location of an existing item.

        Example:
            Exercise OrderedDict.link through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param key: Metadata, identifier or local-variable key.
        :param location: Value supplied for location under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        n = self.keyOrder.index(key)
        del self.keyOrder[n]
        try:
            i = self.index_for_location(location)
            if i is not None:
                self.keyOrder.insert(i, key)
            else:
                self.keyOrder.append(key)
        except Exception as e:
            # restore to prevent data loss and reraise
            self.keyOrder.insert(n, key)
            raise e
