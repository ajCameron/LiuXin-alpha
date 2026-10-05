"""
Provide inject meta charset utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise inject meta charset through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib.filters import _base


class Filter(_base.Filter):
    """
    Provide the Filter utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Filter through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, source, encoding):
        """
        Initialize and validate the Filter state.

        Example:
            Exercise Filter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param source: Value supplied for source under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        _base.Filter.__init__(self, source)
        self.encoding = encoding

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise Filter.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        state = "pre_head"
        meta_found = self.encoding is None
        pending = []

        for token in _base.Filter.__iter__(self):
            type = token["type"]
            if type == "StartTag":
                if token["name"].lower() == "head":
                    state = "in_head"

            elif type == "EmptyTag":
                if token["name"].lower() == "meta":
                    # replace charset with actual encoding
                    has_http_equiv_content_type = False
                    for (namespace, name), value in token["data"].items():
                        if namespace is not None:
                            continue
                        elif name.lower() == "charset":
                            token["data"][(namespace, name)] = self.encoding
                            meta_found = True
                            break
                        elif name == "http-equiv" and value.lower() == "content-type":
                            has_http_equiv_content_type = True
                    else:
                        if has_http_equiv_content_type and (None, "content") in token["data"]:
                            token["data"][(None, "content")] = "text/html; charset=%s" % self.encoding
                            meta_found = True

                elif token["name"].lower() == "head" and not meta_found:
                    # insert meta into empty head
                    yield {"type": "StartTag", "name": "head", "data": token["data"]}
                    yield {
                        "type": "EmptyTag",
                        "name": "meta",
                        "data": {(None, "charset"): self.encoding},
                    }
                    yield {"type": "EndTag", "name": "head"}
                    meta_found = True
                    continue

            elif type == "EndTag":
                if token["name"].lower() == "head" and pending:
                    # insert meta into head (if necessary) and flush pending queue
                    yield pending.pop(0)
                    if not meta_found:
                        yield {
                            "type": "EmptyTag",
                            "name": "meta",
                            "data": {(None, "charset"): self.encoding},
                        }
                    while pending:
                        yield pending.pop(0)
                    meta_found = True
                    state = "post_head"

            if state == "in_head":
                pending.append(token)
            else:
                yield token
