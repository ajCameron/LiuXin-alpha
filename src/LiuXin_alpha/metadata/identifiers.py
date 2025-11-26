
"""
Tools for working with identifiers.
"""
from __future__ import division, absolute_import, print_function, annotations

clean_id_key = lambda typ: typ.lower().strip().replace(":", "").replace(",", "")
clean_id_value = lambda val: val.strip().replace(",", "|")
