
"""
Expose string cleaners for identifier keys and values.

Keys are lowercased, stripped, and cleared of colons and commas; values are stripped
and replace commas with vertical bars. These helpers do not validate identifier
schemes or values.

Example:
    >>> clean_id_key(' ISBN:, ')
    'isbn'
    >>> clean_id_value(' a,b ')
    'a|b'
"""
from __future__ import division, absolute_import, print_function, annotations

clean_id_key = lambda typ: typ.lower().strip().replace(":", "").replace(",", "").strip()
clean_id_value = lambda val: val.strip().replace(",", "|")
