# -*- coding: utf-8 -*-
"""
Provide freetype utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise freetype through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Face:
    """
    Provide the Face utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Face through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    _font_bytes: bytes
    _index: int = 0

    def supports_text(self, text: str) -> bool:
        # Best-effort: assume coverage.
        """
        Perform the supports text utility operation under explicit compatibility rules.

        Example:
            Exercise Face.supports text through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return True

    def glyph_id(self, ch: str) -> int:
        """
        Perform the glyph id utility operation under explicit compatibility rules.

        Example:
            Exercise Face.glyph id through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param ch: Value supplied for ch under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not ch:
            return 0
        return ord(ch[0])


def load_font(font_data: bytes, index: int = 0) -> Face:
    """
    Perform the load font utility operation under explicit compatibility rules.

    Example:
        Exercise load font through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param font_data: Value supplied for font data under the utility contract.
    :param index: Value supplied for index under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not isinstance(font_data, (bytes, bytearray, memoryview)):
        raise TypeError("load_font expects font bytes")
    return Face(bytes(font_data), int(index))
