"""
Optimize PyLRS content before LRF binary serialization.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pylrfopt through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
"""
from __future__ import annotations

import typing as _typing
def _optimize(tagList: _typing.Any, tagName: _typing.Any, conversion: _typing.Any) -> None:
    # copy the tag of interest plus any text
    """
    Perform the optimize operation under explicit file-format and conversion rules.

    Example:
        Exercise  optimize through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param tagList: Value supplied for tagList under the utility contract.
    :param tagName: Value supplied for tagName under the utility contract.
    :param conversion: Value supplied for conversion under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    new_tag_list = []
    for tag in tagList:
        if tag.name == tagName or tag.name == "rawtext":
            new_tag_list.append(tag)

    # now, eliminate any duplicates (leaving the last one)
    for i, newTag in enumerate(new_tag_list[:-1]):
        if newTag.name == tagName and new_tag_list[i + 1].name == tagName:
            tagList.remove(newTag)

    # eliminate redundant settings to same value across text strings
    new_tag_list = []
    for tag in tagList:
        if tag.name == tagName:
            new_tag_list.append(tag)

    for i, newTag in enumerate(new_tag_list[:-1]):
        value = conversion(newTag.parameter)
        nextValue = conversion(new_tag_list[i + 1].parameter)
        if value == nextValue:
            tagList.remove(new_tag_list[i + 1])

    # eliminate any setting that don't have text after them
    while len(tagList) > 0 and tagList[-1].name == tagName:
        del tagList[-1]


def tagListOptimizer(tagList: _typing.Any) -> _typing.Any:
    # this function eliminates redundant or unnecessary tags
    # it scans a list of tags, looking for text settings that are
    # changed before any text is output
    # for example,
    #  fontsize=100, fontsize=200, text, fontsize=100, fontsize=200
    # should be:
    # fontsize=200 text
    """
    Perform the tagListOptimizer operation under explicit file-format and conversion rules.

    Example:
        Exercise tagListOptimizer through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param tagList: Value supplied for tagList under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    old_size = len(tagList)
    _optimize(tagList, "fontsize", int)
    _optimize(tagList, "fontweight", int)
    return old_size - len(tagList)
