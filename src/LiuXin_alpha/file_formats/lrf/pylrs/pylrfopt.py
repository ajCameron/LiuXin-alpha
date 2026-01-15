def _optimize(tagList, tagName, conversion):
    # copy the tag of interest plus any text
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


def tagListOptimizer(tagList):
    # this function eliminates redundant or unnecessary tags
    # it scans a list of tags, looking for text settings that are
    # changed before any text is output
    # for example,
    #  fontsize=100, fontsize=200, text, fontsize=100, fontsize=200
    # should be:
    # fontsize=200 text
    old_size = len(tagList)
    _optimize(tagList, "fontsize", int)
    _optimize(tagList, "fontweight", int)
    return old_size - len(tagList)
