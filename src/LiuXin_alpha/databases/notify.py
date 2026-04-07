def dummy_notify(event, ids, cc_class):
    """
    Dummy for the notify class
    :param cc_class:
    :param event:
    :param ids:
    :return:
    """
    if cc_class.embed:
        raise NotImplementedError("This method should not be called when the class is embedded")
    else:
        pass


def dummy_dirtied(book_ids, commit, cc_class):
    """
    Dummty for the dirtied class - the original notes that this object has changed in the dirtied table of the database.
    :param cc_class:
    :param book_ids:
    :param commit:
    :return:
    """
    if cc_class.embed:
        raise NotImplementedError("This method should not be called when the class is embedded")
    else:
        pass
