from LiuXin_alpha.utils.resources import resource_to_path

_mt_inited = False


def _init_mimetypes():
    global _mt_inited
    import mimetypes

    target_path = resource_to_path("mime.types")

    mimetypes.init(
        [
            target_path,
        ]
    )

    _mt_inited = True


def guess_all_extensions(*args, **kwargs):
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    return mimetypes.guess_all_extensions(*args, **kwargs)


def guess_extension(*args, **kwargs):
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    ext = mimetypes.guess_extension(*args, **kwargs)
    if not ext and args and args[0] == "application/x-palmreader":
        ext = ".pdb"
    return ext


def get_types_map():
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    return mimetypes.types_map




# probably safe
def guess_type(*args, **kwargs):
    import mimetypes

    if not _mt_inited:
        _init_mimetypes()
    return mimetypes.guess_type(*args, **kwargs)

