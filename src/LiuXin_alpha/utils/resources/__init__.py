
"""
LiuXin/calibre uses a number of resources - this is a unified way to access them.
"""


def resource_to_path(target_path: str) -> str:
    """
    Get the resource path for the given named resource.

    Currently a shim.
    :param target_path:
    :return:
    """
    raise NotImplementedError


def resource_to_resource(target_path: str) -> bytes:
    """
    Get the given resource as bytes.

    :param target_path:
    :return:
    """
    raise NotImplementedError


