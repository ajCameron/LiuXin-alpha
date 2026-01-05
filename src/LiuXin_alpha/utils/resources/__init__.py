#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
LiuXin/calibre uses a number of resources - this is a unified way to access them.
"""

from __future__ import with_statement, print_function

import sys
import os

import builtins as __builtin__

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"



def resource_to_path(target_path: str) -> str:
    """
    Get the resource path for the given named resource.

    Currently, a shim.
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


class PathResolver:
    """
    Resolve the path to the requested resource.
    """
    def __init__(self) -> None:
        """
        Startup the resolver.
        """
        from LiuXin_alpha.constants.paths import LiuXin_calibre_resources_folder

        config_dir = LiuXin_calibre_resources_folder
        self.locations = [config_dir, ]
        self.cache = {}

        def suitable(path):
            try:
                return os.path.exists(path) and os.path.isdir(path) and os.listdir(path)
            except:
                pass
            return False

        self.default_path = config_dir

        dev_path = os.environ.get("CALIBRE_DEVELOP_FROM", None)
        self.using_develop_from = False
        if dev_path is not None:
            dev_path = os.path.join(os.path.abspath(os.path.dirname(dev_path)), "resources")

            if suitable(dev_path):
                self.locations.insert(0, dev_path)
                self.default_path = dev_path
                self.using_develop_from = True

        user_path = os.path.join(config_dir, "resources")
        self.user_path = None
        if suitable(user_path):
            self.locations.insert(0, user_path)
            self.user_path = user_path

    def __call__(self, path, allow_user_override=True):
        path = path.replace(os.sep, "/")
        key = (path, allow_user_override)
        ans = self.cache.get(key, None)
        if ans is None:
            for base in self.locations:
                if not allow_user_override and base == self.user_path:
                    continue
                fpath = os.path.join(base, *path.split("/"))
                if os.path.exists(fpath):
                    ans = fpath
                    break

            if ans is None:
                ans = os.path.join(self.default_path, *path.split("/"))

            self.cache[key] = ans

        return ans


_resolver = PathResolver()


def get_path(path, data=False, allow_user_override=True):
    """
    get a path to a resource in the calibre_prefs folder.
    :param path: The path to the resource
    :param data: Return the data as a string or return the path to the data
    :param allow_user_override:
    :return:
    """
    fpath = _resolver(path, allow_user_override=allow_user_override)
    if data:
        with open(fpath, "rb") as f:
            return f.read()
    return fpath


def get_image_path(path, data=False, allow_user_override=True):
    if not path:
        return get_path("images", allow_user_override=allow_user_override)
    return get_path("images/" + path, data=data, allow_user_override=allow_user_override)


def js_name_to_path(name, ext=".coffee"):
    path = ("/".join(name.split("."))) + ext
    d = os.path.dirname
    base = d(d(os.path.abspath(__file__)))
    return os.path.join(base, path)


def _compile_coffeescript(name):
    from LiuXin.utils.serve_coffee import compile_coffeescript

    src = js_name_to_path(name)
    with open(src, "rb") as f:
        cs, errors = compile_coffeescript(f.read(), src)
        if errors:
            for line in errors:
                print(line)
            raise Exception("Failed to compile coffeescript: %s" % src)
        return cs


def compiled_coffeescript(name, dynamic=False):
    import zipfile

    zipf = get_path("compiled_coffeescript.zip", allow_user_override=False)
    with zipfile.ZipFile(zipf, "r") as zf:
        if dynamic:
            import json

            existing_hash = json.loads(zf.comment or "{}").get(name + ".js")
            if existing_hash is not None:
                import hashlib

                with open(js_name_to_path(name), "rb") as f:
                    if existing_hash == hashlib.sha1(f.read()).hexdigest():
                        return zf.read(name + ".js")
            return _compile_coffeescript(name)
        else:
            return zf.read(name + ".js")


P = get_path
I = get_image_path
__builtin__.__dict__["P"] = get_path
__builtin__.__dict__["I"] = get_image_path
