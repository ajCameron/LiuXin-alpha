#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Expose the supported resources compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/resources/test_resources.py
"""


import os

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"



def resource_to_path(target_path: str) -> str:
    """
    Get the resource path for the given named resource.

    Example:
        Exercise resource to path through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py


    :param target_path: Value supplied for target path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return str(P(target_path))


def resource_to_resource(target_path: str) -> bytes:
    """
    Get the given resource as bytes.

    Example:
        Exercise resource to resource through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py


    :param target_path: Value supplied for target path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return get_path(target_path, data=True)


class PathResolver:
    """
    Resolve the path to the requested resource.

    Example:
        Exercise PathResolver through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py
    """
    def __init__(self) -> None:
        """
        Startup the resolver.

        Example:
            Exercise PathResolver.  init   through a consuming regression::

                python -m pytest -q tests/utils/resources/test_resources.py


        :return: None; validated state is stored on the receiving object.
        """
        from LiuXin_alpha.constants.paths import (
            LiuXin_calibre_resources_folder,
            LiuXin_data_folder,
            LiuXin_packaged_calibre_resources_folder,
        )

        config_dir = LiuXin_calibre_resources_folder
        legacy_data_resources = os.path.join(LiuXin_data_folder, "calibre_resources")
        self.locations = []
        self.cache = {}

        def suitable(path):
            """
            Perform the suitable utility operation under explicit compatibility rules.

            Example:
                Exercise PathResolver.  init  .suitable through a consuming regression::

                    python -m pytest -q tests/utils/resources/test_resources.py


            :param path: Filesystem path read, written, normalized or validated by the
                operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            try:
                return os.path.exists(path) and os.path.isdir(path) and os.listdir(path)
            except OSError:
                pass
            return False

        # The selected external directory acts as an overlay. Missing files
        # continue to resolve from the immutable package-owned bundle.
        for candidate in (
            config_dir,
            legacy_data_resources,
            LiuXin_packaged_calibre_resources_folder,
        ):
            if candidate in self.locations:
                continue
            if suitable(candidate):
                self.locations.append(candidate)

        if not self.locations:
            self.locations.append(config_dir)

        self.default_path = self.locations[0]

        dev_path = os.environ.get("CALIBRE_DEVELOP_FROM", None)
        self.using_develop_from = False
        if dev_path is not None:
            dev_path = os.path.join(os.path.abspath(os.path.dirname(dev_path)), "resources")

            if suitable(dev_path):
                self.locations.insert(0, dev_path)
                self.default_path = dev_path
                self.using_develop_from = True

        user_path = os.path.join(self.default_path, "resources")
        self.user_path = None
        if suitable(user_path):
            self.locations.insert(0, user_path)
            self.user_path = user_path

    def __call__(self, path, allow_user_override=True):
        """
        Perform the call utility operation under explicit compatibility rules.

        Example:
            Exercise PathResolver.  call   through a consuming regression::

                python -m pytest -q tests/utils/resources/test_resources.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param allow_user_override: Value supplied for allow user override under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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

    Example:
        Exercise get path through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param data: Value supplied for data under the utility contract.
    :param allow_user_override: Value supplied for allow user override under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fpath = _resolver(path, allow_user_override=allow_user_override)
    if data:
        with open(fpath, "rb") as f:
            return f.read()
    return fpath


def get_image_path(path, data=False, allow_user_override=True):
    """
    Return image path under the documented compatibility and safety rules.

    Example:
        Exercise get image path through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param data: Value supplied for data under the utility contract.
    :param allow_user_override: Value supplied for allow user override under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not path:
        return get_path("images", allow_user_override=allow_user_override)
    return get_path("images/" + path, data=data, allow_user_override=allow_user_override)


def js_name_to_path(name, ext=".coffee"):
    """
    Perform the js name to path utility operation under explicit compatibility rules.

    Example:
        Exercise js name to path through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param ext: Value supplied for ext under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    path = ("/".join(name.split("."))) + ext
    d = os.path.dirname
    base = d(d(os.path.abspath(__file__)))
    return os.path.join(base, path)


def _compile_coffeescript(name):
    """
    Perform the compile coffeescript utility operation under explicit compatibility rules.

    Example:
        Exercise  compile coffeescript through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.serve_coffee import compile_coffeescript

    src = js_name_to_path(name)
    with open(src, "rb") as f:
        cs, errors = compile_coffeescript(f.read(), src)
        if errors:
            for line in errors:
                print(line)
            raise Exception(f"Failed to compile coffeescript: {src}")
        return cs


def compiled_coffeescript(name, dynamic=False):
    """
    Perform the compiled coffeescript utility operation under explicit compatibility rules.

    Example:
        Exercise compiled coffeescript through a consuming regression::

            python -m pytest -q tests/utils/resources/test_resources.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param dynamic: Value supplied for dynamic under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
I = get_image_path  # noqa: E741 - compatibility alias used by Calibre-derived code

calibreP = P
calibreI = I
