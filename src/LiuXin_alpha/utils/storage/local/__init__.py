

import os


class CurrentDir(object):
    def __init__(self, path, workaround_temp_folder_permissions=False):
        self.path = path
        self.cwd = None
        self.workaround_temp_folder_permissions = workaround_temp_folder_permissions

    def __enter__(self, *args):
        self.cwd = os.getcwd()
        try:
            os.chdir(self.path)
        except OSError:
            if not self.workaround_temp_folder_permissions:
                raise
            from LiuXin_alpha.utils.ptempfiles import reset_temp_folder_permissions

            reset_temp_folder_permissions()
            os.chdir(self.path)
        return self.cwd

    def __exit__(self, *args):
        try:
            os.chdir(self.cwd)
        except:
            # The previous CWD no longer exists
            pass