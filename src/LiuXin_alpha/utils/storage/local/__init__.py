

"""
Expose the supported local compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
"""
import os


class CurrentDir(object):
    """
    Provide the CurrentDir utility contract with explicit state and cleanup behavior.

    Example:
        Exercise CurrentDir through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
    """
    def __init__(self, path, workaround_temp_folder_permissions=False):
        """
        Initialize and validate the CurrentDir state.

        Example:
            Exercise CurrentDir.  init   through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param workaround_temp_folder_permissions: Value supplied for workaround temp folder
            permissions under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.path = path
        self.cwd = None
        self.workaround_temp_folder_permissions = workaround_temp_folder_permissions

    def __enter__(self, *args):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise CurrentDir.  enter   through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise CurrentDir.  exit   through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            os.chdir(self.cwd)
        except:
            # The previous CWD no longer exists
            pass
