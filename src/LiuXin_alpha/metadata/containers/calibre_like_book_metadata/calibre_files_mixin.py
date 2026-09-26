
"""
Manage file payload records, original-path history, and a legacy cleanup registry.

Registration alone does not close resources. close_cleanup_files attempts each
recorded close method without clearing the registry.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py
"""

import os



class FileMethodsMixin:
    """
    Provide file insertion and cleanup helpers for an owner with _data and _files_for_cleanup.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py
    """

    # Todo: Keep consistent with the cover
    def add_file(self, data, typ="path", file_id=None):
        """
        Store a file tuple and optional row id, consuming readable inputs from their current position.

        A stream is read into memory without being closed; the resulting payload is
        registered for cleanup, not the original stream. No path existence or payload format
        validation is performed. The tuple must be hashable for mapping storage.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


        :param data: Path, bytes, or readable object supplying the file payload.
        :param typ: Payload marker, defaulting to path.
        :param file_id: Optional database id associated with the file tuple.
        :return: None.
        """
        # Open file handles will probabl be closed when return is called with this metadata object - so read them into
        # memory to be safe
        if hasattr(data, "read"):
            data = data.read()

        file_tuple = (typ, data)

        _data = object.__getattribute__(self, "_data")
        _data["files"][file_tuple] = file_id

        self.register_file_for_cleanup(data)

    def record_path_and_file_name(self, file_path):
        """
        Append the original path and its basename to metadata history.

        This neither opens the path nor registers a file payload; use add_file for payload
        storage.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


        :param file_path: Original path accepted by os.path.split.
        :return: None.
        """
        object.__getattribute__(self, "_data")["filepath"].append(file_path)

        file_name = os.path.split(file_path)[1]
        object.__getattribute__(self, "_data")["filename"].append(file_name)

    def register_file_for_cleanup(self, file_pointer):
        """
        Append an object to the owner's cleanup registry without validation or deduplication.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py


        :param file_pointer: Object whose close method should be attempted later.
        :return: None.
        """
        open_files = object.__getattribute__(self, "_files_for_cleanup")
        open_files.append(file_pointer)

    def close_cleanup_files(self):
        """
        Attempt to close every registered object, ignoring AttributeError only.

        The registry is not cleared, so repeated calls may close a resource again. Other
        close failures propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
            >>> book = CalibreLikeLiuXinBookMetaData()
            >>> import io
            >>> stream = io.BytesIO(b'payload')
            >>> book.register_file_for_cleanup(stream)
            >>> book.close_cleanup_files()
            >>> stream.closed
            True


        :return: None.
        """
        open_files = object.__getattribute__(self, "_files_for_cleanup")
        for file_pointer in open_files:
            try:
                file_pointer.close()
            except AttributeError:
                pass
