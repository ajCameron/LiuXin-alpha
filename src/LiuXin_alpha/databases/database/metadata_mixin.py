
import uuid

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class DatabaseMetadataMixin:
    """
    Mixin to handle the database metadata.
    """
    @property
    def uuid(self):
        if self._uuid is not None:
            return self._uuid
        else:
            self._uuid = self.driver_wrapper.get_uuid()
            return self._uuid

    @uuid.setter
    def uuid(self, value):
        self._uuid = value
        self.driver_wrapper.set_uuid(value)

    @property
    def library_id(self):
        """
        The UUID for this library. As long as the user only operates on libraries with LiuXin, it will be unique.

        :return:
        """
        if getattr(self, "_library_id_", None) is None:
            ans = self.driver_wrapper.get("SELECT library_id_uuid FROM library_id", all=False)
            if ans is None:
                ans = str(uuid.uuid4())
                self.library_id = ans
            else:
                self._library_id_ = ans
        return self._library_id_

    @library_id.setter
    def library_id(self, value):
        """
        Setter function for the library id - handles updating the database with the new id.
        :param value:
        :return:
        """
        self._library_id_ = six_unicode(value)
        self.macros.set_library_id(value)

    @property
    def database_version(self):
        """
        The UUID for this library. As long as the user only operates on libraries with LiuXin, it will be unique.
        :return:
        """
        if getattr(self, "_database_version_", None) is None:
            c = self.conn.cursor()
            version_val = None

            for row in c.execute("SELECT database_version_version FROM database_version;"):
                version_val = row[0]
            self._database_version_ = version_val
        return self._database_version_

    @database_version.setter
    def database_version(self, value):
        """
        Setter function for the library id - handles updating the database with the new id.
        :param value:
        :return:
        """
        self._database_version_ = six_unicode(value)
        self.macros.set_database_version(value)

