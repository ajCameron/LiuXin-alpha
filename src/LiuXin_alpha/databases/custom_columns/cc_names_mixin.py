


class CCNamesMixin:
    """
    Names methods for custom columns.
    """

    def custom_field_name(self, label=None, num=None):
        """
        Gets the name for a custom field.

        :param label:
        :param num:
        :return:
        """
        if label is not None:
            return self.field_metadata.custom_field_prefix + label
        return self.field_metadata.custom_field_prefix + self.custom_column_num_to_label_map[num]
