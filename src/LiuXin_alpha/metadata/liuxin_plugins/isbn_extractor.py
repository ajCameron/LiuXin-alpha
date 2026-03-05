__author__ = "root"

# Extracts all strings shaped like ISBNs from a file.

from LiuXin.customize import MDInputTransform
from LiuXin.utils.logger import default_log


class ISBNMDInputTransform(MDInputTransform):
    """
    Non-functional test.
    """

    def transform_metadata(self, *md_collection):
        return md_collection
