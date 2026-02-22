
from LiuXin_alpha.databases.api import RowAPI

from typing import Optional


class WEMIAdderMixin:
    """
    Add methods for the basic WEMI classes.
    """
    def work(self,
             work_type: str,
             work_medium: str,
             work_title: str,
             work_canonical_title: str,
             work_sort_title: str,
             work_original_language:) -> RowAPI:



