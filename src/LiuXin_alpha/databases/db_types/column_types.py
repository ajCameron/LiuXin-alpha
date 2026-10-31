
from typing import Mapping


type ColumnOptionValue = (
    str
    | int
    | float
    | bool
    | None
    | tuple[ColumnOptionValue, ...]
    | Mapping[str, ColumnOptionValue]
)
type ColumnOptions = Mapping[str, ColumnOptionValue]