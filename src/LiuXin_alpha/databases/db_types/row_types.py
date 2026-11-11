"""Local row payloads, including the existing missing-record sentinels.

An empty dictionary is an unidentified draft. False and None come from backend
lookups and remain observable on Row.row_dict; they are not empty draft values.
Database cell values are intentionally backend-defined.
"""

from typing import Any, Literal

type RowPayload = dict[str, Any] | Literal[False] | None
