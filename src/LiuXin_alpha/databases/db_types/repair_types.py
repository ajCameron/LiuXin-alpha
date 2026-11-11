"""
Encode database scalar values losslessly in versioned Catalog repair receipts.
"""

import base64
from datetime import date, datetime, time
from decimal import Decimal


# Todo: We can type this
def repair_json_scalar(value):
    """
    Preserve native temporal, decimal and binary values with explicit type tags.

    JSON-native values retain their ordinary shape. This default hook is used for
    both state fingerprints and stored receipts; unsupported values are refused,
    never silently replaced by repr or truncated. Receipts are inspection records,
    not a JSON command format for replaying database writes.

    :param value:
    :return:
    """
    if isinstance(value, (datetime, date, time)):
        return {"$type": type(value).__name__, "iso": value.isoformat()}

    if isinstance(value, Decimal):
        return {"$type": "decimal", "value": str(value)}

    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "$type": "bytes",
            "base64": base64.b64encode(bytes(value)).decode("ascii"),
        }

    raise TypeError(f"Unsupported repair receipt value: {type(value).__name__}")
