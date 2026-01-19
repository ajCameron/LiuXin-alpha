__author__ = "Cameron"

import uuid
import time



def get_unique_group_id() -> str:
    """
    Produces a unique string intended to be used as an id.

    :return id_string:
    """
    return str(uuid.uuid4()) + str(time.clock())
