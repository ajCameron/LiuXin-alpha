
"""
Base class for ALL errors that LiuXin should ever throw.
"""


class LiuXinException(Exception):
    """
    Base LiuXin exception - should be at the root of every exception.
    """


class BadInputException(LiuXinException):
    """
    Are you _sure_ you meant that?
    """


InputIntegrityError = BadInputException