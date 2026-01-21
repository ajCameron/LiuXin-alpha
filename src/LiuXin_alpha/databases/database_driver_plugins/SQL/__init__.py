
"""
Common base driver for all SQL based database solution.

SQL based implementations are going to, inevitable, have a lot of code in common.
As such, this provides a base to build them from more easily.
Should not be used directly as a driver.
Should be subclasses for the particular dialect of SQL you're using.
"""


