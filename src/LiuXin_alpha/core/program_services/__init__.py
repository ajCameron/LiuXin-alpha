"""Implementation owners for the program-facing Core operation families.

Endpoint providers own transport declarations; these modules own execution.
Dependencies flow to payload helpers and subsystem contracts, never through
the compatibility facade in ``core.program_api``.
"""
