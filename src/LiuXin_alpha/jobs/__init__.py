"""
Expose the supported jobs compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_repository.py
"""

from LiuXin_alpha.jobs.api import *  # noqa: F401,F403
