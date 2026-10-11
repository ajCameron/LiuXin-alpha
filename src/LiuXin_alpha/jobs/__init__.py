"""
Expose the supported jobs compatibility surface.

Separation of concerns: jobs own durable task definitions, scheduling, execution
records, progress, heartbeats, retries, and worker coordination. A job invokes
the workflow that owns a domain action; it does not acquire Catalog validation,
storage placement, or lifecycle policy merely because it runs that action in the
background. Low-level ``utils.jobs`` helpers remain execution primitives rather
than the durable source of truth.

See ``dev-docs/04 - Seperation of Concerns.md``.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_repository.py
"""

from LiuXin_alpha.jobs.api import *  # noqa: F401,F403
