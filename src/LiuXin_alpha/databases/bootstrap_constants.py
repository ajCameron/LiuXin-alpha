"""
Small, dependency-light constants used during DB bootstrap.

These are intentionally kept in their own module so they can be imported by
both production code and test utilities without pulling in the full Database
module (which has heavier imports and side effects).
"""

from __future__ import annotations


# In FRBR-first/WEMI, "publishers" became agents (agent_type='organisation').
# Historically LiuXin used id = 0 sentinel rows in some tables; for agents we
# cannot store a SQL NULL because agents.agent_canonical_name is NOT NULL.
#
# This value is intentionally *obvious* in UI/debugging and unlikely to collide
# with real-world data.
AGENTS_NULL_CANONICAL_NAME: str = "DELIBERATELY SET NULL"
