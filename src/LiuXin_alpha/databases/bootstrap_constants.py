"""
Define the dependency-light sentinel text used for the null Agent bootstrap row.

AGENTS_NULL_CANONICAL_NAME is a conspicuous string for schemas where agent_canonical_name is NOT NULL. It represents the historical null-row identity without using SQL NULL and can be imported by bootstrap code and test utilities without loading Database.
"""

from __future__ import annotations


# In FRBR-first/WEMI, "publishers" became agents (agent_type='organisation').
# Historically LiuXin used id = 0 sentinel rows in some tables; for agents we
# cannot store a SQL NULL because agents.agent_canonical_name is NOT NULL.
#
# This value is intentionally *obvious* in UI/debugging and unlikely to collide
# with real-world data.
AGENTS_NULL_CANONICAL_NAME: str = "DELIBERATELY SET NULL"
