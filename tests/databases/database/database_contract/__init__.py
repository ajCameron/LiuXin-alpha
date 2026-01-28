"""Database-level contract tests.

These tests validate the higher-level :class:`LiuXin_alpha.databases.database.Database`
behavior.

They intentionally exercise real driver backends (via the same driver
parametrization used by driver contract tests) so they also act as proxy tests
for driver lifecycle correctness (resource cleanup, handle release, etc.).
"""
