"""Catalogue remote observations and optionally acquire their bytes through storage.

Remote candidates are not verified Assets or local Replicas. The application
workflow owns crawl receipts and restart state; the storage manager alone owns
publication, byte identity and retention.
"""
