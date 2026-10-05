"""
Group storage CLI implementations behind the public compatibility module.

Parsers depend on command owners. Commands use Core or the ingest application
service; path, process, and presentation helpers never construct subsystems.
"""
