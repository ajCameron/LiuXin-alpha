
# Core

# Storage

LiuXin is, at heart, two things.
 - A metadata store
 - A file store

The job of storage is to store files and metadata somewhere persistent.
Disc, TCP servers, tape archives.
It doesn't matter. The interface should be compatible.

Storage is also responsible for
 - space optimisation
 - backup
 - protecting from delete

My view is that archival software should _never delete anything_.
At least not without considerable checks.

