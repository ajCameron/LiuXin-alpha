# Protected live storage contracts

The `Live Storage Read Contracts` GitHub Actions workflow exercises the four
official remote read paths (HTTP, FTP, rclone, and S3) on a weekly schedule and
by explicit dispatch. It is deliberately absent from pull-request triggers.

The jobs use the protected `live-storage-readonly` GitHub Environment. Configure
that environment with approval rules and credentials which can only read the
dedicated test objects. Each backend requires matching `*_ROOT` and `*_KEY`
secrets named in the workflow. S3 accepts the standard read-only AWS credential
secrets; rclone optionally accepts `LIUXIN_LIVE_RCLONE_CONFIG`.

Each matrix job fails when its endpoint pair is absent, so a green workflow
cannot mean that a supported backend silently skipped. The test contract only
performs location construction, stat, and a bounded read. It never calls Store
write, replace, or delete operations.
