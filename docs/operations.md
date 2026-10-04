# operations

## upgrade an existing collector

1. Confirm the release image and OCI chart were verified and review the effective
   values. Keep one replica, `Recreate`, UID/GID 1000, the existing connection
   Secret, and the retained claim. Record the claim UID, bound PV, storage class,
   requested capacity, ingress routes, TLS Secret and authentication middleware.
2. Create a consistent SQLite backup using SQLite's online backup API while the
   original collector runs, or stop its writer and copy the database together
   with any WAL/SHM files. Never copy only `events.sqlite` from a running writer.
   Save configuration and the last complete report separately.
3. Validate a copy in an isolated namespace/volume with fixture credentials.
   Never attach a migration test to the live PVC or pass the live secret into a
   disposable fixture. Compare stored multiplicity, report counts, classifications,
   cursor and retention against the original.
4. Stop the original writer and renderer. Confirm the pod is gone before
   starting accessrelay on the same claim. Update the deployment using the
   verified image and chart. Do not shrink, replace or delete the claim.
5. Check `/health`, `/ready`, `/status.json`, a complete report, WebSocket upgrades,
   backend query success and expected classifications. Confirm TLS and
   unauthenticated/authenticated ingress behavior and network-policy selectors.
   Confirm the claim UID, PV and capacity are unchanged.

The first run replays retained SQLite records with `restore false`; old GoAccess
database/report generations are derived caches and are cleaned after a complete
replacement report is published. Runtime replay files are disposable.

## rollback

Stop accessrelay and confirm its process and child have exited. Restore the
original deployment configuration, keeping the same claim and Secret. The
original collector can read the unchanged SQLite format, including records
collected by accessrelay; restoring the backup is unnecessary for a normal
application rollback. Restart with a fresh runtime replay and `restore false`.

For confirmed database corruption, preserve the damaged database and its WAL/SHM
files, stop all writers, then restore the consistent backup to an isolated copy
and validate it before replacing the damaged store. Restoring an older backup
moves the cursor backward; overlap/backfill recovers data still retained upstream.
Loss outside local/backend retention cannot be recovered and should be recorded
in the incident. Never restore or test against a database with an active writer.

## diagnose

- `backend_request`, `incomplete_response`, `invalid_record`: inspect backend
  availability and framing without printing connection credentials. The cursor
  remains before the failed window. Oversized/timeout windows split; failures at
  the minimum size require increasing limits or fixing the backend/record.
- `database_transaction`, `database_integrity`, `database_format`: retain the
  store, check filesystem health, and use an isolated backup for diagnosis.
- `database_limit`, `runtime_limit`, `storage_reserve`: expand volume/limits or
  reduce report history after considering retained data requirements. Collection
  stops committing under pressure; the last report stays available.
- `export_failed`, `renderer_start`, `renderer_exit`: inspect local permissions,
  GoAccess version and runtime capacity. Accessrelay automatically starts a fresh
  replay after failures. Repeated failures retain the last complete report.
- `writer_in_use`: another collector owns `collector.lock`; stop the competing
  workload. Deleting the lock file while a process holds it defeats exclusion.

After stopping all consumers, old files under runtime `generations/` and
`window-*` may be removed for capacity recovery. Keep `events.sqlite` and its
metadata. If startup encounters too much inherited disposable runtime data,
clean it only after confirming no GoAccess process still uses those files.
Diagnostics contain request data; secure the PVC and do not publish its contents.
Status exposes opaque reason codes and contains no backend credentials.

## retention and sizing

Set local and backend retention together. Local pruning occurs at UTC day
rollover even during an outage. Only twenty lost intervals are retained in
status, bounding metadata. A successful empty backend response cannot identify
expired upstream logs, so backend retention should be monitored independently.

Provision state for the database budget plus WAL, complete reports and bounded
logs. Provision runtime for old/new replay overlap, staging, GoAccess caches and
report output. `maxDatabaseBytes` also bounds SQLite's page count, while WAL and
transaction staging require additional capacity. The application checks runtime
usage and available disk before collection; filesystem/PVC quotas cap temporary
bursts. Shrinking a budget below existing data stops new work, not deletion of
committed events.
