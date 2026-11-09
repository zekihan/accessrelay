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

On first startup, accessrelay binds an unbound inherited database to the configured
cluster, namespace and container. The inherited database does not record this
identity, so verify that the configured source is the same source used by the
original collector before starting on a retained database. Later changes to any
of those three fields are rejected with `source_mismatch`; credentials and
connection headers may rotate without changing the binding.

The first run replays retained SQLite records with `restore false`; old GoAccess
database/report generations are derived caches and are cleaned after a complete
replacement report is published. Runtime replay files are disposable.

## rollback

Stop accessrelay and confirm its process and child have exited. Restore the
original deployment configuration, keeping the same claim and Secret. The
original collector can read the unchanged SQLite format, including records
collected by accessrelay; restoring the backup is unnecessary for a normal
application rollback. Restart with a fresh runtime replay and `restore false`.
Clean shutdown restores ordinary diagnostic files at the inherited paths. If
accessrelay was killed abruptly, check `/state/logs/unknowns.txt` and
`/state/logs/invalid-requests.log` after all consumers have stopped: remove either
path if it is a FIFO before starting the original renderer. Bounded diagnostic
copies remain in the corresponding `.snapshot` files; SQLite is unaffected.

For confirmed database corruption, preserve the damaged database and its WAL/SHM
files, stop all writers, then restore the consistent backup to an isolated copy
and validate it before replacing the damaged store. Restoring an older backup
moves the cursor backward; overlap/backfill recovers data still retained upstream.
Loss outside local/backend retention cannot be recovered and should be recorded
in the incident. Never restore or test against a database with an active writer.

## diagnose

Startup and UTC rollover emit `phase_start`, `phase_progress` and `phase_finish`
logs on stderr. Each includes a fixed `phase` identifier; progress and finish
include `elapsed_seconds` and `completed`, and finish includes `success`.
No paths, request contents, connection details or raw errors are logged.

- `database_open` covers opening and validating SQLite, including the nested
  `database_integrity` check. During initialization, `/status.json` is available with
  `phase: initializing`, elapsed seconds and readiness false. `/health` and
  `/ready` return 503 until database validation finishes, preserving the startup
  probe budget. After validation, readiness still waits for collector initialization
  and renderer readiness. Use a pod port-forward to inspect status while Kubernetes keeps
  the pod out of ready Service endpoints.
- `retention` covers `retention_delete`, `source_cleanup` and
  `retention_checkpoint`. Deletion progress counts committed expired events.
- `replay_build` covers stopping the old renderer, reclaiming its generation,
  full export, file sync and starting the replacement child. `replay_export`
  measures SQLite reads and buffered output, including the final buffer flush;
  `replay_sync` measures the subsequent file sync.
- `renderer_initialize` runs from child preparation until a complete report is
  published and its WebSocket listener is reachable. It finishes unsuccessfully
  if initialization fails, times out or is stopped before readiness.

Replay progress counts lines accepted by the output buffer, including repeated
occurrences; it does not establish that those bytes have been flushed or synced. Deletion
and replay progress logs are limited to one per 30 seconds of advancing work;
a blocked operation has a start record but no advancing progress. Counts are
zero for phases without an event/line counter. Parent phases include nested
durations, so do not add them together. Routine incremental exports do not emit
phase logs. Use the last unfinished phase and completed phase durations to
separate slow database checks, retention, replay writes and renderer startup.

Full and incremental replay use a bounded 256KiB output buffer. The buffer must
flush successfully before the caller syncs the file and advances its exported
sequence or starts the renderer. A failed replay remains disposable and is
rebuilt from SQLite; collection cursor and multiplicity semantics are unchanged.

- `source_mismatch`: restore the cluster, namespace and container settings used
  when the database was bound. Use separate state for a different source; do not
  remove the binding to repurpose a populated database.
- `database_metadata`: preserve the database and inspect an isolated backup;
  missing or malformed fields in an existing source binding are rejected.
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
  Collection continues into SQLite while rendering is unavailable.
- `renderer_shutdown`: the renderer could not finish shutdown cleanup, including
  restoration of ordinary diagnostic files. Check the diagnostic paths before
  rollback; the collector still closes SQLite and releases its writer lock.
- `writer_in_use`: another collector owns `collector.lock`; stop the competing
  workload. Deleting the lock file while a process holds it defeats exclusion.

After stopping all consumers, old files under runtime `generations/` and
`window-*` may be removed for capacity recovery. Keep `events.sqlite` and its
metadata. If startup encounters too much inherited disposable runtime data,
clean it only after confirming no GoAccess process still uses those files.
Diagnostics contain request data; secure the PVC and do not publish its contents.
Read `logs/unknowns.txt.snapshot` and `logs/invalid-requests.log.snapshot` for
bounded diagnostic output; the original paths are supervised FIFOs while running.
Status exposes opaque reason codes and contains no backend credentials.

## retention and sizing

SQLite memory-mapped reads are opt-in with `database.mmapBytes` (Helm:
`config.database.mmapBytes`). The default `0` disables mapping, including when
the setting is omitted. Values from `0` through `2147483648` are accepted;
SQLite may clamp the requested limit to its compiled maximum. The current
Linux build supports `2147418112` bytes, the value used for copy-based testing.
Only the beginning of the database up to that limit is eligible for mapped
reads; pages beyond it use ordinary reads. The mapping does not reserve that
amount of RAM, but resident mapped pages consume memory within the container's
limit alongside the collector, renderer and filesystem cache.

Mapping can speed up integrity checks and replay on high-latency storage, but
can also amplify physical reads or regress performance. Test startup, retention
rollover and memory usage with an isolated database copy under the intended
resource limits before enabling it. WAL, full synchronous writes, integrity
validation and the database schema are unchanged. A mapped I/O error can
terminate the process instead of returning a recoverable SQLite error; see
[SQLite's memory-mapped I/O documentation](https://sqlite.org/mmap.html).
Set the value back to `0` and restart to disable mapping; no database conversion
is required.

Startup runs SQLite `PRAGMA quick_check` before retention, replay and report
rendering. On high-latency storage this check reads most of the database with
small random reads and can dominate restart time. Setting
`database.integrityCheck` (Helm: `config.database.integrityCheck`) to
`background` runs the same check on a separate query-only connection after the
first replay build instead, so the report becomes ready without waiting for it.
Collection and rendering proceed while the check runs; if it fails, the
collector stops with `database_integrity` and the process exits, exactly as a
failed startup check would. The default `startup` keeps the blocking check.
No database conversion is involved in switching between the two.

The full replay that rebuilds the report reads every event in time order.
Retention and reconciliation leave those rows scattered across the database
file, so on high-latency storage the replay is dominated by one small random
read at a time. `database.replayPrefetch` (Helm:
`config.database.replayPrefetch`) sets a number of query-only readers, `0`
through `64`, that read upcoming time ranges concurrently a bounded distance
ahead of the replay to warm the page cache. Their results are discarded; the
replay still reads every row itself in one snapshot, so its output is
unchanged. The cache they fill counts towards the container's memory limit but
is reclaimable. The default `0` disables prefetching.

`report.streamReplay` (Helm: `config.report.streamReplay`) starts GoAccess
while the full replay is still being written instead of after it, so the
export and GoAccess's initial parse overlap. GoAccess ends its initial parse at
the first end of file and never rewrites that snapshot, so the collector reads
GoAccess's offset in the replay from `/proc/<pid>/fdinfo` and suspends it with
`SIGSTOP` whenever it is within 64 MiB of the written length, resuming it with
`SIGCONT`. GoAccess reads past that point only after the replay is complete
and synced. If its offset cannot be observed, it stays suspended until then.
The replay path, GoAccess arguments and report contents are unchanged, the
renderer's initialization timeout starts only once the replay is complete, and
a report from a GoAccess stopped before then is never published. Streaming
requires Linux; elsewhere, or if GoAccess cannot be started early, it starts
after the replay as usual. The default `false` keeps the sequential start.

Set local and backend retention together. Local pruning occurs at UTC day
rollover even during an outage. Expired events and their change records are
removed in transactions of at most 1,000 events. During retention the WAL may
grow to about 64 MiB before an automatic checkpoint, so index pages touched by
several batches are written back once; the WAL is truncated when retention
finishes. Each committed batch updates
the collector heartbeat, so a large daily cleanup can make progress without
failing liveness. Full and incremental replays also report progress every 1,000 written lines.
Shutdown cancels retention and replay SQL; committed batches remain
pruned and the next run resumes the same cutoff before rebuilding the report.
A stalled batch still fails the normal progress health check. Only twenty lost intervals are retained in
status, bounding metadata. A successful empty backend response cannot identify
expired upstream logs, so backend retention should be monitored independently.

Provision state for the database budget plus WAL, complete reports and bounded
logs. Provision runtime for old/new replay overlap, staging, GoAccess caches and
report output. `maxDatabaseBytes` also bounds SQLite's page count, while WAL and
transaction staging require additional capacity. The application checks runtime
usage and available disk before collection; filesystem/PVC quotas cap temporary
bursts. Startup rejects a database budget below the existing database file's
page count with `database_limit`, even if pruning has freed pages inside that
file. Keep the budget at least as large as that high-water size; freed pages
remain reusable at the existing budget. The application does not shrink the
database or delete committed events to make a lower budget fit.
