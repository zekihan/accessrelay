# accessrelay

Collect Traefik access logs from VictoriaLogs, retain them in SQLite, supervise
GoAccess, and serve its HTML and WebSocket reports from one application image.
GoAccess 1.12 remains the report renderer. Accessrelay owns collection, replay,
process supervision and HTTP serving.

| endpoint | behavior |
| --- | --- |
| `/`, `/index.html` | last complete GoAccess HTML; 503 until the first report exists |
| `/ws` | GoAccess WebSocket proxy on loopback; 503 while the renderer is unavailable |
| `/status.json` | cursor, freshness, collection errors, retention gaps, storage and renderer state |
| `/metrics` | Prometheus collection, lag, retry, export, renderer and storage metrics |
| `/health` | local collector progress, independent of VictoriaLogs availability |
| `/ready` | 200 with a report after replay initialization and renderer readiness; otherwise 503 |

## install

Create a namespace and a Secret named `accessrelay-connection` containing a file
named `connection.json`. Supply its contents through your secret-management
workflow. The file contains a backend `url` and an optional string-to-string
`headers` object. Use headers for tenant routing and authentication. Do not put
credentials in values, URLs, container arguments, or committed files.

```sh
helm upgrade --install accessrelay oci://ghcr.io/zekihan/charts/accessrelay \
  --version 0.1.1 --namespace accessrelay --create-namespace \
  --values examples/values.yaml
kubectl -n accessrelay port-forward service/accessrelay 8080:8080
```

Images: `docker.io/zekihan/accessrelay:0.1.1` and
`ghcr.io/zekihan/accessrelay:0.1.1`, for Linux amd64 and arm64.
The [chart reference](charts/accessrelay/README.md) describes all configuration.
Set the source cluster and the network policy's backend and ingress peers before
installing. Default policies allow DNS and deny other inbound/outbound traffic.
TLS and authentication belong at your ingress. The service has no built-in
account system and needs no Kubernetes API permissions.

For a standalone process, supply `--config /path/accessrelay.json` using
[the example configuration](examples/config.json). GoAccess 1.12 must be on
`PATH`, or set `goaccessBinary`. Use `--validate` to check configuration without
opening the database and `--version` to inspect version metadata. The container
bundles GoAccess and its dependencies; startup downloads nothing.

## collection contract

Queries use exclusive `[start,end)` windows and explicitly prohibit partial
responses. A response must end normally with complete, bounded NDJSON records.
Malformed records, missing source identity, embedded line breaks, oversized
responses, truncated bodies and redirects fail the complete window. Timeout and
size failures split windows down to `minimumWindow`. A failed half prevents
advancement over that half; completed earlier halves remain committed.

Every response is staged on disposable storage before one SQLite transaction
updates events, change records, the cursor, and the last-success timestamp.
Nanosecond timestamps and stream/pod/container identities are retained. Identical
records have an occurrence count: overlapping queries keep the maximum observed
multiplicity and append only increases. Two identical source records remain two
requests. Upstream delivery duplicates remain visible because the backend
provides no independent unique event ID.

Collection uses an ingestion delay, overlap on polling, bounded initial
backfill, and periodic reconciliation across retention for older late arrivals.
Reconciliation does not advance the collection cursor. Operational messages are
skipped using the inherited CLF shape check. CLF-shaped parsing failures remain
in the replay and GoAccess's failed-request counts and diagnostic files.
Connection files are reopened before every query, including split queries and
Kubernetes projected-secret replacements. Backend errors are opaque codes;
credentials never enter logs, status, metrics or process arguments.

## storage and recovery

SQLite `events.sqlite` is authoritative. Accessrelay preserves the original
Python collector's schema, JSON metadata, digest encoding and `user_version=0`.
No format migration is required for this release. The original collector can
reopen the database for rollback. See [compatibility](docs/compatibility.md) and
[upgrade and recovery instructions](docs/operations.md).

Use one replica, `Recreate`, and a retained ReadWriteOnce PVC. The application
also takes the inherited `collector.lock` using an exclusive POSIX file lock.
Storage must support file locks, atomic rename and durable sync. Network file
systems with unreliable locks are unsupported. Never run two independent PVCs
as collectors for the same report and expect their cursors to merge.

Retention covers UTC calendar dates including today; seven days means today and
the previous six dates. Restart and day rollover create a fresh replay with
`restore false`. The previous child is stopped and reaped before transferring
input ownership. A failed append invalidates that generation, stops its consumer,
and rebuilds from committed SQLite data. Published HTML is an independent atomic
snapshot, so regeneration and backend outages retain the last complete report.
GoAccess sends new report data over WebSocket; its HTML snapshot is refreshed on
renderer startup and graceful termination. The report shows collector freshness
and retention gaps using `/status.json`.

If a cursor falls behind the retained dates, accessrelay records the lost interval
and resumes from the retention boundary. Backend retention must cover the desired
history: a successful empty query cannot prove that a backend retained older
logs. Data lost before ingestion, late data arriving after retention, and silent
upstream omissions cannot be reconstructed.

## limits and health

Default state size is 2Gi, with a 1.5Gi database page budget and 32MiB free-space
reserve. Runtime storage defaults to a 3Gi `emptyDir`, a 2.5Gi application budget,
and a 1Gi limit per replay. A window response is limited to 256MiB, a line to 1MiB,
a report to 64MiB, and each diagnostic file to 8MiB. Diagnostics are truncated
in place every 250ms after exceeding the limit; short bursts can exceed that
threshold between checks. Filesystem/volume quotas provide the absolute disk cap.
SQLite reuses deleted pages; retention does not shrink its high-water file size.

Measure after initial backfill, adjust limits together with volume capacity, and
expand before sustained PVC usage reaches 80%. Storage pressure starts at 70%
used capacity. Runtime sizing must accommodate two replays, one staged window,
GoAccess's derived database, and report output during replacement. Never shrink
or replace an existing claim to reduce the configured size.

A VictoriaLogs outage makes status and the report stale while local health and
existing reports stay available. Readiness depends on replay and renderer state,
not a fresh backend response. Empty first boot produces a valid empty report;
long initial backfills continue behind a serving report. Renderer failures use
bounded exponential backoff and rebuild from SQLite. Shutdown cancels requests,
stops and reaps GoAccess, then releases the writer lock. Linux kills the child
if the parent dies unexpectedly; on other operating systems run under a
supervisor that terminates the whole process group after a hard parent failure.

## development and releases

```sh
make lint test test/race test/integration build
python3 -m venv /tmp/accessrelay-venv
/tmp/accessrelay-venv/bin/pip install -r tests/helm/requirements.txt
make chart-package PYTHON=/tmp/accessrelay-venv/bin/python
make container-test release-check
```

The integration suite uses disposable fixtures and isolated state directories.
It checks legacy database compatibility, HTTP/WS, live collection, repeated
records, late arrivals, failed windows and exports, transaction rollback,
restart, day rollover, backend outages, empty boot, secret rotation, writer
exclusion, diagnostics and cleanup. The container suite runs the original
Python/shell/Nginx deployment alongside accessrelay and compares actual GoAccess
request counts, bandwidth, failures and browser classifications.

Update `VERSION`, chart `version`, and chart `appVersion` together, validate, then
tag `v<version>`. Pull-request jobs have read-only permissions and no publishing
secrets. Release jobs rerun checks, publish both image platforms to both
registries, publish the OCI chart, verify anonymous manifests and chart rendering,
and attach chart and standalone binary archives to the GitHub release.

## license

Accessrelay is AGPL-3.0-or-later. The inherited collector contract, settings,
browser mappings and fixtures retain Apache-2.0 attribution. Bundled GoAccess is
MIT licensed and runs as a separate executable. See [NOTICE](NOTICE), [LICENSE](LICENSE)
and [LICENSES](LICENSES). The image includes the pinned GoAccess source archive
and license; the report links to accessrelay's source repository.
