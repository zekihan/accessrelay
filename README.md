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
Backend URLs must use HTTPS, including when no headers are configured. For a
loopback backend or a service mesh that supplies authenticated encryption, an
operator may explicitly set `"allowInsecureHTTP": true` in `connection.json`.
This acknowledges that accessrelay itself sends both headers and logs in
cleartext on that hop; verify the compensating boundary before opting in. TLS
verification and redirects remain unchanged. Existing HTTP connections must
migrate to HTTPS or receive this explicit operator acknowledgement before upgrade.

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
The shipped installation example disables Ingress and denies application ingress;
view reports through the documented local port-forward. Reports retain client IPs
and query strings. Before enabling a routable Ingress, configure controller-specific
authentication for **every** route above, including `/ws`, `/status.json`,
`/metrics`, `/health`, and `/ready`, and verify unauthenticated requests are denied.
TLS encrypts transport but does not authenticate viewers. Keep Ingress disabled
until that boundary is verified; this repository does not configure a live
controller or authentication provider. The service has no built-in
account system and needs no Kubernetes API permissions.
Browser WebSocket requests must have an Origin matching the public host, scheme,
and port in `report.websocketURL` (with `ws`/`wss` mapped to `http`/`https`).
Configure that URL for the address used by report viewers. Clients without an
Origin header remain supported; origin validation does not replace authentication.

For a standalone process, supply `--config /path/accessrelay.json` using
[the example configuration](examples/config.json). GoAccess 1.12 must be on
`PATH`, or set `goaccessBinary`. Use `--validate` to check configuration without
opening the database and `--version` to inspect version metadata. The container
bundles GoAccess and its dependencies; startup downloads nothing.

## collection contract

Queries use exclusive `[start,end)` windows and explicitly prohibit partial
responses. A response must end normally with complete, bounded NDJSON records.
Cluster, namespace, and container names use exact field matching. This prevents
similarly named sources from entering new query results; already retained rows
are not reclassified and remain subject to normal retention.
SQLite records the configured source identity and rejects subsequent changes
with `source_mismatch`. Inherited databases are bound on first upgraded startup;
verify their original source settings before adoption (see [operations](docs/operations.md)).
Malformed records, missing source identity, embedded line breaks, oversized
responses, truncated bodies and redirects fail the complete window. Timeout and
size failures split windows down to `minimumWindow`. A failed half prevents
advancement over that half; completed earlier halves remain committed.
Each root window, including all split children and discarded responses, shares
`maxWindowQueries` (64), `maxWindowTotalBytes` (512MiB), `maxWindowRecords`
(1,000,000 parsed/staged input records, including duplicates and operational
records), and `windowTimeout` (2m). Response bytes are charged when read, including
buffer read-ahead, with at most one byte of overshoot to detect an exact-bound EOF.
The timeout covers requests and context-aware staging/commits. Exhaustion returns
opaque `window_budget`, stops splitting, and uses the normal collection backoff.
Completed earlier children remain committed; retry starts from durable progress.
Per-response `maxWindowBytes`, line and query timeout limits still apply. Large
existing split trees may now stop early; tune these explicit aggregate bounds
alongside runtime capacity after measuring representative trusted workloads.
Unpaired Unicode surrogate escapes are rejected before decoding so different
input records cannot collapse into the same event identity.

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
Collection continues into SQLite while report rendering is unavailable, subject
to storage limits. Recovery rebuilds the report from committed records.

If a cursor falls behind the retained dates, accessrelay records the lost interval
and resumes from the retention boundary. Backend retention must cover the desired
history: a successful empty query cannot prove that a backend retained older
logs. Data lost before ingestion, late data arriving after retention, and silent
upstream omissions cannot be reconstructed.

## limits and health

Default state size is 2Gi, with a 1.5Gi database page budget and 32MiB free-space
reserve. Runtime storage defaults to a 3Gi `emptyDir`, a 2.5Gi application budget,
and a 1Gi limit per replay. A window response is limited to 256MiB, a line to 1MiB,
a report to 64MiB, and each diagnostic snapshot to 8MiB. GoAccess writes to
supervised FIFOs at its existing diagnostic paths. Read the bounded regular files
`/state/logs/unknowns.txt.snapshot` and
`/state/logs/invalid-requests.log.snapshot` for diagnostics. The supervisor drains
the FIFOs without restarting GoAccess when a snapshot reaches its limit.
Filesystem/volume quotas provide the absolute disk cap.
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
A renderer that does not produce a complete report and open its listener within
five minutes is stopped and retried. Boolean report overrides are normalized to
the spelling required by each GoAccess option.

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
