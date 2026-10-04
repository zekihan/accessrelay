# compatibility

This release ports `charts/goaccess` from `zekihan/argocd` revision
`41326211e9777480ecb337d5af4a17793108d39d`. The archived Python collector, shell
launcher and Nginx configuration in `tests/legacy/` are executable compatibility
fixtures, retain Apache-2.0 attribution, and are not runtime components.

GoAccess remains pinned to 1.12 and the inherited multi-platform runtime image
digest. The image build verifies the matching source archive SHA-256. GoAccess
uses TRAEFIKCLF, UTC, `persist true`, `restore false`, loopback port 7890, the same
report defaults, and the inherited browser classifications. Deployment-specific
uptime-agent tokens are supplied through `report.extraBrowsers` rather than
shipped in portable defaults. Runtime-owned settings and paths cannot be
changed through report overrides.

The database retains these tables and their original column order:

- `metadata(key,value)` with JSON values, including integer nanosecond cursor;
- `sources(id,identity)` with Python-compatible compact ASCII JSON identities;
- `events(id,digest,time_ns,source_id,message,occurrences)` with SHA-256 digests;
- `changes(seq,event_id,copies)` with an autoincremented append sequence.

Digests use the original JSON serialization of `[identity,time_ns,message]`,
including Unicode escaping, control-character escaping and surrogate pairs.
Compatibility tests generate legacy databases on isolated copies, replay the
same windows using Go, and assert no extra events or occurrences. Tests also
reopen the result with Python for rollback. Unknown SQLite schema versions,
missing/changed columns, extra application tables and failed integrity checks
are rejected. This release does not rewrite the schema or require a migration.

VictoriaLogs must implement the documented exclusive `end` parameter and honor
`options(allow_partial_response=false)`. See
[query semantics](https://docs.victoriametrics.com/victorialogs/querying/) and
[LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/).
HTTP framing can detect transport truncation, but cannot detect a server that
returns a normally terminated, syntactically valid subset in violation of this
contract. Do not add result-limiting pipes or permit partial responses.

See the [GoAccess manual](https://goaccess.io/man) for report settings and its
real-time HTML/WebSocket behavior.
