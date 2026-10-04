# accessrelay

Keep SQLite authoritative and retain compatibility with the inherited schema.
Every query window must commit completely before advancing its cursor. Preserve
nanosecond identity and occurrence multiplicity. Reopen connection secrets for
every query and expose only opaque error codes.

Run `make lint test test/race test/integration chart-package release-check` and
`make container-test` before release commits. Use Conventional Commits and no
author/co-author metadata. Update VERSION and chart version/appVersion together.
Tests use disposable fixtures; never mount the live PVC or connection Secret.
Do not modify runtime-owned GoAccess paths, format, UTC, restore mode or listener.
