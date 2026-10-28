# Midnight probe failures — 2026-10-28

Read-only inspection found the GoAccess pod running accessrelay 0.1.3 with five
restarts. Kubernetes reported `/health` and `/ready` probe failures; the two
retained application log entries were `shutdown_timeout` at 00:05:08 and
00:23:38 UTC. By 00:41 UTC the existing pod recovered without intervention:
health/readiness returned 200, the renderer was ready, cursor lag was about
32 seconds, and there was no storage pressure or collection error.

## Cause and limits of evidence

UTC day rollover runs retention followed by a complete replay. The collector's
health clock was updated only by the outer loop and backend-query starts.
Neither retention nor replay renewed it while completing useful storage work.
A multi-minute operation therefore looked unresponsive to the 110-second
progress watchdog (20-second query timeout plus 90 seconds). Six failing
30-second liveness probes could restart it. Retention and replay SQL also used
uncancellable database calls, explaining how shutdown could exceed its deadline.

The database schema page contained `events_source`, `events_time` and
`changes_event`; this recurrence does not establish a missing-index problem.
No stack/profile was captured during the earlier stall, so the exact time spent
in retention versus replay cannot be reconstructed from the available logs.
No live database was copied, modified or used for a test.

## Prepared correction

Retention commits at most 1,000 expired events per transaction, with dependent
change records removed by the existing cascade. Each completed batch updates
the progress clock. Cancellation preserves completed batches, and a subsequent
run resumes the cutoff. Cursor and gap metadata remain unchanged.

Full and incremental replay update progress after each 1,000 written lines.
Both storage paths propagate cancellation through SQL; replay also checks it
between line writes. The existing health deadline, probe settings and readiness
requirements remain unchanged. A blocked batch or export without further
progress can still fail liveness.

Fixture tests cover batch boundaries, cancellation/resumption, cascades, retained
multiplicity and metadata, indexed cutoff selection, replay cancellation and
health-clock renewal. Production acceptance still requires a separately
published/deployed version and observation across UTC rollover; no rollout or
live maintenance was performed for this fix.
