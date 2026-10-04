"""Collect complete VictoriaLogs windows and export crash-recoverable CLF files."""

import argparse
import contextlib
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

SECOND = 1_000_000_000
TIME = re.compile(
    r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)$"
)
CLF = re.compile(r'^\S+ - \S+ \[[^\]\r\n]+\] "(?:[^"\\]|\\.)*" ')
FIELDS = ("_stream_id", "kubernetes.pod_id", "kubernetes.docker_id")


def timestamp_ns(value):
    match = TIME.fullmatch(value)
    if not match:
        raise ValueError("Invalid backend timestamp")
    stamp, fraction, offset = match.groups()
    seconds = int(
        datetime.fromisoformat(stamp + offset.replace("Z", "+00:00")).timestamp()
    )
    return seconds * SECOND + int((fraction or "").ljust(9, "0"))


def timestamp(value):
    date = datetime.fromtimestamp(value // SECOND, timezone.utc)
    return date.strftime("%Y-%m-%dT%H:%M:%S") + f".{value % SECOND:09d}Z"


def history_start(now, days):
    date = datetime.fromtimestamp(now // SECOND, timezone.utc)
    return (
        int(
            (
                date.replace(hour=0, minute=0, second=0, microsecond=0)
                - timedelta(days=days - 1)
            ).timestamp()
        )
        * SECOND
    )


def duration(value):
    match = re.fullmatch(r"([1-9]\d*)(s|m|h)", value)
    if not match:
        raise ValueError("Duration must be a positive integer with s, m or h suffix")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]]


def atomic_write(path, data):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as target:
        target.write(data)
        target.flush()
        os.fsync(target.fileno())
    temporary.replace(path)


class QueryError(Exception):
    def __init__(self, reason, split=False):
        super().__init__(reason)
        self.split = split


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fp.close()
        raise QueryError("redirect")


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA journal_size_limit=8388608")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sources (id INTEGER PRIMARY KEY, identity TEXT UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, digest BLOB UNIQUE NOT NULL,
                time_ns INTEGER NOT NULL, source_id INTEGER REFERENCES sources(id),
                message TEXT NOT NULL, occurrences INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS events_time ON events(time_ns);
            CREATE TABLE IF NOT EXISTS changes (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,
                copies INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS changes_event ON changes(event_id);
        """)

    def get(self, key, default=None):
        row = self.db.execute(
            "SELECT value FROM metadata WHERE key=?", (key,)
        ).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, key, value):
        self.db.execute(
            "INSERT OR REPLACE INTO metadata VALUES (?, ?)", (key, json.dumps(value))
        )

    def commit_window(self, stage, end, advance=True):
        self.db.execute("ATTACH DATABASE ? AS window", (str(stage),))
        try:
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO sources(identity) SELECT DISTINCT source FROM window.records"
                )
                self.db.execute("""
                    INSERT INTO changes(event_id, copies)
                    SELECT e.id, r.copies-e.occurrences FROM window.records r
                    JOIN events e ON e.digest=r.digest WHERE r.copies>e.occurrences
                """)
                self.db.execute("""
                    UPDATE events SET occurrences=(SELECT copies FROM window.records r WHERE r.digest=events.digest)
                    WHERE digest IN (SELECT digest FROM window.records)
                      AND occurrences < (SELECT copies FROM window.records r WHERE r.digest=events.digest)
                """)
                self.db.execute("""
                    INSERT INTO events(digest, time_ns, source_id, message, occurrences)
                    SELECT r.digest, r.time_ns, s.id, r.message, r.copies
                    FROM window.records r JOIN sources s ON s.identity=r.source
                    WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.digest=r.digest)
                """)
                self.db.execute("""
                    INSERT INTO changes(event_id, copies)
                    SELECT e.id, e.occurrences FROM window.records r CROSS JOIN events e ON e.digest=r.digest
                    WHERE NOT EXISTS (SELECT 1 FROM changes c WHERE c.event_id=e.id)
                """)
                if advance:
                    self.put("cursor", max(end, self.get("cursor", end)))
                self.put("last_success", time.time())
        finally:
            self.db.execute("DETACH DATABASE window")

    def prune(self, cutoff):
        with self.db:
            removed = self.db.execute(
                "DELETE FROM events WHERE time_ns<?", (cutoff,)
            ).rowcount
            self.db.execute(
                "DELETE FROM sources WHERE id NOT IN (SELECT source_id FROM events)"
            )
        self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return removed

    def latest_sequence(self):
        return self.db.execute("SELECT COALESCE(MAX(seq), 0) FROM changes").fetchone()[
            0
        ]

    def close(self):
        self.db.close()


class Client:
    def __init__(self, settings, runtime):
        self.settings = settings
        self.runtime = Path(runtime)
        self.opener = urllib.request.build_opener(NoRedirect)
        self.operational = 0

    def fetch(self, start, end):
        connection = json.loads(Path(self.settings["connectionFile"]).read_text())
        url = urllib.parse.urlsplit(connection["url"])
        if (
            url.scheme not in ("http", "https")
            or not url.netloc
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise QueryError("connection_configuration")
        source = self.settings["source"]
        filters = " ".join(
            f"{key}:{json.dumps(value)}"
            for key, value in (
                ("cluster", source["cluster"]),
                ("kubernetes.namespace_name", source["namespace"]),
                ("kubernetes.container_name", source["container"]),
            )
        )
        query = (
            "options(allow_partial_response=false) "
            + filters
            + " | fields _time, _stream_id, kubernetes.pod_id, kubernetes.docker_id, _msg"
        )
        params = {
            "query": query,
            "start": timestamp(start),
            "end": timestamp(end),
            "timeout": self.settings["queryTimeout"],
        }
        headers = connection.get("headers", {})
        if not isinstance(headers, dict) or any(
            not isinstance(v, str) for v in headers.values()
        ):
            raise QueryError("connection_headers")
        request = urllib.request.Request(
            connection["url"].rstrip("/") + "/select/logsql/query",
            urllib.parse.urlencode(params).encode(),
            headers,
        )
        stage = self.runtime / "window.sqlite"
        stage.unlink(missing_ok=True)
        db = sqlite3.connect(stage)
        db.execute("""CREATE TABLE records(digest BLOB PRIMARY KEY, time_ns INTEGER, source TEXT,
                   message TEXT, copies INTEGER)""")
        self.operational = 0
        try:
            with (
                db,
                self.opener.open(
                    request, timeout=duration(self.settings["queryTimeout"]) + 5
                ) as response,
            ):
                received = 0
                while True:
                    raw = response.readline(self.settings["maxLineBytes"] + 1)
                    if not raw:
                        break
                    received += len(raw)
                    if len(raw) > self.settings["maxLineBytes"] or not raw.endswith(
                        b"\n"
                    ):
                        raise QueryError("incomplete_or_oversized_line")
                    try:
                        row = json.loads(raw)
                        when = timestamp_ns(row["_time"])
                        message = row["_msg"]
                        if not isinstance(message, str) or not isinstance(
                            row["_stream_id"], str
                        ):
                            raise ValueError()
                        if not start <= when < end:
                            raise ValueError()
                        message = message.removesuffix("\n").removesuffix("\r")
                        if "\n" in message or "\r" in message or "\x00" in message:
                            raise ValueError()
                        identity = [row.get(field, "") for field in FIELDS]
                        if any(not isinstance(value, str) for value in identity):
                            raise ValueError()
                    except (ValueError, KeyError, TypeError) as error:
                        raise QueryError("invalid_record") from error
                    if not CLF.match(message):
                        self.operational += 1
                        continue
                    encoded_source = json.dumps(identity, separators=(",", ":"))
                    digest = hashlib.sha256(
                        json.dumps(
                            [identity, when, message], separators=(",", ":")
                        ).encode()
                    ).digest()
                    db.execute(
                        """INSERT INTO records VALUES (?, ?, ?, ?, 1)
                                  ON CONFLICT(digest) DO UPDATE SET copies=copies+1""",
                        (digest, when, encoded_source, message),
                    )
                expected = response.headers.get("Content-Length")
                if expected is not None and received != int(expected):
                    raise QueryError("incomplete_response", split=True)
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            http.client.IncompleteRead,
        ) as error:
            if isinstance(error, urllib.error.HTTPError):
                error.close()
            stage.unlink(missing_ok=True)
            split = (
                isinstance(error, (TimeoutError, http.client.IncompleteRead))
                or isinstance(getattr(error, "reason", None), TimeoutError)
                or getattr(error, "code", None) in (408, 504)
            )
            raise QueryError("backend_request", split=split) from error
        finally:
            db.close()
        return stage


class Collector:
    def __init__(self, settings, state, runtime, client=None):
        self.settings = settings
        self.state, self.runtime = Path(state), Path(runtime)
        self.state.mkdir(parents=True, exist_ok=True)
        self.runtime.mkdir(parents=True, exist_ok=True)
        self.lock = (self.state / "collector.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise
        self.store = Store(self.state / "events.sqlite")
        self.client = client or Client(settings, runtime)
        self.generation = None
        self.exported = 0
        self.dirty = True
        self.cutoff = None
        self.next_poll = 0
        self.reconcile = None
        self.next_reconcile = 0
        self.failure = None
        self.stopping = threading.Event()

    def rotate(self):
        identifier = uuid.uuid4().hex
        directory = self.runtime / "generations" / identifier
        directory.mkdir(parents=True)
        try:
            with (directory / "access.log").open("w") as output:
                for message, copies in self.store.db.execute(
                    "SELECT message, occurrences FROM events ORDER BY time_ns, id"
                ):
                    for _ in range(copies):
                        output.write(message + "\n")
                output.flush()
                os.fsync(output.fileno())
            atomic_write(self.runtime / "generation", identifier + "\n")
        except BaseException:
            shutil.rmtree(directory)
            raise
        self.generation = identifier
        self.exported = self.store.latest_sequence()
        self.dirty = False
        self.cleanup_generations()

    def cleanup_generations(self):
        acknowledged = self.runtime / "consumer"
        if (
            not acknowledged.exists()
            or acknowledged.read_text().strip() != self.generation
        ):
            return
        for path in (self.runtime / "generations").iterdir():
            if path.name != self.generation:
                shutil.rmtree(path)

    def export(self):
        if self.dirty:
            self.rotate()
            return
        try:
            with (self.runtime / "generations" / self.generation / "access.log").open(
                "a"
            ) as output:
                for sequence, message, copies in self.store.db.execute(
                    """
                    SELECT c.seq, e.message, c.copies FROM changes c
                    JOIN events e ON c.event_id=e.id WHERE c.seq>? ORDER BY c.seq
                """,
                    (self.exported,),
                ):
                    for _ in range(copies):
                        output.write(message + "\n")
                    self.exported = sequence
                output.flush()
                os.fsync(output.fileno())
        except OSError:
            self.dirty = True
            raise

    def window(self, start, end, advance):
        if self.stopping.is_set():
            return
        self.status()
        try:
            stage = self.client.fetch(start, end)
        except QueryError as error:
            if (
                error.split
                and end - start >= 2 * duration(self.settings["minimumWindow"]) * SECOND
            ):
                middle = start + (end - start) // 2
                self.window(start, middle, advance)
                self.window(middle, end, advance)
                return
            raise
        try:
            self.store.commit_window(stage, end, advance)
        finally:
            stage.unlink(missing_ok=True)
        self.export()
        self.cleanup_generations()

    def tick(self, now, monotonic):
        cutoff = history_start(now, self.settings["historyDays"])
        target = now - duration(self.settings["ingestionDelay"]) * SECOND
        if cutoff != self.cutoff:
            self.store.prune(cutoff)
            self.cutoff = cutoff
            self.dirty = True
            self.reconcile = None
            self.next_reconcile = 0
        cursor = self.store.get("cursor", cutoff)
        if cursor < cutoff:
            with self.store.db:
                gaps = self.store.get("gaps", [])[-19:]
                self.store.put(
                    "gaps",
                    gaps + [{"start": timestamp(cursor), "end": timestamp(cutoff)}],
                )
                self.store.put("cursor", cutoff)
            cursor = cutoff
        self.export()
        size = duration(self.settings["backfillWindow"]) * SECOND
        if cursor < target and (target - cursor > size or monotonic >= self.next_poll):
            end = min(cursor + size, target)
            start = (
                cursor
                if target - cursor > size
                else max(cutoff, cursor - duration(self.settings["overlap"]) * SECOND)
            )
            self.window(start, end, True)
            self.next_poll = monotonic + duration(self.settings["pollInterval"])
            self.failure = None
            return target - end > size
        if (
            self.reconcile is None
            and monotonic >= self.next_reconcile
            and cursor >= target - size
        ):
            self.reconcile = (cutoff, min(cursor, target))
        if self.reconcile:
            start, until = self.reconcile
            end = min(start + size, until)
            if start < end:
                self.window(start, end, False)
                self.reconcile = (end, until)
                self.failure = None
                return True
            self.reconcile = None
            self.next_reconcile = monotonic + duration(
                self.settings["reconciliationInterval"]
            )
        return False

    def status(self):
        now = time.time()
        last_success = self.store.get("last_success")
        cursor = self.store.get("cursor")
        disk = shutil.disk_usage(self.state)
        status = {
            "heartbeat": now,
            "initialized": self.generation is not None,
            "lastSuccessfulQuery": last_success,
            "cursor": timestamp(cursor) if cursor else None,
            "lagSeconds": max(0, now - cursor / SECOND) if cursor else None,
            "stale": last_success is None
            or now - last_success > duration(self.settings["staleAfter"]),
            "error": self.failure,
            "skippedOperationalLastWindow": self.client.operational,
            "storageUsedPercent": round((disk.total - disk.free) * 100 / disk.total, 1),
            "storagePressure": disk.free / disk.total < 0.3,
            "unrecoverableIntervals": self.store.get("gaps", []),
        }
        atomic_write(self.runtime / "status.json", json.dumps(status) + "\n")

    def run(self):
        backoff = 1
        while not self.stopping.is_set():
            try:
                immediate = self.tick(time.time_ns(), time.monotonic())
                backoff = 1
                self.status()
                if not immediate:
                    self.stopping.wait(1)
            except (QueryError, OSError, sqlite3.Error, ValueError, KeyError) as error:
                self.failure = (
                    str(error)
                    if isinstance(error, QueryError)
                    else type(error).__name__
                )
                print(
                    json.dumps({"event": "collector_retry", "reason": self.failure}),
                    flush=True,
                )
                with contextlib.suppress(OSError, sqlite3.Error):
                    self.status()
                for _ in range(backoff):
                    if self.stopping.wait(1):
                        break
                    with contextlib.suppress(OSError, sqlite3.Error):
                        self.status()
                backoff = min(backoff * 2, 60)

    def close(self):
        self.store.close()
        self.lock.close()


def health(runtime, readiness=False):
    try:
        status = json.loads((Path(runtime) / "status.json").read_text())
        return time.time() - status["heartbeat"] < 120 and (
            not readiness or status["initialized"]
        )
    except (OSError, ValueError, KeyError):
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="/config/collector.json")
    parser.add_argument("--state", default="/state")
    parser.add_argument("--runtime", default="/runtime")
    parser.add_argument("--health", choices=("live", "ready"))
    args = parser.parse_args()
    if args.health:
        raise SystemExit(0 if health(args.runtime, args.health == "ready") else 1)
    settings = json.loads(Path(args.config).read_text())
    collector = Collector(settings, args.state, args.runtime)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: collector.stopping.set())
    try:
        collector.run()
    finally:
        collector.close()


if __name__ == "__main__":
    main()
