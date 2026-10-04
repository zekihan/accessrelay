"""Run replacement and inherited runtime on disposable fixtures; compare real GoAccess reports."""
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
IMAGE = os.environ.get("ACCESSRELAY_IMAGE", "accessrelay:test")
PYTHON = "docker.io/library/python:3.14.5-alpine3.22@sha256:6b91e66ab2a880ce9ca5a1b91c70f45963ff71ff68268df056336e1a657d5efd"
GOACCESS = "docker.io/allinurl/goaccess:1.12@sha256:9e6dfd4abce94bd7c2907c1840b7b35f666587d56b24ffbb930b4782a87ea172"
NGINX = "docker.io/nginxinc/nginx-unprivileged:1.31.6-alpine@sha256:26b0bf6fbf07297983cb341998d79c831508787de26627dd2a112321b9c3a4af"


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, text=True, capture_output=True, timeout=180)


def wait(check, seconds=90):
    until = time.monotonic() + seconds
    last = None
    while time.monotonic() < until:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            last = error
        time.sleep(0.25)
    raise AssertionError(f"condition timed out: {last}")


def report(html):
    match = re.search(r"(?:var|let|const) json_data\s*=\s*", html)
    if match is None:
        raise AssertionError("missing GoAccess report data")
    return json.JSONDecoder().raw_decode(html[match.end():])[0]


class Backend(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        from urllib.parse import parse_qs
        params = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
        start, end = (datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", params[k][0]).replace("Z", "+00:00")) for k in ("start", "end"))
        with self.server.mutex:
            code = self.server.code
            rows = list(self.server.rows)
            self.server.tokens.append(self.headers.get("Authorization"))
        body = b"".join(json.dumps(row).encode() + b"\n" for row in rows if start <= datetime.fromisoformat(row["_time"]) < end)
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class RuntimeTests(unittest.TestCase):
    def test_parity_empty_backfill_late_outage_crash_rotation_and_cleanup(self):
        backend = ThreadingHTTPServer(("0.0.0.0", 0), Backend)
        backend.mutex = threading.Lock()
        backend.rows = []
        backend.code = 200
        backend.tokens = []
        thread = threading.Thread(target=backend.serve_forever, daemon=True)
        thread.start()
        names = {key: "accessrelay-test-" + uuid.uuid4().hex[:12] for key in ["app", "web", "goaccess", "collector"]}
        started = []
        with tempfile.TemporaryDirectory(prefix="accessrelay-fixture-") as temporary:
            root = Path(temporary)
            config = json.loads((ROOT / "examples/config.json").read_text())
            config["report"].update(historyDays=1, jobs=2)
            config["collection"].update(pollInterval="1s", ingestionDelay="1s", backfillWindow="1h", reconciliationInterval="1s", staleAfter="1s")
            config["limits"]["diagnosticBytes"] = 128
            for kind in ["app", "legacy"]:
                for directory in ["state", "runtime", "config", "connection"]:
                    path = root / kind / directory
                    path.mkdir(parents=True, mode=0o777)
                    path.chmod(0o777)
                (root / kind / "config/accessrelay.json").write_text(json.dumps(config))
                (root / kind / "connection/connection.json").write_text(json.dumps({"url": f"http://host.docker.internal:{backend.server_port}", "headers": {"Authorization": "fixture-a"}}))
            legacy = root / "legacy/config"
            for name in ["collector.py", "goaccess-start.sh", "nginx.conf"]:
                (legacy / name).write_text((ROOT / "tests/legacy" / name).read_text())
            (legacy / "browsers.list").write_text((ROOT / "internal/assets/browsers.list").read_text())
            (legacy / "goaccess.conf").write_text((ROOT / "internal/assets/goaccess.conf").read_text().replace("{{WS}}", "ws://localhost:8080/ws").replace("{{DAYS}}", "1").replace("{{BROWSERS}}", "/config/browsers.list"))
            (legacy / "collector.json").write_text(json.dumps(dict(config["collection"], historyDays=1, connectionFile="/connection/connection.json")))
            common = ["--user", "1000:1000", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,size=67108864,uid=1000,gid=1000"]

            def mounts(kind):
                return [arg for directory in ["state", "runtime", "config", "connection"] for arg in ["-v", f"{root}/{kind}/{directory}:/{directory}" + (":ro" if directory in {"config", "connection"} else "")]]

            ports = {}

            def request(kind, path="/"):
                with urllib.request.urlopen(f"http://127.0.0.1:{ports[kind]}{path}", timeout=3) as response:
                    return response.read().decode()

            def websocket(kind):
                with socket.create_connection(("127.0.0.1", ports[kind]), timeout=3) as connection:
                    connection.sendall(b"GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
                    return b"101 Switching Protocols" in connection.recv(4096)

            def replay_count(kind):
                if kind == "app":
                    paths = list((root / kind / "runtime/generations").glob("*/access.log"))
                    return max((len(path.read_text().splitlines()) for path in paths), default=-1)
                generation = (root / kind / "runtime/generation").read_text().strip()
                return len((root / kind / "runtime/generations" / generation / "access.log").read_text().splitlines())

            def restart(expected):
                docker("restart", "-t", "30", names["app"])
                docker("restart", "-t", "110", names["goaccess"])
                for kind, key in [("app", "app"), ("legacy", "web")]:
                    ports[kind] = int(docker("port", names[key], "8080/tcp").stdout.strip().rsplit(":", 1)[1])
                    wait(lambda: websocket(kind))
                    wait(lambda: report(request(kind))["general"]["valid_requests"] == expected)

            try:
                docker("run", "-d", "--name", names["app"], *common, *mounts("app"), "-p", "127.0.0.1::8080", IMAGE)
                started.append(names["app"])
                docker("run", "-d", "--name", names["web"], *common, *mounts("legacy"), "-p", "127.0.0.1::8080", "--entrypoint", "nginx", NGINX, "-c", "/config/nginx.conf", "-g", "daemon off;")
                started.append(names["web"])
                for key, image, command in [("goaccess", GOACCESS, ["sh", "/config/goaccess-start.sh"]), ("collector", PYTHON, ["python", "-u", "/config/collector.py"])]:
                    docker("run", "-d", "--name", names[key], *common, *mounts("legacy"), "--network", "container:" + names["web"], "-e", "PYTHONDONTWRITEBYTECODE=1", "--entrypoint", command[0], image, *command[1:])
                    started.append(names[key])
                for kind, key in [("app", "app"), ("legacy", "web")]:
                    ports[kind] = int(docker("port", names[key], "8080/tcp").stdout.strip().rsplit(":", 1)[1])
                    wait(lambda: "</html>" in request(kind))
                    wait(lambda: websocket(kind))
                    self.assertEqual(report(request(kind))["general"]["valid_requests"], 0)
                self.assertIn("accessrelay-status", request("app"))
                now = datetime.now(timezone.utc) - timedelta(seconds=15)

                def row(path="/hello?q=1", status="200", size="42", agent="Miniflux Client Library", pod="a"):
                    message = f'2001:db8::1 - - [{now.strftime("%d/%b/%Y:%H:%M:%S +0000")}] "GET {path} HTTP/2.0" {status} {size} "-" "{agent}" 1 "router" "http://192.0.2.2:80" 2ms'
                    return {"_time": now.isoformat(), "_stream_id": "stream-a", "kubernetes.pod_id": pod, "kubernetes.docker_id": "container-a", "_msg": message}

                rows = [row(), row(), row("/missing", "404", "7"), dict(row(), _msg="level=info started")]
                with backend.mutex:
                    backend.rows = rows
                for kind in ["app", "legacy"]:
                    wait(lambda: replay_count(kind) == 3)
                restart(3)
                reports = [report(request(kind)) for kind in ["app", "legacy"]]
                self.assertEqual(reports[0]["general"]["valid_requests"], 3)
                self.assertEqual(reports[0]["general"]["bandwidth"], 91)
                for report_data in reports:
                    self.assertEqual(report_data["general"]["valid_requests"], 3)
                    self.assertEqual(report_data["general"]["bandwidth"], 91)
                self.assertEqual(reports[0]["browsers"]["data"], reports[1]["browsers"]["data"])
                self.assertIn("Miniflux SDK", request("app"))
                with backend.mutex:
                    backend.code = 500
                wait(lambda: json.loads(request("app", "/status.json"))["stale"])
                for kind in ["app", "legacy"]:
                    wait(lambda: json.loads(request(kind, "/status.json"))["error"] == "backend_request")
                    self.assertIn("</html>", request(kind))
                    self.assertEqual(request(kind, "/health"), "ok\n")
                    self.assertTrue(websocket(kind))
                with backend.mutex:
                    backend.code = 200
                # Replace the projected symlink, as Kubernetes does when refreshing a Secret.
                connection = root / "app/connection"
                (connection / "next").mkdir()
                (connection / "next/connection.json").write_text(json.dumps({"url": f"http://host.docker.internal:{backend.server_port}", "headers": {"Authorization": "fixture-b"}}))
                (connection / "connection.json").unlink()
                (connection / "connection.json").symlink_to("next/connection.json")
                wait(lambda: "fixture-b" in backend.tokens)
                with backend.mutex:
                    backend.rows += [row("/late", agent="Home Assistant"), row("/invalid", "BAD", "bad")]
                for kind in ["app", "legacy"]:
                    wait(lambda: replay_count(kind) == 5)
                # Crash only the supervised renderer; accessrelay rebuilds without restore.
                docker("exec", names["app"], "sh", "-c", "kill -KILL $(pidof goaccess)")
                wait(lambda: json.loads(request("app", "/status.json"))["renderer"]["restarts"] >= 1)
                wait(lambda: json.loads(request("app", "/status.json"))["renderer"]["ready"])
                wait(lambda: report(request("app"))["general"]["valid_requests"] == 4)
                restart(4)
                for kind in ["app", "legacy"]:
                    data = report(request(kind))
                    self.assertEqual(data["general"]["valid_requests"], 4)
                    self.assertEqual(data["general"]["failed_requests"], 1)
                    self.assertIn("Home Assistant", request(kind))
                self.assertIn("accessrelay_cursor_lag_seconds", request("app", "/metrics"))
                # A competing process sharing the state exits before starting a renderer.
                result = docker("run", "--rm", *common, *mounts("app"), IMAGE, check=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("writer_in_use", result.stderr)
                for batch in range(3):
                    with backend.mutex:
                        backend.rows += [row(f"/bad/{batch}/{i}", "BAD", "bad") for i in range(20)]
                    wait(lambda: replay_count("app") == 5 + 20 * (batch + 1))
                    invalid = root / "app/state/logs/invalid-requests.log"
                    wait(lambda: invalid.exists() and invalid.stat().st_size <= 128)
                wait(lambda: len(list((root / "app/runtime/generations").iterdir())) == 1)
                self.assertLessEqual(len(list((root / "app/state/www").iterdir())), 1)
                self.assertFalse(list((root / "app/runtime").glob("window-*")))
                # Graceful termination must reap GoAccess and release the writer lock.
                before = time.monotonic()
                docker("stop", "-t", "30", names["app"])
                self.assertLess(time.monotonic() - before, 25)
                self.assertEqual(docker("inspect", "--format", "{{.State.ExitCode}}", names["app"]).stdout.strip(), "0")
            except BaseException:
                for name in started:
                    logs = docker("logs", "--tail", "10", name, check=False)
                    print(name, logs.stdout, logs.stderr)
                    print(docker("inspect", "--format", "{{.State.Status}} {{.State.ExitCode}} {{json .NetworkSettings.Ports}}", name, check=False).stdout)
                raise
            finally:
                for name in reversed(started):
                    docker("rm", "-f", name, check=False)
                for kind in ["app", "legacy"]:
                    docker("run", "--rm", "--user", "1000:1000", "--network", "none", *mounts(kind), "--entrypoint", "sh", IMAGE, "-c", "rm -rf /state/* /runtime/*", check=False)
                backend.shutdown()
                backend.server_close()
                thread.join()


if __name__ == "__main__":
    unittest.main()
