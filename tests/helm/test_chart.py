"""Validate rendered runtime and storage contracts, including rejected overrides."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "charts/accessrelay"


def render(*args, release="goaccess"):
    result = subprocess.run(
        ["helm", "template", release, str(CHART), "-n", "goaccess", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return [x for x in yaml.safe_load_all(result.stdout) if x]


class ChartTests(unittest.TestCase):
    def test_secure_runtime_and_default_config(self):
        objects = render()
        pod = next(x for x in objects if x["kind"] == "Deployment")["spec"]
        self.assertEqual(pod["replicas"], 1)
        self.assertEqual(pod["strategy"], {"type": "Recreate"})
        spec = pod["template"]["spec"]
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertEqual(len(spec["containers"]), 1)
        security = spec["containers"][0]["securityContext"]
        self.assertTrue(security["readOnlyRootFilesystem"])
        self.assertEqual(security["capabilities"]["drop"], ["ALL"])
        self.assertFalse(security["allowPrivilegeEscalation"])
        self.assertEqual(spec["securityContext"]["runAsUser"], 1000)
        self.assertFalse(
            any(x["kind"] in {"Role", "ClusterRole", "Secret"} for x in objects)
        )
        pvc = next(x for x in objects if x["kind"] == "PersistentVolumeClaim")
        self.assertEqual(pvc["metadata"]["name"], "goaccess-state")
        self.assertEqual(
            pvc["metadata"]["annotations"]["helm.sh/resource-policy"], "keep"
        )
        policy = next(x for x in objects if x["kind"] == "NetworkPolicy")["spec"]
        self.assertEqual(policy["podSelector"], pod["selector"])
        self.assertFalse(policy["ingress"])
        self.assertEqual(len(policy["egress"]), 1)
        config = next(x for x in objects if x["kind"] == "ConfigMap")["data"][
            "accessrelay.json"
        ]
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write(config)
            f.flush()
            binary = os.environ.get("ACCESSRELAY_BINARY", ROOT / "dist/accessrelay")
            subprocess.run([str(binary), "--config", f.name, "--validate"], check=True)
        self.assertNotIn("url", json.loads(config))
        self.assertEqual(json.loads(config)["database"]["mmapBytes"], 0)
        self.assertEqual(json.loads(config)["database"]["integrityCheck"], "startup")
        self.assertEqual(json.loads(config)["database"]["replayPrefetch"], 0)
        self.assertFalse(json.loads(config)["report"]["streamReplay"])
        self.assertEqual(json.loads(config)["database"]["synchronous"], "full")

    def test_runtime_storage_is_disposable_and_independent_of_state(self):
        default, scratch = [
            next(obj for obj in objects if obj["kind"] == "Deployment")["spec"][
                "template"
            ]["spec"]
            for objects in (
                render(),
                render("--set", "runtimeStorageClass=scratch,runtimeSizeLimit=5Gi"),
            )
        ]
        default_vols = {vol["name"]: vol for vol in default["volumes"]}
        scratch_vols = {vol["name"]: vol for vol in scratch["volumes"]}
        self.assertEqual(default_vols["runtime"]["emptyDir"], {"sizeLimit": "3Gi"})
        self.assertEqual(scratch_vols["state"], default_vols["state"])
        self.assertEqual(scratch_vols["tmp"], default_vols["tmp"])
        self.assertNotIn("emptyDir", scratch_vols["runtime"])
        self.assertEqual(
            scratch_vols["runtime"]["ephemeral"]["volumeClaimTemplate"]["spec"],
            {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "scratch",
                "resources": {"requests": {"storage": "5Gi"}},
            },
        )

    def test_probe_timing_overrides_keep_default_endpoints(self):
        objects = render(
            "--set",
            "probes.startup.initialDelaySeconds=5,"
            "probes.liveness.terminationGracePeriodSeconds=40,"
            "probes.readiness.successThreshold=2",
        )
        deployment = next(x for x in objects if x["kind"] == "Deployment")
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        for probe, path, timing, expected in [
            ("startup", "/health", "initialDelaySeconds", 5),
            ("liveness", "/health", "terminationGracePeriodSeconds", 40),
            ("readiness", "/ready", "successThreshold", 2),
        ]:
            with self.subTest(probe=probe):
                self.assertEqual(
                    container[f"{probe}Probe"]["httpGet"], {"path": path, "port": "http"}
                )
                self.assertEqual(container[f"{probe}Probe"][timing], expected)

    def test_probe_handler_overrides_fail(self):
        handlers = {
            "httpGet": {"path": "/status.json", "port": "http"},
            "tcpSocket": {"port": "http"},
            "exec": {"command": ["true"]},
            "grpc": {"port": 8080},
        }
        for probe in ("startup", "liveness", "readiness"):
            for handler, settings in handlers.items():
                key = f"probes.{probe}.{handler}"
                with self.subTest(key=key):
                    with self.assertRaises(subprocess.CalledProcessError) as failure:
                        render("--set-json", f"{key}={json.dumps(settings)}")
                    self.assertIn(handler, failure.exception.stderr)

    def test_release_names_and_labels_render_as_strings(self):
        objects = render(
            "--set-string",
            "nameOverride=off,storage.existingClaim=off,connectionSecret=on,"
            "authService.name=false,authService.externalName=on",
            "--set",
            "ingress.enabled=true,authService.enabled=true",
            release="true",
        )
        auth = next(
            x for x in objects if x["kind"] == "Service" and x["spec"]["type"] == "ExternalName"
        )
        workload = {x["kind"]: x for x in objects if x is not auth}
        for kind in ["Deployment", "Service", "Ingress", "NetworkPolicy"]:
            with self.subTest(kind=kind):
                self.assertEqual(workload[kind]["metadata"]["name"], "true")
        expected = {"app.kubernetes.io/name": "off", "app.kubernetes.io/instance": "true"}
        deployment = workload["Deployment"]["spec"]
        self.assertEqual(deployment["selector"]["matchLabels"], expected)
        self.assertEqual(deployment["template"]["metadata"]["labels"], expected)
        self.assertEqual(workload["Service"]["spec"]["selector"], expected)
        pod_selector = workload["NetworkPolicy"]["spec"]["podSelector"]
        self.assertEqual(pod_selector["matchLabels"], expected)
        ingress_path = workload["Ingress"]["spec"]["rules"][0]["http"]["paths"][0]
        self.assertEqual(ingress_path["backend"]["service"]["name"], "true")
        self.assertEqual(auth["metadata"]["name"], "false")
        self.assertEqual(auth["spec"]["externalName"], "on")
        volumes = {v["name"]: v for v in deployment["template"]["spec"]["volumes"]}
        self.assertEqual(volumes["state"]["persistentVolumeClaim"]["claimName"], "off")
        self.assertEqual(volumes["connection"]["secret"]["secretName"], "on")

    def test_mapped_reads_opt_in(self):
        objects = render("--set", "config.database.mmapBytes=2147418112")
        config = next(x for x in objects if x["kind"] == "ConfigMap")["data"][
            "accessrelay.json"
        ]
        self.assertEqual(json.loads(config)["database"]["mmapBytes"], 2147418112)
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write(config)
            f.flush()
            binary = os.environ.get("ACCESSRELAY_BINARY", ROOT / "dist/accessrelay")
            subprocess.run([str(binary), "--config", f.name, "--validate"], check=True)

    def test_background_integrity_opt_in(self):
        objects = render("--set", "config.database.integrityCheck=background")
        config = next(x for x in objects if x["kind"] == "ConfigMap")["data"][
            "accessrelay.json"
        ]
        self.assertEqual(json.loads(config)["database"]["integrityCheck"], "background")
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write(config)
            f.flush()
            binary = os.environ.get("ACCESSRELAY_BINARY", ROOT / "dist/accessrelay")
            subprocess.run([str(binary), "--config", f.name, "--validate"], check=True)

    def test_replay_startup_opt_in(self):
        objects = render(
            "--set",
            "config.database.replayPrefetch=16",
            "--set",
            "config.report.streamReplay=true",
        )
        config = next(x for x in objects if x["kind"] == "ConfigMap")["data"][
            "accessrelay.json"
        ]
        self.assertEqual(json.loads(config)["database"]["replayPrefetch"], 16)
        self.assertTrue(json.loads(config)["report"]["streamReplay"])

    def test_report_limit_boundaries(self):
        render("--set", "config.report.jobs=1,config.report.chunkSize=32")
        render("--set", "config.report.jobs=12,config.report.chunkSize=1048576")
        for flag in ["config.report.jobs=13", "config.report.chunkSize=31"]:
            with (
                self.subTest(flag=flag),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                render("--set", flag)

    def test_normal_synchronous_opt_in(self):
        objects = render("--set", "config.database.synchronous=normal")
        config = next(x for x in objects if x["kind"] == "ConfigMap")["data"][
            "accessrelay.json"
        ]
        self.assertEqual(json.loads(config)["database"]["synchronous"], "normal")
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write(config)
            f.flush()
            binary = os.environ.get("ACCESSRELAY_BINARY", ROOT / "dist/accessrelay")
            subprocess.run([str(binary), "--config", f.name, "--validate"], check=True)

    def test_excluded_routers_opt_in(self):
        router = "traefik-mtls-journald-upload-fcc44d81276fc53b472d@kubernetescrd"
        objects = render("--set-json", f'config.collection.excludeRouters=["{router}"]')
        config = next(x for x in objects if x["kind"] == "ConfigMap")["data"][
            "accessrelay.json"
        ]
        self.assertEqual(json.loads(config)["collection"]["excludeRouters"], [router])
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write(config)
            f.flush()
            binary = os.environ.get("ACCESSRELAY_BINARY", ROOT / "dist/accessrelay")
            subprocess.run([str(binary), "--config", f.name, "--validate"], check=True)

    def test_existing_pvc_ingress_and_checksum(self):
        a = render()
        b = render(
            "--set",
            "storage.existingClaim=goaccess-state",
            "--set",
            "config.report.historyDays=3",
            "--set",
            "ingress.enabled=true",
            "--set",
            "networkPolicy.victoriaLogsPeers[0].ipBlock.cidr=192.0.2.1/32",
        )
        self.assertFalse(any(x["kind"] == "PersistentVolumeClaim" for x in b))
        self.assertTrue(any(x["kind"] == "Ingress" for x in b))
        checksum = lambda objects: next(
            x for x in objects if x["kind"] == "Deployment"
        )["spec"]["template"]["metadata"]["annotations"]["checksum/config"]
        self.assertNotEqual(checksum(a), checksum(b))
        policy = next(x for x in b if x["kind"] == "NetworkPolicy")["spec"]
        self.assertEqual(
            policy["egress"][1]["ports"], [{"protocol": "TCP", "port": 9428}]
        )

    def test_preserve_inherited_selector(self):
        objects = render(
            "--set",
            "nameOverride=goaccess",
            "--set",
            "selectorLabels.app\\.kubernetes\\.io/controller=goaccess",
        )
        deployment = next(x for x in objects if x["kind"] == "Deployment")["spec"]
        expected = {
            "app.kubernetes.io/name": "goaccess",
            "app.kubernetes.io/instance": "goaccess",
            "app.kubernetes.io/controller": "goaccess",
        }
        self.assertEqual(deployment["selector"]["matchLabels"], expected)
        labels = deployment["template"]["metadata"]["labels"]
        self.assertTrue(all(labels[k] == v for k, v in expected.items()))
        self.assertEqual(
            next(x for x in objects if x["kind"] == "Service")["spec"]["selector"],
            expected,
        )

    def test_unsafe_overrides_fail(self):
        for flag in [
            "replicaCount=2",
            "config.report.historyDays=0",
            "config.report.configOverrides.restore=true",
            "config.collection.pollInterval=0s",
            "config.stateDir=/other",
            "config.database.mmapBytes=-1",
            "config.database.mmapBytes=2147483649",
            "config.database.integrityCheck=never",
            "config.database.replayPrefetch=-1",
            "config.database.replayPrefetch=65",
            "config.database.synchronous=off",
            "terminationGracePeriodSeconds=5",
        ]:
            with (
                self.subTest(flag=flag),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                render("--set", flag)
        for value in ['[""]', '["a b"]', '["a\\"b"]', '["a","a"]']:
            with (
                self.subTest(excludeRouters=value),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                render("--set-json", f"config.collection.excludeRouters={value}")


if __name__ == "__main__":
    unittest.main()
