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


def render(*args):
    result = subprocess.run(["helm", "template", "goaccess", str(CHART), "-n", "goaccess", *args], check=True, capture_output=True, text=True)
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
        self.assertFalse(any(x["kind"] in {"Role", "ClusterRole", "Secret"} for x in objects))
        pvc = next(x for x in objects if x["kind"] == "PersistentVolumeClaim")
        self.assertEqual(pvc["metadata"]["name"], "goaccess-state")
        self.assertEqual(pvc["metadata"]["annotations"]["helm.sh/resource-policy"], "keep")
        policy = next(x for x in objects if x["kind"] == "NetworkPolicy")["spec"]
        self.assertEqual(policy["podSelector"], pod["selector"])
        self.assertFalse(policy["ingress"])
        self.assertEqual(len(policy["egress"]), 1)
        config = next(x for x in objects if x["kind"] == "ConfigMap")["data"]["accessrelay.json"]
        with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
            f.write(config)
            f.flush()
            binary = os.environ.get("ACCESSRELAY_BINARY", ROOT / "dist/accessrelay")
            subprocess.run([str(binary), "--config", f.name, "--validate"], check=True)
        self.assertNotIn("url", json.loads(config))

    def test_existing_pvc_ingress_and_checksum(self):
        a = render()
        b = render("--set", "storage.existingClaim=goaccess-state", "--set", "config.report.historyDays=3", "--set", "ingress.enabled=true", "--set", "networkPolicy.victoriaLogsPeers[0].ipBlock.cidr=192.0.2.1/32")
        self.assertFalse(any(x["kind"] == "PersistentVolumeClaim" for x in b))
        self.assertTrue(any(x["kind"] == "Ingress" for x in b))
        checksum = lambda objects: next(x for x in objects if x["kind"] == "Deployment")["spec"]["template"]["metadata"]["annotations"]["checksum/config"]
        self.assertNotEqual(checksum(a), checksum(b))
        policy = next(x for x in b if x["kind"] == "NetworkPolicy")["spec"]
        self.assertEqual(policy["egress"][1]["ports"], [{"protocol": "TCP", "port": 9428}])

    def test_unsafe_overrides_fail(self):
        for flag in ["replicaCount=2", "config.report.historyDays=0", "config.report.configOverrides.restore=true", "config.collection.pollInterval=0s", "config.stateDir=/other", "terminationGracePeriodSeconds=5"]:
            with self.subTest(flag=flag), self.assertRaises(subprocess.CalledProcessError):
                render("--set", flag)


if __name__ == "__main__":
    unittest.main()
