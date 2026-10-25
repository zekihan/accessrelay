# accessrelay chart

The chart deploys one secure accessrelay container, a ClusterIP Service,
configuration, a retained ReadWriteOnce PVC, optional Ingress and optional
NetworkPolicy. No RBAC resources or service-account tokens are needed.

| values | default / purpose |
| --- | --- |
| `image.repository`, `tag`, `digest` | GHCR; tag defaults to chart `appVersion`; optional digest pin |
| `selectorLabels` | extra immutable selector labels for preserving existing Deployments; base name/instance labels cannot be overridden |
| `replicaCount` | 1; only 0 or 1 permitted; strategy is always Recreate |
| `connectionSecret` | existing Secret with `connection.json`; mounted as a directory to allow rotation |
| `storage.existingClaim` | reuse an existing retained claim; suppresses PVC creation |
| `storage.size`, `storageClass`, `labels`, `annotations` | 2Gi; default class; deployment-specific policy |
| `runtimeSizeLimit`, `tmpSizeLimit` | 3Gi and 64Mi emptyDirs |
| `config.collection` | source, delay, polling overlap, reconciliation, backfill and query/line/window bounds, plus shared root-window query/byte/record/deadline budgets |
| `config.report` | UTC history, workers, chunk size, public WebSocket URL, safe report overrides and extra browser mappings |
| `config.limits` | database/runtime/replay/report/diagnostic budgets and free-space reserve |
| `resources` | CPU/memory/ephemeral-storage requests and limits |
| `probes` | startup/liveness/readiness timing; endpoints remain runtime owned |
| `service` | configurable service port/type/annotations; runtime port 8080 |
| `ingress` | host, class, annotations, TLS and additional routes; disabled by default |
| `authService` | optional namespace-local ExternalName Service for an ingress authentication outpost |
| `networkPolicy.ingressPeers` | clients allowed to TCP 8080; empty list denies ingress |
| `networkPolicy.victoriaLogsPeers`, `victoriaLogsPort` | backend peers and TCP port; empty list omits the allow rule |
| `networkPolicy.extraIngress`, `extraEgress` | explicit extra rules; default egress permits CoreDNS only |
| `nodeSelector`, `tolerations`, `affinity` | scheduling |
| `podAnnotations`, `podLabels`, `imagePullSecrets` | workload metadata and pull configuration |

Keep service/ingress selectors aligned with chart labels. HTTP routes, network
policy ports, mounted paths, UTC and replay settings are runtime owned. Safe
report overrides use string booleans (`"true"`, `"false"`, `"yes"`, `"no"`) or
supported date/hour values. Unknown or multiline overrides are rejected.

The chart retains created claims using `helm.sh/resource-policy: keep` and
`Prune=false,Delete=false`. An existing claim remains managed by its owner.
Expanding a PVC may require storage-provider support; no chart upgrade should
shrink, replace or delete retained storage. See [operations](../../docs/operations.md).

The documented `examples/values.yaml` installation keeps Ingress disabled and
application ingress denied. Use local port-forwarding until an operator configures
and verifies controller-specific authentication on every public route, including
WebSocket, status, metrics and probes. TLS alone does not protect report contents.
No controller authentication policy is assumed or installed by this chart.

Chart scalar settings are typed and reject line breaks; unknown settings in
scalar-bearing sections fail schema validation. Image policy is limited to
`Always`, `IfNotPresent`, or `Never`. Names, image references, ports and quantities
have explicit constraints, and direct string fields are YAML-quoted as a second
boundary. Kubernetes scheduling, NetworkPolicy rule and extra Ingress path objects
are serialized with `toYaml`; their API semantics remain the operator's
responsibility. Existing custom values should be rendered and reviewed before
upgrading to these stricter constraints.
