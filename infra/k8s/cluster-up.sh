#!/usr/bin/env bash
# Create the local k3d cluster for BlockPulse and wait until it can run pods.
#
#   bash infra/k8s/cluster-up.sh            # create (or reuse) the cluster "blockpulse"
#   k3d cluster delete blockpulse           # remove it and everything in it
#
# Two optional fixes make the same cluster work inside a restricted container (a cloud dev session, some CI
# runners). On a normal laptop neither applies and the script is just `k3d cluster create`.
#
#   K3D_CA_BUNDLE=/path/ca.crt   Trust this CA inside the node. Needed when image pulls go through a TLS-inspecting
#                                proxy; without it pods stay in ContainerCreating with "x509: certificate signed by
#                                unknown authority".
#   K3D_OOM_SHIM=auto|on|off     The kubelet gives pods a negative oom_score_adj (protected from the OOM killer).
#                                A host that forbids lowering that value makes every pod fail with "can't get final
#                                child's PID from pipe". The shim clamps negative values to the node's own value.
#                                "auto" (default) installs it only when the node cannot lower its own value.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cluster="blockpulse"
node="k3d-${cluster}-server-0"

for tool in docker k3d kubectl; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

if k3d cluster list "$cluster" >/dev/null 2>&1; then
  echo "cluster $cluster already exists; reusing it"
  k3d cluster start "$cluster" >/dev/null
else
  extra=()
  if [[ -n "${K3D_CA_BUNDLE:-}" ]]; then
    [[ -f "$K3D_CA_BUNDLE" ]] || { echo "K3D_CA_BUNDLE is not a file: $K3D_CA_BUNDLE" >&2; exit 1; }
    extra+=(--volume "${K3D_CA_BUNDLE}:/etc/ssl/certs/ca-certificates.crt@server:*")
  fi
  k3d cluster create --config "$here/k3d.yaml" "${extra[@]}"
fi

shim="${K3D_OOM_SHIM:-auto}"
if [[ "$shim" == "auto" ]]; then
  if docker exec "$node" sh -c 'echo -1 > /proc/self/oom_score_adj' 2>/dev/null; then shim="off"; else shim="on"; fi
fi
if [[ "$shim" == "on" ]] && ! docker exec "$node" test -x /bin/runc.real; then
  echo "this host forbids lowering oom_score_adj; installing the runc shim in $node"
  docker exec "$node" mv /bin/runc /bin/runc.real
  docker cp "$here/runc-oom-shim.sh" "$node:/bin/runc"
  docker exec "$node" chmod 755 /bin/runc
fi

kubectl config use-context "k3d-${cluster}" >/dev/null
kubectl wait --for=condition=Ready node --all --timeout=120s
# The node reports Ready before its first pods run; CoreDNS running proves image pulls and the runtime both work.
# `rollout status` fails at once if the deployment doesn't exist yet, so retry while k3s applies its manifests.
ok=""
for _ in $(seq 1 30); do
  if kubectl -n kube-system rollout status deployment/coredns --timeout=10s >/dev/null 2>&1; then ok="yes"; break; fi
  sleep 10
done
if [[ -z "$ok" ]]; then
  echo "CoreDNS did not start; inspect with: kubectl -n kube-system describe pods" >&2
  exit 1
fi
kubectl get nodes
echo "cluster $cluster is ready (context k3d-${cluster})"
