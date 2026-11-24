#!/usr/bin/env bash
# Build the BlockPulse image and load it into the k3d node (the cluster has no registry to pull it from).
#
#   bash infra/k8s/image-load.sh
#
# With K3D_CA_BUNDLE set (see cluster-up.sh), the build trusts that CA and uses the host network, which a
# TLS-inspecting proxy needs. The CA is passed as a build secret and is not stored in the image.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
image="${BLOCKPULSE_IMAGE:-blockpulse:dev}"

extra=()
if [[ -n "${K3D_CA_BUNDLE:-}" ]]; then
  extra+=(--network host --secret "id=ca,src=${K3D_CA_BUNDLE}")
fi
docker build "${extra[@]}" -f "$root/infra/k8s/Dockerfile" -t "$image" "$root"
k3d image import "$image" -c blockpulse
echo "loaded $image into cluster blockpulse"
