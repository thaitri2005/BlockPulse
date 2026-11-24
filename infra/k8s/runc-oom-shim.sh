#!/bin/sh
# Installed as /bin/runc inside the k3d node by cluster-up.sh, only on hosts that forbid lowering oom_score_adj.
# The kubelet asks for negative values (-998 for the pause container, -997 for guaranteed pods); writing one is
# refused there and the container never starts. Rewrite negative values to the node's own value, then run the real
# runc. The cost: pods lose their OOM-killer protection relative to each other, which only matters under memory
# pressure. Never use this on a real cluster.
cur=$(cat /proc/self/oom_score_adj)
prev=""
for arg in "$@"; do
  case "$prev" in
    --bundle) [ -f "$arg/config.json" ] && sed -i "s/\"oomScoreAdj\":-[0-9]*/\"oomScoreAdj\":$cur/" "$arg/config.json" ;;
    --process) [ -f "$arg" ] && sed -i "s/\"oomScoreAdj\":-[0-9]*/\"oomScoreAdj\":$cur/" "$arg" ;;
  esac
  prev="$arg"
done
exec /bin/runc.real "$@"
