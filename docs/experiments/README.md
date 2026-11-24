# Experiments and run logs

Dated records of what was measured and what happened. These are evidence, not guidance: for how the system works read [ARCHITECTURE.md](../ARCHITECTURE.md), and for how to run it read the [guides](../guides/).

| Report | Subject |
| --- | --- |
| [2026-10-08 source connectivity](2026-10-08-source-connectivity.md) | Which mempool.space hosts are reachable, and why the regional node is used |
| [2026-10-09 phase 0 and 1 validation](2026-10-09-phase0-phase1-validation.md) | Capture and replay checked end to end |
| [2026-10-14 first evaluation](2026-10-14-m3-first-evaluation.md) | The first Isolation Forest evaluation, 15,515 rows |
| [2026-10-14 synthetic sweep](2026-10-14-m3-synthetic-sweep.md) | Rules and model on labelled synthetic patterns |
| [2026-10-19 rerun on AWS data](2026-10-19-m3-rerun-aws-data.md) | The evaluation repeated on 2.65 million rows |
| [2026-11-24 Kubernetes](2026-11-24-de7-kubernetes.md) | The session log for DE7: cluster, chart, pod-kill drills |
| [2026-11-24 live capture and stream path](2026-11-24-live-capture-and-stream-path.md) | A live capture into Kafka; the streamed lake, compaction and stream serving checked on recorded traffic |

`reports/` holds raw machine output that is small enough to commit: the JSON report of each Kubernetes drill run, and the console output and capture summaries of the live checks. Larger evidence (captures, lake files, model artifacts) stays in the git-ignored `data/` folder on the machine that produced it.

Results measured during roadmap steps DE3 to DE6 are recorded in their guides (09 to 13), and the step-by-step account is in [archive/milestone-history.md](../archive/milestone-history.md).
