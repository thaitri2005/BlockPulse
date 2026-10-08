# BlockPulse

BlockPulse is a planned real-time Bitcoin transaction anomaly pipeline. It will collect mempool observations, turn transactions into reproducible features, identify statistically unusual transaction structures, and make results available through an API and Grafana.

The project is a practical way to learn streaming data engineering, MLOps, reliability, networking, and deployment while keeping hardware requirements and cloud spending manageable. An anomaly score describes unusual behavior; it is not a probability of fraud or evidence of wrongdoing.

**Current status:** planning. No application has been implemented yet.

The [project plan and system description](docs/PROJECT_PLAN.md) is the canonical reference for the idea, architecture, scope, milestones, cost assumptions, and outstanding decisions. Start there when returning to the project.

The intended first deliverable runs locally: ingestion, Kafka, transaction features, baseline detection, replayable archives, queryable results, and observability. Entity analysis, Kubernetes, and an ephemeral AWS deployment are later extensions.
