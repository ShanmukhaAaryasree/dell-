# Project Meridian

Meridian cleans historical shipment data, publishes delivery KPIs, and replays tracking events through Kafka and Spark. The capstone brief describes 8,768 shipment records. The trainer supplied a mini pack with 141 shipment rows, including eight re-sent versions. The reported results cover that mini pack. Full-dataset and production-volume validation require a separate run.

See the [requirements and evidence matrix](docs/requirements-compliance.md) and the [final presentation walkthrough](demo-guide.md).

## Architecture

```mermaid
flowchart LR
  CSV["Four historical CSVs"] --> DB["Azure Databricks · Scala Spark"]
  DB --> D["Delta · raw, curated and rejected data"]
  DB --> G["Four KPI tables · Delta and Iceberg"]
  R["Keyed tracking replay"] --> K["Kafka on Kubernetes"]
  K --> S["Spark Structured Streaming"]
  S --> I["Iceberg · replay records and 5-minute windows"]
  S --> A["Delta · live event dispositions"]
  S --> P["Persistent volume or ADLS Gen2 · warehouse and checkpoint"]
  K --> E["Kafka export"]
  E --> DB
```

## Run the batch notebook

1. Open a Scala notebook on Azure Databricks classic compute. For the managed Iceberg steps, use Databricks Runtime 16.4 LTS or above and an output schema with predictive optimization enabled.
2. Upload the four CSVs under `Logistics/Data/batch/raw` to the same Unity Catalog volume folder. Import `01_Shipments_Batch.scala` as a source notebook. Set the **Input volume** and **Output catalog and schema** widgets for your workspace.
3. Run the batch, KPI and Iceberg cells in order. Use a dedicated writable schema for verification. The notebook merges existing curated shipments and refreshes the other Meridian outputs. Run the tracking audit cells after uploading `meridian-kafka-events.jsonl` alongside the CSVs.

The batch starts with 141 shipment rows and retains 133 latest shipment versions before the quality split. The four KPI outputs cover monthly delivery performance, carrier SLA, lane performance, and delay reasons.

Run `meridian-business-queries.sql` in Databricks SQL for shipment counts by status and monthly freight revenue by customer segment.

## Run the portable batch job

Build `docker build -f meridian-batch.Dockerfile -t meridian-batch:v1 .` and run `docker run --rm meridian-batch:v1` for a local smoke test. To run it on Kubernetes, make the image available to the node and apply `meridian-batch-job.yaml`. Use `kubectl --context docker-desktop logs job/meridian-batch` for the completion counts. The packaged job writes four raw Delta datasets and the curated and rejected shipment datasets under `/tmp/meridian-batch` inside the container. Its default output is temporary, while the Databricks tables retain the published outputs.

## Run the Kubernetes replay

Prerequisites are Docker Desktop with Kubernetes enabled, `kubectl`, a Kafka Service named `meridian-kafka`, and a writable `ReadWriteOnce` PVC named `meridian-stream-state`. The demo used the `docker-desktop` context and the default namespace.

Apply `kubectl --context docker-desktop apply -f meridian-kafka.yaml` to create the demo broker, Service, and streaming PVC. The broker is disposable and the PVC stores the consumer state.

1. Use a single Kafka partition for `shipment-tracking-events`. Replay the supplied keyed text file with Kafka's console producer and `key.separator=|`. Confirm the topic end offset is 43 before starting Spark.
2. Build the versioned Spark image and make it available to the Kubernetes node. Create the consumer ConfigMap and apply the deployment from the repository root.

   ```powershell
   docker build -f meridian-stream.Dockerfile -t meridian-stream:v1 .
   kubectl --context docker-desktop create configmap meridian-stream `
     --from-literal=KAFKA_BOOTSTRAP_SERVERS=meridian-kafka:9092 `
     --from-literal=KAFKA_TOPIC=shipment-tracking-events
   kubectl --context docker-desktop apply -f meridian-stream-deployment.yaml
   kubectl --context docker-desktop rollout status deployment/meridian-stream --timeout=300s
   kubectl --context docker-desktop logs deployment/meridian-stream
   ```

3. For the producer deployment, build `docker build -f meridian-producer.Dockerfile -t meridian-producer:v1 .`. Make the image available to the Kubernetes node, create the single-partition topic `shipment-tracking-producer-demo`, and apply `meridian-producer-deployment.yaml`. The separate topic lets the producer scaling demo run independently of the consumer recovery test. Each new producer replica publishes a full 43-event replay.
4. For recovery, save the consumer logs, replace the Spark pod, and wait for the deployment to become ready. Compare its output with the first run. The same checkpoint should yield zero new input records, 43 persisted records, and identical windows.
5. For the Databricks audit, consume 43 values from `shipment-tracking-events` in order, save them as `meridian-kafka-events.jsonl`, and upload the file to the notebook's input volume.

The initial local replay and its replacement both retained 43 raw Kafka records. The replacement processed zero new records and reproduced the same 12 windows. The separate ADLS run started with the same 43-event fixture plus one deliberately late event. It recovered 44 raw records after pod replacement, then processed one additional accepted event. The final 45 raw records comprise 33 accepted, 7 duplicate, 4 rejected, and 1 late. See the [reproduction steps](demo-guide.md#reproduce-the-adls-event-counts) and [ADLS verification log](meridian-adls-recovery-verification.log).

## Run real-time streaming locally

Docker Compose can run the Kafka broker and continuous Spark consumer without Kubernetes. This path uses a durable Kafka volume and a separate named volume for the Spark checkpoint and Iceberg warehouse.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start-meridian-realtime.ps1
docker compose -f compose.realtime.yaml ps
docker compose -f compose.realtime.yaml logs -f stream
```

The default trigger interval is two seconds and can be changed with `MERIDIAN_TRIGGER_INTERVAL`. The consumer starts when Kafka and the live topic are ready. Publish a new event from another PowerShell window:

```powershell
$event = @{
  event_id = 'EVT9999001'
  shipment_id = 'SHP2026008426'
  hub_id = 'HUB06'
  event_type = 'ROUTE_CHECKPOINT'
  event_time = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
  ingest_time = (Get-Date).ToUniversalTime().AddSeconds(1).ToString('yyyy-MM-ddTHH:mm:ssZ')
  vehicle_id = 'VEH-1054'
  delay_minutes = 0
} | ConvertTo-Json -Compress
.\publish-meridian-event.ps1 -EventJson $event
```

The stream logs show the micro-batch and event dispositions. The checkpoint resumes after a consumer restart. Stop the containers with `docker compose -f compose.realtime.yaml down`; the named Kafka and state volumes remain available for restart.

## Run the continuous consumer on Kubernetes

With the Kubernetes Kafka broker and PVC running, use `powershell -NoProfile -ExecutionPolicy Bypass -File .\start-meridian-live.ps1`. This creates a separate `shipment-tracking-live` topic and deploys a Spark consumer that checks for events every two seconds by default. Set `MERIDIAN_TRIGGER_INTERVAL` to change the interval. It uses a separate checkpoint and Iceberg warehouse so the finite replay remains reproducible. Publish keyed events to the live topic to see new micro-batches in `kubectl --context docker-desktop logs -f deployment/meridian-stream-live`.

To use ADLS Gen2, provision an authorized Entra service principal and put its tenant ID, app client ID, and client secret in the `meridian-adls-auth` Kubernetes Secret as `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`. Use `meridian-adls-probe.yaml` to verify a Spark pod can write and read the project storage folder. Then pass `-Adls` to `start-meridian-live.ps1`. That mode uses its own Kafka topic and writes its checkpoint, Iceberg tables, and Delta event audit under the ADLS `meridian/streaming` prefix. Use `-RunId <suffix>` to start an independent run with a separate deployment, topic, checkpoint and warehouse. Keep the topic and storage paths consistent across pod replacements.

The streaming Deployment uses `Recreate` so Kubernetes terminates the old pod before starting its replacement during a Deployment update. One Spark writer owns the checkpoint and warehouse at a time, including when using ADLS. The update creates a short processing pause, then the replacement resumes from the durable checkpoint. The stateless producer uses a rolling update. Its update and rollback are in the [Kubernetes rollout log](evidence/kubernetes-producer-rollout.log).

## Assumptions and limits

- Batch keeps the newest shipment version by `last_updated` and the earliest signup date for repeated customer IDs. When those fields tie, lexical order of the serialized source row gives a deterministic result. Malformed shipment IDs go to the rejected table and are retained individually. Exact carrier and hub duplicates collapse. If distinct rows share an ID, the corresponding shipments are rejected because the reference data has no version timestamp. Historical shipments may still reference inactive hubs or carriers. Blank distance is permitted. Delivered and returned shipments require a completion date.
- Each batch input is a complete historical snapshot. After initial publication, the curated Delta table merges changed rows by shipment ID, inserts new rows, and removes rows absent from the current validated snapshot. Supply a complete snapshot when rerunning the batch.
- Delivery KPIs use delivered shipments and shipment month. Revenue and volume exclude cancellations. The streaming replay uses UTC event time, five-minute windows, and a five-minute arrival delay threshold. Clock-skewed events are flagged. The first valid copy of an event ID wins in the single-partition replay.
- Delta stores batch layers and the live event audit. The consumer writes one Delta table at `event_audit` under the streaming warehouse, partitioned by disposition. The Databricks notebook can also create separate audit tables from a Kafka export. Iceberg stores published KPIs, raw Kafka replay records and streaming aggregates.
- The Kubernetes consumer supports finite replay and continuous modes. The versioned, non-root image includes its Scala code and Kafka, Delta, Iceberg, and ADLS dependencies. A failed continuous query exits so Kubernetes can restart it from its configured checkpoint.
- GitHub Actions builds the three workload images and checks the producer fixture count, non-root users, streaming dependencies, and packaged batch reconciliation.
- For larger datasets, broadcast sufficiently small reference tables to reduce shipment join shuffles. Replace full window recomputation with incremental updates and compact small Iceberg files as the number of writes grows.

## Delta Lake and Iceberg

Delta Lake fits the Databricks quality layers because the notebook can use native Spark writes and transaction history to trace curation and update shipments by key. Iceberg fits the published KPI and tracking outputs because analytics engines that support the Iceberg catalog can read the same snapshots and evolved schema. Publishing in both formats gives a portable analytics copy and requires extra storage, writes, and a parity check. The notebook compares all four published Iceberg KPI tables with their Delta sources.

The `Logistics` folder contains the original capstone brief and earlier companion copies of the Scala consumer and Kafka export. Build the current consumer from `meridian-stream.scala` at the repository root.

## Validate locally

Run `powershell -NoProfile -ExecutionPolicy Bypass -File .\validate-project.ps1` from this folder with Docker Desktop running. It builds all three images, checks the producer fixture and packaged dependencies, and executes a fresh batch run against the supplied mini dataset. To run Kafka, Structured Streaming, checkpoint recovery, Kubernetes rollout, or ADLS tests, follow the environment-specific walkthroughs above. The archived logs are historical evidence from earlier runs; they are not a substitute for rerunning those environment-dependent checks. See the [verification scope](docs/requirements-compliance.md#verification-scope).

## GitHub Actions

The active workflow for this copy is [`../.github/workflows/project-meridian-ci.yml`](../.github/workflows/project-meridian-ci.yml) at the repository root. It builds the producer, batch, and streaming images; checks non-root execution and packaged dependencies; runs the batch reconciliation; and uploads its log. It runs for `main`, `feature/**`, `bugfix/**`, and `chore/**` pushes and pull requests targeting `main`. The linked upstream workflow results in the archived evidence refer to a different repository and do not mean this copy has passed GitHub CI. A run for this repository will be verifiable after the branch is pushed to GitHub.

## Dataset coverage

The supplied trainer pack contains 141 shipment rows (133 after keeping the latest version), 78 customer rows, 13 carrier rows, 25 hub rows, and 43 tracking events. The original brief describes 8,768 shipments. This implementation is exercised against the supplied mini pack; results and performance for the full dataset have not been measured here.

## Git and pull request workflow

Create a branch from an up-to-date `main` using `feature/<description>`, `bugfix/<description>`, or `chore/<description>`. Keep commits focused and use a conventional subject such as `feat: validate shipment references` or `fix: reject malformed tracking events`. Before opening a pull request, run `powershell -NoProfile -ExecutionPolicy Bypass -File .\validate-project.ps1`, inspect `git diff`, and confirm no secrets or generated data are included. Push the branch and open a pull request targeting `main`; wait for `Project Meridian CI`, request review, address feedback, and merge through GitHub. Do not develop directly on `main`.
