# Project Meridian Command Cheat Sheet

Use Windows PowerShell. Keep two windows in the same repository folder. Use Terminal A for status and writes, and Terminal B for `logs -f`. Press Ctrl+C in Terminal B to stop following logs. The consumer keeps running. The examples assume the `docker-desktop` context and the `default` namespace used by the project.

## A Read-only preparation

```powershell
Set-Location -LiteralPath 'C:/Users/shanm/Downloads/Logistics-20261120T140325Z-1-001'
git status --short --branch
git rev-parse --short HEAD
docker info --format '{{.ServerVersion}}'
kubectl --context docker-desktop get nodes --request-timeout=8s
kubectl --context docker-desktop -n default get deployments,jobs,pods,svc,pvc --request-timeout=8s
kubectl --context docker-desktop -n default get secret meridian-adls-auth -o name
docker image ls meridian-batch
docker image ls meridian-stream
docker image ls meridian-producer
```

Healthy signs are a Docker server version, a Kubernetes node at `Ready`, `meridian-kafka` available, and the `meridian-stream-state` PVC bound for the supplied deployment. The Secret command prints its name only. Omit it if presenting the local PVC mode. Topic listing requires a healthy broker.

```powershell
kubectl --context docker-desktop -n default exec deployment/meridian-kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server meridian-kafka:9092 --list
kubectl --context docker-desktop -n default rollout history deployment/meridian-producer
Get-Content meridian-adls-recovery-verification.log -Tail 18
Get-Content evidence/meridian-kafka-lag.csv
```

Opening the repository is safe while Kubernetes is unavailable. Use the saved outputs when the Docker or API preflight fails.

## B Input and quality checks

```powershell
Get-ChildItem Logistics/Data/batch/raw -Recurse -Filter '*.csv' | ForEach-Object {
  [pscustomobject]@{ File=$_.Name; Rows=@(Import-Csv -LiteralPath $_.FullName).Count }
}
Import-Csv Logistics/Data/batch/raw/shipments/shipments.csv |
  Group-Object shipment_id | Where-Object Count -gt 1 | Select-Object -First 3 Name,Count
Import-Csv Logistics/Data/batch/raw/shipments/shipments.csv |
  Where-Object { !$_.customer_id } | Select-Object -First 1 shipment_id,customer_id,carrier_id
(Get-Content Logistics/Data/streaming/tracking_events_kafka_keyed.txt).Count
Select-String -Path meridian-batch-kubernetes.log -Pattern 'Batch verified'
```

Expect 141 shipment, 78 customer, 25 hub, 13 carrier rows and 43 fixture lines. The saved batch reconciliation reads `raw=141 latest=133 curated=122 rejected=11`.

## C Batch execution

**Local write, isolated container.** This builds or updates the local image tag and runs a disposable container. The image uses a temporary output directory by default.

```powershell
docker build -f meridian-batch.Dockerfile -t meridian-batch:v1 .
docker run --rm meridian-batch:v1
```

**Optional isolated Kubernetes Job.** Use a unique lowercase Job suffix. Leave the existing archived Job in place.

```powershell
$job = 'meridian-batch-eval01'
(Get-Content meridian-batch-job.yaml -Raw).Replace('name: meridian-batch', "name: $job") |
  kubectl --context docker-desktop -n default create -f -
kubectl --context docker-desktop -n default wait --for=condition=complete "job/$job" --timeout=7m
kubectl --context docker-desktop -n default logs "job/$job" | Select-String 'Batch verified'
```

Use an image available to the node. If the run stops, inspect the Job and its pod events before retrying. A previous final demo attempt timed out during the local batch stage.

## D Databricks read-only display

In Catalog Explorer, open the actual output schema selected in the notebook. The examples use the default `aarya.default`. Run these in a Databricks SQL editor or SQL cell with read access.

```sql
SELECT 'Raw' AS layer, COUNT(*) AS row_count FROM aarya.default.meridian_shipments_raw
UNION ALL SELECT 'Curated', COUNT(*) FROM aarya.default.meridian_shipments_curated
UNION ALL SELECT 'Rejected', COUNT(*) FROM aarya.default.meridian_shipments_rejected;

SELECT shipment_id, rejection_reason
FROM aarya.default.meridian_shipments_rejected
ORDER BY shipment_id LIMIT 5;

DESCRIBE DETAIL aarya.default.meridian_shipments_curated;

SELECT * FROM aarya.default.meridian_kpi_monthly_delivery ORDER BY shipment_month;
SELECT * FROM aarya.default.meridian_kpi_carrier_scorecard LIMIT 12;
SELECT * FROM aarya.default.meridian_kpi_lane_performance LIMIT 10;
SELECT * FROM aarya.default.meridian_kpi_delay_reasons LIMIT 14;
DESCRIBE TABLE aarya.default.meridian_kpi_delay_reasons_iceberg;
```

Expect 141, 122, and 11 in the batch count query on the recorded dataset. The intermediate 133 appears in the notebook and batch log. `DESCRIBE DETAIL` shows the actual recorded table location if your role has permission. Do not infer a managed-table location from the notebook's source volume or the streaming warehouse.

**Optional cloud write.** For a fresh notebook run, import `01_Shipments_Batch.scala`, attach the previously used Scala-compatible compute, confirm the input widget points to a Unity Catalog volume containing the four CSVs plus the Kafka export, and use a dedicated writable output schema. Managed Iceberg and compute compatibility must be confirmed in your workspace. The batch snapshot merge deletes target rows absent from the complete source, so an existing shared schema is unsuitable for rehearsal. Prefer saved results during evaluation.

## E Prepare streaming resources

**Local infrastructure write.** Run only if the broker needs deploying or you're setting up a fresh demo environment. If the current broker already works, use the read-only checks above and leave it running.

```powershell
docker build -f meridian-stream.Dockerfile -t meridian-stream:v1 .
kubectl --context docker-desktop -n default apply -f meridian-kafka.yaml
kubectl --context docker-desktop -n default rollout status deployment/meridian-kafka --timeout=3m
```

Choose one target. Local mode uses the PVC. ADLS mode requires the existing Secret and access to the configured Azure account.

```powershell
$run = 'eval01'  # Choose an unused suffix.
$topic = "shipment-tracking-live-$run"
$deployment = "meridian-stream-live-$run"
.\start-meridian-live.ps1 -RunId $run
```

```powershell
$run = 'eval02'  # Choose an unused suffix.
$topic = "shipment-tracking-adls-$run"
$deployment = "meridian-stream-adls-$run"
.\start-meridian-live.ps1 -Adls -RunId $run
```

These commands create or update a topic, ConfigMap and Deployment. The suffix isolates the topic, checkpoint and warehouse. Keep the same suffix during that recovery test. Retain it for later inspection. Keep your credentials out of terminal and screen share. Non-secret settings include `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `MERIDIAN_STREAM_MODE`, `MERIDIAN_CHECKPOINT`, `MERIDIAN_WAREHOUSE`, and in ADLS mode `MERIDIAN_ADLS_HOST`. The Secret supplies `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`.

## F Publish and recover

**Writes.** Send the 43-line fixture once to a fresh local topic for the basic demo.

```powershell
$fixture = Get-Content Logistics/Data/streaming/tracking_events_kafka_keyed.txt
$fixture |
  kubectl --context docker-desktop -n default exec -i deployment/meridian-kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server meridian-kafka:9092 --topic $topic --reader-property parse.key=true --reader-property 'key.separator=|'
```

For the dedicated ADLS recovery run, use the following instead. Send the fixture with one extra late event to a fresh ADLS topic. Run this block once.

```powershell
$fixture = Get-Content Logistics/Data/streaming/tracking_events_kafka_keyed.txt
$late = ($fixture[0] -split '\|', 2)[1] | ConvertFrom-Json
$late.event_id = 'EVT9999999'
$late.ingest_time = ([datetimeoffset]$late.event_time).AddMinutes(6).ToString("yyyy-MM-ddTHH:mm:ss'Z'")
$fixture + "$($late.shipment_id)|$($late | ConvertTo-Json -Compress)" |
  kubectl --context docker-desktop -n default exec -i deployment/meridian-kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server meridian-kafka:9092 --topic $topic --reader-property parse.key=true --reader-property 'key.separator=|'
```

**Read-only.** Tail the intended Deployment after defining the same variables in Terminal B. Local replay should reach 43 saved records. ADLS baseline should reach 44 saved, 12 windows and 32 accepted events.

```powershell
kubectl --context docker-desktop -n default logs -f "deployment/$deployment" --tail=40
```

**Controlled restart, opt-in only.** Use the recorded log unless restart is permitted. Run this solely against your isolated ADLS demo. Confirm the topic and deployment variables, wait for 44 committed records, and leave the checkpoint and warehouse in place.

```powershell
if ($deployment -ne "meridian-stream-adls-$run" -or $topic -ne "shipment-tracking-adls-$run") {
  throw 'The isolated ADLS demo target was not selected.'
}
$pods = @(kubectl --context docker-desktop -n default get pods -l "app=$deployment" -o name)
if ($pods.Count -ne 1) { throw 'Expected one isolated Spark pod.' }
$pods[0]
kubectl --context docker-desktop -n default delete $pods[0]
kubectl --context docker-desktop -n default wait --for=condition=Ready pod -l "app=$deployment" --timeout=300s
kubectl --context docker-desktop -n default logs "deployment/$deployment" --tail=40
```

Verify the replacement reports 44 saved records, 12 windows and 32 accepted records before sending the next event. The recorded run had four checkpoint commits. Empty startup batches can change the commit count.

```powershell
$onTime = ($fixture[0] -split '\|', 2)[1] | ConvertFrom-Json
$onTime.event_id = 'EVT9999998'
"$($onTime.shipment_id)|$($onTime | ConvertTo-Json -Compress)" |
  kubectl --context docker-desktop -n default exec -i deployment/meridian-kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server meridian-kafka:9092 --topic $topic --reader-property parse.key=true --reader-property 'key.separator=|'
kubectl --context docker-desktop -n default logs "deployment/$deployment" --tail=50
.\measure-meridian-kafka-lag.ps1 -Deployment $deployment -Topic $topic
```

Check again after the commit if the first lag reading is positive. The recorded run ended with Spark offset 45, broker end offset 45, and lag 0.

## G Snapshot and deployment inspection

Run the following only in a Spark SQL session already configured with the streaming `meridian` Iceberg catalog, its warehouse and storage access. The ordinary Databricks KPI notebook uses different catalog names.

```sql
SELECT committed_at, snapshot_id, operation
FROM meridian.tracking.event_windows.snapshots ORDER BY committed_at;
SELECT COUNT(*) AS windows, SUM(total_events) AS events
FROM meridian.tracking.event_windows;
-- Replace SNAPSHOT_ID with an actual ID from the first query.
-- SELECT * FROM meridian.tracking.event_windows VERSION AS OF SNAPSHOT_ID;
```

The evidence records five snapshots and the comparison. Exact snapshot IDs must be read from the target catalog. Do not invent a value.

```powershell
kubectl --context docker-desktop -n default get deploy,rs,pods
kubectl --context docker-desktop -n default describe deployment "meridian-stream-adls-recovery"
kubectl --context docker-desktop -n default get events --sort-by=.lastTimestamp
Get-Content evidence/kubernetes-producer-rollout.log
```

The recorded producer update used `kubectl set env`, then `kubectl rollout status`, followed by `kubectl rollout undo --to-revision=1` on `meridian-producer-evidence`. Use that log in the evaluation. Changing env, rolling back, or scaling a live producer can replay the full fixture.
