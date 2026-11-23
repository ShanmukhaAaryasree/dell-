# Project Meridian Final Presentation and Demo Guide

**Opening note.** The capstone brief describes 8,768 shipment records. This presentation uses the trainer-supplied pack of 141 shipment rows and a 43-event tracking fixture. The results demonstrate the pipeline on that sample. Throughput and correctness at full-dataset scale still need a separate run. See the [verification scope](docs/requirements-compliance.md#verification-scope) for what ran and what remains to be rerun.

1. **Architecture.** Open the README diagram. Explain how Databricks processes the CSVs and how Kubernetes runs the Kafka replay and Spark consumer.
2. **Batch quality.** Show the raw counts, the latest shipment version check, and the curated and rejected Delta outputs. Reconcile 141 input rows to 133 latest versions, then to 122 curated and 11 rejected shipments. Open `meridian-batch-kubernetes.log` for the Kubernetes Job counts and the [Delta verification log](evidence/meridian-delta-upsert-verification.log) for the update of a re-sent shipment.
3. **KPIs.** Open the monthly, carrier, lane and delay tables. Use the one-page insight summary to explain the operational findings and actions.
4. **Iceberg.** Show the four analytics tables and the added delay category column. In Spark SQL with the streaming `meridian` catalog configured, run `SELECT committed_at, snapshot_id, operation FROM meridian.tracking.event_windows.snapshots ORDER BY committed_at`. Use an earlier snapshot ID in `SELECT * FROM meridian.tracking.event_windows VERSION AS OF <snapshot_id>` and compare the rows with the current table.
5. **Recovery.** Show the [43-record local replay and restart](meridian-stream-after-restart.log), then the [separate ADLS run](meridian-adls-recovery-verification.log). ADLS recovered 44 raw records and 12 windows after pod replacement, then reached 45 records after one new accepted event.
6. **Operations.** Show the producer's separate demo topic and its [rolling update and rollback](evidence/kubernetes-producer-rollout.log). Each producer replica replays the full fixture. The streaming Deployment uses `Recreate` during updates to avoid overlapping Spark writers against the same checkpoint and warehouse. Processing pauses during replacement and resumes from the checkpoint.
7. **Verification.** Show the [requirements and evidence matrix](docs/requirements-compliance.md). The hosted CI link in the matrix is historical evidence for the upstream Meridian repository. For this Dell Upskilling checkout, show a fresh run of the repository-root `Project Meridian CI` workflow after it has been pushed.

Measure Kafka lag with `./measure-meridian-kafka-lag.ps1`. It compares each broker partition's end offset with the latest committed Spark checkpoint offset. Pass `-Deployment` and `-Topic` when checking another run. The [ADLS acceptance run](evidence/meridian-kafka-lag.csv) measured zero lag at offset 45.

## Reproduce the ADLS event counts

The local Kubernetes baseline used the 43-event fixture and a PVC. [Initial logs](meridian-stream-before-restart.log) show 43 new records and 12 windows. [Replacement logs](meridian-stream-after-restart.log) show zero new records, 43 retained records, and identical window rows.

Run the ADLS demonstration separately using a fresh run ID. The commands assume the Kafka broker, Spark image, and ADLS Secret are ready.

```powershell
$run = 'review'  # Use a fresh suffix for each attempt.
$topic = "shipment-tracking-adls-$run"
$deployment = "meridian-stream-adls-$run"
./start-meridian-live.ps1 -Adls -RunId $run

$fixture = Get-Content Logistics/Data/streaming/tracking_events_kafka_keyed.txt
$late = ($fixture[0] -split '\|', 2)[1] | ConvertFrom-Json
$late.event_id = 'EVT9999999'
$late.ingest_time = ([datetimeoffset]$late.event_time).AddMinutes(6).ToString("yyyy-MM-ddTHH:mm:ss'Z'")
$fixture + "$($late.shipment_id)|$($late | ConvertTo-Json -Compress)" |
  kubectl --context docker-desktop exec -i deployment/meridian-kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server meridian-kafka:9092 --topic $topic --reader-property parse.key=true --reader-property 'key.separator=|'

kubectl --context docker-desktop logs -f "deployment/$deployment" --tail=40
```

Wait for **44 saved records, 12 windows, and 32 window events**, then press Ctrl+C to stop following the logs. The 44 records comprise 32 accepted, 7 duplicate, 4 rejected, and 1 late. Replace the pod and check its startup state before sending another event.

```powershell
kubectl --context docker-desktop delete pod -l "app=$deployment"
kubectl --context docker-desktop wait --for=condition=Ready pod -l "app=$deployment" --timeout=300s
kubectl --context docker-desktop logs -f "deployment/$deployment" --tail=40
```

The startup line should show 44 saved records, 12 windows, and 32 window events. The recorded acceptance run had four checkpoint commits. The commit count can vary if Spark starts an empty batch before the fixture arrives. Press Ctrl+C, then send a second synthetic event with a fresh ID and the first fixture event's original on-time timestamps.

```powershell
$onTime = ($fixture[0] -split '\|', 2)[1] | ConvertFrom-Json
$onTime.event_id = 'EVT9999998'
"$($onTime.shipment_id)|$($onTime | ConvertTo-Json -Compress)" |
  kubectl --context docker-desktop exec -i deployment/meridian-kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server meridian-kafka:9092 --topic $topic --reader-property parse.key=true --reader-property 'key.separator=|'
kubectl --context docker-desktop logs -f "deployment/$deployment" --tail=40
```

The final Delta audit should show **45 raw records** split into **33 accepted, 7 duplicate, 4 rejected, and 1 late**. The saved query compared five Iceberg snapshots and found one changed window after the new accepted event. [ADLS verification log](meridian-adls-recovery-verification.log)

## Inspect a failed deployment

```powershell
kubectl --context docker-desktop apply -f meridian-failure-demo.yaml
kubectl --context docker-desktop scale deployment/meridian-failure-demo --replicas=1
kubectl --context docker-desktop get pods -l app=meridian-failure-demo
kubectl --context docker-desktop logs deployment/meridian-failure-demo
kubectl --context docker-desktop describe pods -l app=meridian-failure-demo
kubectl --context docker-desktop get events --sort-by=.lastTimestamp
kubectl --context docker-desktop scale deployment/meridian-failure-demo --replicas=0
```

The container logs show the simulated missing configuration. The pod description shows exit code `42` and Kubernetes records the restart backoff in its events.
