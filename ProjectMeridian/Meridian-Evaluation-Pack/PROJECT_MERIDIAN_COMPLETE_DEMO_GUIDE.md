# Project Meridian Complete Demo Guide

Use this guide to understand the system before rehearsing. During the evaluation, use the [click-by-click walkthrough](PROJECT_MERIDIAN_CLICK_BY_CLICK_WALKTHROUGH.md), [spoken timelines](PROJECT_MERIDIAN_PRESENTATION_SCRIPT.md), and [command sheet](PROJECT_MERIDIAN_COMMAND_CHEAT_SHEET.md).

## Project scope and verification

Project Meridian combines historical logistics analytics with near-real-time tracking. Operations managers can measure service quality, carrier teams can compare service performance with SLA targets, and data engineers can trace rejected rows, replays, and recovery. Finance analysts can also query freight revenue and shipment volume by customer segment. The solution aims to preserve raw input, explain data quality decisions, publish useful business measures, and resume processing after a Spark pod replacement.

| Evidence category | What is available |
|---|---|
| Source reviewed | The original 11-page capstone PDF in Downloads, its DOCX counterpart under `Logistics/`, BR-01 to BR-14, the tracked repository files, source configurations, saved exports, and run logs. |
| Current repository | GitHub `main` and local source matched commit `7d5412c1401caf7d734ee1b9b8636d233864c4eb` at verification. Hosted CI passed for that commit. |
| Archived execution | Batch reconciliation, Delta merge regression, Databricks KPI parity and schema evolution, PVC replay recovery, ADLS checkpoint recovery, Iceberg time travel, Kafka lag, and producer rollout and rollback. |
| Live access unverified | Azure Portal resource group, compute name and settings, current Unity Catalog table locations, current storage contents, and current workspace notebook title. A prior notebook URL is available. The current local Kubernetes API timed out. This pack does not record a fresh pipeline rerun. |
| Dataset limit | The brief describes 8,768 shipment records and a larger event dataset. The available training pack has 141 shipment rows and a 43-event fixture. Results and business findings have that scope. |

[Repository](https://github.com/ShanmukhaAaryasree/project-meridian) · [Hosted run](https://github.com/ShanmukhaAaryasree/project-meridian/actions/runs/44896883090) · [Original brief in repository](https://github.com/ShanmukhaAaryasree/project-meridian/blob/main/Logistics/PROJECT_MERIDIAN_Capstone_Logistics_Intelligence_Real-Time_Tracking.docx)

## Data and keys

| Dataset | Rows | Grain and business role |
|---|---:|---|
| `Logistics/Data/batch/raw/shipments/shipments.csv` | 141 | One source shipment version. Shipment ID identifies the business shipment, and `last_updated` orders versions. Customer, carrier, origin hub and destination hub IDs refer to the other files. |
| `Logistics/Data/batch/raw/customers/customers.csv` | 78 | Customer information and segment. Duplicate customer IDs require a selection rule. |
| `Logistics/Data/batch/raw/carriers/carriers.csv` | 13 | Carrier identity and target service level. The scorecard uses the carrier SLA percentage. |
| `Logistics/Data/batch/raw/hubs/hubs.csv` | 25 | Hub, city and location information. The same hub table supplies origin and destination roles. |
| `Logistics/Data/batch/data_dictionary.csv` | 45 | Field descriptions, data types, and validation guidance. It is reference documentation. |
| `Logistics/Data/streaming/tracking_events.jsonl` | 43 | One tracking message per line. Fields include `event_id`, shipment and hub IDs, `event_type`, `event_time`, `ingest_time`, vehicle ID, and delay minutes. The keyed text fixture adds a Kafka message key. |

Examples include old shipment versions, absent or unknown shipment references, invalid weights or charges, changed date formats, an event with no shipment ID, and duplicate event IDs. A primary key identifies an entity. A foreign key refers to an entity from another dataset. A shipment ID can appear multiple times in the raw feed because Bronze preserves source history.

## Architecture and execution order

```text
Historical CSVs -> Unity Catalog volume -> Azure Databricks Scala notebook
  -> Bronze raw Delta -> quality, versions and reference joins
  -> Silver curated Delta + rejected Delta
  -> shipment analytics view -> Gold KPI Delta tables -> Iceberg KPI copies

Keyed fixture -> Kafka producer -> Kafka Service and topic -> Spark consumer
  -> Iceberg raw replay -> Delta event audit -> Iceberg tracking windows
  -> Spark checkpoint on local PVC or ADLS Gen2

Git commit -> GitHub Actions -> container builds and packaged batch check
```

1. CSVs arrive in the notebook's input volume. Scala calls Spark to read them into DataFrames. Bronze retains the source rows, including bad data and older versions.
2. Silver ranks shipment versions by parsed `last_updated`, with a deterministic serialized-row tie-breaker. Malformed shipment IDs are retained for rejection. Validation checks dates, weights, freight charges, status, distance and references. A Delta merge applies new or changed curated rows on repeat runs of a complete snapshot.
3. Enrichment creates the `meridian_shipment_analytics` temporary view. Shipments form the fact grain, and customer, carrier and hub lookups provide dimension-like context. The view supports star-style analysis. Dedicated persisted fact and dimension tables would require a separate warehouse model.
4. Gold SQL calculates monthly delivery performance, carrier SLA, lane performance and delay reasons. The notebook saves each KPI to Delta, publishes an Iceberg copy and compares both directions with `exceptAll`.
5. The producer sends a finite fixture to a Kafka topic. Spark reads by partition and offset, retains the raw payload in Iceberg and classifies events in Delta. Accepted events contribute to five-minute UTC windows. The continuous mode checks for work on a ten-second trigger. The lateness policy compares ingest time with event time and uses a five-minute threshold. The streaming source has no Spark `withWatermark` operation.
6. Local PVC recovery and ADLS-backed recovery are separate tests. The recorded ADLS test went from 44 saved records and 12 windows before replacement to the same recovery state on the replacement, then 45 raw records after one additional event. The final audit totals 33 accepted, 7 duplicate, 4 rejected and 1 late.

## Technology choices and integration

| Technology | Why and where |
|---|---|
| Scala and Spark | Scala expresses typed application logic and uses Spark DataFrames and SQL. Databricks manages the batch Spark environment. Kubernetes runs Spark in local mode inside the packaged container for the demo. |
| Delta Lake | Keeps raw and quality outputs in transactional tables. Curated shipment writes use merge for the complete snapshot. Streaming Delta stores auditable dispositions. |
| Apache Iceberg | Provides a queryable analytics copy and snapshot history for the window results. It supports schema evolution and historical queries. Publishing to two formats adds storage, writes and parity checks. |
| Kafka | Decouples fixture publication from Spark processing and provides partition offsets. A fresh single-partition topic supports a predictable demo. |
| ADLS Gen2 | Durable Azure object storage for the cloud-backed checkpoint, Iceberg warehouse and Delta audit. Spark remains on local Kubernetes for this path. |
| Docker and Kubernetes | Docker packages code and dependencies. Kubernetes manages Pods, Services, Jobs and Deployments. The producer rolls out gradually. Spark uses `Recreate` on deployment updates to avoid overlapping checkpoint writers. |
| GitHub Actions | Builds the images and checks fixture count, user, dependencies and packaged batch results. Azure Databricks, Kubernetes and ADLS have separately collected evidence. |

## Example shipment journey

Use this as an illustrative journey. A source system sends a shipment ID twice with a newer timestamp on the second version. Bronze retains both. Silver selects the latest, verifies the shipment fields and references, and either accepts the row or records the rejection reason. If delivered on or before its promised date, it contributes to the corresponding delivery KPI. Later, a tracking event with that shipment ID arrives through Kafka. Spark validates it, records its disposition, and adds an accepted scan to its event-time window. The batch shipment and tracking event share a business ID. The current streaming query validates ID shape and does not join to the Databricks shipment table.

## Business outcomes and what comes next

- 63 of 78 delivered shipments arrived on time, or 80.77%. It gives operations a baseline to compare with future runs.
- Konkan Carriers delivered 3 of 5 on time, or 60%, compared with an 82% SLA. Review the two misses and gather a larger sample before changing allocations.
- Weather and traffic accounted for 11 of 15 late deliveries. Pilot weather alerts and earlier dispatch where delays recur, then measure the outcome.

This project goes beyond periodic CSV transformation by including event ingestion, replay safety, cloud-backed recovery, deployment operations and CI. Follow-on work includes full-data and throughput validation, a durable Kafka broker, incremental window updates instead of archive recomputation, stronger automated tests, and cross-partition ordering rules. The capstone establishes functional evidence on the mini dataset.

[Business findings](https://github.com/ShanmukhaAaryasree/project-meridian/blob/main/project-insights.md) · [Requirements evidence](PROJECT_MERIDIAN_REQUIREMENTS_EVIDENCE_MATRIX.md)
