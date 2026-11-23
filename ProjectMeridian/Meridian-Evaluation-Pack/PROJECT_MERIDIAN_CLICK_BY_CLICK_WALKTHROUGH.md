# Project Meridian Click by Click Walkthrough

Use Chrome, VS Code, and two PowerShell windows. The [pre-demo checklist](PROJECT_MERIDIAN_PRE_DEMO_CHECKLIST.md) sets up tabs and read-only checks. The [command sheet](PROJECT_MERIDIAN_COMMAND_CHEAT_SHEET.md) gives complete copyable command blocks. **Live read** means inspecting current resources. **Isolated live write** means a rehearsed run using a dedicated topic, warehouse or schema. **Archived** means the result comes from a saved execution. At review time Kubernetes timed out, so stages 9, 10, 12 and 13 default to archived evidence.

## Step 1 Project introduction

**Time and requirements:** 1 minute. BR-12. **Application and path:** Chrome, [repository README](https://github.com/ShanmukhaAaryasree/project-meridian/blob/main/README.md).

**Click and show:** Open the repository README in its own tab. Scroll to the opening paragraph. Point to the name, the historical and live workloads, and the 141-row scope.

**Say:** “I'm [your name], and this is Project Meridian. A logistics team needs consistent delivery figures and visibility into tracking events. I built a batch workflow for shipment analytics and a streaming workflow that can recover after a Spark pod replacement.”

**Execute and expect:** No command. The README says the capstone describes 8,768 rows and the trainer pack contains 141.

**Meaning and value:** Sets the business question and scope before numbers appear.

**Questions:** Who uses it? Operations reviews delivery and exceptions, carrier managers review SLA, and data engineers investigate rejection and recovery. What did the project validate? Functional behavior on the trainer pack and the recorded replay scenarios.

**Mistake to avoid:** Claiming national throughput or full-data validation. **Backup:** Local `README.md` and the pitch DOCX. **Transition:** “Here is how I organized the project and its evidence.”

## Step 2 Repository overview

**Time and requirements:** 1 minute. BR-11, BR-12. **Application and path:** Chrome, [GitHub repository](https://github.com/ShanmukhaAaryasree/project-meridian).

**Click and show:** Click **Code**. Confirm the branch selector says **main**. Open `Logistics/Data`, then return to the root. Point to `01_Shipments_Batch.scala`, `meridian-stream.scala`, the Dockerfiles and manifests. Open `docs` → `requirements-compliance.md` in a separate tab.

**Say:** “The source data, application code, infrastructure files and evidence are all traceable here. The compliance matrix links every requirement to code and a result.”

**Execute and expect:** Optional local PowerShell read, `git rev-parse --short HEAD`. Review recorded `7d5412c`. A newer checkout can have another hash.

**Meaning and value:** Allows an evaluator to reproduce or inspect results without navigating the entire repository.

**Questions:** Which Scala consumer should I open? The root `meridian-stream.scala`; the copy under `Logistics` is earlier. Does GitHub deploy the production system? The workflow builds and tests images and runs packaged batch validation.

**Mistake to avoid:** Opening the historical companion copy as the current consumer. **Backup:** VS Code Explorer in the project folder. **Transition:** “Let's follow the two paths in the architecture.”

## Step 3 Architecture

**Time and requirements:** 2 minutes. BR-01, BR-05, BR-06, BR-08, BR-10, BR-13, BR-14. **Application and path:** Chrome, [README architecture](https://github.com/ShanmukhaAaryasree/project-meridian/blob/main/README.md#architecture).

**Click and show:** Scroll to the Mermaid diagram. Trace four CSV inputs to Azure Databricks. Follow the notebook writes to raw, curated and rejected Delta data and to KPI tables in Delta and Iceberg. Trace the keyed fixture to Kafka, then Spark, then the Iceberg replay and window output and the Delta audit. Point to the checkpoint and warehouse storage. Follow the separate Kafka export to Databricks for its offline audit.

**Say:** “Azure Databricks runs the Scala batch notebook. Kafka and the streaming Spark job run on local Kubernetes. The cloud-backed streaming test uses ADLS for durable storage. The export path gives Databricks a separate copy for audit.”

**Execute and expect:** No command. The rendered diagram shows both compute paths. The mini pack has no persisted dimensional warehouse tables.

**Meaning and value:** Separates compute, storage, data quality and business consumption.

**Questions:** Is streaming on an Azure cluster? Spark runs on local Kubernetes and uses ADLS in the cloud-backed test. Is Gold joined directly to the live stream? Batch KPIs and streaming windows are separate outputs linked by the domain identifiers.

**Mistake to avoid:** Calling ADLS a compute service. **Backup:** Local `README.md` and the execution-order diagram in the complete guide. **Transition:** “The inputs make those two paths concrete.”

## Step 4 Input datasets

**Time and requirements:** 2 minutes. BR-01 to BR-04, BR-06. **Application and path:** VS Code Explorer → `Logistics/Data/batch` and `Logistics/Data/streaming`.

**Click and show:** Open each file under `raw` → `shipments/shipments.csv`, `customers/customers.csv`, `carriers/carriers.csv`, `hubs/hubs.csv`. Point to shipment ID, `last_updated`, customer and carrier IDs, origin and destination hubs. In customers point to segment. In carriers point to SLA target. In hubs point to hub ID and city. Open `data_dictionary.csv`. Open `tracking_events.jsonl` and point to its event ID and two timestamps.

**Say:** “The shipment CSV stores versions, so a repeated shipment ID can represent an update. Customer, carrier and hub IDs are references. Bronze keeps the source values and Silver separates bad rows from usable shipments.”

**Execute and expect:** Use [command sheet B](PROJECT_MERIDIAN_COMMAND_CHEAT_SHEET.md#b-input-and-quality-checks) in PowerShell. Expected 141 shipments, 78 customers, 13 carriers, 25 hubs, 43 tracking lines. Show the repeated IDs and one missing customer reference returned by the commands.

**Meaning and value:** Demonstrates why record counts and unique business keys differ.

**Questions:** Why have two hub columns? Each shipment has an origin and destination, both pointing to the same reference entity. Is the data dictionary an input fact table? It defines fields and rules.

**Mistake to avoid:** Counting CSV headers as rows. **Backup:** `Logistics/Data/README.md` and the data dictionary. **Transition:** “Now I'll show where these inputs and outputs live.”

## Step 5 Azure infrastructure

**Time and requirements:** 2 minutes. BR-10. **Application and path:** Chrome, [Azure Portal](https://portal.azure.com/) and [known Databricks notebook URL](https://adb-7405616909283691.11.azuredatabricks.net/editor/notebooks/712596596865286?o=7405616909283691).

**Click and show:** Sign in privately. In the portal search bar type **Storage accounts**, open it, search **adlsgen2dbs**, then click the matching account. On **Overview**, read the resource group from the actual screen. Under **Data storage**, open **Containers** → **datasets** → **meridian** → **streaming**. For the recorded run, look for `checkpoints/event_windows-recovery` and `warehouse-recovery`. In the Databricks tab, click the workspace **Catalog** and locate the input volume selected in the notebook. Read the attached compute name from the notebook's compute selector or **Compute**, if your permissions allow it.

**Say:** “The streaming configuration uses the datasets container on adlsgen2dbs. It stores the recovery checkpoint and the table files. Unity Catalog governs access to the batch notebook's volume and output tables. Here is the actual compute and storage configuration I have permission to display.”

**Execute and expect:** Read-only click-through. Configured streaming root is `abfss://datasets@adlsgen2dbs.dfs.core.windows.net/meridian/streaming`. Resource group, compute name, live folders and active permissions were unavailable to the repository review. Show values only when visible.

**Meaning and value:** Establishes physical storage separately from batch and streaming compute.

**Questions:** Is the Databricks managed-table path the streaming root? The paths come from separate configurations. What if the Azure folder is forbidden? The signed-in identity may lack access. Use the saved read/write probe and recovery log.

**Mistake to avoid:** Opening access keys, secrets or networking changes on screen. **Backup:** `meridian-adls-write-verification.log` and `start-meridian-live.ps1` configuration. **Transition:** “With the storage boundaries clear, I'll show how the notebook handles the records.”

## Step 6 Databricks batch

**Time and requirements:** 3 minutes. BR-01 to BR-04. **Application and path:** Databricks notebook previously run, or VS Code `01_Shipments_Batch.scala`.

**Click and show:** Open the notebook from the known URL, or **Workspace** → your folder → the notebook copy you ran. Check the `input_volume` and `output_schema` widgets. Scroll through `spark.read`, `latestFirst`, `validatedShipments`, `shipmentsWithReferences`, and the curated `MERGE INTO`. Point to `row_number`, newest timestamp, tie-breaker, the left joins, and the rejection reason. The [code explanation](PROJECT_MERIDIAN_CODE_EXPLANATION.md) has line-level cues.

**Say:** “I read the CSV headers and convert values to their business types. I rank shipment versions by the newest timestamp, then use a deterministic tie-breaker. Left joins let me keep unresolved references long enough to explain their rejection. The curated Delta merge applies new or changed rows on repeat complete-snapshot runs.”

**Execute and expect:** Prefer saved notebook outputs or the [command sheet D](PROJECT_MERIDIAN_COMMAND_CHEAT_SHEET.md#d-databricks-read-only-display) count and rejection SQL. Archived 141 raw → 133 latest → 122 curated + 11 rejected. The independent Docker merge evidence shows one updated row and zero inserts or deletes for the tested resend.

**Meaning and value:** Provides reproducible KPI inputs and traceable rejection reasons. If counts differ, confirm input volume, schema, source version, and rejection reasons.

**Questions:** Why a left join? It exposes missing matches without discarding the offending row. Why can the merge delete rows? The curated source is a full validated snapshot, so absent keys should leave the current curated state.

**Mistake to avoid:** Running all cells against a shared schema or with a partial source. **Backup:** `meridian-batch-kubernetes.log` and `evidence/meridian-delta-upsert-verification.log`. **Transition:** “Those transformations feed three layers.”

## Step 7 Medallion tables and lineage

**Time and requirements:** 2 minutes. BR-01, BR-02, BR-04, BR-08. **Application and path:** Azure Databricks **Catalog** → output catalog → schema from the widget. Defaults are `aarya` and `default`.

**Click and show:** Open **Tables**. Select `meridian_shipments_raw`, then `meridian_shipments_curated`, then `meridian_shipments_rejected`. Use **Sample Data** when visible. Open `meridian_kpi_monthly_delivery` as an example of Gold. Point to source fields in Bronze, validated fields in Silver, rejection reason in quarantine, and monthly grain in Gold.

**Say:** “Bronze keeps the received information. Silver publishes clean rows and preserves rejected records with reasons. Gold changes the grain to business measures. I can trace a metric back to a shipment and a source version.”

**Execute and expect:** In the SQL editor run `DESCRIBE DETAIL aarya.default.meridian_shipments_curated` with the actual schema substituted. Point to the format and location if authorized. Copy no storage credentials.

**Meaning and value:** Distinguishes logical layers from actual table storage.

**Questions:** Are Bronze, Silver and Gold separate database schemas? They are logical layers with named Meridian tables. Are these persisted fact and dimension tables? The notebook uses an enriched star-style temporary view.

**Mistake to avoid:** Guessing a managed table's storage URL. **Backup:** Batch notebook and saved counts. **Transition:** “I'll use Gold to answer the business questions.”

## Step 8 Business KPI analytics

**Time and requirements:** 3 minutes. BR-05, BR-12. **Application and path:** Databricks **Catalog** → actual output schema, and [business insight summary](https://github.com/ShanmukhaAaryasree/project-meridian/blob/main/project-insights.md).

**Click and show:** Open in this order: `meridian_kpi_monthly_delivery`, `meridian_kpi_carrier_scorecard`, `meridian_kpi_lane_performance`, `meridian_kpi_delay_reasons`. Use each table's preview. Point to on-time rate and average transit days, SLA gap, route volume and revenue per kilogram, and late deliveries grouped by reason.

**Say:** “A shipment is on time when delivery happens on or before the promised date. Delivery-rate denominators use delivered shipments. The lane KPI excludes cancellations from volume and revenue, and calculates delivery metrics separately. The delay table focuses on late deliveries.”

**Execute and expect:** Read-only queries in [command sheet D](PROJECT_MERIDIAN_COMMAND_CHEAT_SHEET.md#d-databricks-read-only-display). The parity export recorded 11 monthly rows, 12 carrier rows, 89 lane rows, and 14 delay rows.

**Meaning and value:** Separates operational volume from the SLA denominator and routes the findings to the right business users.

**Questions:** What is an SLA gap? Actual on-time percent minus target percent, in percentage points. Why can a lane have a null on-time rate? It can have non-cancelled shipments and zero delivered shipments.

**Mistake to avoid:** Treating the row count of the carrier scorecard as the number of shipment records. **Backup:** KPI CSV and `project-insights.md`. **Transition:** “Next I'll look at tracking events as they arrive.”
