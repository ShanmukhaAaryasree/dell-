// Databricks notebook source
dbutils.widgets.text("input_volume", "/Volumes/aarya/default/application_files", "Input volume")
dbutils.widgets.text("output_schema", "aarya.default", "Output catalog and schema")
val inputVolume = dbutils.widgets.get("input_volume").trim.stripSuffix("/")
val outputSchema = dbutils.widgets.get("output_schema").trim
require(inputVolume.startsWith("/Volumes/"), "Input volume must be a Unity Catalog volume path.")
require(outputSchema.matches("[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*"),
  "Output schema must be in catalog.schema format.")
println(s"Scala + Spark ready. Row count = ${spark.range(3).count()}")

// COMMAND ----------

val shipments = spark.read
  .option("header", "true")
  .csv(s"$inputVolume/shipments.csv")

println(s"Raw shipment rows = ${shipments.count()}")
display(shipments.limit(5))

// COMMAND ----------

import org.apache.spark.sql.functions._

val resentShipmentIds = shipments
  .groupBy("shipment_id")
  .count()
  .filter(col("count") > 1)
  .select("shipment_id")

display(
  shipments
    .join(resentShipmentIds, Seq("shipment_id"))
    .select("shipment_id", "last_updated", "shipment_status", "freight_charge_inr")
    .orderBy(col("shipment_id"), col("last_updated").desc)
)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

val shipmentIdPattern = "^SHP2026[0-9]{6}$"
val validShipmentId = coalesce(trim(col("shipment_id")).rlike(shipmentIdPattern), lit(false))
val latestFirst = Window
  .partitionBy(trim(col("shipment_id")))
  .orderBy(expr("try_cast(last_updated as timestamp)").desc_nulls_last,
    to_json(struct(shipments.columns.map(col): _*)).asc)

val latestShipments = shipments
  .withColumn("version_rank", row_number().over(latestFirst))
  .filter(!validShipmentId || col("version_rank") === 1)
  .drop("version_rank")

val rawCount = shipments.count()
val latestCount = latestShipments.count()

println(s"Raw rows = $rawCount")
println(s"Rows after deduplication = $latestCount")
println(s"Older versions removed = ${rawCount - latestCount}")

// COMMAND ----------

val checkedShipments = latestShipments
  .withColumn("ship_dt", expr("""
    coalesce(try_cast(trim(ship_date) as date),
             try_cast(concat(substr(ship_date, 7, 4), '-',
                             substr(ship_date, 4, 2), '-',
                             substr(ship_date, 1, 2)) as date))
  """))
  .withColumn("weight", expr("try_cast(weight_kg as decimal(18, 3))"))
  .withColumn("charge", expr("try_cast(freight_charge_inr as decimal(18, 2))"))
  .withColumn("status", upper(trim(col("shipment_status"))))
  .withColumn("status", when(col("status") === "DLVRD", "DELIVERED")
    .otherwise(col("status")))
  .withColumn("rejection_reason", expr("""
    concat_ws('; ',
      CASE WHEN coalesce(trim(shipment_id), '') = '' THEN 'Missing shipment ID'
        WHEN trim(shipment_id) NOT RLIKE '^SHP2026[0-9]{6}$' THEN 'Invalid shipment ID' END,
      CASE WHEN ship_dt IS NULL THEN 'Invalid ship date' END,
      CASE WHEN weight IS NULL OR weight <= 0 THEN 'Invalid weight' END,
      CASE WHEN charge IS NULL OR charge < 0 THEN 'Invalid freight charge' END,
      CASE WHEN status IS NULL OR status NOT IN
        ('DELIVERED', 'IN_TRANSIT', 'BOOKED', 'CANCELLED', 'RETURNED')
        THEN 'Invalid status' END
    )
  """))

display(
  checkedShipments.filter(col("rejection_reason") =!= "")
    .select("shipment_id", "ship_date", "weight_kg", "freight_charge_inr",
      "shipment_status", "rejection_reason")
)

// COMMAND ----------

def parseShipmentDate(field: String) = expr(s"""
  coalesce(
    try_cast(trim($field) as date),
    CASE WHEN trim($field) RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4}$$'
      THEN try_cast(concat(substr(trim($field), 7, 4), '-',
                           substr(trim($field), 4, 2), '-',
                           substr(trim($field), 1, 2)) as date)
    END
  )
""")

val validatedShipments = checkedShipments
  .withColumn("promised_dt", parseShipmentDate("promised_date"))
  .withColumn("delivery_dt", parseShipmentDate("delivery_date"))
  .withColumn("distance", expr("try_cast(distance_km as decimal(18, 3))"))
  .withColumn("rejection_reason", expr("""
    concat_ws('; ',
      nullif(rejection_reason, ''),
      CASE WHEN promised_dt IS NULL OR promised_dt < ship_dt
        THEN 'Invalid promised date' END,
      CASE WHEN delivery_dt < ship_dt
        OR (coalesce(trim(delivery_date), '') <> '' AND delivery_dt IS NULL)
        OR (status IN ('DELIVERED', 'RETURNED') AND delivery_dt IS NULL)
        THEN 'Invalid delivery date' END,
      CASE WHEN coalesce(trim(distance_km), '') <> ''
        AND (distance IS NULL OR distance <= 0)
        THEN 'Invalid distance' END,
      CASE WHEN trim(origin_hub_id) = trim(destination_hub_id)
        THEN 'Origin and destination are the same hub' END
    )
  """))

display(
  validatedShipments
    .filter(col("rejection_reason") =!= "")
    .select("shipment_id", "ship_date", "promised_date", "delivery_date",
      "distance_km", "rejection_reason")
)

// COMMAND ----------

val referenceFolder = inputVolume

val customersRaw = spark.read.option("header", "true")
  .csv(s"$referenceFolder/customers.csv")
val carriersRaw = spark.read.option("header", "true")
  .csv(s"$referenceFolder/carriers.csv")
val hubsRaw = spark.read.option("header", "true")
  .csv(s"$referenceFolder/hubs.csv")

println(s"Customers = ${customersRaw.count()}")
println(s"Carriers = ${carriersRaw.count()}")
println(s"Hubs = ${hubsRaw.count()}")

// COMMAND ----------

val customerOrder = Window
  .partitionBy(trim(col("customer_id")))
  .orderBy(expr("try_cast(signup_date as date)").asc_nulls_last,
    to_json(struct(customersRaw.columns.map(col): _*)).asc)

val customersUnique = customersRaw
  .withColumn("row_rank", row_number().over(customerOrder))
  .filter(col("row_rank") === 1)
  .drop("row_rank")

def unambiguousReferences(raw: org.apache.spark.sql.DataFrame, id: String): org.apache.spark.sql.DataFrame = {
  val distinctRows = raw.withColumn(id, trim(col(id))).distinct()
  val unambiguousIds = distinctRows.groupBy(id).count().filter(col("count") === 1).select(id)
  distinctRows.join(unambiguousIds, Seq(id), "left_semi")
}

val carriersUnique = unambiguousReferences(carriersRaw, "carrier_id")
val hubsUnique = unambiguousReferences(hubsRaw, "hub_id")

println(s"Customers after deduplication = ${customersUnique.count()}")
println(s"Carriers after deduplication = ${carriersUnique.count()}")
println(s"Hubs after deduplication = ${hubsUnique.count()}")

// COMMAND ----------

val customerKeys = customersUnique.select(trim(col("customer_id")).as("matched_customer"))
val carrierKeys = carriersUnique.select(col("carrier_id").as("matched_carrier"))
val originKeys = hubsUnique.select(col("hub_id").as("matched_origin"))
val destinationKeys = hubsUnique.select(col("hub_id").as("matched_destination"))

val shipmentsWithReferences = validatedShipments
  .join(customerKeys, trim(col("customer_id")) === col("matched_customer"), "left")
  .join(carrierKeys, trim(col("carrier_id")) === col("matched_carrier"), "left")
  .join(originKeys, trim(col("origin_hub_id")) === col("matched_origin"), "left")
  .join(destinationKeys, trim(col("destination_hub_id")) === col("matched_destination"), "left")
  .withColumn("rejection_reason", expr("""
    concat_ws('; ',
      nullif(rejection_reason, ''),
      CASE WHEN matched_customer IS NULL THEN 'Missing or unknown customer ID' END,
      CASE WHEN matched_carrier IS NULL THEN 'Missing, unknown or conflicting carrier ID' END,
      CASE WHEN matched_origin IS NULL THEN 'Missing, unknown or conflicting origin hub ID' END,
      CASE WHEN matched_destination IS NULL THEN 'Missing, unknown or conflicting destination hub ID' END
    )
  """))
  .drop("matched_customer", "matched_carrier", "matched_origin", "matched_destination")

println(s"Before reference checks = ${validatedShipments.count()}")
println(s"After reference checks = ${shipmentsWithReferences.count()}")

display(
  shipmentsWithReferences
    .filter(col("rejection_reason").contains("Missing"))
    .select("shipment_id", "customer_id", "carrier_id",
      "origin_hub_id", "destination_hub_id", "rejection_reason")
)

// COMMAND ----------

display(Seq(
  ("Before reference checks", validatedShipments.count()),
  ("After reference checks", shipmentsWithReferences.count())
).toDF("check", "row_count"))

// COMMAND ----------

val curatedShipments = shipmentsWithReferences
  .filter(col("rejection_reason") === "")
  .withColumn("shipment_id", trim(col("shipment_id")))
  .drop("rejection_reason")

val rejectedShipments = shipmentsWithReferences
  .filter(col("rejection_reason") =!= "")

val curatedCount = curatedShipments.count()
val rejectedCount = rejectedShipments.count()

display(Seq(
  ("Curated shipments", curatedCount),
  ("Rejected shipments", rejectedCount),
  ("Total after split", curatedCount + rejectedCount)
).toDF("check", "row_count"))

// COMMAND ----------

// DBTITLE 1,Write curated and rejected shipments

val curatedTable = s"$outputSchema.meridian_shipments_curated"
val rejectedTable = s"$outputSchema.meridian_shipments_rejected"

if (spark.catalog.tableExists(curatedTable)) {
  curatedShipments.createOrReplaceTempView("meridian_curated_run")
  val changed = curatedShipments.columns
    .map(name => s"NOT (target.`$name` <=> source.`$name`)")
    .mkString(" OR ")
  spark.sql(s"""
    MERGE INTO $curatedTable AS target
    USING meridian_curated_run AS source
    ON trim(target.shipment_id) = source.shipment_id
    WHEN MATCHED AND ($changed) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
  """)
} else {
  curatedShipments.write.format("delta").saveAsTable(curatedTable)
}
rejectedShipments.write.format("delta").mode("overwrite").saveAsTable(rejectedTable)

// COMMAND ----------

// DBTITLE 1,Write raw datasets to Delta
val rawDatasets = Seq(
  "shipments" -> shipments,
  "customers" -> customersRaw,
  "carriers"  -> carriersRaw,
  "hubs"      -> hubsRaw
)

val rawTableCounts = rawDatasets.map { case (name, data) =>
  val tableName = s"$outputSchema.meridian_${name}_raw"
  data.write.format("delta").mode("overwrite").saveAsTable(tableName)
  (tableName, spark.table(tableName).count())
}

display(rawTableCounts.toDF("delta_table", "row_count"))

// COMMAND ----------

val customerDetails = customersUnique.select(
  trim(col("customer_id")).as("customer_id"),
  col("segment").as("customer_segment")
)

val carrierDetails = carriersUnique.select(
  trim(col("carrier_id")).as("carrier_id"),
  col("carrier_name"),
  expr("try_cast(sla_on_time_pct as decimal(5, 2))").as("carrier_sla_pct")
)

val originDetails = hubsUnique.select(
  trim(col("hub_id")).as("origin_hub_id"),
  col("hub_name").as("origin_hub_name")
)

val destinationDetails = hubsUnique.select(
  trim(col("hub_id")).as("destination_hub_id"),
  col("hub_name").as("destination_hub_name")
)

val shipmentAnalytics = spark.table(curatedTable)
  .withColumn("customer_id", trim(col("customer_id")))
  .withColumn("carrier_id", trim(col("carrier_id")))
  .withColumn("origin_hub_id", trim(col("origin_hub_id")))
  .withColumn("destination_hub_id", trim(col("destination_hub_id")))
  .join(customerDetails, Seq("customer_id"), "left")
  .join(carrierDetails, Seq("carrier_id"), "left")
  .join(originDetails, Seq("origin_hub_id"), "left")
  .join(destinationDetails, Seq("destination_hub_id"), "left")

val enrichedCount = shipmentAnalytics.count()
require(enrichedCount == curatedCount,
  s"Expected $curatedCount curated rows but found $enrichedCount after joining")

shipmentAnalytics.createOrReplaceTempView("meridian_shipment_analytics")

display(shipmentAnalytics.select(
  "shipment_id", "customer_segment", "carrier_name", "carrier_sla_pct",
  "origin_hub_name", "destination_hub_name"
).limit(10))

// COMMAND ----------

// DBTITLE 1,Monthly delivery KPI
val monthlyDeliveryKpi = spark.sql("""
  SELECT
    trunc(ship_dt, 'MONTH') AS shipment_month,
    COUNT(*) AS delivered_shipments,
    COUNT_IF(delivery_dt <= promised_dt) AS on_time_shipments,
    ROUND(100.0 * COUNT_IF(delivery_dt <= promised_dt) / COUNT(*), 2)
      AS on_time_delivery_pct,
    ROUND(AVG(datediff(delivery_dt, ship_dt)), 2) AS avg_transit_days
  FROM meridian_shipment_analytics
  WHERE status = 'DELIVERED'
  GROUP BY trunc(ship_dt, 'MONTH')
""")

val monthlyKpiTable = s"$outputSchema.meridian_kpi_monthly_delivery"
monthlyDeliveryKpi.write.format("delta").mode("overwrite").saveAsTable(monthlyKpiTable)

display(spark.table(monthlyKpiTable).orderBy("shipment_month"))

// COMMAND ----------

// DBTITLE 1,Carrier scorecard KPI
val carrierScorecard = spark.sql("""
  WITH carrier_performance AS (
    SELECT
      carrier_id,
      carrier_name,
      carrier_sla_pct,
      COUNT(*) AS delivered_shipments,
      COUNT_IF(delivery_dt <= promised_dt) AS on_time_shipments
    FROM meridian_shipment_analytics
    WHERE status = 'DELIVERED'
    GROUP BY carrier_id, carrier_name, carrier_sla_pct
  )
  SELECT
    carrier_id,
    carrier_name,
    delivered_shipments,
    on_time_shipments,
    carrier_sla_pct,
    ROUND(100.0 * on_time_shipments / delivered_shipments, 2)
      AS actual_on_time_pct,
    ROUND(100.0 * on_time_shipments / delivered_shipments - carrier_sla_pct, 2)
      AS sla_gap_pp
  FROM carrier_performance
""")

val carrierKpiTable = s"$outputSchema.meridian_kpi_carrier_scorecard"
carrierScorecard.write.format("delta").mode("overwrite").saveAsTable(carrierKpiTable)

display(spark.table(carrierKpiTable).orderBy(col("sla_gap_pp").asc_nulls_last))

// COMMAND ----------

// DBTITLE 1,Lane performance KPI

val lanePerformanceKpi = spark.sql("""
  SELECT
    origin_hub_id,
    origin_hub_name,
    destination_hub_id,
    destination_hub_name,
    COUNT(*) AS shipment_volume,
    ROUND(SUM(weight), 3) AS total_weight_kg,
    ROUND(SUM(charge), 2) AS freight_revenue_inr,
    ROUND(SUM(charge) / NULLIF(SUM(weight), 0), 2) AS revenue_per_kg_inr,
    COUNT_IF(status = 'DELIVERED') AS delivered_shipments,
    ROUND(
      100.0 * COUNT_IF(status = 'DELIVERED' AND delivery_dt <= promised_dt)
      / NULLIF(COUNT_IF(status = 'DELIVERED'), 0),
      2
    ) AS on_time_delivery_pct
  FROM meridian_shipment_analytics
  WHERE status <> 'CANCELLED'
  GROUP BY origin_hub_id, origin_hub_name,
           destination_hub_id, destination_hub_name
""")

val laneKpiTable = s"$outputSchema.meridian_kpi_lane_performance"
lanePerformanceKpi.write.format("delta").mode("overwrite").saveAsTable(laneKpiTable)

display(spark.table(laneKpiTable).orderBy(col("shipment_volume").desc))


// COMMAND ----------

// DBTITLE 1,Delay reasons KPI
val delayReasonKpi = spark.sql("""
  WITH late_deliveries AS (
    SELECT
      trunc(ship_dt, 'MONTH') AS shipment_month,
      carrier_id,
      carrier_name,
      coalesce(nullif(upper(trim(delay_reason)), ''), 'NOT_RECORDED')
        AS delay_reason
    FROM meridian_shipment_analytics
    WHERE status = 'DELIVERED'
      AND delivery_dt > promised_dt
  )
  SELECT
    shipment_month,
    carrier_id,
    carrier_name,
    delay_reason,
    COUNT(*) AS late_shipments
  FROM late_deliveries
  GROUP BY shipment_month, carrier_id, carrier_name, delay_reason
""")

val delayKpiTable = s"$outputSchema.meridian_kpi_delay_reasons"
delayReasonKpi.write.format("delta").mode("overwrite").saveAsTable(delayKpiTable)

display(
  spark.table(delayKpiTable)
    .orderBy(col("late_shipments").desc, col("shipment_month").asc)
)

// COMMAND ----------

// DBTITLE 1,Copy KPI tables to Iceberg and compare
val deltaKpiTables = Seq(
  monthlyKpiTable,
  carrierKpiTable,
  laneKpiTable,
  delayKpiTable
)

val icebergChecks = deltaKpiTables.map { deltaTable =>
  val icebergTable = s"${deltaTable}_iceberg"

  spark.sql(s"""
    CREATE OR REPLACE TABLE $icebergTable
    USING ICEBERG
    AS SELECT * FROM $deltaTable
  """)

  val deltaData = spark.table(deltaTable)
  val icebergData = spark.table(icebergTable)

  val rowsMatch =
    deltaData.exceptAll(icebergData).limit(1).count() == 0 &&
    icebergData.exceptAll(deltaData).limit(1).count() == 0

  (icebergTable, icebergData.count(), rowsMatch)
}

display(icebergChecks.toDF("iceberg_table", "row_count", "rows_match_delta"))
require(icebergChecks.forall { case (_, count, rowsMatch) => count > 0 && rowsMatch },
  "All four Iceberg KPI tables must contain the same rows as their Delta counterparts.")

// COMMAND ----------

// DBTITLE 1,Verify Iceberg schema evolution
val icebergDelayTable = s"${delayKpiTable}_iceberg"
val rowsBeforeEvolution = spark.table(icebergDelayTable).count()

if (!spark.table(icebergDelayTable).columns.contains("delay_category")) {
  spark.sql(s"""
    ALTER TABLE $icebergDelayTable
    ADD COLUMNS (delay_category STRING)
  """)
}

val evolvedDelayData = spark.table(icebergDelayTable)
val rowsAfterEvolution = evolvedDelayData.count()
require(rowsBeforeEvolution == rowsAfterEvolution &&
  evolvedDelayData.columns.contains("delay_category"),
  "Adding delay_category must preserve the existing Iceberg rows.")

display(Seq(
  ("Rows before adding column", rowsBeforeEvolution, ""),
  ("Rows after adding column", rowsAfterEvolution, "delay_category")
).toDF("check", "row_count", "added_column"))

// COMMAND ----------

// DBTITLE 1,Iceberg time travel demo
val timeTravelDemo = s"$outputSchema.meridian_monthly_iceberg_time_travel_demo"

spark.sql(s"""
  CREATE OR REPLACE TABLE $timeTravelDemo
  USING ICEBERG
  AS SELECT *
  FROM $monthlyKpiTable
  ORDER BY shipment_month
  LIMIT 1
""")

// Read the initial snapshot before loading the remaining months.
display(spark.sql(s"SELECT * FROM $timeTravelDemo"))

val firstVersion = spark.sql(s"DESCRIBE HISTORY $timeTravelDemo")
  .orderBy(col("version").desc)
  .select("version")
  .first()
  .getLong(0)

spark.sql(s"""
  INSERT INTO $timeTravelDemo
  SELECT source.*
  FROM $monthlyKpiTable AS source
  LEFT ANTI JOIN $timeTravelDemo AS demo
    ON source.shipment_month = demo.shipment_month
""")

display(spark.sql(s"DESCRIBE HISTORY $timeTravelDemo")
  .select("version", "timestamp", "operation"))

display(spark.sql(s"""
  SELECT 'Earlier version' AS table_state, COUNT(*) AS month_rows
  FROM $timeTravelDemo VERSION AS OF $firstVersion
  UNION ALL
  SELECT 'Current version' AS table_state, COUNT(*) AS month_rows
  FROM $timeTravelDemo
"""))


// COMMAND ----------

// DBTITLE 1,Load Kafka event export (kafkaExport)
import org.apache.spark.sql.functions._

// Read the Kafka export as raw text so each line keeps its original bytes
// (BOM, trailing \r) and its position in the replay order.
val kafkaExportPath = s"$inputVolume/meridian-kafka-events.jsonl"

val kafkaExport = spark.read
  .option("wholetext", "true")
  .text(kafkaExportPath)
  .select(posexplode(split(col("value"), "\n")).as(Seq("replay_position", "event_json")))
  .filter(col("event_json") =!= "")

println(s"Kafka events loaded = ${kafkaExport.count()}")

// COMMAND ----------

// DBTITLE 1,Validate and classify tracking audit events
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val auditEventSchema =
  "event_id STRING, shipment_id STRING, hub_id STRING, event_type STRING, " +
  "event_time TIMESTAMP, ingest_time TIMESTAMP, vehicle_id STRING, delay_minutes INT"

val parsedAuditEvents = kafkaExport
  .withColumn("event_json", regexp_replace(col("event_json"), "^\uFEFF", ""))
  .withColumn("event", from_json(col("event_json"), auditEventSchema, Map.empty[String, String]))
  .selectExpr("replay_position", "event_json", "event.*")

val validatedAuditEvents = parsedAuditEvents.withColumn("rejection_reason", expr("""
  concat_ws('; ',
    CASE WHEN event_id IS NULL OR event_id NOT RLIKE '^EVT[0-9]{7}$'
      THEN 'Invalid event ID' END,
    CASE WHEN shipment_id IS NULL OR shipment_id NOT RLIKE '^SHP2026[0-9]{6}$'
      THEN 'Missing or invalid shipment ID' END,
    CASE WHEN hub_id IS NULL OR hub_id NOT RLIKE '^HUB[0-9]{2}$'
      THEN 'Invalid hub ID' END,
    CASE WHEN event_type IS NULL OR event_type NOT IN (
      'PICKED_UP', 'DEPARTED_HUB', 'ARRIVED_AT_HUB', 'ROUTE_CHECKPOINT',
      'OUT_FOR_DELIVERY', 'DELIVERED', 'DELAYED', 'EXCEPTION'
    ) THEN 'Invalid event type' END,
    CASE WHEN event_time IS NULL OR ingest_time IS NULL
      THEN 'Missing or invalid timestamp' END,
    CASE WHEN delay_minutes < 0 THEN 'Negative delay' END
  )
"""))

val auditArrivalOrder = Window
  .partitionBy("event_id", "rejection_reason")
  .orderBy("replay_position")

val trackingAudit = validatedAuditEvents
  .withColumn("arrival_rank", row_number().over(auditArrivalOrder))
  .withColumn("disposition", expr("""
    CASE
      WHEN rejection_reason <> '' THEN 'REJECTED'
      WHEN arrival_rank > 1 THEN 'DUPLICATE'
      WHEN ingest_time < event_time THEN 'CLOCK_SKEW'
      WHEN ingest_time > event_time + INTERVAL 5 MINUTES THEN 'LATE'
      ELSE 'ACCEPTED'
    END
  """))

display(trackingAudit.groupBy("disposition").count().orderBy("disposition"))

// COMMAND ----------

display(trackingAudit.selectExpr("COUNT(*) AS total_classified_events"))

// COMMAND ----------

val classifiedTotal = trackingAudit.count()
require(classifiedTotal == 43, s"Expected 43 classified events, found $classifiedTotal")

val auditOutputs = Seq(
  "meridian_tracking_rejected"   -> "disposition IN ('REJECTED', 'CLOCK_SKEW')",
  "meridian_tracking_duplicates" -> "disposition = 'DUPLICATE'",
  "meridian_tracking_late"       -> "disposition = 'LATE'"
)

val savedAuditCounts = auditOutputs.map { case (name, condition) =>
  val tableName = s"$outputSchema.$name"
  trackingAudit.filter(condition).write.format("delta").mode("overwrite").saveAsTable(tableName)
  (tableName, spark.table(tableName).count())
}

display(savedAuditCounts.toDF("delta_table", "saved_rows"))

// COMMAND ----------

display(trackingAudit.selectExpr(
  "COUNT(*) AS total_events",
  "COUNT_IF(disposition = 'ACCEPTED') AS accepted_events",
  "COUNT_IF(disposition <> 'ACCEPTED') AS flagged_events"
))
