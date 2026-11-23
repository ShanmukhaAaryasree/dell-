import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import io.delta.tables.DeltaTable
import scala.util.control.NonFatal

try {
  val inputRoot = sys.env.getOrElse("MERIDIAN_INPUT", "/app/data")
  val outputRoot = sys.env.getOrElse("MERIDIAN_OUTPUT", "/tmp/meridian-batch")
  def readCsv(name: String) = spark.read.option("header", "true").csv(s"$inputRoot/$name/$name.csv")

  val shipments = readCsv("shipments")
  val customersRaw = readCsv("customers")
  val carriersRaw = readCsv("carriers")
  val hubsRaw = readCsv("hubs")

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

  val curatedShipments = shipmentsWithReferences
    .filter(col("rejection_reason") === "")
    .withColumn("shipment_id", trim(col("shipment_id")))
    .drop("rejection_reason")

  val rejectedShipments = shipmentsWithReferences
    .filter(col("rejection_reason") =!= "")

  val curatedCount = curatedShipments.count()
  val rejectedCount = rejectedShipments.count()

  require(curatedCount + rejectedCount == latestCount, "Shipment counts do not reconcile")
  val outputs = Seq(
    "raw_shipments" -> shipments,
    "raw_customers" -> customersRaw,
    "raw_carriers" -> carriersRaw,
    "raw_hubs" -> hubsRaw,
    "rejected_shipments" -> rejectedShipments
  )
  outputs.foreach { case (name, data) =>
    data.write.format("delta").mode("overwrite").save(s"$outputRoot/$name")
  }
  val curatedPath = s"$outputRoot/curated_shipments"
  if (DeltaTable.isDeltaTable(spark, curatedPath)) {
    val changed = curatedShipments.columns
      .map(name => s"NOT (target.`$name` <=> source.`$name`)")
      .mkString(" OR ")
    DeltaTable.forPath(spark, curatedPath).as("target")
      .merge(curatedShipments.as("source"), "trim(target.shipment_id) = source.shipment_id")
      .whenMatched(changed).updateAll()
      .whenNotMatched().insertAll()
      .whenNotMatchedBySource().delete()
      .execute()
    println("Delta curated shipments updated with MERGE")
  } else {
    curatedShipments.write.format("delta").save(curatedPath)
  }
  println(s"Batch verified: raw=$rawCount latest=$latestCount curated=$curatedCount rejected=$rejectedCount")
  spark.stop()
  sys.exit(0)
} catch {
  case NonFatal(error) =>
    error.printStackTrace()
    sys.exit(1)
}
