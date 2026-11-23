import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import scala.util.control.NonFatal

try {
  val broker = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "meridian-kafka:9092")
  val topic = sys.env.getOrElse("KAFKA_TOPIC", "shipment-tracking-events")
  val checkpoint = sys.env.getOrElse("MERIDIAN_CHECKPOINT", "/meridian/checkpoints/event_windows")
  val warehouse = sys.env.getOrElse("MERIDIAN_WAREHOUSE", "file:///meridian/warehouse")
  val auditPath = s"${warehouse.stripSuffix("/")}/event_audit"
  val continuous = sys.env.get("MERIDIAN_STREAM_MODE").contains("continuous")
  val triggerInterval = sys.env.getOrElse("MERIDIAN_TRIGGER_INTERVAL", "2 seconds")
  val maxOffsetsPerTrigger = sys.env.getOrElse("MERIDIAN_MAX_OFFSETS_PER_TRIGGER", "12")
  val trigger = if (continuous) Trigger.ProcessingTime(triggerInterval) else Trigger.AvailableNow()

  sys.env.get("MERIDIAN_ADLS_HOST").foreach { host =>
    val config = spark.sparkContext.hadoopConfiguration
    config.set(s"fs.azure.account.auth.type.$host", "OAuth")
    config.set(s"fs.azure.account.oauth.provider.type.$host",
      "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    config.set(s"fs.azure.account.oauth2.client.id.$host", sys.env("AZURE_CLIENT_ID"))
    config.set(s"fs.azure.account.oauth2.client.secret.$host", sys.env("AZURE_CLIENT_SECRET"))
    config.set(s"fs.azure.account.oauth2.client.endpoint.$host",
      s"https://login.microsoftonline.com/${sys.env("AZURE_TENANT_ID")}/oauth2/token")
  }

  spark.conf.set("spark.sql.session.timeZone", "UTC")
  spark.sql("CREATE NAMESPACE IF NOT EXISTS meridian.tracking")
  spark.sql("""
    CREATE TABLE IF NOT EXISTS meridian.tracking.replay_records (
      kafka_partition INT, kafka_offset BIGINT, event_json STRING
    ) USING ICEBERG
  """)
  spark.sql("""
    CREATE TABLE IF NOT EXISTS meridian.tracking.event_windows (
      window_start_utc TIMESTAMP, window_end_utc TIMESTAMP,
      total_events BIGINT, delay_events BIGINT, exception_events BIGINT
    ) USING ICEBERG
  """)

  val usesAdls = sys.env.contains("MERIDIAN_ADLS_HOST")

  def reportWindowState(session: org.apache.spark.sql.SparkSession): String = {
    val summary = session.sql("""
      SELECT count(*) AS window_count, coalesce(sum(total_events), 0) AS event_count
      FROM meridian.tracking.event_windows
    """).first()
    s"window_rows=${summary.getLong(0)} window_events=${summary.getLong(1)}"
  }

  if (usesAdls) {
    val savedCount = spark.table("meridian.tracking.replay_records").count()
    val commits = new org.apache.hadoop.fs.Path(s"$checkpoint/commits")
    val fs = commits.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val existingCommits =
      if (fs.exists(commits)) {
        fs.listStatus(commits).map(_.getPath.getName).count(_.forall(_.isDigit))
      } else 0
    println(s"State before starting stream: saved_records=$savedCount ${reportWindowState(spark)} checkpoint_commits=$existingCommits")
  }

  val replay = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
    .load()
    .selectExpr("partition AS kafka_partition", "offset AS kafka_offset",
      "CAST(value AS STRING) AS event_json")

  val query = replay.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(trigger)
    .foreachBatch { (batch: DataFrame, batchId: Long) =>
      val batchSpark = batch.sparkSession
      batch.createOrReplaceTempView("meridian_incoming_batch")
      batchSpark.sql("""
        MERGE INTO meridian.tracking.replay_records AS saved
        USING meridian_incoming_batch AS incoming
        ON saved.kafka_partition = incoming.kafka_partition
          AND saved.kafka_offset = incoming.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
      """)
      val classified = batchSpark.sql("""
        WITH parsed AS (
          SELECT kafka_partition, kafka_offset,
            event_json,
            from_json(event_json, 'event_id STRING, shipment_id STRING,
              hub_id STRING, event_type STRING, event_time TIMESTAMP,
              ingest_time TIMESTAMP, vehicle_id STRING, delay_minutes INT') AS e
          FROM meridian.tracking.replay_records
        ), validated AS (
          SELECT *, coalesce(
            e.event_id RLIKE '^EVT[0-9]{7}$'
            AND e.shipment_id RLIKE '^SHP2026[0-9]{6}$'
            AND e.hub_id RLIKE '^HUB[0-9]{2}$'
            AND e.event_type IN (
              'PICKED_UP', 'DEPARTED_HUB', 'ARRIVED_AT_HUB', 'ROUTE_CHECKPOINT',
              'OUT_FOR_DELIVERY', 'DELIVERED', 'DELAYED', 'EXCEPTION'
            )
            AND e.event_time IS NOT NULL AND e.ingest_time IS NOT NULL
            AND (e.delay_minutes IS NULL OR e.delay_minutes >= 0), false) AS valid
          FROM parsed
        ), ranked AS (
          SELECT *, count_if(valid) OVER (
            PARTITION BY e.event_id ORDER BY kafka_offset, kafka_partition
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS valid_arrival_rank
          FROM validated
        )
        SELECT kafka_partition, kafka_offset, event_json, e.*,
          CASE
            WHEN NOT valid THEN 'REJECTED'
            WHEN valid_arrival_rank > 1 THEN 'DUPLICATE'
            WHEN e.ingest_time < e.event_time THEN 'REJECTED'
            WHEN e.ingest_time > e.event_time + INTERVAL 5 MINUTES THEN 'LATE'
            ELSE 'ACCEPTED'
          END AS disposition
        FROM ranked
      """)
      classified.write.format("delta").mode("overwrite").partitionBy("disposition").save(auditPath)
      batchSpark.read.format("delta").load(auditPath).createOrReplaceTempView("meridian_event_audit")
      batchSpark.sql("""
        INSERT OVERWRITE meridian.tracking.event_windows
        WITH accepted AS (
          SELECT window(event_time, '5 minutes') AS event_window, event_type
          FROM meridian_event_audit
          WHERE disposition = 'ACCEPTED'
        )
        SELECT event_window.start, event_window.end, count(*),
          count_if(event_type = 'DELAYED'), count_if(event_type = 'EXCEPTION')
        FROM accepted
        GROUP BY event_window
      """)
      println(s"Finished streaming batch $batchId")
    }
    .start()

  if (continuous) {
    java.nio.file.Files.createFile(java.nio.file.Paths.get("/tmp/meridian-stream-ready"))
    println(s"Continuous tracking consumer started for $topic")
    println(s"Checkpoint location = $checkpoint")
  }
  query.awaitTermination()
  println(s"New Kafka records processed = ${query.recentProgress.map(_.numInputRows).sum}")
  println(s"Saved Kafka records = ${spark.table("meridian.tracking.replay_records").count()}")
  spark.table("meridian.tracking.event_windows").orderBy("window_start_utc").show(20, false)
  java.nio.file.Files.createFile(java.nio.file.Paths.get("/tmp/meridian-stream-complete"))
  spark.stop()
  sys.exit(0)
} catch {
  case error: LinkageError =>
    error.printStackTrace()
    sys.exit(1)
  case NonFatal(error) =>
    error.printStackTrace()
    sys.exit(1)
}
