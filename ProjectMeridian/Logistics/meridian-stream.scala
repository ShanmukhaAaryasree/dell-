import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import scala.util.control.NonFatal

try {
  val broker = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "meridian-kafka:9092")
  val topic = sys.env.getOrElse("KAFKA_TOPIC", "shipment-tracking-events")
  val checkpoint = "/meridian/checkpoints/event_windows"

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

  val replay = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "12")
    .load()
    .selectExpr("partition AS kafka_partition", "offset AS kafka_offset",
      "CAST(value AS STRING) AS event_json")

  val query = replay.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(Trigger.AvailableNow())
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
      batchSpark.sql("""
        INSERT OVERWRITE meridian.tracking.event_windows
        WITH parsed AS (
          SELECT kafka_partition, kafka_offset,
            from_json(event_json, 'event_id STRING, shipment_id STRING,
              hub_id STRING, event_type STRING, event_time TIMESTAMP,
              ingest_time TIMESTAMP, vehicle_id STRING, delay_minutes INT') AS e
          FROM meridian.tracking.replay_records
        ), ranked AS (
          SELECT e.*, row_number() OVER (
            PARTITION BY e.event_id ORDER BY kafka_offset, kafka_partition
          ) AS arrival_rank
          FROM parsed
          WHERE e.event_id RLIKE '^EVT[0-9]{7}$'
            AND e.shipment_id RLIKE '^SHP2026[0-9]{6}$'
            AND e.hub_id RLIKE '^HUB[0-9]{2}$'
            AND e.event_type IN (
              'PICKED_UP', 'DEPARTED_HUB', 'ARRIVED_AT_HUB', 'ROUTE_CHECKPOINT',
              'OUT_FOR_DELIVERY', 'DELIVERED', 'DELAYED', 'EXCEPTION'
            )
            AND e.event_time IS NOT NULL AND e.ingest_time IS NOT NULL
            AND (e.delay_minutes IS NULL OR e.delay_minutes >= 0)
        ), accepted AS (
          SELECT window(event_time, '5 minutes') AS event_window, event_type
          FROM ranked
          WHERE arrival_rank = 1
            AND ingest_time >= event_time
            AND ingest_time <= event_time + INTERVAL 5 MINUTES
        )
        SELECT event_window.start, event_window.end, count(*),
          count_if(event_type = 'DELAYED'), count_if(event_type = 'EXCEPTION')
        FROM accepted
        GROUP BY event_window
      """)
      println(s"Finished streaming batch $batchId")
    }
    .start()

  query.awaitTermination()
  println(s"New Kafka records processed = ${query.recentProgress.map(_.numInputRows).sum}")
  println(s"Saved Kafka records = ${spark.table("meridian.tracking.replay_records").count()}")
  spark.table("meridian.tracking.event_windows").orderBy("window_start_utc").show(20, false)
} catch {
  case NonFatal(error) =>
    error.printStackTrace()
    sys.exit(1)
}
