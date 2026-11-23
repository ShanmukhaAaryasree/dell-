# PROJECT MERIDIAN - Mini Training Data Pack

Small subset of the full dataset. Quality problems are kept on purpose.

## batch/raw/  (ADLS Gen2 -> Databricks)
  hubs/hubs.csv            25 rows  (1 duplicate hub, 1 bad latitude, 1 inactive hub)
  carriers/carriers.csv    13 rows  (1 duplicate carrier, 1 inactive)
  customers/customers.csv  78 rows  (duplicate customer ids)
  shipments/shipments.csv 141 rows  (8 re-sent versions, invalid weight/charge, blank distance,
                                     orphan customer/carrier ids, DD-MM-YYYY dates, mixed status spellings,
                                     same origin/destination hub)
  data_dictionary.csv      field rules
The tracking fixture includes one event with a missing shipment ID. All nonempty shipment IDs in the fixture match a row in `shipments.csv`.

## streaming/  (Kafka -> Spark Structured Streaming -> Iceberg, on Kubernetes)
  tracking_events.jsonl            43 events, arrival (ingest_time) order, 06:00-07:00 UTC 19-Nov-2026
  tracking_events_kafka_keyed.txt  key|json lines (key = shipment_id, else event_id)
  micro_batches/events_001..004.json   4 files (~11 events each) = 4 micro-batches / 4 Iceberg snapshots
  sample_event.json, tracking_event.schema.json
Built-in issues: 7 duplicate event_id rows, 1 null shipment_id, 2 invalid event_type,
late events (ingest > 5 min after event_time), clock-skewed events, DELAYED/EXCEPTION events.
