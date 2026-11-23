FROM apache/kafka:4.4.0 AS replay-data
COPY Logistics/Data/streaming/tracking_events_kafka_keyed.txt /tmp/meridian-events.txt

FROM apache/kafka:4.4.0
COPY --from=replay-data /tmp/meridian-events.txt /opt/kafka/meridian-events.txt
USER 1000:1000
CMD ["/bin/sh", "-ec", "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server \"${KAFKA_BOOTSTRAP_SERVERS:?Missing Kafka broker}\" --topic \"${KAFKA_TOPIC:?Missing Kafka topic}\" --property parse.key=true --property 'key.separator=|' < /opt/kafka/meridian-events.txt; touch /tmp/meridian-replay-complete; exec sleep infinity"]
