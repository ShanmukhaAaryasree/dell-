FROM apache/spark:3.5.3-java17-scala AS dependencies
USER 185:185
RUN printf '%s\n' 'try { Seq("org.apache.kafka.common.serialization.ByteArraySerializer", "org.apache.spark.kafka010.KafkaDelegationTokenProvider", "org.apache.iceberg.spark.SparkCatalog", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem", "io.delta.tables.DeltaTable").foreach(Class.forName); sys.exit(0) } catch { case error: Throwable => error.printStackTrace(); sys.exit(1) }' >/tmp/check-dependencies.scala && \
    /opt/spark/bin/spark-shell --master 'local[1]' --conf spark.jars.ivy=/tmp/meridian-ivy \
      --repositories https://repo.maven.apache.org/maven2 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0,org.apache.hadoop:hadoop-azure:3.3.4,io.delta:delta-spark_2.12:3.2.1 \
      -i /tmp/check-dependencies.scala </dev/null && \
    test -n "$(find /tmp/meridian-ivy/jars -name '*kafka-clients*.jar' -print -quit)"

FROM apache/spark:3.5.3-java17-scala
COPY --from=dependencies /tmp/meridian-ivy/jars/ /opt/meridian/jars/
COPY meridian-stream.scala meridian-stream.sh /opt/meridian/
USER root
RUN mkdir -p /meridian && chown -R 185:185 /meridian
USER 185:185
ENTRYPOINT ["/bin/bash", "/opt/meridian/meridian-stream.sh"]
