import os
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

JARS_DIR = Path(os.getcwd()) / "data_ingestion" / "kafka_connect" / "jars"

if __name__ == "__main__":
    # Environment setup
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode()
    )
    t_env.get_config().set("table.local-time-zone", "UTC")
    t_env.get_config().set("pipeline.operator-chaining", "false")
    t_env.get_config().set("execution.checkpointing.interval", "5s")
    t_env.get_config().set("pipeline.auto-watermark-interval", "1s")

    # Load JARs with escaped URIs (handles spaces)
    jar_names = [
        "flink-connector-kafka-1.17.1.jar",
        "flink-json-1.17.1.jar",
        "flink-table-api-java-1.17.1.jar",
        "flink-avro-confluent-registry-1.17.1.jar",
        "flink-avro-1.17.1.jar",
        "avro-1.11.1.jar",
        "jackson-databind-2.14.2.jar",
        "jackson-core-2.14.2.jar",
        "jackson-annotations-2.14.2.jar",
        "kafka-schema-registry-client-7.5.0.jar",
        "kafka-clients-3.4.0.jar",
    ]
    jar_uris = ";".join((JARS_DIR / j).resolve().as_uri() for j in jar_names)
    t_env.get_config().set("pipeline.jars", jar_uris)

    # Source: matches `table_api.py` sink (device_id, created, feature_1)
    source_ddl = """
        CREATE TABLE device (
            device_id INT,
            created STRING,
            feature_1 FLOAT,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sink_device_0',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'table-api-agg-consumer-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(source_ddl)

    # 5s tumbling window on processing time
    tumbling_w = t_env.from_path("device") \
        .window(Tumble.over(lit(5).seconds).on(col("proctime")).alias('w')) \
        .group_by(col('w'), col("device_id")) \
        .select(
            col("device_id"),
            col('w').start.alias('window_start'),
            col('w').end.alias('window_end'),
            col("feature_1").avg.alias('avg_feature')
        )

    # Sink
    sink_ddl = """
        CREATE TABLE sink_table (
            device_id INT,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            avg_feature FLOAT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sink_device_3',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    t_env.execute_sql(sink_ddl)
    # Specify table program
    # device = t_env.from_path("device")
    # print("Source Table Schema and Sample Data:")
    # device.print_schema()
    # Data Stream
    tumbling_w.execute_insert("sink_table").wait()