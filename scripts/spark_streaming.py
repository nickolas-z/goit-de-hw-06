from __future__ import annotations

import os
import time
import logging
import argparse
import threading
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from configs import kafka_config
from logging_config import setup_logging
logger = logging.getLogger(__name__)

def build_spark(app_name: str = "SensorWindowAveraging") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
        .getOrCreate()
    )


def read_alert_conditions(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Read static alert conditions from CSV file.
    CSV schema: id,humidity_min,humidity_max,temperature_min,temperature_max,code,message
    """
    df = (
        spark.read.option("header", True)
        .csv(csv_path)
        .select(
            F.col("id").cast("int").alias("cond_id"),
            F.col("humidity_min").cast("double"),
            F.col("humidity_max").cast("double"),
            F.col("temperature_min").cast("double"),
            F.col("temperature_max").cast("double"),
            F.col("code").cast("string"),
            F.col("message").cast("string"),
        )
    )
    return df

def run_streaming(prefix: str, stop_event: threading.Event | None = None, checkpoint_dir: Optional[str] = None, starting_offsets: str = "earliest") -> None:
    spark = build_spark()

    project_root = Path(__file__).resolve().parents[1]
    conditions_csv = str(project_root / "data" / "alerts_conditions.csv")

    bs = ",".join(kafka_config["bootstrap_servers"])
    security_protocol = kafka_config["security_protocol"]
    sasl_mechanism = kafka_config["sasl_mechanism"]
    username = kafka_config["username"]
    password = kafka_config["password"]
    jaas = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";'

    topic_in = f"{prefix}_building_sensors"
    topic_out = f"{prefix}_alerts"

    # Schema for input JSON
    in_schema = T.StructType(
        [
            T.StructField("sensor_id", T.IntegerType(), True),
            T.StructField("ts", T.StringType(), True),
            T.StructField("temperature", T.DoubleType(), True),
            T.StructField("humidity", T.DoubleType(), True),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bs)
        .option("subscribe", topic_in)
        .option("startingOffsets", starting_offsets)
        .option("kafka.security.protocol", security_protocol)
        .option("kafka.sasl.mechanism", sasl_mechanism)
        .option("kafka.sasl.jaas.config", jaas)
        .load()
    )

    parsed = (
        raw.select(F.col("value").cast("string").alias("value"))
        .select(F.from_json("value", in_schema).alias("json"))
        .select(
            F.col("json.sensor_id").alias("sensor_id"),
            F.col("json.ts").alias("ts_str"),
            F.col("json.temperature").alias("temperature"),
            F.col("json.humidity").alias("humidity"),
        )
    )

    # Parse ISO8601 with timezone, e.g. 2024-08-18T13:08:00.123+00:00
    event_time = F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    events = parsed.withColumn("event_time", event_time).drop("ts_str")

    # Windowed averages with watermark
    agg = (
        events.withWatermark("event_time", "10 seconds")
        .groupBy(F.window(F.col("event_time"), "1 minute", "30 seconds"))
        .agg(
            F.avg("temperature").alias("t_avg"),
            F.avg("humidity").alias("h_avg"),
        )
    )

    # Static conditions
    cond = read_alert_conditions(spark, conditions_csv)

    # Cross join and filter by rules (ignore -999 bounds)
    joined = agg.crossJoin(cond)

    # Apply alert conditions
    # Treat -999 as "unused" per bound: trigger if h_avg < humidity_min (when min != -999)

    filt = joined.where(
        (
            ((F.col("humidity_min") != -999) & (F.col("h_avg") < F.col("humidity_min")))
            | ((F.col("humidity_max") != -999) & (F.col("h_avg") > F.col("humidity_max")))
        )
        |
        (
            ((F.col("temperature_min") != -999) & (F.col("t_avg") < F.col("temperature_min")))
            | ((F.col("temperature_max") != -999) & (F.col("t_avg") > F.col("temperature_max")))
        )
    )

    # window.start/end -> ISO8601 with offset; add current timestamp
    ts_fmt = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    out_struct = F.struct(
        F.struct(
            F.date_format(F.col("window.start"), ts_fmt).alias("start"),
            F.date_format(F.col("window.end"), ts_fmt).alias("end"),
        ).alias("window"),
        F.col("t_avg").alias("t_avg"),
        F.col("h_avg").alias("h_avg"),
        F.col("code").cast("string").alias("code"),
        F.col("message").alias("message"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("timestamp"),
    )

    out_df = filt.select(F.to_json(out_struct).alias("value"))

    # Checkpoint directory
    
    if checkpoint_dir is None:
        checkpoint_dir = str(project_root / "out" / f"chk-{prefix}-alerts")
    os.makedirs(checkpoint_dir, exist_ok=True)

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """
        Process each micro-batch: log alerts and write to Kafka output topic.
        """
        try:
            if batch_df.rdd.isEmpty():
                return
        except Exception:
            if batch_df.count() == 0:
                return

        try:
            vals = [r.value for r in batch_df.select("value").collect()]
            for v in vals:
                logger.info("Spark alert: %s", v)
        except Exception:
            logger.exception("Failed to log batch alerts")

        try:
            (batch_df.selectExpr("value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", bs)
            .option("topic", topic_out)
            .option("kafka.security.protocol", security_protocol)
            .option("kafka.sasl.mechanism", sasl_mechanism)
            .option("kafka.sasl.jaas.config", jaas)
            .save()
            )
        except Exception:
            logger.exception("Failed to write batch to Kafka")

    query = (
        out_df.writeStream
        .outputMode("append")
        .foreachBatch(_process_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Handle stop_event if provided
    try:
        if stop_event is None:
            logger.info("Spark streaming started, awaiting termination...")
            query.awaitTermination()
        else:
            logger.info("Spark streaming started, waiting for stop_event...")
            while not stop_event.is_set():
                time.sleep(0.5)
            logger.info("Stop event set, stopping Spark query...")
            query.stop()
            query.awaitTermination(10)
    finally:
        logger.info("Spark streaming stopped.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True, help="Topic prefix, e.g. demo")
    ap.add_argument("--checkpoint", help="Checkpoint directory (optional)")
    ap.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    args = ap.parse_args()
    run_streaming(args.prefix, checkpoint_dir=args.checkpoint, starting_offsets=args.starting_offsets)


if __name__ == "__main__":
    setup_logging()
    main()
