from typing import Any, Optional, Iterable
import argparse, json, signal, threading, time, logging
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config

_stop = threading.Event()

from logging_config import setup_logging
logger = logging.getLogger(__name__)

def build_consumer(topic: str) -> Any:
    return KafkaConsumer(
        topic,
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=None,
        consumer_timeout_ms=1000,
    )

def build_producer() -> Any:
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def run_processor(prefix: str, temp_threshold: float = 40.0, hum_low: float = 20.0, hum_high: float = 80.0, stop_event: threading.Event | None = None) -> None:
    """
    Run the sensor data processor. It listens to a Kafka topic for sensor data,
    checks temperature and humidity against thresholds, and sends alerts if thresholds are breached.
    If stop_event is provided, it will be used to stop the loop.
    This can be called from another thread.
    """
    stop = stop_event or _stop

    topic_in = f"{prefix}_building_sensors"
    topic_temp = f"{prefix}_temperature_alerts"
    topic_hum = f"{prefix}_humidity_alerts"

    cons = build_consumer(topic_in)
    prod = build_producer()

    logger.info("Processor listening on %s", topic_in)

    try:
        while not stop.is_set():
            try:
                for msg in cons:
                    if stop.is_set():
                        break
                    data = msg.value
                    t = data.get("temperature")
                    h = data.get("humidity")
                    sid = data.get("sensor_id")
                    ts = data.get("ts")
                    if t is not None and t > temp_threshold:
                        alert = {
                            "sensor_id": sid,
                            "ts": ts,
                            "metric": "temperature",
                            "value": t,
                            "threshold": f">{temp_threshold}",
                            "message": "Temperature threshold exceeded",
                        }
                        prod.send(topic_temp, value=alert)
                        logger.info("ALERT TEMP %s", alert)
                    if h is not None and (h > hum_high or h < hum_low):
                        cond = f">{hum_high}" if h > hum_high else f"<{hum_low}"
                        alert = {
                            "sensor_id": sid,
                            "ts": ts,
                            "metric": "humidity",
                            "value": h,
                            "threshold": cond,
                            "message": "Humidity threshold breached",
                        }
                        prod.send(topic_hum, value=alert)
                        logger.info("ALERT HUM %s", alert)
            except Exception:
                time.sleep(0.5)
    finally:
        try:
            cons.close()
        except Exception:
            pass
        try:
            prod.close()
        except Exception:
            pass
        logger.info("Processor stopped.")


def main() -> None:
    """
    Main entry point for the sensor processor script.
    Parses command line arguments for prefix and thresholds,
    sets up signal handlers for graceful shutdown,
    and starts the processor.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--temp-threshold", type=float, default=40.0)
    ap.add_argument("--hum-low", type=float, default=20.0)
    ap.add_argument("--hum-high", type=float, default=80.0)
    args = ap.parse_args()

    def stop_signal(*_: Any) -> None:
        logger.info("Stopping processor...")
        _stop.set()
    signal.signal(signal.SIGINT, stop_signal)
    signal.signal(signal.SIGTERM, stop_signal)

    run_processor(args.prefix, temp_threshold=args.temp_threshold, hum_low=args.hum_low, hum_high=args.hum_high)


if __name__ == "__main__":
    setup_logging()
    main()
