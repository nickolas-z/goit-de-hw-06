from typing import Any, Optional, Iterable
import argparse, json, signal, threading, time, logging
from kafka import KafkaConsumer
from configs import kafka_config

_stop = threading.Event()

from logging_config import setup_logging
logger = logging.getLogger(__name__)

def build_consumer(topics: Iterable[str]) -> Any:
    return KafkaConsumer(
        *topics,
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


def run_alert_consumer(prefix: str, stop_event: threading.Event | None = None) -> None:
    """
    Run the alert consumer for temperature and humidity alerts.
    """
    stop = stop_event or _stop
    topics = [
        f"{prefix}_temperature_alerts",
        f"{prefix}_humidity_alerts",
        f"{prefix}_alerts",  # windowed alerts from Spark Streaming
    ]
    cons = build_consumer(topics)
    logger.info("Alert consumer listening on: %s", ", ".join(topics))

    try:
        while not stop.is_set():
            try:
                for msg in cons:
                    if stop.is_set():
                        break
                    logger.info("[%s] %s", msg.topic, msg.value)
                # loop back to check stop
            except Exception:
                time.sleep(0.5)
    finally:
        try:
            cons.close()
        except Exception:
            pass
        logger.info("Alert consumer stopped.")


def main() -> None:
    """
    Main entry point for the alert consumer script.
    Parses command line arguments for prefix,
    sets up signal handlers for graceful shutdown,
    and starts the alert consumer.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True)
    args = ap.parse_args()

    def stop_signal(*_: Any) -> None:
        logger.info("Stopping alert consumer...")
        _stop.set()
    signal.signal(signal.SIGINT, stop_signal)
    signal.signal(signal.SIGTERM, stop_signal)

    run_alert_consumer(args.prefix)


if __name__ == "__main__":
    setup_logging()
    main()
