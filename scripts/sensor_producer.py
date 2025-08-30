from typing import Any, Optional, Iterable
import argparse, random, json, signal, threading, time, logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from configs import kafka_config

_stop = threading.Event()

from logging_config import setup_logging
logger = logging.getLogger(__name__)

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def build_producer() -> Any:
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def run_producer(prefix: str, sensor_id: int | None = None, interval: float = 2.0, count: int = 0, stop_event: threading.Event | None = None) -> None:
    """
    Run the producer loop. If stop_event is provided it will be used to stop the loop.
    This can be called from another thread.
    """
    stop = stop_event or _stop
    sensor_id = sensor_id or random.randint(1000, 9999)
    topic = f"{prefix}_building_sensors"
    prod = build_producer()
    logger.info("Sensor started id=%s topic=%s", sensor_id, topic)

    sent = 0
    try:
        while not stop.is_set():
            payload = {
                "sensor_id": sensor_id,
                "ts": iso_now(),
                "temperature": round(random.uniform(25, 45), 2),
                "humidity": round(random.uniform(15, 85), 2),
            }
            prod.send(topic, value=payload)
            logger.debug("SENT %s", payload)
            try:
                prod.flush(5)
            except Exception:
                logger.error("Failed to flush producer", exc_info=True)
                pass
            sent += 1
            if count and sent >= count:
                break
            stop.wait(interval)
    finally:
        try:
            prod.close()
        except Exception:
            pass
        logger.info("Producer closed.")


def main() -> None:
    """
    Main entry point for the sensor producer script.
    Parses command line arguments for prefix, sensor_id, interval, and count,
    sets up signal handlers for graceful shutdown,
    and starts the producer.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--sensor-id", type=int)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--count", type=int, default=0, help="If >0 send N messages then exit")
    args = ap.parse_args()

    def stop_signal(*_: Any) -> None:
        logger.info("Stopping sensor...")
        _stop.set()
    signal.signal(signal.SIGINT, stop_signal)
    signal.signal(signal.SIGTERM, stop_signal)

    run_producer(args.prefix, sensor_id=args.sensor_id, interval=args.interval, count=args.count)


if __name__ == "__main__":
    setup_logging()
    main()
