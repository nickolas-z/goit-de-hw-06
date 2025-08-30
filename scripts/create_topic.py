from kafka.admin import KafkaAdminClient, NewTopic
import sys, logging
from configs import kafka_config

DEFAULT_PREFIX = "demo"
from logging_config import setup_logging
logger = logging.getLogger(__name__)
setup_logging()

def main() -> None:
    """
    Create Kafka topics for the demo application.
    Topics are created with a prefix that can be specified as a command line argument.
    If no prefix is provided, it defaults to "demo".
    """
    prefix = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PREFIX
    topics = [
        f"{prefix}_building_sensors",
        f"{prefix}_temperature_alerts",
        f"{prefix}_humidity_alerts",
        f"{prefix}_alerts",  # windowed alerts produced by Spark Streaming
    ]
    admin = KafkaAdminClient(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
    )
    existing = set(admin.list_topics())
    to_create = []
    for t in topics:
        if t in existing:
            logger.info("Exists: %s", t)
        else:
            logger.info("Create: %s", t)
            to_create.append(NewTopic(name=t, num_partitions=2, replication_factor=1))
    if to_create:
        try:
            admin.create_topics(new_topics=to_create, validate_only=False)
            logger.info("Created: %s", ", ".join(n.name for n in to_create))
        except Exception as e:
            logger.exception("Create error: %s", e)
    else:
        logger.info("Nothing to create.")
    admin.close()

if __name__ == "__main__":
    main()