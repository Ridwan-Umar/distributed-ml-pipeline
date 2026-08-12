"""
Distributed ML Data Pipeline & Model Serving
=============================================
Kafka producer: streams raw events into the Kafka topic
for downstream Spark Structured Streaming consumption.

Status: In Development
"""

import json
import time
import logging
from kafka import KafkaProducer
from typing import Any, Dict

logger = logging.getLogger(__name__)


class EventProducer:
    """
    Kafka producer that streams ML training events to the ingestion topic.

    Events include:
    - User interaction records (clicks, impressions, views)
    - Ad metadata updates
    - User profile change signals
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )
        logger.info(f"EventProducer initialized for topic: {topic}")

    def send_event(self, key: str, event: Dict[str, Any]) -> None:
        """Send a single event record to Kafka."""
        # TODO: implement event sending with error callback
        raise NotImplementedError("Event sending under development.")

    def send_batch(self, events: list) -> None:
        """Send a batch of event records and flush."""
        for event in events:
            # TODO: implement batch sending
            pass
        self.producer.flush()

    def close(self) -> None:
        """Gracefully shut down the producer."""
        self.producer.close()
        logger.info("EventProducer closed.")
