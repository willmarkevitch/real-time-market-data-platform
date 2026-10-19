from confluent_kafka import Producer


class DeadLetterPublisher:
    """Publish malformed Kafka messages to a dead-letter topic."""

    def __init__(
        self,
        bootstrap_servers: str = "127.0.0.1:9092",
        topic: str = "market.trades.dlq",
    ):
        self.topic = topic

        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
        })

    def publish(self, message) -> None:
        """Publish a message and wait for confirmed Kafka delivery."""
        delivery_errors = []
        delivered = False

        def on_delivery(error, delivered_message):
            nonlocal delivered

            if error is not None:
                delivery_errors.append(error)
            else:
                delivered = True

        self.producer.produce(
            topic=self.topic,
            key=message.key(),
            value=message.value(),
            on_delivery=on_delivery,
        )

        remaining = self.producer.flush(timeout=10.0)

        if delivery_errors:
            raise RuntimeError(
                f"DLQ delivery failed: {delivery_errors[0]}"
            )

        if remaining > 0 or not delivered:
            raise TimeoutError(
                "DLQ delivery was not confirmed within 10 seconds."
            )
