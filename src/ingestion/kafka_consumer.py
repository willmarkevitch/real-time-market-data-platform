from confluent_kafka import Consumer, KafkaException


class MarketTradeConsumer:
    """Consume normalized market trades from Kafka."""

    def __init__(self, bootstrap_servers: str = "127.0.0.1:9092"):
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": "market-trade-consumers",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })

        self.consumer.subscribe(["market.trades"])

    def poll(self, timeout: float = 1.0):
        """Retrieve the next Kafka message, raising on Kafka errors."""
        message = self.consumer.poll(timeout)

        if message is None:
            return None

        error = message.error()

        if error is not None:
            raise KafkaException(error)

        return message

    def close(self) -> None:
        """Close the Kafka consumer and release its resources."""
        self.consumer.close()

    def commit(self, message) -> None:
        """Synchronously commit the offset of a processed Kafka message."""
        self.consumer.commit(
            message=message,
            asynchronous=False,
        )