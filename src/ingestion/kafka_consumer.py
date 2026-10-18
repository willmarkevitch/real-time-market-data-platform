from confluent_kafka import Consumer


class MarketTradeConsumer:
    """Consume normalized market trades from Kafka."""

    def __init__(self, bootstrap_servers: str = "127.0.0.1:9092"):
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": "market-trade-consumers",
            "auto.offset.reset": "earliest",
        })

        self.consumer.subscribe(["market.trades"])

    def poll(self, timeout: float = 1.0):
        """Retrieve the next Kafka message, or None if no message is available."""
        return self.consumer.poll(timeout)