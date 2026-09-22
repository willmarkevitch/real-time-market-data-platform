from confluent_kafka import Producer

from src.models.market_trade import MarketTrade
from src.ingestion.trade_serializer import serialize_trade


class MarketTradeProducer:
    """Publish normalized market trades to Kafka."""

    def __init__(self, bootstrap_servers: str = "127.0.0.1:9092"):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
        })
        self.delivery_failure_count = 0

    def publish(self, trade: MarketTrade) -> None:
        """Queue a trade for delivery to Kafka."""
        self.producer.produce(
            topic="market.trades",
            key=trade.symbol.encode("utf-8"),
            value=serialize_trade(trade),
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)
        self.check_delivery_errors()

    def _on_delivery(self, error, message) -> None:
        """Track unsuccessful Kafka deliveries."""
        if error is not None:
            self.delivery_failure_count += 1

    def flush(self, timeout: float = 10.0) -> None:
        """Wait for outstanding Kafka messages to finish delivery."""
        remaining = self.producer.flush(timeout)

        if remaining > 0:
            raise TimeoutError(
                f"{remaining} Kafka message(s) still pending after flush"
            )

        self.check_delivery_errors()

        if self.delivery_failure_count > 0:
            raise RuntimeError(
                f"{self.delivery_failure_count} Kafka message(s) failed delivery"
            )

    def check_delivery_errors(self) -> None:
        """Raise if Kafka has reported any delivery failures."""
        if self.delivery_failure_count > 0:
            raise RuntimeError(
                f"{self.delivery_failure_count} Kafka message(s) failed delivery"
            )