
from src.streaming.trade_aggregator import TradeBar
from src.streaming.trade_bar_serializer import serialize_trade_bar


class KafkaCandlePublisher:
    """Publish finalized one-minute candles to Kafka."""

    def __init__(
        self,
        producer,
        topic: str = "market.candles.1m",
    ):
        self.producer = producer
        self.topic = topic



    def publish(self, bar: TradeBar) -> None:
        """Publish a candle and verify Kafka delivery."""
        delivery_errors = []
        delivery_confirmed = False

        def on_delivery(error, message):
            nonlocal delivery_confirmed

            if error is not None:
                delivery_errors.append(error)
            else:
                delivery_confirmed = True

        self.producer.produce(
            topic=self.topic,
            key=bar.symbol.encode("utf-8"),
            value=serialize_trade_bar(bar),
            on_delivery=on_delivery,
        )

        remaining = self.producer.flush(timeout=10.0)

        if delivery_errors:
            raise RuntimeError(
                f"Kafka candle delivery failed: {delivery_errors[0]}"
            )

        if remaining > 0 or not delivery_confirmed:
            raise TimeoutError(
                "Kafka candle delivery was not confirmed"
            )
