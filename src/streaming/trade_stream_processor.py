
class TradeStreamProcessor:
    """Coordinate Kafka trade consumption, aggregation, and publishing."""

    def __init__(
        self,
        consumer,
        deserializer,
        aggregator,
        publisher=None,
        dlq_publisher=None,
    ):
        self.consumer = consumer
        self.deserializer = deserializer
        self.aggregator = aggregator
        self.publisher = publisher
        self.dlq_publisher = dlq_publisher

    def publish_ready_windows(self):
        """Publish completed candles and remove them after success."""
        if self.publisher is None:
            return

        ready_bars = self.aggregator.get_ready_windows()

        for bar in ready_bars:
            self.publisher.publish(bar)

            self.aggregator.remove_bar(
                bar.symbol,
                bar.window_start,
            )

    def process_next(self):
        """Process a Kafka trade and retry pending candle publications."""
        message = self.consumer.poll()

        if message is None:
            self.publish_ready_windows()
            return None

        try:
            trade = self.deserializer(message.value())
        except (ValueError, KeyError, TypeError):
            if self.dlq_publisher is None:
                raise

            # Publish the original Kafka message to the DLQ.
            # Any DLQ delivery failure propagates to the caller.
            self.dlq_publisher.publish(message)

            # Retry any previously finalized candles.
            self.publish_ready_windows()
            return None

        self.aggregator.add_trade(trade)
        self.publish_ready_windows()

        return trade
