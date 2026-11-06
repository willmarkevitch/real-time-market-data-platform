from confluent_kafka import TopicPartition
from datetime import timezone

from src.streaming.offset_tracker import OffsetTracker


class TradeStreamProcessor:
    """Coordinate Kafka trade consumption, aggregation, and publishing."""

    def __init__(
        self,
        consumer,
        deserializer,
        aggregator,
        publisher=None,
        dlq_publisher=None,
        offset_tracker=None,
    ):
        self.consumer = consumer
        self.deserializer = deserializer
        self.aggregator = aggregator
        self.publisher = publisher
        self.dlq_publisher = dlq_publisher

        self.offset_tracker = (
            offset_tracker
            if offset_tracker is not None
            else OffsetTracker()
        )

    def publish_ready_windows(self):
        """Publish finalized candles and complete their Kafka records."""
        if self.publisher is None:
            return

        ready_bars = self.aggregator.get_ready_windows()

        for bar in ready_bars:
            candle_key = (bar.symbol, bar.window_start)

            # Returns successfully only after confirmed delivery.
            self.publisher.publish(bar)

            # The candle is now published, so its records are complete.
            self.offset_tracker.complete_candle(candle_key)

            # Remove the candle only after successful publication.
            self.aggregator.remove_bar(bar.symbol, bar.window_start)

    def commit_completed_offsets(self, topic, partition):
        """Commit completed offsets and verify Kafka acknowledged them."""
        offset = self.offset_tracker.committable_offset(
            topic,
            partition,
        )

        if offset is None:
            return

        committed_partitions = self.consumer.commit(
            offsets=[
                TopicPartition(
                    topic,
                    partition,
                    offset,
                )
            ],
            asynchronous=False,
        )

        if not committed_partitions:
            raise RuntimeError(
                "Kafka offset commit failed: no confirmation returned"
            )

        if len(committed_partitions) != 1:
            raise RuntimeError(
                "Kafka commit confirmation mismatch: "
                "unexpected number of partitions"
            )

        committed_partition = committed_partitions[0]

        if committed_partition.error is not None:
            raise RuntimeError(
                f"Kafka offset commit failed: "
                f"{committed_partition.error}"
            )

        if (
            committed_partition.topic != topic
            or committed_partition.partition != partition
            or committed_partition.offset != offset
        ):
            raise RuntimeError(
                "Kafka commit confirmation mismatch"
            )

        # Advance our tracker only after validating Kafka's response.
        self.offset_tracker.confirm_commit(
            topic,
            partition,
            offset,
        )

    def retry_pending_commits(self):
        """Retry completed offsets that have not been committed."""
        for topic, partition in self.offset_tracker.tracked_partitions():
            self.commit_completed_offsets(topic, partition)
        
    def process_next(self):
        """Process a Kafka trade and retry pending candle publications."""

        # Retry completed offsets before consuming another message.
        # If the commit fails, no new message has been polled.
        self.retry_pending_commits()

        message = self.consumer.poll()

        if message is None:
            # Retry finalized candle publications during idle periods.
            self.publish_ready_windows()

            # Publishing may have completed additional offsets.
            self.retry_pending_commits()
            return None

        topic = message.topic()
        partition = message.partition()
        offset = message.offset()

        # Register this record as pending.
        self.offset_tracker.register(topic, partition, offset)

        try:
            trade = self.deserializer(message.value())

        except (ValueError, KeyError, TypeError):
            if self.dlq_publisher is None:
                raise

            # Publish the original message to the DLQ.
            # Delivery failures must propagate.
            self.dlq_publisher.publish(message)

            # Successful DLQ delivery completes this record.
            self.offset_tracker.mark_complete(
                topic,
                partition,
                offset,
            )

            # Commit only if all earlier registered records
            # are also complete.
            self.commit_completed_offsets(topic, partition)

            self.publish_ready_windows()
            return None

        # Aggregation alone is not durable.
        # Keep this offset pending until its candle is safely published.
        try:
            self.aggregator.add_trade(trade)

        except ValueError as exc:
            # Handle only expected late-trade rejections.
            # Unexpected aggregation errors must propagate.
            if str(exc) != "Trade arrived too late for its window":
                raise

            if self.dlq_publisher is None:
                raise

            # Route the rejected trade to the DLQ.
            # A failed DLQ delivery must not complete its offset.
            self.dlq_publisher.publish(message)

            # The rejected record is now durably handled.
            self.offset_tracker.mark_complete(
                topic,
                partition,
                offset,
            )

            # Commit only contiguous completed records.
            self.commit_completed_offsets(topic, partition)

            self.publish_ready_windows()
            return None

        # Accepted trades are associated with their one-minute candle.
        window_start = (
            trade.timestamp
            .astimezone(timezone.utc)
            .replace(second=0, microsecond=0)
        )

        candle_key = (trade.symbol, window_start)

        self.offset_tracker.associate_candle(
            topic,
            partition,
            offset,
            candle_key,
        )

        # Publish finalized candles and complete their offsets
        # only after confirmed Kafka delivery.
        self.publish_ready_windows()

        return trade
