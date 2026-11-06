from confluent_kafka import TopicPartition

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
        message = self.consumer.poll()

        if message is None:
            self.publish_ready_windows()
            self.retry_pending_commits()
            return None

        # Retry previously completed offsets even during active traffic.
        self.retry_pending_commits()

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
        self.aggregator.add_trade(trade)
        self.publish_ready_windows()

        return trade
