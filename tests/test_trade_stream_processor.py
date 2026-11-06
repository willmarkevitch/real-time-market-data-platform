import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock
from confluent_kafka import KafkaError, TopicPartition

from src.models.market_trade import MarketTrade
from src.streaming.trade_aggregator import TradeBarAggregator
from src.streaming.trade_stream_processor import TradeStreamProcessor


def make_kafka_message(
    payload=b"serialized-trade",
    topic="market.trades",
    partition=0,
    offset=0,
):
    """Create a mocked Kafka message with realistic metadata."""
    message = Mock()

    message.value.return_value = payload
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset

    return message

def test_processor_adds_consumed_trade_to_aggregator():
    trade = MarketTrade(
        "AAPL",
        Decimal("200"),
        10,
        datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
        "NASDAQ",
    )

    # Simulate an incoming Kafka message.
    message = Mock()
    message.value.return_value = b"serialized-trade"

    consumer = Mock()
    consumer.poll.return_value = message

    deserializer = Mock(return_value=trade)
    aggregator = TradeBarAggregator()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
    )

    processor.process_next()

    bar = aggregator.get_bar(
        "AAPL",
        datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
    )

    assert bar.open == Decimal("200")
    assert bar.volume == 10
    deserializer.assert_called_once_with(b"serialized-trade")



def test_processor_handles_no_available_message():
    consumer = Mock()
    consumer.poll.return_value = None

    deserializer = Mock()
    aggregator = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
    )

    result = processor.process_next()

    assert result is None

    consumer.poll.assert_called_once()
    deserializer.assert_not_called()
    aggregator.add_trade.assert_not_called()


def test_processor_does_not_commit_when_deserialization_fails():
    message = Mock()
    message.value.return_value = b"invalid-trade-data"

    consumer = Mock()
    consumer.poll.return_value = message

    deserializer = Mock(
        side_effect=ValueError("Invalid trade data")
    )
    aggregator = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
    )

    with pytest.raises(ValueError, match="Invalid trade data"):
        processor.process_next()

    deserializer.assert_called_once_with(b"invalid-trade-data")
    aggregator.add_trade.assert_not_called()
    consumer.commit.assert_not_called()



def test_processor_does_not_commit_when_aggregation_fails():
    message = Mock()
    message.value.return_value = b"serialized-trade"

    consumer = Mock()
    consumer.poll.return_value = message

    trade = MarketTrade(
        "AAPL",
        Decimal("200"),
        10,
        datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
        "NASDAQ",
    )

    deserializer = Mock(return_value=trade)
    aggregator = Mock()
    aggregator.add_trade.side_effect = ValueError(
        "Trade arrived too late for its window"
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
    )

    with pytest.raises(ValueError, match="too late"):
        processor.process_next()

    deserializer.assert_called_once_with(b"serialized-trade")
    aggregator.add_trade.assert_called_once_with(trade)
    consumer.commit.assert_not_called()



def test_processor_publishes_finalized_candle():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]
    
    messages = [
        make_kafka_message(offset=41),
        make_kafka_message(offset=42),
    ]

    consumer = Mock()
    consumer.poll.side_effect = messages

    deserializer = Mock(side_effect=trades)
    aggregator = TradeBarAggregator()
    publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    processor.process_next()
    publisher.publish.assert_not_called()

    processor.process_next()

    publisher.publish.assert_called_once()

    bar = publisher.publish.call_args.args[0]

    assert bar.symbol == "AAPL"
    assert bar.window_start == datetime(
        2026, 10, 22, 14, 30, tzinfo=timezone.utc
    )
    assert bar.open == Decimal("200")
    assert bar.close == Decimal("200")
    assert bar.volume == 10



def test_publishing_failure_preserves_finalized_candle():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    messages = [
        make_kafka_message(offset=offset)
        for offset in range(41, 41 + len(trades))
    ]

    consumer = Mock()
    consumer.poll.side_effect = messages

    deserializer = Mock(side_effect=trades)
    aggregator = TradeBarAggregator()

    publisher = Mock()
    publisher.publish.side_effect = RuntimeError(
        "Kafka publishing failed"
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    # First trade creates the 14:30 candle.
    processor.process_next()

    # Second trade advances the watermark and triggers publishing.
    with pytest.raises(RuntimeError, match="Kafka publishing failed"):
        processor.process_next()

    # The failed candle must still exist for retry.
    bar = aggregator.get_bar(
        "AAPL",
        datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
    )

    assert bar.open == Decimal("200")
    assert bar.volume == 10

    consumer.commit.assert_not_called()



def test_failed_candle_can_be_published_on_next_trade():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("206"),
            5,
            datetime(2026, 10, 22, 14, 32, 20, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    messages = [
        make_kafka_message(offset=offset)
        for offset in range(41, 41 + len(trades))
    ]

    consumer = Mock()
    consumer.poll.side_effect = messages

    deserializer = Mock(side_effect=trades)
    aggregator = TradeBarAggregator()

    publisher = Mock()
    publisher.publish.side_effect = [
        RuntimeError("Kafka publishing failed"),
        None,
    ]

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    processor.process_next()

    with pytest.raises(RuntimeError, match="Kafka publishing failed"):
        processor.process_next()

    # A subsequent trade triggers another publishing attempt.
    processor.process_next()

    assert publisher.publish.call_count == 2

    with pytest.raises(KeyError):
        aggregator.get_bar(
            "AAPL",
            datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
        )

    consumer.commit.assert_not_called()



def test_retries_failed_publication_when_no_new_trade_arrives():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    messages = [
        make_kafka_message(offset=offset)
        for offset in range(41, 41 + len(trades))
    ]

    consumer = Mock()
    consumer.poll.side_effect = [*messages, None]

    committed_partition = Mock()
    committed_partition.topic = "market.trades"
    committed_partition.partition = 0
    committed_partition.offset = 42
    committed_partition.error = None

    consumer.commit.return_value = [committed_partition]

    deserializer = Mock(side_effect=trades)
    aggregator = TradeBarAggregator()

    publisher = Mock()
    publisher.publish.side_effect = [
        RuntimeError("Kafka publishing failed"),
        None,
    ]

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    # First trade creates the 14:30 candle.
    processor.process_next()

    # Second trade advances the watermark.
    # Publishing fails, so the candle remains in memory.
    with pytest.raises(RuntimeError, match="Kafka publishing failed"):
        processor.process_next()

    # No new Kafka message arrives, but publication should be retried.
    result = processor.process_next()

    assert result is None
    assert publisher.publish.call_count == 2

    assert consumer.commit.call_count == 1

    committed_offsets = consumer.commit.call_args.kwargs["offsets"]

    assert len(committed_offsets) == 1
    assert committed_offsets[0].topic == "market.trades"
    assert committed_offsets[0].partition == 0
    assert committed_offsets[0].offset == 42

    # Successfully published candle should now be removed.
    with pytest.raises(KeyError):
        aggregator.get_bar(
            "AAPL",
            datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
        )



def test_processor_publishes_multiple_finalized_candles():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "MSFT",
            Decimal("400"),
            5,
            datetime(2026, 10, 22, 14, 30, 20, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    messages = [Mock() for _ in trades]

    for offset, message in enumerate(messages, start=41):
        message.value.return_value = b"serialized-trade"
        message.topic.return_value = "market.trades"
        message.partition.return_value = 0
        message.offset.return_value = offset

    consumer = Mock()
    consumer.poll.side_effect = messages

    deserializer = Mock(side_effect=trades)
    aggregator = TradeBarAggregator()
    publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    for _ in trades:
        processor.process_next()

    assert publisher.publish.call_count == 2

    published_bars = [
        call.args[0]
        for call in publisher.publish.call_args_list
    ]

    assert {bar.symbol for bar in published_bars} == {
        "AAPL",
        "MSFT",
    }

    assert all(
        bar.window_start == datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        )
        for bar in published_bars
    )

    assert {
        bar.symbol: bar.volume
        for bar in published_bars
    } == {
        "AAPL": 10,
        "MSFT": 5,
    }

    assert aggregator.get_ready_windows() == []



def test_processor_routes_invalid_trade_to_dead_letter_queue():
    """Invalid trade payloads should be routed to the DLQ."""

    message = Mock()
    message.value.return_value = b"invalid-json"
    message.topic.return_value = "market.trades"
    message.partition.return_value = 0
    message.offset.return_value = 42

    consumer = Mock()
    consumer.poll.return_value = message

    successful_partition = Mock()
    successful_partition.topic = "market.trades"
    successful_partition.partition = 0
    successful_partition.offset = 43
    successful_partition.error = None

    consumer.commit.return_value = [successful_partition]

    deserializer = Mock(
        side_effect=ValueError("Invalid JSON payload")
    )

    aggregator = Mock()
    dlq_publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        dlq_publisher=dlq_publisher,
    )

    result = processor.process_next()

    assert result is None

    deserializer.assert_called_once_with(b"invalid-json")
    aggregator.add_trade.assert_not_called()

    dlq_publisher.publish.assert_called_once_with(message)



def test_processor_propagates_dlq_publishing_failure():
    """A failed DLQ publication must not be silently ignored."""

    message = Mock()
    message.value.return_value = b"invalid-json"

    consumer = Mock()
    consumer.poll.return_value = message

    deserializer = Mock(
        side_effect=ValueError("Invalid JSON payload")
    )

    aggregator = Mock()
    dlq_publisher = Mock()
    dlq_publisher.publish.side_effect = RuntimeError(
        "DLQ delivery failed"
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        dlq_publisher=dlq_publisher,
    )

    with pytest.raises(RuntimeError, match="DLQ delivery failed"):
        processor.process_next()

    deserializer.assert_called_once_with(b"invalid-json")
    dlq_publisher.publish.assert_called_once_with(message)

    aggregator.add_trade.assert_not_called()
    consumer.commit.assert_not_called()


def test_processor_does_not_commit_offset_for_unpublished_candle():
    """An in-memory candle is not durable enough to commit its trade offset."""
    message = Mock()
    message.value.return_value = b"valid-trade"

    consumer = Mock()
    consumer.poll.return_value = message

    trade = Mock()
    deserializer = Mock(return_value=trade)

    aggregator = Mock()
    aggregator.get_ready_windows.return_value = []

    publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    result = processor.process_next()

    assert result is trade
    aggregator.add_trade.assert_called_once_with(trade)
    publisher.publish.assert_not_called()
    consumer.commit.assert_not_called()


def test_processor_commits_offset_after_successful_dlq_delivery():
    """Commit a malformed record only after confirmed DLQ delivery."""
    message = Mock()
    message.value.return_value = b"invalid-json"
    message.topic.return_value = "market.trades"
    message.partition.return_value = 0
    message.offset.return_value = 42

    consumer = Mock()
    consumer.poll.return_value = message

    successful_partition = Mock()
    successful_partition.topic = "market.trades"
    successful_partition.partition = 0
    successful_partition.offset = 43
    successful_partition.error = None

    consumer.commit.return_value = [successful_partition]

    deserializer = Mock(
        side_effect=ValueError("Invalid JSON payload")
    )

    aggregator = Mock()
    dlq_publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        dlq_publisher=dlq_publisher,
    )

    result = processor.process_next()

    assert result is None
    dlq_publisher.publish.assert_called_once_with(message)
    consumer.commit.assert_called_once_with(
        offsets=[
            TopicPartition(
                message.topic(),
                message.partition(),
                message.offset() + 1,
            )
        ],
        asynchronous=False,
    )


def test_processor_does_not_commit_dlq_offset_past_unpublished_trade():
    """A DLQ success must not acknowledge an earlier pending trade."""
    valid_message = Mock()
    valid_message.value.return_value = b"valid-trade"
    valid_message.topic.return_value = "market.trades"
    valid_message.partition.return_value = 0
    valid_message.offset.return_value = 41

    invalid_message = Mock()
    invalid_message.value.return_value = b"invalid-json"
    invalid_message.topic.return_value = "market.trades"
    invalid_message.partition.return_value = 0
    invalid_message.offset.return_value = 42

    consumer = Mock()
    consumer.poll.side_effect = [valid_message, invalid_message]

    trade = Mock()
    deserializer = Mock(
        side_effect=[
            trade,
            ValueError("Invalid JSON payload"),
        ]
    )

    aggregator = Mock()
    aggregator.get_ready_windows.return_value = []

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=Mock(),
        dlq_publisher=Mock(),
    )

    processor.process_next()
    processor.process_next()

    aggregator.add_trade.assert_called_once_with(trade)
    processor.dlq_publisher.publish.assert_called_once_with(
        invalid_message
    )
    consumer.commit.assert_not_called()


def test_processor_does_not_confirm_failed_kafka_commit():
    """Failed Kafka commits must leave completed offsets pending."""
    message = Mock()
    message.value.return_value = b"invalid-json"
    message.topic.return_value = "market.trades"
    message.partition.return_value = 0
    message.offset.return_value = 42

    consumer = Mock()
    consumer.poll.return_value = message

    failed_partition = Mock()
    failed_partition.topic = "market.trades"
    failed_partition.partition = 0
    failed_partition.offset = 43
    failed_partition.error = KafkaError(KafkaError._TIMED_OUT)

    consumer.commit.return_value = [failed_partition]

    deserializer = Mock(
        side_effect=ValueError("Invalid JSON payload")
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=Mock(),
        dlq_publisher=Mock(),
    )

    with pytest.raises(RuntimeError, match="Kafka offset commit failed"):
        processor.process_next()

    # The DLQ record was delivered, but Kafka did not
    # acknowledge its offset.
    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) == 43


def test_processor_rejects_incorrect_kafka_commit_confirmation():
    """Reject Kafka confirmations that do not match the requested offset."""
    message = Mock()
    message.value.return_value = b"invalid-json"
    message.topic.return_value = "market.trades"
    message.partition.return_value = 0
    message.offset.return_value = 42

    consumer = Mock()
    consumer.poll.return_value = message

    incorrect_partition = Mock()
    incorrect_partition.topic = "market.trades"
    incorrect_partition.partition = 0
    incorrect_partition.offset = 42
    incorrect_partition.error = None

    consumer.commit.return_value = [incorrect_partition]

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=Mock(
            side_effect=ValueError("Invalid JSON payload")
        ),
        aggregator=Mock(),
        dlq_publisher=Mock(),
    )

    with pytest.raises(
        RuntimeError,
        match="Kafka commit confirmation mismatch",
    ):
        processor.process_next()

    # An incorrect confirmation must not advance our tracker.
    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) == 43


def test_processor_retries_failed_offset_commit():
    """Retry a failed commit without repeating DLQ publication."""
    message = Mock()
    message.value.return_value = b"invalid-json"
    message.topic.return_value = "market.trades"
    message.partition.return_value = 0
    message.offset.return_value = 42

    consumer = Mock()
    consumer.poll.return_value = message

    failed_partition = Mock()
    failed_partition.topic = "market.trades"
    failed_partition.partition = 0
    failed_partition.offset = 43
    failed_partition.error = KafkaError(KafkaError._TIMED_OUT)

    successful_partition = Mock()
    successful_partition.topic = "market.trades"
    successful_partition.partition = 0
    successful_partition.offset = 43
    successful_partition.error = None

    consumer.commit.side_effect = [
        [failed_partition],
        [successful_partition],
    ]

    dlq_publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=Mock(
            side_effect=ValueError("Invalid JSON payload")
        ),
        aggregator=Mock(),
        dlq_publisher=dlq_publisher,
    )

    # First attempt: DLQ succeeds, Kafka commit fails.
    with pytest.raises(RuntimeError, match="Kafka offset commit failed"):
        processor.process_next()

    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) == 43

    # No new Kafka message arrives.
    consumer.poll.return_value = None

    # Retry the pending commit.
    processor.process_next()

    assert consumer.commit.call_count == 2
    dlq_publisher.publish.assert_called_once_with(message)

    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) is None


def test_processor_retries_pending_commit_when_new_trade_arrives():
    """Retry a pending commit even when Kafka keeps delivering messages."""
    invalid_message = Mock()
    invalid_message.value.return_value = b"invalid-json"
    invalid_message.topic.return_value = "market.trades"
    invalid_message.partition.return_value = 0
    invalid_message.offset.return_value = 42

    valid_message = Mock()
    valid_message.value.return_value = b"valid-trade"
    valid_message.topic.return_value = "market.trades"
    valid_message.partition.return_value = 0
    valid_message.offset.return_value = 43

    consumer = Mock()
    consumer.poll.side_effect = [
        invalid_message,
        valid_message,
    ]

    failed_partition = Mock()
    failed_partition.topic = "market.trades"
    failed_partition.partition = 0
    failed_partition.offset = 43
    failed_partition.error = KafkaError(KafkaError._TIMED_OUT)

    successful_partition = Mock()
    successful_partition.topic = "market.trades"
    successful_partition.partition = 0
    successful_partition.offset = 43
    successful_partition.error = None

    consumer.commit.side_effect = [
        [failed_partition],
        [successful_partition],
    ]

    trade = Mock()
    deserializer = Mock(
        side_effect=[
            ValueError("Invalid JSON payload"),
            trade,
        ]
    )

    aggregator = Mock()
    aggregator.get_ready_windows.return_value = []
    dlq_publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        dlq_publisher=dlq_publisher,
    )

    # First record reaches the DLQ, but its offset commit fails.
    with pytest.raises(RuntimeError, match="Kafka offset commit failed"):
        processor.process_next()

    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) == 43

    # Another message arrives immediately; no idle poll occurs.
    result = processor.process_next()

    assert result is trade
    assert consumer.commit.call_count == 2

    dlq_publisher.publish.assert_called_once_with(
        invalid_message
    )

    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) is None


def test_processor_associates_accepted_trade_with_candle():
    trade = MarketTrade(
        "AAPL",
        Decimal("200"),
        10,
        datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
        "NASDAQ",
    )

    message = make_kafka_message(offset=41)

    consumer = Mock()
    consumer.poll.return_value = message

    deserializer = Mock(return_value=trade)
    aggregator = TradeBarAggregator()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
    )

    assert processor.process_next() == trade

    candle_key = (
        "AAPL",
        datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
    )

    assert processor.offset_tracker.offsets_for_candle(candle_key) == [
        ("market.trades", 0, 41)
    ]

    # Aggregation alone must not make the offset committable.
    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) is None


def test_processor_completes_offsets_after_candle_publication():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    messages = [
        make_kafka_message(offset=41),
        make_kafka_message(offset=42),
    ]

    consumer = Mock()
    consumer.poll.side_effect = messages

    deserializer = Mock(side_effect=trades)
    aggregator = TradeBarAggregator()
    publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    processor.process_next()
    processor.process_next()

    # The first candle should have been published.
    publisher.publish.assert_called_once()

    # Offset 41 belongs to the published candle.
    # Offset 42 belongs to an unfinished candle.
    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) == 42


def test_failed_candle_publication_keeps_offsets_pending():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    consumer = Mock()
    consumer.poll.side_effect = [
        make_kafka_message(offset=41),
        make_kafka_message(offset=42),
    ]

    publisher = Mock()
    publisher.publish.side_effect = RuntimeError(
        "Kafka publishing failed"
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=Mock(side_effect=trades),
        aggregator=TradeBarAggregator(),
        publisher=publisher,
    )

    processor.process_next()

    with pytest.raises(RuntimeError, match="Kafka publishing failed"):
        processor.process_next()

    candle_key = (
        "AAPL",
        datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
    )

    # The failed candle still owns offset 41.
    assert processor.offset_tracker.offsets_for_candle(candle_key) == [
        ("market.trades", 0, 41)
    ]

    # Neither record is safe to commit.
    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) is None

    consumer.commit.assert_not_called()


def test_processor_routes_late_trade_to_dlq_and_commits_offset():
    trade = MarketTrade(
        "AAPL",
        Decimal("200"),
        10,
        datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
        "NASDAQ",
    )

    message = make_kafka_message(offset=41)

    consumer = Mock()
    consumer.poll.return_value = message

    committed_partition = Mock()
    committed_partition.topic = "market.trades"
    committed_partition.partition = 0
    committed_partition.offset = 42
    committed_partition.error = None

    consumer.commit.return_value = [committed_partition]

    aggregator = Mock()
    aggregator.add_trade.side_effect = ValueError(
        "Trade arrived too late for its window"
    )

    dlq_publisher = Mock()

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=Mock(return_value=trade),
        aggregator=aggregator,
        dlq_publisher=dlq_publisher,
    )

    result = processor.process_next()

    assert result is None

    dlq_publisher.publish.assert_called_once_with(message)

    consumer.commit.assert_called_once_with(
        offsets=[
            TopicPartition("market.trades", 0, 42)
        ],
        asynchronous=False,
    )

    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) is None


def test_processor_retries_pending_commits_before_polling():
    consumer = Mock()
    consumer.poll.return_value = None

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=Mock(),
        aggregator=Mock(),
    )

    # Simulate an earlier record whose processing completed,
    # but whose Kafka offset has not yet been committed.
    processor.offset_tracker.register(
        "market.trades", 0, 41
    )
    processor.offset_tracker.mark_complete(
        "market.trades", 0, 41
    )

    # Simulate a failed Kafka commit.
    consumer.commit.side_effect = RuntimeError(
        "Kafka commit unavailable"
    )

    with pytest.raises(RuntimeError, match="Kafka commit unavailable"):
        processor.process_next()

    # A failed commit retry must not consume another message.
    consumer.poll.assert_not_called()

    # The completed offset must remain eligible for retry.
    assert processor.offset_tracker.committable_offset(
        "market.trades", 0
    ) == 42
