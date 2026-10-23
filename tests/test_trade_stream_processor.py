import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

from src.models.market_trade import MarketTrade
from src.streaming.trade_aggregator import TradeBarAggregator
from src.streaming.trade_stream_processor import TradeStreamProcessor


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

    messages = [Mock(), Mock()]
    for message in messages:
        message.value.return_value = b"serialized-trade"

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

    messages = [Mock(), Mock()]
    for message in messages:
        message.value.return_value = b"serialized-trade"

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

    messages = [Mock() for _ in trades]

    for message in messages:
        message.value.return_value = b"serialized-trade"

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

    messages = [Mock(), Mock()]

    for message in messages:
        message.value.return_value = b"serialized-trade"

    consumer = Mock()
    consumer.poll.side_effect = [*messages, None]

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

    # Successfully published candle should now be removed.
    with pytest.raises(KeyError):
        aggregator.get_bar(
            "AAPL",
            datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
        )

    consumer.commit.assert_not_called()



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

    for message in messages:
        message.value.return_value = b"serialized-trade"

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

    consumer = Mock()
    consumer.poll.return_value = message

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
