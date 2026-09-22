import json
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from src.ingestion.kafka_producer import MarketTradeProducer
from src.models.market_trade import MarketTrade


def test_publish_queues_serialized_trade():
    # Arrange: create a sample trade.
    trade = MarketTrade(
        symbol="AAPL",
        price=Decimal("341.49"),
        size=10,
        timestamp=datetime(2026, 9, 22, 17, 46, 12, tzinfo=timezone.utc),
        exchange="V",
    )

    # Replace the Kafka Producer with a mock.
    with patch("src.ingestion.kafka_producer.Producer") as mock_producer_class:

        # Arrange: create our MarketTradeProducer.
        market_producer = MarketTradeProducer()

        # Act: publish the sample trade.
        market_producer.publish(trade)

        # Assert: verify Kafka producer configuration.
        mock_producer_class.assert_called_once_with({
            "bootstrap.servers": "127.0.0.1:9092",
            "acks": "all",
        })

        # Get the mocked Kafka client created by the constructor.
        mock_client = mock_producer_class.return_value

        # Assert: verify the message was queued correctly.
        mock_client.produce.assert_called_once()

        # Inspect the arguments passed to produce().
        kwargs = mock_client.produce.call_args.kwargs

        assert kwargs["topic"] == "market.trades"
        assert kwargs["key"] == b"AAPL"

        payload = json.loads(kwargs["value"].decode("utf-8"))

        assert payload == {
            "symbol": "AAPL",
            "price": "341.49",
            "size": 10,
            "timestamp": "2026-09-22T17:46:12+00:00",
            "exchange": "V",
        }

        # Assert: verify the producer processes delivery events.
        mock_client.poll.assert_called_once_with(0)


def test_publish_propagates_buffer_error():
    trade = MarketTrade(
        symbol="AAPL",
        price=Decimal("341.49"),
        size=10,
        timestamp=datetime(2026, 9, 22, 17, 46, 12, tzinfo=timezone.utc),
        exchange="V",
    )

    with patch("src.ingestion.kafka_producer.Producer") as mock_producer_class:
        mock_client = mock_producer_class.return_value

        # Arrange: simulate a full producer queue.
        mock_client.produce.side_effect = BufferError("Queue full")

        market_producer = MarketTradeProducer()

        # Act + Assert: publishing must raise the error.
        with pytest.raises(BufferError, match="Queue full"):
            market_producer.publish(trade)

        # Because produce() raised, execution never reached poll().
        mock_client.poll.assert_not_called()


def test_delivery_callback_records_failure():
    with patch("src.ingestion.kafka_producer.Producer"):
        market_producer = MarketTradeProducer()

        delivery_error = Exception("Broker unavailable")

        market_producer._on_delivery(delivery_error, None)

        assert market_producer.delivery_failure_count == 1


def test_flush_succeeds_when_all_messages_delivered():
    with patch("src.ingestion.kafka_producer.Producer") as mock_producer_class:
        mock_client = mock_producer_class.return_value
        mock_client.flush.return_value = 0

        market_producer = MarketTradeProducer()

        market_producer.flush()

        mock_client.flush.assert_called_once_with(10.0)


def test_flush_raises_when_messages_remain():
    with patch("src.ingestion.kafka_producer.Producer") as mock_producer_class:
        mock_client = mock_producer_class.return_value
        mock_client.flush.return_value = 2

        market_producer = MarketTradeProducer()

        with pytest.raises(TimeoutError, match="2 Kafka message"):
            market_producer.flush()


def test_flush_raises_when_delivery_failed():
    with patch("src.ingestion.kafka_producer.Producer") as mock_producer_class:
        mock_client = mock_producer_class.return_value
        mock_client.flush.return_value = 0

        market_producer = MarketTradeProducer()

        # Simulate Kafka reporting an asynchronous delivery failure.
        market_producer._on_delivery(Exception("Broker unavailable"), None)

        with pytest.raises(RuntimeError, match="1 Kafka message"):
            market_producer.flush()

        mock_client.flush.assert_called_once_with(10.0)


def test_check_delivery_errors_raises_after_failure():
    with patch("src.ingestion.kafka_producer.Producer"):
        market_producer = MarketTradeProducer()

        # Simulate a delivery failure.
        market_producer._on_delivery(Exception("Broker unavailable"), None)

        with pytest.raises(RuntimeError, match="1 Kafka message"):
            market_producer.check_delivery_errors()


def test_publish_raises_after_reported_delivery_failure():
    trade = MarketTrade(
        symbol="AAPL",
        price=Decimal("341.49"),
        size=10,
        timestamp=datetime(2026, 9, 22, 17, 46, 12, tzinfo=timezone.utc),
        exchange="V",
    )

    with patch("src.ingestion.kafka_producer.Producer") as mock_producer_class:
        market_producer = MarketTradeProducer()
        mock_client = mock_producer_class.return_value

        # Simulate a delivery callback being processed during poll().
        mock_client.poll.side_effect = lambda timeout: market_producer._on_delivery(
            Exception("Broker unavailable"), None
        )

        with pytest.raises(RuntimeError, match="1 Kafka message"):
            market_producer.publish(trade)
