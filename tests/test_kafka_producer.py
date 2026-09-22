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
            "bootstrap.servers": "localhost:9092",
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
