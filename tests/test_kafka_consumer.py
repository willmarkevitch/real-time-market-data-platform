from unittest.mock import patch

from src.ingestion.kafka_consumer import MarketTradeConsumer


@patch("src.ingestion.kafka_consumer.Consumer")
def test_consumer_subscribes_to_market_trades(mock_consumer_class):
    consumer = MarketTradeConsumer()

    mock_consumer_class.assert_called_once_with({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": "market-trade-consumers",
        "auto.offset.reset": "earliest",
    })

    mock_consumer_class.return_value.subscribe.assert_called_once_with(
        ["market.trades"]
    )


def test_poll_returns_message():
    with patch("src.ingestion.kafka_consumer.Consumer") as mock_class:
        mock_message = mock_class.return_value.poll.return_value

        consumer = MarketTradeConsumer()
        result = consumer.poll(timeout=1.0)

        mock_class.return_value.poll.assert_called_once_with(1.0)
        assert result is mock_message


def test_poll_returns_none_when_no_message():
    with patch("src.ingestion.kafka_consumer.Consumer") as mock_class:
        mock_class.return_value.poll.return_value = None

        consumer = MarketTradeConsumer()
        result = consumer.poll(timeout=1.0)

        assert result is None
