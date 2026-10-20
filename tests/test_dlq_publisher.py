import pytest

from unittest.mock import Mock, patch

from src.ingestion.dlq_publisher import DeadLetterPublisher


def test_dlq_publisher_preserves_original_message():
    message = Mock()
    message.key.return_value = b"AAPL"
    message.value.return_value = b'{"symbol": "AAPL", invalid JSON'

    with patch(
        "src.ingestion.dlq_publisher.Producer"
    ) as mock_producer_class:
        producer = mock_producer_class.return_value

        def simulate_delivery(**kwargs):
            callback = kwargs["on_delivery"]
            callback(None, Mock())

        producer.produce.side_effect = simulate_delivery
        producer.flush.return_value = 0

        publisher = DeadLetterPublisher()
        publisher.publish(message)

        producer.produce.assert_called_once()

        kwargs = producer.produce.call_args.kwargs

        assert kwargs["topic"] == "market.trades.dlq"
        assert kwargs["key"] == b"AAPL"
        assert kwargs["value"] == message.value()


def test_dlq_publisher_waits_for_delivery_confirmation():
    message = Mock()
    message.key.return_value = b"AAPL"
    message.value.return_value = b"invalid JSON"

    with patch(
        "src.ingestion.dlq_publisher.Producer"
    ) as mock_producer_class:
        producer = mock_producer_class.return_value

        def simulate_delivery(**kwargs):
            callback = kwargs["on_delivery"]
            callback(None, Mock())

        producer.produce.side_effect = simulate_delivery
        producer.flush.return_value = 0

        publisher = DeadLetterPublisher()
        publisher.publish(message)

        producer.flush.assert_called_once_with(timeout=10.0)
        assert callable(
            producer.produce.call_args.kwargs["on_delivery"]
        )


def test_dlq_publisher_raises_on_delivery_failure():
    message = Mock()
    message.key.return_value = b"AAPL"
    message.value.return_value = b"invalid JSON"

    with patch(
        "src.ingestion.dlq_publisher.Producer"
    ) as mock_producer_class:
        producer = mock_producer_class.return_value

        def simulate_failed_delivery(**kwargs):
            callback = kwargs["on_delivery"]
            callback(RuntimeError("Broker unavailable"), Mock())

        producer.produce.side_effect = simulate_failed_delivery
        producer.flush.return_value = 0

        publisher = DeadLetterPublisher()

        with pytest.raises(RuntimeError, match="DLQ delivery failed"):
            publisher.publish(message)

        producer.produce.assert_called_once()
        producer.flush.assert_called_once_with(timeout=10.0)


def test_dlq_publisher_raises_on_delivery_timeout():
    message = Mock()
    message.key.return_value = b"AAPL"
    message.value.return_value = b"invalid JSON"

    with patch(
        "src.ingestion.dlq_publisher.Producer"
    ) as mock_producer_class:
        producer = mock_producer_class.return_value

        # Kafka has not confirmed delivery.
        producer.flush.return_value = 1

        publisher = DeadLetterPublisher()

        with pytest.raises(
            TimeoutError,
            match="DLQ delivery was not confirmed",
        ):
            publisher.publish(message)

        producer.produce.assert_called_once()
        producer.flush.assert_called_once_with(timeout=10.0)


def test_dlq_publisher_includes_original_message_metadata():
    message = Mock()

    message.key.return_value = b"AAPL"
    message.value.return_value = b"invalid JSON"
    message.topic.return_value = "market.trades"
    message.partition.return_value = 2
    message.offset.return_value = 145

    with patch(
        "src.ingestion.dlq_publisher.Producer"
    ) as mock_producer_class:
        producer = mock_producer_class.return_value

        def simulate_delivery(**kwargs):
            kwargs["on_delivery"](None, Mock())

        producer.produce.side_effect = simulate_delivery
        producer.flush.return_value = 0

        publisher = DeadLetterPublisher()
        publisher.publish(message)

        kwargs = producer.produce.call_args.kwargs

        assert kwargs["headers"] == [
            ("original_topic", b"market.trades"),
            ("original_partition", b"2"),
            ("original_offset", b"145"),
        ]
