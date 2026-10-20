import pytest

from unittest.mock import Mock, call

from datetime import datetime, timezone
from decimal import Decimal

from src.ingestion.trade_processor import process_next_trade
from src.models.market_trade import MarketTrade


def test_process_next_trade_commits_after_success():
    events = Mock()
    consumer = Mock()
    message = Mock()

    message.value.return_value = (
        b'{"symbol":"AAPL","price":"341.49","size":10,'
        b'"timestamp":"2026-09-22T20:58:25+00:00","exchange":"V"}'
    )

    consumer.poll.return_value = message

    def process_trade(trade):
        assert isinstance(trade, MarketTrade)
        assert trade.symbol == "AAPL"
        events.process(trade)

    consumer.commit.side_effect = lambda msg: events.commit(msg)

    process_next_trade(
        consumer=consumer,
        process_trade=process_trade,
    )

    assert events.mock_calls == [
        call.process(
            MarketTrade(
                symbol="AAPL",
                price=Decimal("341.49"),
                size=10,
                timestamp=datetime(
                    2026, 9, 22, 20, 58, 25,
                    tzinfo=timezone.utc,
                ),
                exchange="V",
            )
        ),
        call.commit(message),
    ]


def test_process_next_trade_does_not_commit_on_processing_failure():
    consumer = Mock()
    message = Mock()

    message.value.return_value = (
        b'{"symbol":"AAPL","price":"341.49","size":10,'
        b'"timestamp":"2026-09-22T20:58:25+00:00","exchange":"V"}'
    )

    consumer.poll.return_value = message

    process_trade = Mock(side_effect=RuntimeError("Database unavailable"))

    with pytest.raises(RuntimeError, match="Database unavailable"):
        process_next_trade(
            consumer=consumer,
            process_trade=process_trade,
        )

    process_trade.assert_called_once()
    consumer.commit.assert_not_called()


def test_process_next_trade_does_not_commit_invalid_json():
    consumer = Mock()
    message = Mock()

    message.value.return_value = b'{"symbol": "AAPL", invalid JSON'
    consumer.poll.return_value = message

    process_trade = Mock()

    with pytest.raises(ValueError):
        process_next_trade(
            consumer=consumer,
            process_trade=process_trade,
        )

    process_trade.assert_not_called()
    consumer.commit.assert_not_called()


def test_process_next_trade_does_nothing_when_no_message():
    consumer = Mock()
    process_trade = Mock()

    consumer.poll.return_value = None

    process_next_trade(
        consumer=consumer,
        process_trade=process_trade,
    )

    consumer.poll.assert_called_once_with(timeout=1.0)
    process_trade.assert_not_called()
    consumer.commit.assert_not_called()


def test_invalid_trade_published_to_dlq_before_commit():
    events = Mock()
    consumer = Mock()
    message = Mock()
    process_trade = Mock()

    invalid_payload = b'{"symbol": "AAPL", invalid JSON'

    message.value.return_value = invalid_payload
    consumer.poll.return_value = message

    def publish_to_dlq(failed_message):
        events.publish(failed_message)

    consumer.commit.side_effect = lambda msg: events.commit(msg)

    process_next_trade(
        consumer=consumer,
        process_trade=process_trade,
        publish_to_dlq=publish_to_dlq,
    )

    process_trade.assert_not_called()

    assert events.mock_calls == [
        call.publish(message),
        call.commit(message),
    ]


def test_failed_dlq_publication_does_not_commit():
    consumer = Mock()
    message = Mock()
    process_trade = Mock()

    message.value.return_value = b'{"symbol": "AAPL", invalid JSON'
    consumer.poll.return_value = message

    publish_to_dlq = Mock(
        side_effect=RuntimeError("DLQ delivery failed")
    )

    with pytest.raises(RuntimeError, match="DLQ delivery failed"):
        process_next_trade(
            consumer=consumer,
            process_trade=process_trade,
            publish_to_dlq=publish_to_dlq,
        )

    publish_to_dlq.assert_called_once_with(message)
    process_trade.assert_not_called()
    consumer.commit.assert_not_called()
