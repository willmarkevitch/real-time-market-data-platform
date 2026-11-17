
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

from src.ingestion.demo_replay import replay_demo_trades
from src.ingestion.demo_replay import build_demo_trades
from src.ingestion.demo_replay import main


def test_build_demo_trades_produces_predictable_market_data():
    trades = build_demo_trades()

    assert len(trades) == 5

    assert all(trade.symbol == "AAPL" for trade in trades)
    assert all(trade.timestamp.tzinfo is not None for trade in trades)

    first_four = trades[:4]

    assert [trade.price for trade in first_four] == [
        Decimal("250.00"),
        Decimal("252.00"),
        Decimal("249.00"),
        Decimal("251.00"),
    ]

    assert [trade.size for trade in first_four] == [10, 20, 15, 5]

    assert first_four[0].timestamp == datetime(
        2026, 1, 5, 14, 30, 5, tzinfo=timezone.utc
    )

    # Advance the event-time watermark beyond the first candle's
    # end plus the configured 30-second allowed lateness.
    assert trades[4].timestamp == datetime(
        2026, 1, 5, 14, 31, 31, tzinfo=timezone.utc
    )


def test_replay_demo_trades_publishes_and_flushes():
    producer = Mock()
    expected_trades = build_demo_trades()

    replay_demo_trades(producer)

    assert producer.publish.call_count == len(expected_trades)

    published_trades = [
        call.args[0]
        for call in producer.publish.call_args_list
    ]

    assert published_trades == expected_trades
    producer.flush.assert_called_once_with()


def test_main_creates_producer_and_replays_trades():
    with patch(
        "src.ingestion.demo_replay.MarketTradeProducer"
    ) as mock_producer_class:
        with patch(
            "src.ingestion.demo_replay.replay_demo_trades"
        ) as mock_replay:
            main()

    mock_producer_class.assert_called_once_with()
    mock_replay.assert_called_once_with(
        mock_producer_class.return_value
    )
