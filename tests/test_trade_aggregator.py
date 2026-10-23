import pytest
from datetime import datetime, timezone
from decimal import Decimal

from src.models.market_trade import MarketTrade
from src.streaming.trade_aggregator import TradeBarAggregator


def test_single_trade_creates_one_minute_bar():
    trade = MarketTrade(
        symbol="AAPL",
        price=Decimal("200.50"),
        size=10,
        timestamp=datetime(2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc),
        exchange="NASDAQ",
    )

    aggregator = TradeBarAggregator()
    aggregator.add_trade(trade)

    bar = aggregator.get_bar(
        symbol="AAPL",
        window_start=datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
    )

    assert bar.symbol == "AAPL"
    assert bar.window_start == datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc)
    assert bar.open == Decimal("200.50")
    assert bar.high == Decimal("200.50")
    assert bar.low == Decimal("200.50")
    assert bar.close == Decimal("200.50")
    assert bar.volume == 10
    assert bar.vwap == Decimal("200.50")



def test_multiple_trades_aggregate_into_same_minute():
    timestamps = [
        datetime(2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc),
        datetime(2026, 10, 22, 14, 30, 20, tzinfo=timezone.utc),
        datetime(2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc),
    ]

    trades = [
        MarketTrade("AAPL", Decimal("200"), 10, timestamps[0], "NASDAQ"),
        MarketTrade("AAPL", Decimal("202"), 5, timestamps[1], "NASDAQ"),
        MarketTrade("AAPL", Decimal("199"), 15, timestamps[2], "NASDAQ"),
    ]

    aggregator = TradeBarAggregator()

    for trade in trades:
        aggregator.add_trade(trade)

    bar = aggregator.get_bar(
        symbol="AAPL",
        window_start=datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
    )

    assert bar.open == Decimal("200")
    assert bar.high == Decimal("202")
    assert bar.low == Decimal("199")
    assert bar.close == Decimal("199")
    assert bar.volume == 30
    assert bar.vwap == Decimal("199.8333333333333333333333333")



def test_multiple_symbols_aggregate_independently():
    timestamp = datetime(
        2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc
    )

    trades = [
        MarketTrade("AAPL", Decimal("200"), 10, timestamp, "NASDAQ"),
        MarketTrade("MSFT", Decimal("400"), 5, timestamp, "NASDAQ"),
        MarketTrade("AAPL", Decimal("202"), 20, timestamp, "NASDAQ"),
        MarketTrade("MSFT", Decimal("405"), 15, timestamp, "NASDAQ"),
    ]

    aggregator = TradeBarAggregator()

    for trade in trades:
        aggregator.add_trade(trade)

    window_start = datetime(
        2026, 10, 22, 14, 30, tzinfo=timezone.utc
    )

    aapl_bar = aggregator.get_bar("AAPL", window_start)
    msft_bar = aggregator.get_bar("MSFT", window_start)

    assert aapl_bar.open == Decimal("200")
    assert aapl_bar.close == Decimal("202")
    assert aapl_bar.high == Decimal("202")
    assert aapl_bar.volume == 30
    assert aapl_bar.vwap == Decimal("201.3333333333333333333333333")

    assert msft_bar.open == Decimal("400")
    assert msft_bar.close == Decimal("405")
    assert msft_bar.high == Decimal("405")
    assert msft_bar.volume == 20
    assert msft_bar.vwap == Decimal("403.75")



def test_trades_in_different_minutes_create_separate_bars():
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
            Decimal("202"),
            5,
            datetime(2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 31, 5, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    aggregator = TradeBarAggregator()

    for trade in trades:
        aggregator.add_trade(trade)

    first_window = datetime(
        2026, 10, 22, 14, 30, tzinfo=timezone.utc
    )
    second_window = datetime(
        2026, 10, 22, 14, 31, tzinfo=timezone.utc
    )

    first_bar = aggregator.get_bar("AAPL", first_window)
    second_bar = aggregator.get_bar("AAPL", second_window)

    assert first_bar.open == Decimal("200")
    assert first_bar.close == Decimal("202")
    assert first_bar.volume == 15
    assert first_bar.vwap == Decimal("200.6666666666666666666666667")

    assert second_bar.open == Decimal("205")
    assert second_bar.close == Decimal("205")
    assert second_bar.volume == 20
    assert second_bar.vwap == Decimal("205")



def test_out_of_order_trades_use_event_time_for_open_and_close():
    trades = [
        MarketTrade(
            "AAPL",
            Decimal("202"),
            10,
            datetime(2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("200"),
            5,
            datetime(2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc),
            "NASDAQ",
        ),
        MarketTrade(
            "AAPL",
            Decimal("201"),
            15,
            datetime(2026, 10, 22, 14, 30, 20, tzinfo=timezone.utc),
            "NASDAQ",
        ),
    ]

    aggregator = TradeBarAggregator()

    for trade in trades:
        aggregator.add_trade(trade)

    bar = aggregator.get_bar(
        "AAPL",
        datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
    )

    assert bar.open == Decimal("200")
    assert bar.high == Decimal("202")
    assert bar.low == Decimal("200")
    assert bar.close == Decimal("202")
    assert bar.volume == 30
    assert bar.vwap == Decimal("201.1666666666666666666666667")



def test_finalizes_completed_windows_after_watermark():
    aggregator = TradeBarAggregator()

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
            Decimal("202"),
            5,
            datetime(2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc),
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

    for trade in trades:
        aggregator.add_trade(trade)

    finalized = aggregator.finalize_ready_windows()

    assert len(finalized) == 1

    bar = finalized[0]

    assert bar.symbol == "AAPL"
    assert bar.window_start == datetime(
        2026, 10, 22, 14, 30, tzinfo=timezone.utc
    )
    assert bar.open == Decimal("200")
    assert bar.close == Decimal("202")
    assert bar.volume == 15



def test_finalized_windows_are_removed_from_active_state():
    aggregator = TradeBarAggregator()

    aggregator.add_trade(
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        )
    )

    aggregator.add_trade(
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        )
    )

    finalized = aggregator.finalize_ready_windows()

    assert len(finalized) == 1

    # The finalized 14:30 candle should no longer be accessible.
    with pytest.raises(KeyError):
        aggregator.get_bar(
            "AAPL",
            datetime(2026, 10, 22, 14, 30, tzinfo=timezone.utc),
        )

    # The active 14:32 candle should still be accessible.
    active_bar = aggregator.get_bar(
        "AAPL",
        datetime(2026, 10, 22, 14, 32, tzinfo=timezone.utc),
    )

    assert active_bar.open == Decimal("205")
    assert active_bar.volume == 20



def test_rejects_trade_arriving_after_window_finalization():
    aggregator = TradeBarAggregator()

    aggregator.add_trade(
        MarketTrade(
            "AAPL",
            Decimal("200"),
            10,
            datetime(2026, 10, 22, 14, 30, 15, tzinfo=timezone.utc),
            "NASDAQ",
        )
    )

    aggregator.add_trade(
        MarketTrade(
            "AAPL",
            Decimal("205"),
            20,
            datetime(2026, 10, 22, 14, 32, 10, tzinfo=timezone.utc),
            "NASDAQ",
        )
    )

    aggregator.finalize_ready_windows()

    with pytest.raises(ValueError, match="late"):
        aggregator.add_trade(
            MarketTrade(
                "AAPL",
                Decimal("202"),
                5,
                datetime(2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc),
                "NASDAQ",
            )
        )



def test_rejects_trade_when_window_end_equals_watermark():
    aggregator = TradeBarAggregator(allowed_lateness_seconds=30)

    # Advances the watermark to exactly 14:31:00
    aggregator.add_trade(
        MarketTrade(
            "AAPL",
            Decimal("205"),
            10,
            datetime(2026, 10, 22, 14, 31, 30, tzinfo=timezone.utc),
            "NASDAQ",
        )
    )

    # The 14:30 window ended at 14:31:00
    with pytest.raises(ValueError, match="late"):
        aggregator.add_trade(
            MarketTrade(
                "AAPL",
                Decimal("200"),
                5,
                datetime(2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc),
                "NASDAQ",
            )
        )



def test_accepts_out_of_order_trade_in_open_window():
    aggregator = TradeBarAggregator(allowed_lateness_seconds=30)

    aggregator.add_trade(
        MarketTrade(
            "AAPL",
            Decimal("205"),
            10,
            datetime(2026, 10, 22, 14, 31, 45, tzinfo=timezone.utc),
            "NASDAQ",
        )
    )

    # Older than the watermark (14:31:15),
    # but its 14:31 window remains open.
    aggregator.add_trade(
        MarketTrade(
            "AAPL",
            Decimal("200"),
            5,
            datetime(2026, 10, 22, 14, 31, 10, tzinfo=timezone.utc),
            "NASDAQ",
        )
    )

    bar = aggregator.get_bar(
        "AAPL",
        datetime(2026, 10, 22, 14, 31, tzinfo=timezone.utc),
    )

    assert bar.open == Decimal("200")
    assert bar.close == Decimal("205")
    assert bar.volume == 15
