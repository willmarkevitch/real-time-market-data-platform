
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
