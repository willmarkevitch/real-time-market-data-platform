from datetime import datetime, timezone
from decimal import Decimal

from src.models.market_trade import MarketTrade


def test_market_trade_creation():
    trade = MarketTrade(
        symbol="AAPL",
        price=Decimal("331.335"),
        size=40,
        timestamp=datetime(2026, 9, 15, 20, 0, tzinfo=timezone.utc),
        exchange="V",
    )

    # Assertions to verify the correctness of the MarketTrade instance
    assert trade.symbol == "AAPL"
    assert trade.price == Decimal("331.335")
    assert trade.size == 40
    assert trade.exchange == "V"
    assert trade.timestamp == datetime(2026, 9, 15, 20, 0, tzinfo=timezone.utc)
    assert trade.timestamp.utcoffset() != None  # Ensure timestamp is timezone-aware