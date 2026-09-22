import json
from datetime import datetime, timezone
from decimal import Decimal

from src.models.market_trade import MarketTrade
from src.ingestion.trade_serializer import serialize_trade


def test_serialize_trade():
    trade = MarketTrade(
        symbol="AAPL",
        price=Decimal("341.49"),
        size=10,
        timestamp=datetime(2026, 9, 22, 17, 46, 12, tzinfo=timezone.utc),
        exchange="V",
    )

    result = serialize_trade(trade)

    assert isinstance(result, bytes)

    payload = json.loads(result.decode("utf-8"))

    assert payload == {
        "symbol": "AAPL",
        "price": "341.49",
        "size": 10,
        "timestamp": "2026-09-22T17:46:12+00:00",
        "exchange": "V",
    }
