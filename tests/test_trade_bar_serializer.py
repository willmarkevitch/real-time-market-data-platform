
import json
from datetime import datetime, timezone
from decimal import Decimal

from src.streaming.trade_aggregator import TradeBar
from src.streaming.trade_bar_serializer import serialize_trade_bar


def test_serializes_trade_bar_to_json_bytes():
    bar = TradeBar(
        symbol="AAPL",
        window_start=datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        ),
        open=Decimal("200"),
        high=Decimal("205"),
        low=Decimal("199"),
        close=Decimal("202"),
        volume=30,
        vwap=Decimal("201.5"),
        weighted_total=Decimal("6045"),
        first_trade_time=datetime(
            2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc
        ),
        last_trade_time=datetime(
            2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc
        ),
    )

    payload = serialize_trade_bar(bar)

    assert isinstance(payload, bytes)

    data = json.loads(payload.decode("utf-8"))

    assert data == {
        "symbol": "AAPL",
        "window_start": "2026-10-22T14:30:00+00:00",
        "open": "200",
        "high": "205",
        "low": "199",
        "close": "202",
        "volume": 30,
        "vwap": "201.5",
    }



def test_serialization_preserves_decimal_precision():
    bar = TradeBar(
        symbol="AAPL",
        window_start=datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        ),
        open=Decimal("200.123456789123456789"),
        high=Decimal("205.987654321987654321"),
        low=Decimal("199.000000000000000001"),
        close=Decimal("202.123456789123456789"),
        volume=30,
        vwap=Decimal("201.123456789123456789"),
        weighted_total=Decimal("6033.703703673703703670"),
        first_trade_time=datetime(
            2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc
        ),
        last_trade_time=datetime(
            2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc
        ),
    )

    payload = serialize_trade_bar(bar)
    data = json.loads(payload.decode("utf-8"))

    assert data["open"] == "200.123456789123456789"
    assert data["high"] == "205.987654321987654321"
    assert data["low"] == "199.000000000000000001"
    assert data["close"] == "202.123456789123456789"
    assert data["vwap"] == "201.123456789123456789"
