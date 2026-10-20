import pytest

from datetime import datetime, timezone
from decimal import Decimal

from src.ingestion.trade_deserializer import deserialize_trade
from src.models.market_trade import MarketTrade


def test_deserialize_valid_trade():
    payload = (
        b'{"symbol":"AAPL","price":"341.49","size":10,'
        b'"timestamp":"2026-09-22T20:58:25+00:00","exchange":"V"}'
    )

    result = deserialize_trade(payload)

    assert result == MarketTrade(
        symbol="AAPL",
        price=Decimal("341.49"),
        size=10,
        timestamp=datetime(2026, 9, 22, 20, 58, 25, tzinfo=timezone.utc),
        exchange="V",
    )

    assert isinstance(result.price, Decimal)
    assert result.timestamp.tzinfo is not None


def test_deserialize_rejects_negative_price():
    payload = (
        b'{"symbol":"AAPL","price":"-50.00","size":10,'
        b'"timestamp":"2026-09-22T20:58:25+00:00","exchange":"V"}'
    )

    with pytest.raises(ValueError):
        deserialize_trade(payload)


def test_deserialize_rejects_zero_size():
    payload = (
        b'{"symbol":"AAPL","price":"341.49","size":0,'
        b'"timestamp":"2026-09-22T20:58:25+00:00","exchange":"V"}'
    )

    with pytest.raises(ValueError):
        deserialize_trade(payload)
