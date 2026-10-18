from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.models.market_trade import MarketTrade
from src.models.trade_validation import validate_market_trade


def make_valid_trade(**overrides):
    values = {
        "symbol": "AAPL",
        "price": Decimal("341.49"),
        "size": 10,
        "timestamp": datetime(2026, 9, 22, tzinfo=timezone.utc),
        "exchange": "V",
    }
    values.update(overrides)
    return MarketTrade(**values)


def test_validate_accepts_valid_trade():
    trade = make_valid_trade()

    assert validate_market_trade(trade) is trade


@pytest.mark.parametrize(
    "overrides",
    [
        {"symbol": ""},
        {"symbol": "   "},
        {"exchange": ""},
        {"price": Decimal("-1")},
        {"price": Decimal("NaN")},
        {"size": 0},
        {"size": 1.5},
        {"size": True},
        {"timestamp": datetime(2026, 9, 22)},
    ],
)
def test_validate_rejects_invalid_trade(overrides):
    trade = make_valid_trade(**overrides)

    with pytest.raises(ValueError):
        validate_market_trade(trade)
