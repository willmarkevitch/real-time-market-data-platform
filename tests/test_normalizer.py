from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

import pytest

from src.ingestion.normalizer import normalize_alpaca_trade


@pytest.fixture
def valid_trade():
    """Create a valid synthetic Alpaca trade for testing."""
    return Mock(
        symbol="AAPL",
        price=332.49,
        size=40.0,
        timestamp=datetime(2026, 9, 16, 20, 0, tzinfo=timezone.utc),
        exchange="V",
    )


def test_normalize_valid_trade(valid_trade):
    result = normalize_alpaca_trade(valid_trade)

    assert result.symbol == "AAPL"

    assert result.price == Decimal("332.49")
    assert isinstance(result.price, Decimal)

    assert result.size == 40
    assert isinstance(result.size, int)

    assert result.timestamp == datetime(
        2026, 9, 16, 20, 0, tzinfo=timezone.utc
    )
    assert result.exchange == "V"


# Size validation tests
def test_reject_fractional_size(valid_trade):
    valid_trade.size = 40.5

    with pytest.raises(ValueError, match="whole number"):
        normalize_alpaca_trade(valid_trade)


def test_reject_zero_size(valid_trade):
    valid_trade.size = 0.0

    with pytest.raises(ValueError, match="positive"):
        normalize_alpaca_trade(valid_trade)


def test_reject_negative_size(valid_trade):
    valid_trade.size = -10.0

    with pytest.raises(ValueError, match="positive"):
        normalize_alpaca_trade(valid_trade)


def test_reject_nan_size(valid_trade):
    valid_trade.size = float("nan")

    with pytest.raises(ValueError, match="finite number"):
        normalize_alpaca_trade(valid_trade)


def test_reject_infinite_size(valid_trade):
    valid_trade.size = float("inf")

    with pytest.raises(ValueError, match="finite number"):
        normalize_alpaca_trade(valid_trade)