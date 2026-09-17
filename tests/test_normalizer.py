from datetime import datetime, timezone, timedelta
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

def test_accept_integer_size(valid_trade):
    valid_trade.size = 40

    result = normalize_alpaca_trade(valid_trade)

    assert result.size == 40
    assert isinstance(result.size, int)


# Price validation tests

def test_reject_zero_price(valid_trade):
    valid_trade.price = 0.0

    with pytest.raises(ValueError, match="positive"):
        normalize_alpaca_trade(valid_trade)


def test_reject_negative_price(valid_trade):
    valid_trade.price = -10.0

    with pytest.raises(ValueError, match="positive"):
        normalize_alpaca_trade(valid_trade)


def test_reject_nan_price(valid_trade):
    valid_trade.price = float("nan")

    with pytest.raises(ValueError, match="finite"):
        normalize_alpaca_trade(valid_trade)


def test_reject_infinite_price(valid_trade):
    valid_trade.price = float("inf")

    with pytest.raises(ValueError, match="finite"):
        normalize_alpaca_trade(valid_trade)

# Timestamp Validation Tests

def test_reject_missing_timezone(valid_trade):
    valid_trade.timestamp = datetime(2026, 9, 16, 20, 0)

    with pytest.raises(ValueError, match="timezone-aware"):
        normalize_alpaca_trade(valid_trade)


def test_reject_invalid_timestamp_type(valid_trade):
    valid_trade.timestamp = "2026-09-16T20:00:00"

    with pytest.raises(ValueError, match="datetime"):
        normalize_alpaca_trade(valid_trade)


def test_normalize_timestamp_to_utc(valid_trade):
    valid_trade.timestamp = datetime(
        2026, 9, 16, 16, 0,
        tzinfo=timezone(timedelta(hours=-4))
    )

    result = normalize_alpaca_trade(valid_trade)

    assert result.timestamp == datetime(
        2026, 9, 16, 20, 0, tzinfo=timezone.utc
    )
    assert result.timestamp.tzinfo is timezone.utc

# Symbol Validation Tests


def test_reject_empty_symbol(valid_trade):
    valid_trade.symbol = ""

    with pytest.raises(ValueError, match="symbol"):
        normalize_alpaca_trade(valid_trade)


def test_reject_whitespace_only_symbol(valid_trade):
    valid_trade.symbol = " "
    with pytest.raises(ValueError, match="symbol"):
        normalize_alpaca_trade(valid_trade)


def test_reject_none_symbol(valid_trade):
    valid_trade.symbol = None

    with pytest.raises(ValueError, match="symbol"):
        normalize_alpaca_trade(valid_trade)


def test_reject_strip_symbol(valid_trade):
    valid_trade.symbol = " AAPL "

    result = normalize_alpaca_trade(valid_trade)
    assert result.symbol == "AAPL"

# Exchange Validation Tests


def test_reject_empty_exchange(valid_trade):
    valid_trade.exchange = ""

    with pytest.raises(ValueError, match="exchange"):
        normalize_alpaca_trade(valid_trade)


def test_reject_whitespace_only_exchange(valid_trade):
    valid_trade.exchange = " "
    with pytest.raises(ValueError, match="exchange"):
        normalize_alpaca_trade(valid_trade)


def test_reject_none_exchange(valid_trade):
    valid_trade.exchange = None

    with pytest.raises(ValueError, match="exchange"):
        normalize_alpaca_trade(valid_trade)


def test_reject_strip_exchange(valid_trade):
    valid_trade.exchange = " V "

    result = normalize_alpaca_trade(valid_trade)
    assert result.exchange == "V"