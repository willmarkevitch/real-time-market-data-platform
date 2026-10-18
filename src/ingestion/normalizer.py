from datetime import datetime, timezone
from decimal import Decimal
from math import isfinite

from alpaca.data.models.trades import Trade

from src.models.market_trade import MarketTrade
from src.models.trade_validation import validate_market_trade

def normalize_alpaca_trade(trade: Trade) -> MarketTrade:
    """
    Normalizes an Alpaca Trade object into a MarketTrade object.

    Args:
        trade (Trade): The Alpaca Trade object to normalize.

    Returns:
        MarketTrade: The normalized MarketTrade object.
    """

    # Size Validation
    if not isfinite(trade.size):
        raise ValueError("Trade size must be a finite number.")

    if trade.size <= 0:
        raise ValueError("Trade size must be positive.")

    if trade.size != int(trade.size):
        raise ValueError("Trade size must be a whole number.")

    size = int(trade.size)

    # Price Validation
    price = Decimal(str(trade.price))
    if not price.is_finite():
        raise ValueError("Trade price must be a finite number.")

    if price <= 0:
        raise ValueError("Trade price must be positive.")

    # Timestamp Validation
    if not isinstance(trade.timestamp, datetime):
        raise ValueError("Trade timestamp must have datetime data type.")

    if (
        trade.timestamp.tzinfo is None
        or trade.timestamp.utcoffset() is None
    ):
        raise ValueError("Trade timestamp must be timezone-aware.")

    timestamp = trade.timestamp.astimezone(timezone.utc)

    # Symbol Validation
    if not isinstance(trade.symbol, str) or not trade.symbol.strip():
        raise ValueError("Trade symbol must be a non-empty string.")

    symbol = trade.symbol.strip()

    # Exchange Validation
    if not isinstance(trade.exchange, str) or not trade.exchange.strip():
        raise ValueError("Trade exchange must be a non-empty string.")

    exchange = trade.exchange.strip()

    # Create and return the MarketTrade object

    return validate_market_trade(
        MarketTrade(
            symbol=symbol,
            price=price,
            size=size,
            timestamp=timestamp,
            exchange=exchange
        )
    )