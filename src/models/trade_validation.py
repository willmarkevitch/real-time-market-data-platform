from datetime import datetime
from decimal import Decimal

from src.models.market_trade import MarketTrade


def validate_market_trade(trade: MarketTrade) -> MarketTrade:
    """Validate a MarketTrade against the internal data contract."""

    # Symbol validation
    if not isinstance(trade.symbol, str) or not trade.symbol.strip():
        raise ValueError("Trade symbol must be a non-empty string.")

    # Exchange validation
    if not isinstance(trade.exchange, str) or not trade.exchange.strip():
        raise ValueError("Trade exchange must be a non-empty string.")

    # Price validation
    if not isinstance(trade.price, Decimal):
        raise ValueError("Trade price must be a Decimal.")

    if not trade.price.is_finite() or trade.price <= 0:
        raise ValueError("Trade price must be finite and positive.")

    # Size validation
    if type(trade.size) is not int or trade.size <= 0:
        raise ValueError("Trade size must be a positive integer.")

    # Timestamp validation
    if not isinstance(trade.timestamp, datetime):
        raise ValueError("Trade timestamp must be a datetime.")

    if (
        trade.timestamp.tzinfo is None
        or trade.timestamp.utcoffset() is None
    ):
        raise ValueError("Trade timestamp must be timezone-aware.")

    return trade
