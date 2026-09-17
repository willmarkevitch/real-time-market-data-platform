from decimal import Decimal
from math import isfinite

from alpaca.data.models.trades import Trade

from src.models.market_trade import MarketTrade

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

    if not trade.size.is_integer():
        raise ValueError("Trade size must be a whole number.")

    return MarketTrade(
        symbol=trade.symbol,
        price=Decimal(str(trade.price)),
        size=int(trade.size),
        timestamp=trade.timestamp,
        exchange=trade.exchange
    )
