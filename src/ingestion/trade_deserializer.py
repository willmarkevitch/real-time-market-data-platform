import json
from datetime import datetime
from decimal import Decimal

from src.models.market_trade import MarketTrade


def deserialize_trade(payload: bytes) -> MarketTrade:
    """Deserialize and validate a JSON payload into a MarketTrade."""
    data = json.loads(payload)

    trade = MarketTrade(
        symbol=data["symbol"],
        price=Decimal(data["price"]),
        size=data["size"],
        timestamp=datetime.fromisoformat(data["timestamp"]),
        exchange=data["exchange"],
    )

    if not trade.price.is_finite() or trade.price <= 0:
        raise ValueError("Trade price must be finite and positive")

    if type(trade.size) is not int or trade.size <= 0:
        raise ValueError("Trade size must be a positive integer")

    return trade
