import json
from datetime import datetime
from decimal import Decimal

from src.models.market_trade import MarketTrade
from src.models.trade_validation import validate_market_trade


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

    return validate_market_trade(trade)
