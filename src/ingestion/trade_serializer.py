import json

from src.models.market_trade import MarketTrade


def serialize_trade(trade: MarketTrade) -> bytes:
    """Serialize a normalized market trade into UTF-8 JSON bytes."""
    trade_dict = {
        "symbol": trade.symbol,
        "price": str(trade.price),
        "size": trade.size,
        "timestamp": trade.timestamp.isoformat(),
        "exchange": trade.exchange,
    }

    return json.dumps(trade_dict).encode("utf-8")
