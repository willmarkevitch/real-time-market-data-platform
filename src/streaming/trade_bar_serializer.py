
import json

from src.streaming.trade_aggregator import TradeBar


def serialize_trade_bar(bar: TradeBar) -> bytes:
    """Serialize a finalized TradeBar into JSON bytes."""

    payload = {
        "symbol": bar.symbol,
        "window_start": bar.window_start.isoformat(),
        "open": str(bar.open),
        "high": str(bar.high),
        "low": str(bar.low),
        "close": str(bar.close),
        "volume": bar.volume,
        "vwap": str(bar.vwap),
    }

    return json.dumps(payload).encode("utf-8")
