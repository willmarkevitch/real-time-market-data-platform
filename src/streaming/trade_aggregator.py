
from datetime import datetime, timezone
from dataclasses import dataclass
from decimal import Decimal

from src.models.market_trade import MarketTrade


@dataclass
class TradeBar:
    symbol: str
    window_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal
    weighted_total: Decimal
    first_trade_time: datetime
    last_trade_time: datetime


class TradeBarAggregator:
    def __init__(self):
        self._bars = {}

    def add_trade(self, trade: MarketTrade):
        window_start = (
            trade.timestamp
            .astimezone(timezone.utc)
            .replace(second=0, microsecond=0)
        )

        key = (trade.symbol, window_start)

        if key in self._bars:
            bar = self._bars[key]

            bar.high = max(bar.high, trade.price)
            bar.low = min(bar.low, trade.price)
            
            # Update opening price if an earlier trade arrives
            if trade.timestamp < bar.first_trade_time:
                bar.open = trade.price
                bar.first_trade_time = trade.timestamp

            # Update closing price if a later trade arrives
            if trade.timestamp >= bar.last_trade_time:
                bar.close = trade.price
                bar.last_trade_time = trade.timestamp

            # Update cumulative weighted value and volume
            bar.weighted_total += trade.price * trade.size
            bar.volume += trade.size

            # Calculate VWAP from the cumulative weighted value
            bar.vwap = bar.weighted_total / bar.volume
            return

        # Initialize a new one-minute candle
        self._bars[key] = TradeBar(
            symbol=trade.symbol,
            window_start=window_start,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.size,
            vwap=trade.price,
            weighted_total=trade.price * trade.size,
            first_trade_time=trade.timestamp,
            last_trade_time=trade.timestamp,
        )

    def get_bar(self, symbol, window_start):
        return self._bars[(symbol, window_start)]
