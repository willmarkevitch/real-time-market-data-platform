
from datetime import datetime, timezone, timedelta
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
    def __init__(self, allowed_lateness_seconds=30):
        if allowed_lateness_seconds < 0:
            raise ValueError("Allowed lateness cannot be negative")

        self._bars = {}
        self._latest_event_time = None
        self._allowed_lateness = timedelta(
            seconds=allowed_lateness_seconds
        )

    def add_trade(self, trade: MarketTrade):
        event_time = trade.timestamp.astimezone(timezone.utc)

        if (
            self._latest_event_time is None
            or event_time > self._latest_event_time
        ):
            self._latest_event_time = event_time
        
        window_start = (
            trade.timestamp
            .astimezone(timezone.utc)
            .replace(second=0, microsecond=0)
        )

        key = (trade.symbol, window_start)

        
        # Reject trades for windows that are already behind the watermark
        watermark = self._latest_event_time - self._allowed_lateness
        window_end = window_start + timedelta(minutes=1)

        if window_end <= watermark:
            raise ValueError("Trade arrived too late for its window")


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

    
    def finalize_ready_windows(self):
        if self._latest_event_time is None:
            return []

        watermark = self._latest_event_time - self._allowed_lateness

        finalized = []

        for key, bar in list(self._bars.items()):
            window_end = bar.window_start + timedelta(minutes=1)

            if window_end <= watermark:
                finalized.append(bar)
                del self._bars[key]

        return finalized
