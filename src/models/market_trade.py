from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

@dataclass
class MarketTrade:
    """ Represents a normalized market trade. """
    symbol: str
    price: Decimal
    size: int
    timestamp: datetime
    exchange: str