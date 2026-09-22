from confluent_kafka import Producer

from src.models.market_trade import MarketTrade
from src.ingestion.trade_serializer import serialize_trade


class MarketTradeProducer:
    """Publish normalized market trades to Kafka."""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
        })

    def publish(self, trade: MarketTrade) -> None:
        """Queue a trade for delivery to Kafka."""
        self.producer.produce(
            topic="market.trades",
            key=trade.symbol.encode("utf-8"),
            value=serialize_trade(trade),
        )
        self.producer.poll(0)