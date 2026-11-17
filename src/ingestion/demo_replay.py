
"""Deterministic sample trades for the end-to-end Kafka demo."""

from datetime import datetime, timezone
from decimal import Decimal

from src.models.market_trade import MarketTrade
from src.ingestion.kafka_producer import MarketTradeProducer

def build_demo_trades() -> list[MarketTrade]:
    """Build synthetic trades with predictable candle values."""

    samples = [
        (14, 30, 5, "250.00", 10),
        (14, 30, 15, "252.00", 20),
        (14, 30, 35, "249.00", 15),
        (14, 30, 50, "251.00", 5),
        (14, 31, 31, "253.00", 10),
    ]

    return [
        MarketTrade(
            symbol="AAPL",
            price=Decimal(price),
            size=size,
            timestamp=datetime(
                2026, 1, 5, hour, minute, second,
                tzinfo=timezone.utc,
            ),
            exchange="DEMO",
        )
        for hour, minute, second, price, size in samples
    ]



def replay_demo_trades(producer: MarketTradeProducer) -> None:
    """Publish deterministic demo trades and confirm delivery."""

    trades = build_demo_trades()

    for trade in trades:
        producer.publish(trade)

    producer.flush()



def main() -> None:
    """Run the deterministic Kafka trade replay demo."""

    producer = MarketTradeProducer()

    replay_demo_trades(producer)

    print("Successfully published 5 demo trades to Kafka.")


if __name__ == "__main__":
    main()
