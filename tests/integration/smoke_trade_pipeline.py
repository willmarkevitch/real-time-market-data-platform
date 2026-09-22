import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from src.ingestion.kafka_producer import MarketTradeProducer
from src.ingestion.websocket_client import handle_trade


def main():
    # Simulate the fields provided by an incoming Alpaca trade.
    incoming_trade = SimpleNamespace(
        symbol="AAPL",
        price=341.49,
        size=10,
        timestamp=datetime.now(timezone.utc),
        exchange="V",
    )

    producer = MarketTradeProducer()

    # Exercise the real handler, normalizer, serializer, and Kafka producer.
    asyncio.run(handle_trade(incoming_trade, producer))

    # Wait for Kafka delivery and surface any reported delivery failures.
    producer.flush(timeout=10.0)

    print("Simulated trade delivered to Kafka.")


if __name__ == "__main__":
    main()