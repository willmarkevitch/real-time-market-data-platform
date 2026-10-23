
"""End-to-end smoke test for the Kafka streaming candle pipeline."""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from confluent_kafka import Consumer, Producer

from src.ingestion.trade_serializer import serialize_trade
from src.models.market_trade import MarketTrade
from src.streaming.streaming_service import create_stream_processor


BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TRADES_TOPIC = "market.trades"
CANDLES_TOPIC = "market.candles.1m"


def wait_for_assignment(consumer, name, timeout=10):
    """Wait until Kafka assigns partitions to a consumer."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        consumer.poll(0.1)

        if consumer.assignment():
            return

    raise TimeoutError(
        f"{name} did not receive Kafka partition assignments"
    )


def seek_to_end(consumer):
    """Position a consumer at the end of every assigned partition."""
    assignments = consumer.assignment()

    if not assignments:
        raise RuntimeError("Consumer has no assigned partitions")

    for partition in assignments:
        low, high = consumer.get_watermark_offsets(
            partition,
            timeout=10,
            cached=False,
        )

        consumer.seek(
            type(partition)(
                partition.topic,
                partition.partition,
                high,
            )
        )

        print(
            f"Consumer positioned at "
            f"{partition.topic}[{partition.partition}] "
            f"offset={high}"
        )


def main():
    run_id = uuid.uuid4().hex[:12]
    symbol = f"E2E{run_id.upper()}"

    # Choose timestamps in a single minute and a later minute.
    now = datetime.now(timezone.utc)
    first_timestamp = now.replace(second=10, microsecond=0)
    second_timestamp = first_timestamp + timedelta(seconds=95)

    expected_window = first_timestamp.replace(
        second=0,
        microsecond=0,
    ).isoformat()

    first_trade = MarketTrade(
        symbol=symbol,
        price=Decimal("100.25"),
        size=10,
        timestamp=first_timestamp,
        exchange="TEST",
    )

    second_trade = MarketTrade(
        symbol=symbol,
        price=Decimal("101.50"),
        size=5,
        timestamp=second_timestamp,
        exchange="TEST",
    )

    candle_consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"smoke-candles-{run_id}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
    })

    processor = None

    try:
        candle_consumer.subscribe([CANDLES_TOPIC])

        processor = create_stream_processor(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=f"smoke-stream-{run_id}",
            offset_reset="latest",
        )

        print(f"Starting smoke test: {run_id}")
        print(f"Test symbol: {symbol}")

        # Wait for both consumers to join their groups.
        wait_for_assignment(
            candle_consumer,
            "Candle consumer",
        )

        wait_for_assignment(
            processor.consumer,
            "Trade consumer",
        )

        # Explicitly skip all historical Kafka records.
        seek_to_end(candle_consumer)
        seek_to_end(processor.consumer)

        # Publish our two test trades.
        for trade in (first_trade, second_trade):
            producer.produce(
                topic=TRADES_TOPIC,
                key=trade.symbol.encode("utf-8"),
                value=serialize_trade(trade),
            )

        remaining = producer.flush(timeout=10)

        if remaining > 0:
            raise RuntimeError(
                f"{remaining} trade messages were not delivered"
            )

        print("Published two test trades.")
        print("Waiting for finalized candle...")

        # Process Kafka messages and inspect candle output.
        deadline = time.monotonic() + 30

        while time.monotonic() < deadline:
            processor.process_next()

            message = candle_consumer.poll(timeout=0.1)

            if message is None:
                continue

            if message.error():
                raise RuntimeError(message.error())

            if message.value() is None:
                continue

            candle = json.loads(message.value())

            if (
                candle.get("symbol") != symbol
                or candle.get("window_start") != expected_window
            ):
                continue

            assert candle["open"] == "100.25"
            assert candle["high"] == "100.25"
            assert candle["low"] == "100.25"
            assert candle["close"] == "100.25"
            assert candle["volume"] == 10
            assert Decimal(candle["vwap"]) == Decimal("100.25")

            print("\nPASS: End-to-end candle verified!")
            print(json.dumps(candle, indent=2))
            return

        raise AssertionError(
            "No matching candle received within 30 seconds"
        )

    finally:
        if processor is not None:
            processor.consumer.close()

        candle_consumer.close()


if __name__ == "__main__":
    main()
