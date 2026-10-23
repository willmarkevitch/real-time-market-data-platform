
from confluent_kafka import Consumer, Producer

from src.streaming.trade_aggregator import TradeBarAggregator
from src.streaming.trade_stream_processor import TradeStreamProcessor
from src.streaming.kafka_candle_publisher import KafkaCandlePublisher
from src.ingestion.trade_deserializer import deserialize_trade
from src.ingestion.dlq_publisher import DeadLetterPublisher


def create_stream_processor(
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "market-candle-aggregators",
    offset_reset: str = "earliest",
) -> TradeStreamProcessor:
    """Create a trade processor with Kafka dependencies."""

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })

    consumer.subscribe(["market.trades"])

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
    })

    aggregator = TradeBarAggregator(
        allowed_lateness_seconds=30,
    )

    publisher = KafkaCandlePublisher(
        producer=producer,
        topic="market.candles.1m",
    )

    dlq_publisher = DeadLetterPublisher(
        bootstrap_servers=bootstrap_servers,
        topic="market.trades.dlq",
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserialize_trade,
        aggregator=aggregator,
        publisher=publisher,
        dlq_publisher=dlq_publisher,
    )

    return processor


def run_stream_processor(processor: TradeStreamProcessor) -> None:
    """Continuously process incoming Kafka trade messages."""
    while True:
        processor.process_next()



def main() -> None:
    """Start the streaming service and clean up on shutdown."""
    processor = create_stream_processor()

    try:
        run_stream_processor(processor)
    except KeyboardInterrupt:
        print("\nStreaming service stopped by user.")
    finally:
        processor.consumer.close()


if __name__ == "__main__":
    main()
