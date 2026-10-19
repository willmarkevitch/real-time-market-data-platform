from src.ingestion.trade_processor import process_next_trade


def run_consumer(consumer, process_trade, publish_to_dlq=None) -> None:
    """Continuously process Kafka trades until interrupted."""
    try:
        while True:
            if publish_to_dlq is None:
                process_next_trade(consumer, process_trade)
            else:
                process_next_trade(
                    consumer,
                    process_trade,
                    publish_to_dlq=publish_to_dlq,
                )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
