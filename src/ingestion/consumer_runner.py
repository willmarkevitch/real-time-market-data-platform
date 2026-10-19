from src.ingestion.trade_processor import process_next_trade


def run_consumer(consumer, process_trade) -> None:
    """Continuously process Kafka trades until interrupted."""
    try:
        while True:
            process_next_trade(consumer, process_trade)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
