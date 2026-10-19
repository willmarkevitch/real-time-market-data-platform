from src.ingestion.trade_deserializer import deserialize_trade


def process_next_trade(consumer, process_trade) -> None:
    """Poll, deserialize, process, and acknowledge one Kafka message."""
    message = consumer.poll(timeout=1.0)

    if message is None:
        return

    trade = deserialize_trade(message.value())

    process_trade(trade)

    consumer.commit(message)
