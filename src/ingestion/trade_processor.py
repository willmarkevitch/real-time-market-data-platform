from src.ingestion.trade_deserializer import deserialize_trade


def process_next_trade(
    consumer,
    process_trade,
    publish_to_dlq=None,
) -> None:
    """Poll, validate, process, and acknowledge one Kafka message."""
    message = consumer.poll(timeout=1.0)

    if message is None:
        return

    try:
        trade = deserialize_trade(message.value())

    except (ValueError, KeyError, TypeError) as exc:
        if publish_to_dlq is None:
            raise

        publish_to_dlq(message)
        consumer.commit(message)
        return

    process_trade(trade)
    consumer.commit(message)
