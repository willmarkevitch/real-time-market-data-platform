import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition

from src.ingestion.dlq_publisher import DeadLetterPublisher
from src.ingestion.kafka_consumer import MarketTradeConsumer
from src.ingestion.trade_processor import process_next_trade


def test_consumer_receives_published_message():
    topic = "market.trades"
    payload = b'{"integration_test": true}'
    group_id = f"integration-test-{uuid.uuid4()}"

    consumer = MarketTradeConsumer(
        bootstrap_servers="127.0.0.1:9092",
        group_id=group_id,
    )

    producer = Producer({
        "bootstrap.servers": "127.0.0.1:9092",
        "acks": "all",
    })

    try:
        # MarketTradeConsumer subscribes during initialization.
        # Wait until Kafka assigns partitions before publishing.
        deadline = time.monotonic() + 15

        while not consumer.consumer.assignment():
            consumer.poll(0.5)

            if time.monotonic() > deadline:
                raise TimeoutError("Consumer assignment timed out")

        producer.produce(
            topic=topic,
            key=b"integration-test",
            value=payload,
        )

        assert producer.flush(10) == 0

        deadline = time.monotonic() + 15

        while time.monotonic() < deadline:
            message = consumer.poll(1.0)

            if message is None:
                continue

            # MarketTradeConsumer.poll() already raises KafkaException
            # if Kafka reports an error.
            if message.value() == payload:
                assert message.key() == b"integration-test"
                return

        raise AssertionError(
            "Consumer did not receive the test message"
        )

    finally:
        consumer.close()


def test_invalid_trade_is_published_to_dlq():
    payload = f"invalid-json-{uuid.uuid4()}".encode("utf-8")
    group_id = f"dlq-source-test-{uuid.uuid4()}"
    dlq_group_id = f"dlq-reader-test-{uuid.uuid4()}"

    source_consumer = MarketTradeConsumer(
        group_id=group_id,
    )
    dlq_consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": dlq_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers": "127.0.0.1:9092",
        "acks": "all",
    })
    dlq_publisher = DeadLetterPublisher()

    processed_trades = []

    try:
        dlq_consumer.subscribe(["market.trades.dlq"])

        producer.produce(
            topic="market.trades",
            key=b"dlq-integration-test",
            value=payload,
        )
        assert producer.flush(10) == 0

        deadline = time.monotonic() + 20

        while time.monotonic() < deadline:
            process_next_trade(
                source_consumer,
                processed_trades.append,
                publish_to_dlq=dlq_publisher.publish,
            )

            message = dlq_consumer.poll(0.5)

            if message is not None:
                if message.error():
                    raise RuntimeError(message.error())

                if message.value() == payload:
                    headers = dict(message.headers())

                    assert headers["original_topic"] == b"market.trades"
                    assert "original_partition" in headers
                    assert "original_offset" in headers
                    assert processed_trades == []

                    source_partition = int(headers["original_partition"])
                    source_offset = int(headers["original_offset"])

                    committed = source_consumer.consumer.committed(
                        [TopicPartition("market.trades", source_partition)],
                        timeout=10,
                    )

                    assert committed[0].offset == source_offset + 1

                    return

        raise AssertionError(
            "Malformed trade was not found in the dead-letter topic"
        )

    finally:
        source_consumer.close()
        dlq_consumer.close()
