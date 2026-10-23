import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

from src.streaming.trade_aggregator import TradeBarAggregator
from src.streaming.trade_stream_processor import TradeStreamProcessor
from src.streaming.kafka_candle_publisher import KafkaCandlePublisher


def test_trade_pipeline_publishes_completed_candle():
    first_trade = Mock(
        symbol="AAPL",
        price=Decimal("200"),
        size=10,
        timestamp=datetime(
            2026, 10, 22, 14, 30, 5,
            tzinfo=timezone.utc,
        ),
    )

    second_trade = Mock(
        symbol="AAPL",
        price=Decimal("205"),
        size=20,
        timestamp=datetime(
            2026, 10, 22, 14, 31, 40,
            tzinfo=timezone.utc,
        ),
    )

    messages = [Mock(), Mock()]
    consumer = Mock()
    consumer.poll.side_effect = messages

    deserializer = Mock(
        side_effect=[first_trade, second_trade]
    )

    kafka_producer = Mock()
    kafka_producer.flush.return_value = 0

    def acknowledge_delivery(**kwargs):
        kwargs["on_delivery"](None, Mock())

    kafka_producer.produce.side_effect = acknowledge_delivery

    aggregator = TradeBarAggregator(
        allowed_lateness_seconds=30
    )

    publisher = KafkaCandlePublisher(
        producer=kafka_producer,
        topic="market.candles.1m",
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    processor.process_next()
    processor.process_next()

    kafka_producer.produce.assert_called_once()

    kwargs = kafka_producer.produce.call_args.kwargs

    assert kwargs["topic"] == "market.candles.1m"
    assert kwargs["key"] == b"AAPL"

    import json

    candle = json.loads(kwargs["value"])

    assert candle["symbol"] == "AAPL"
    assert candle["open"] == "200"
    assert candle["high"] == "200"
    assert candle["low"] == "200"
    assert candle["close"] == "200"
    assert candle["volume"] == 10
    assert candle["vwap"] == "200"

    with pytest.raises(KeyError):
        aggregator.get_bar(
            "AAPL",
            datetime(
                2026, 10, 22, 14, 30,
                tzinfo=timezone.utc,
            ),
        )



def test_trade_pipeline_preserves_candle_when_kafka_fails():
    first_trade = Mock(
        symbol="AAPL",
        price=Decimal("200"),
        size=10,
        timestamp=datetime(
            2026, 10, 22, 14, 30, 5,
            tzinfo=timezone.utc,
        ),
    )

    second_trade = Mock(
        symbol="AAPL",
        price=Decimal("205"),
        size=20,
        timestamp=datetime(
            2026, 10, 22, 14, 31, 40,
            tzinfo=timezone.utc,
        ),
    )

    consumer = Mock()
    consumer.poll.side_effect = [Mock(), Mock()]

    deserializer = Mock(
        side_effect=[first_trade, second_trade]
    )

    kafka_producer = Mock()
    kafka_producer.flush.return_value = 0

    def fail_delivery(**kwargs):
        kwargs["on_delivery"](
            Exception("Broker unavailable"),
            None,
        )

    kafka_producer.produce.side_effect = fail_delivery

    aggregator = TradeBarAggregator(
        allowed_lateness_seconds=30
    )

    publisher = KafkaCandlePublisher(
        producer=kafka_producer,
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
        publisher=publisher,
    )

    # First trade starts the 14:30 candle.
    processor.process_next()

    # Second trade advances the watermark.
    # Kafka fails to publish the completed candle.
    with pytest.raises(
        RuntimeError,
        match="Broker unavailable",
    ):
        processor.process_next()

    # The completed candle must still exist.
    bar = aggregator.get_bar(
        "AAPL",
        datetime(
            2026, 10, 22, 14, 30,
            tzinfo=timezone.utc,
        ),
    )

    assert bar.symbol == "AAPL"
    assert bar.volume == 10
    assert bar.close == Decimal("200")

    kafka_producer.produce.assert_called_once()



def test_trade_pipeline_does_not_commit_uncheckpointed_trades():
    trade = Mock(
        symbol="AAPL",
        price=Decimal("200"),
        size=10,
        timestamp=datetime(
            2026, 10, 22, 14, 30, 5,
            tzinfo=timezone.utc,
        ),
    )

    consumer = Mock()
    consumer.poll.return_value = Mock()

    deserializer = Mock(return_value=trade)

    aggregator = TradeBarAggregator(
        allowed_lateness_seconds=30
    )

    processor = TradeStreamProcessor(
        consumer=consumer,
        deserializer=deserializer,
        aggregator=aggregator,
    )

    processor.process_next()

    consumer.commit.assert_not_called()
