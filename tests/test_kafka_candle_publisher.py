import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

from src.streaming.trade_aggregator import TradeBar
from src.streaming.trade_bar_serializer import serialize_trade_bar
from src.streaming.kafka_candle_publisher import KafkaCandlePublisher


def test_publishes_candle_to_correct_topic():
    producer = Mock()
    producer.flush.return_value = 0

    def simulate_successful_delivery(**kwargs):
        kwargs["on_delivery"](None, Mock())

    producer.produce.side_effect = simulate_successful_delivery

    publisher = KafkaCandlePublisher(
        producer=producer,
        topic="market.candles.1m",
    )

    bar = TradeBar(
        symbol="AAPL",
        window_start=datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        ),
        open=Decimal("200"),
        high=Decimal("205"),
        low=Decimal("199"),
        close=Decimal("202"),
        volume=30,
        vwap=Decimal("201.5"),
        weighted_total=Decimal("6045"),
        first_trade_time=datetime(
            2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc
        ),
        last_trade_time=datetime(
            2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc
        ),
    )

    publisher.publish(bar)

    producer.produce.assert_called_once()

    kwargs = producer.produce.call_args.kwargs

    assert kwargs["topic"] == "market.candles.1m"
    assert kwargs["key"] == b"AAPL"
    assert kwargs["value"] == serialize_trade_bar(bar)
    assert callable(kwargs["on_delivery"])



def test_publisher_waits_for_delivery_confirmation():
    producer = Mock()
    producer.flush.return_value = 0

    def simulate_successful_delivery(**kwargs):
        kwargs["on_delivery"](None, Mock())

    producer.produce.side_effect = simulate_successful_delivery

    publisher = KafkaCandlePublisher(
        producer=producer,
        topic="market.candles.1m",
    )

    bar = TradeBar(
        symbol="AAPL",
        window_start=datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        ),
        open=Decimal("200"),
        high=Decimal("205"),
        low=Decimal("199"),
        close=Decimal("202"),
        volume=30,
        vwap=Decimal("201.5"),
        weighted_total=Decimal("6045"),
        first_trade_time=datetime(
            2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc
        ),
        last_trade_time=datetime(
            2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc
        ),
    )

    publisher.publish(bar)

    producer.produce.assert_called_once()
    producer.flush.assert_called_once()



def test_publisher_raises_when_kafka_delivery_fails():
    producer = Mock()

    def simulate_failed_delivery(**kwargs):
        callback = kwargs["on_delivery"]
        callback(Exception("Broker unavailable"), None)

    producer.produce.side_effect = simulate_failed_delivery
    producer.flush.return_value = 0

    publisher = KafkaCandlePublisher(
        producer=producer,
        topic="market.candles.1m",
    )

    bar = TradeBar(
        symbol="AAPL",
        window_start=datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        ),
        open=Decimal("200"),
        high=Decimal("205"),
        low=Decimal("199"),
        close=Decimal("202"),
        volume=30,
        vwap=Decimal("201.5"),
        weighted_total=Decimal("6045"),
        first_trade_time=datetime(
            2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc
        ),
        last_trade_time=datetime(
            2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc
        ),
    )

    with pytest.raises(RuntimeError, match="Broker unavailable"):
        publisher.publish(bar)



def test_publisher_raises_when_delivery_is_not_confirmed():
    producer = Mock()
    producer.flush.return_value = 0

    publisher = KafkaCandlePublisher(
        producer=producer,
        topic="market.candles.1m",
    )

    bar = TradeBar(
        symbol="AAPL",
        window_start=datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        ),
        open=Decimal("200"),
        high=Decimal("205"),
        low=Decimal("199"),
        close=Decimal("202"),
        volume=30,
        vwap=Decimal("201.5"),
        weighted_total=Decimal("6045"),
        first_trade_time=datetime(
            2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc
        ),
        last_trade_time=datetime(
            2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc
        ),
    )

    with pytest.raises(
        TimeoutError,
        match="delivery was not confirmed",
    ):
        publisher.publish(bar)



def test_publisher_raises_when_flush_times_out():
    producer = Mock()
    producer.flush.return_value = 1

    publisher = KafkaCandlePublisher(
        producer=producer,
        topic="market.candles.1m",
    )

    bar = TradeBar(
        symbol="AAPL",
        window_start=datetime(
            2026, 10, 22, 14, 30, tzinfo=timezone.utc
        ),
        open=Decimal("200"),
        high=Decimal("205"),
        low=Decimal("199"),
        close=Decimal("202"),
        volume=30,
        vwap=Decimal("201.5"),
        weighted_total=Decimal("6045"),
        first_trade_time=datetime(
            2026, 10, 22, 14, 30, 5, tzinfo=timezone.utc
        ),
        last_trade_time=datetime(
            2026, 10, 22, 14, 30, 45, tzinfo=timezone.utc
        ),
    )

    with pytest.raises(
        TimeoutError,
        match="delivery was not confirmed",
    ):
        publisher.publish(bar)

    producer.flush.assert_called_once_with(timeout=10.0)
