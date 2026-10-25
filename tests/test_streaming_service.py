
from unittest.mock import Mock, patch

from src.streaming.streaming_service import create_stream_processor
from src.ingestion.trade_deserializer import deserialize_trade

def test_create_stream_processor_wires_dependencies():
    with (
        patch(
            "src.streaming.streaming_service.Consumer"
        ) as mock_consumer_class,
        patch(
            "src.streaming.streaming_service.Producer"
        ) as mock_producer_class,
    ):
        processor = create_stream_processor(
            bootstrap_servers="localhost:9092"
        )

        mock_consumer_class.assert_called_once()

        consumer_config = (
            mock_consumer_class.call_args.args[0]
        )

        assert consumer_config["bootstrap.servers"] == "localhost:9092"
        assert consumer_config["enable.auto.commit"] is False
        assert consumer_config["enable.auto.offset.store"] is False

        mock_producer_class.assert_called_once()

        producer_config = (
            mock_producer_class.call_args.args[0]
        )

        assert producer_config["bootstrap.servers"] == "localhost:9092"

        assert processor.consumer is mock_consumer_class.return_value
        assert processor.publisher.producer is mock_producer_class.return_value


def test_create_stream_processor_uses_trade_deserializer():
    with (
        patch("src.streaming.streaming_service.Consumer"),
        patch("src.streaming.streaming_service.Producer"),
    ):
        processor = create_stream_processor()

        assert processor.deserializer is deserialize_trade



import pytest

from src.streaming.streaming_service import run_stream_processor


def test_run_stream_processor_processes_until_interrupted():
    processor = Mock()

    processor.process_next.side_effect = [
        None,
        None,
        KeyboardInterrupt(),
    ]

    with pytest.raises(KeyboardInterrupt):
        run_stream_processor(processor)

    assert processor.process_next.call_count == 3



from src.streaming.streaming_service import main


def test_main_closes_consumer_on_keyboard_interrupt():
    processor = Mock()
    processor.process_next.side_effect = KeyboardInterrupt()

    with (
        patch(
            "src.streaming.streaming_service.create_stream_processor",
            return_value=processor,
        ),
        patch(
            "src.streaming.streaming_service.run_stream_processor",
            side_effect=KeyboardInterrupt(),
        ),
    ):
        main()

    processor.consumer.close.assert_called_once()



def test_create_stream_processor_configures_dlq_publisher():
    """The streaming service should configure a dead-letter publisher."""
    from unittest.mock import patch

    from src.streaming.streaming_service import create_stream_processor

    with (
        patch("src.streaming.streaming_service.Consumer"),
        patch("src.streaming.streaming_service.Producer"),
        patch("src.streaming.streaming_service.DeadLetterPublisher") as mock_dlq,
    ):
        processor = create_stream_processor(
            bootstrap_servers="localhost:9092"
        )

    mock_dlq.assert_called_once_with(
        bootstrap_servers="localhost:9092",
        topic="market.trades.dlq",
    )

    assert processor.dlq_publisher is mock_dlq.return_value
