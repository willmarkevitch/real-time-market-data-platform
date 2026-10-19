import pytest

from unittest.mock import Mock, call, patch

from src.ingestion.consumer_runner import run_consumer


def test_run_consumer_processes_messages_until_stopped():
    consumer = Mock()
    process_trade = Mock()

    # First poll returns a message; second poll signals shutdown.
    consumer.poll.side_effect = [
        Mock(),
        KeyboardInterrupt(),
    ]

    with patch(
        "src.ingestion.consumer_runner.process_next_trade"
    ) as mock_process:
        mock_process.side_effect = [
            None,
            KeyboardInterrupt(),
        ]

        run_consumer(
            consumer=consumer,
            process_trade=process_trade,
        )

        assert mock_process.call_count == 2
        assert mock_process.call_args_list == [
            call(consumer, process_trade),
            call(consumer, process_trade),
        ]

    consumer.close.assert_called_once()


def test_run_consumer_closes_on_processing_failure():
    consumer = Mock()
    process_trade = Mock()

    with patch(
        "src.ingestion.consumer_runner.process_next_trade"
    ) as mock_process:
        mock_process.side_effect = RuntimeError("Database unavailable")

        with pytest.raises(RuntimeError, match="Database unavailable"):
            run_consumer(
                consumer=consumer,
                process_trade=process_trade,
            )

        mock_process.assert_called_once_with(
            consumer,
            process_trade,
        )

    consumer.close.assert_called_once()
