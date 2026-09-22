import asyncio
import logging
import pytest
from unittest.mock import Mock, patch

from src.ingestion.websocket_client import handle_trade, main


def test_handle_trade_calls_normalizer():
    mock_producer = Mock()
    incoming_trade = Mock()
    normalized_trade = Mock()

    with patch(
        "src.ingestion.websocket_client.normalize_alpaca_trade",
        return_value=normalized_trade,
    ) as mock_normalizer:

        asyncio.run(handle_trade(incoming_trade, mock_producer))
        mock_producer.publish.assert_called_once_with(normalized_trade)

        mock_normalizer.assert_called_once_with(incoming_trade)


def test_handle_trade_rejects_invalid_trade(caplog, capsys):
    mock_producer = Mock()
    incoming_trade = Mock()

    with patch(
        "src.ingestion.websocket_client.normalize_alpaca_trade",
        side_effect=ValueError("Trade price must be positive."),
    ) as mock_normalizer:

        asyncio.run(handle_trade(incoming_trade, mock_producer))
        mock_producer.publish.assert_not_called()

        mock_normalizer.assert_called_once_with(incoming_trade)

    assert "Rejected invalid trade: Trade price must be positive." in caplog.text
    assert capsys.readouterr().out == ""


def test_main_configures_and_starts_stream():
    with (
        patch(
            "src.ingestion.websocket_client.get_alpaca_credentials",
            return_value=("test-api-key", "test-secret-key"),
        ) as mock_credentials,
        patch(
            "src.ingestion.websocket_client.StockDataStream",
        ) as mock_stream_class,
        patch(
            "src.ingestion.websocket_client.MarketTradeProducer",
        ) as mock_producer_class,
    ):
        main()

        mock_producer = mock_producer_class.return_value
        mock_producer.flush.assert_called_once_with(timeout=10.0)
        mock_credentials.assert_called_once_with()

        mock_stream_class.assert_called_once_with(
            api_key="test-api-key",
            secret_key="test-secret-key",
        )

        mock_stream = mock_stream_class.return_value

        mock_stream.subscribe_trades.assert_called_once()

        args, kwargs = mock_stream.subscribe_trades.call_args

        assert args[1] == "AAPL"
        assert callable(args[0])
        assert kwargs == {}

        mock_producer_class.assert_called_once_with()
        mock_stream.run.assert_called_once()


def test_main_stops_stream_on_keyboard_interrupt(caplog):
    caplog.set_level(logging.INFO, logger="src.ingestion.websocket_client")
    with (
        patch(
            "src.ingestion.websocket_client.get_alpaca_credentials",
            return_value=("test-api-key", "test-secret-key"),
        ),
        patch("src.ingestion.websocket_client.StockDataStream") as mock_stream_class,
        patch("src.ingestion.websocket_client.threading.Thread") as mock_thread_class,
        patch(
            "src.ingestion.websocket_client.MarketTradeProducer",
        ) as mock_producer_class,
    ):
        mock_worker = mock_thread_class.return_value

        # First check: the worker is running.
        # Second check: the worker has stopped after shutdown.
        mock_worker.is_alive.side_effect = [True, False]

        # Simulate Ctrl+C while the main thread waits for the worker.
        mock_worker.join.side_effect = [KeyboardInterrupt, None]

        main()

        mock_producer = mock_producer_class.return_value
        mock_producer.flush.assert_called_once_with(timeout=10.0)
        mock_stream = mock_stream_class.return_value

        mock_thread_class.assert_called_once_with(target=mock_stream.run)
        mock_worker.start.assert_called_once_with()

        mock_stream.stop.assert_called_once_with()

        assert mock_worker.join.call_count == 2
        assert mock_worker.join.call_args_list[0].kwargs == {"timeout": 0.5}
        assert mock_worker.join.call_args_list[1].kwargs == {"timeout": 10}

        assert "Shutdown requested; stopping Alpaca stream..." in caplog.text


def test_main_raises_if_worker_does_not_stop():
    with (
        patch(
            "src.ingestion.websocket_client.get_alpaca_credentials",
            return_value=("test-api-key", "test-secret-key"),
        ),
        patch("src.ingestion.websocket_client.StockDataStream") as mock_stream_class,
        patch("src.ingestion.websocket_client.MarketTradeProducer") as mock_producer_class,
        patch("src.ingestion.websocket_client.threading.Thread") as mock_thread_class,
    ):
        mock_worker = mock_thread_class.return_value

        # Worker is running before and after the shutdown request.
        mock_worker.is_alive.side_effect = [True, True]

        # Simulate Ctrl+C during the first join.
        mock_worker.join.side_effect = [KeyboardInterrupt, None]

        with pytest.raises(RuntimeError, match="Alpaca worker did not stop"):
            main()

        mock_stream_class.return_value.stop.assert_called_once_with()
        mock_producer_class.return_value.flush.assert_not_called()
