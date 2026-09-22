import asyncio
from unittest.mock import Mock, patch

from src.ingestion.websocket_client import handle_trade, main
from src.ingestion.config import ALPACA_API_KEY, ALPACA_SECRET_KEY


def test_handle_trade_calls_normalizer(capsys):
    incoming_trade = Mock()
    normalized_trade = Mock()

    with patch(
        "src.ingestion.websocket_client.normalize_alpaca_trade",
        return_value=normalized_trade,
    ) as mock_normalizer:

        asyncio.run(handle_trade(incoming_trade))

        mock_normalizer.assert_called_once_with(incoming_trade)

    captured = capsys.readouterr()
    assert captured.out == f"{normalized_trade}\n"


def test_handle_trade_rejects_invalid_trade(caplog, capsys):
    incoming_trade = Mock()

    with patch(
        "src.ingestion.websocket_client.normalize_alpaca_trade",
        side_effect=ValueError("Trade price must be positive."),
    ) as mock_normalizer:

        asyncio.run(handle_trade(incoming_trade))

        mock_normalizer.assert_called_once_with(incoming_trade)

    assert "Rejected invalid trade: Trade price must be positive." in caplog.text
    assert capsys.readouterr().out == ""


def test_main_configures_and_starts_stream():
    with patch(
        "src.ingestion.websocket_client.StockDataStream"
    ) as mock_stream_class:
        main()

        mock_stream_class.assert_called_once_with(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_SECRET_KEY,
        )

        mock_stream = mock_stream_class.return_value

        mock_stream.subscribe_trades.assert_called_once_with(
            handle_trade, "AAPL"
        )
        mock_stream.run.assert_called_once()