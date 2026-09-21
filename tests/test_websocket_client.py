import asyncio
from unittest.mock import Mock, patch

from src.ingestion.websocket_client import handle_trade


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
