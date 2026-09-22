from unittest.mock import patch

import pytest

from src.ingestion.config import get_alpaca_credentials


def test_get_alpaca_credentials_returns_credentials():
    with (
        patch("src.ingestion.config.load_dotenv"),
        patch.dict(
            "os.environ",
            {
                "ALPACA_API_KEY": "test-api-key",
                "ALPACA_SECRET_KEY": "test-secret-key",
            },
        ),
    ):
        assert get_alpaca_credentials() == (
            "test-api-key",
            "test-secret-key",
        )


@pytest.mark.parametrize(
    "api_key, secret_key",
    [
        ("", "test-secret-key"),
        ("test-api-key", ""),
        ("", ""),
    ],
)
def test_get_alpaca_credentials_rejects_missing_values(api_key, secret_key):
    with (
        patch("src.ingestion.config.load_dotenv"),
        patch.dict(
            "os.environ",
            {
                "ALPACA_API_KEY": api_key,
                "ALPACA_SECRET_KEY": secret_key,
            },
        ),
    ):
        with pytest.raises(ValueError, match="must be configured"):
            get_alpaca_credentials()
