import os

from dotenv import load_dotenv


def get_alpaca_credentials():
    """Load and validate Alpaca API credentials."""
    load_dotenv()

    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        raise ValueError("Alpaca API key and secret key must be configured.")

    return api_key, secret_key
