import logging

from alpaca.data.live import StockDataStream

from src.ingestion.config import get_alpaca_credentials
from src.ingestion.normalizer import normalize_alpaca_trade


logger = logging.getLogger(__name__)


async def handle_trade(trade):
    """Normalize an incoming trade and reject invalid records."""
    try:
        normalized_trade = normalize_alpaca_trade(trade)
    except ValueError as exc:
        logger.warning("Rejected invalid trade: %s", exc)
        return

    print(normalized_trade)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    api_key, secret_key = get_alpaca_credentials()

    stream = StockDataStream(
        api_key=api_key,
        secret_key=secret_key,
    )

    stream.subscribe_trades(handle_trade, "AAPL")

    print("Starting Alpaca trade stream for AAPL...")
    stream.run()


if __name__ == "__main__":
    main()
