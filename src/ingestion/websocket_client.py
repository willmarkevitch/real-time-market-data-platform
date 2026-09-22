import logging

from alpaca.data.live import StockDataStream

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
    from src.ingestion.config import ALPACA_API_KEY, ALPACA_SECRET_KEY

    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    stream = StockDataStream(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
    )

    stream.subscribe_trades(handle_trade, "AAPL")

    print("Starting Alpaca trade stream for AAPL...")
    stream.run()


if __name__ == "__main__":
    main()
