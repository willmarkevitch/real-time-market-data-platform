import logging
import threading

from alpaca.data.live import StockDataStream

from src.ingestion.config import get_alpaca_credentials
from src.ingestion.normalizer import normalize_alpaca_trade
from src.ingestion.kafka_producer import MarketTradeProducer


logger = logging.getLogger(__name__)


async def handle_trade(trade, producer: MarketTradeProducer):
    """Normalize an incoming trade and publish it to Kafka."""
    try:
        normalized_trade = normalize_alpaca_trade(trade)
    except ValueError as exc:
        logger.warning("Rejected invalid trade: %s", exc)
        return

    producer.publish(normalized_trade)
    logger.info("Queued trade for Kafka: %s", normalized_trade)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    api_key, secret_key = get_alpaca_credentials()
    producer = MarketTradeProducer()

    stream = StockDataStream(
        api_key=api_key,
        secret_key=secret_key,
    )

    async def kafka_trade_handler(trade):
        await handle_trade(trade, producer)

    stream.subscribe_trades(kafka_trade_handler, "AAPL")

    print("Starting Alpaca trade stream for AAPL...")

    worker = threading.Thread(target=stream.run)
    worker.start()

    try:
        while worker.is_alive():
            worker.join(timeout=0.5)
    except KeyboardInterrupt:
        logger.info("Shutdown requested; stopping Alpaca stream...")
        stream.stop()
        worker.join(timeout=10)

    finally:
        if worker.is_alive():
            logger.error(
                "Alpaca stream is still running; Kafka shutdown cannot safely complete."
            )
            raise RuntimeError("Alpaca worker did not stop")

        logger.info("Flushing Kafka producer...")

        try:
            producer.flush(timeout=10)
            logger.info("Kafka producer flushed successfully.")
        except (TimeoutError, RuntimeError):
            logger.exception("Kafka producer shutdown failed.")
            raise


if __name__ == "__main__":
    main()
