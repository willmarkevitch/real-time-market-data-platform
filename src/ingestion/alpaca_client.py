from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest

from src.ingestion.config import get_alpaca_credentials


def main():
    api_key, secret_key = get_alpaca_credentials()

    client = StockHistoricalDataClient(api_key, secret_key)

    request_params = StockSnapshotRequest(
        symbol_or_symbols=["AAPL", "GOOGL", "MSFT"]
    )

    snapshot = client.get_stock_snapshot(request_params)
    print(snapshot)


if __name__ == "__main__":
    main()
