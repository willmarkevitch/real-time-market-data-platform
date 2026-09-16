from alpaca.data.historical import StockHistoricalDataClient
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from alpaca.data.requests import StockSnapshotRequest

client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

request_params = StockSnapshotRequest(symbol_or_symbols=["AAPL", "GOOGL", "MSFT"])
snapshot = client.get_stock_snapshot(request_params)
print(snapshot)