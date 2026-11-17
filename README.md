
# Real-Time Market Data Platform

![Python](https://img.shields.io/badge/Python-3.14-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1.1-black)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Tests](https://img.shields.io/badge/Tests-144%20passing-brightgreen)

An event-driven data engineering project built with **Python, Apache Kafka, and Docker** that ingests U.S. equity trades and processes them into **one-minute OHLCV candlesticks and volume-weighted average prices (VWAP)**.

The platform supports Alpaca market-data ingestion and a deterministic synthetic replay for demonstrations without API credentials or live market hours. It emphasizes typed data contracts, financial precision, event-time aggregation, dead-letter handling, delivery-confirmed publishing, and partition-aware offset management.

**Current milestone:** The local end-to-end pipeline has been demonstrated successfully: five synthetic AAPL trades were published to Kafka and aggregated into a verified one-minute candle. **144 automated tests passed** at this milestone. PostgreSQL persistence, monitoring, and production deployment are future work.

## Architecture

```text
    Alpaca market-data WebSocket       Synthetic demo replay
                 |                              |
                 v                              |
        Normalize and validate                  |
                 |                              |
                 v                              v
            MarketTrade model / JSON serialization
                              |
                              v
                     Kafka trade producer
                              |
                              v
                      market.trades
                              |
                              v
                    Streaming service
                     /             \
                    v               v
           Valid trade        Invalid / rejected trade
                |                      |
                v                      v
       Event-time aggregation      DLQ publisher
       1-minute OHLCV + VWAP            |
                |                      v
                v               market.trades.dlq
       Candle publisher
                |
                v
        market.candles.1m
```

The streaming processor tracks source offsets by Kafka topic/partition and coordinates offset commits with downstream handling. A completed candle is published before the associated eligible source offsets are acknowledged; rejected records are routed to the dead-letter topic. The design targets **at-least-once processing**, not exactly-once results: a crash between downstream publication and source-offset commit can cause replay and duplicate output.

### Kafka topics

| Topic | Partitions | Purpose |
| --- | ---: | --- |
| `market.trades` | 3 | Serialized trade events, keyed by symbol |
| `market.trades.dlq` | 3 | Rejected records and source metadata for investigation |
| `market.candles.1m` | 1 | Completed one-minute OHLCV/VWAP candles |

The single-node broker and replication factor of one are suitable for local development, **not high-availability production use**.

## Features

### Ingestion and data contracts

- **Alpaca integration:** Receives market-data trade events through a WebSocket client.
- **Typed trade model:** Represents symbol, price, size, timestamp, and exchange using a `MarketTrade` dataclass.
- **Financial precision:** Uses Python `Decimal` for trade prices and derived price calculations.
- **Validation:** Rejects invalid prices, nonpositive or fractional sizes, missing metadata, and timezone-naive timestamps.
- **Kafka serialization:** Encodes trades as JSON and reconstructs validated domain objects when consuming.
- **Deterministic replay:** Publishes five synthetic AAPL trades, then flushes the producer to confirm delivery.

### Stream processing

- **Event-time aggregation:** Groups trades into one-minute windows by symbol, calculating open, high, low, close, volume, and VWAP.
- **Allowed lateness:** Uses a 30-second event-time lateness allowance in the streaming service.
- **Candle publishing:** Publishes completed candles to `market.candles.1m`.
- **Dead-letter handling:** Routes malformed or rejected records to `market.trades.dlq`, retaining source metadata for debugging.
- **Partition-aware offsets:** Tracks processed Kafka offsets and manages eligible contiguous commits, including commit retries.
- **Delivery confirmation:** Coordinates downstream publishing and offset acknowledgment to support at-least-once recovery.

### Local infrastructure and engineering workflow

- **Docker Compose:** Runs Apache Kafka 4.1.1 in single-node KRaft mode and initializes the three topics.
- **Persistent Kafka storage:** Uses a named Docker volume across ordinary container restarts.
- **Automated tests:** Uses pytest, mocks, and broker-backed integration tests; 144 tests passed at the documented milestone.
- **CI:** GitHub Actions runs the broker-independent test suite on pushes and pull requests.
- **Development process:** Built incrementally with test-driven development, feature branches, and pull requests.

## Technology stack

| Area | Technology |
| --- | --- |
| Language | Python 3.14 |
| Market-data source | Alpaca API / WebSocket |
| Event streaming | Apache Kafka 4.1.1, `confluent-kafka` |
| Data representation | Dataclasses, `Decimal`, JSON |
| Infrastructure | Docker, Docker Compose |
| Testing | pytest, `unittest.mock`, Kafka integration tests |
| Continuous integration | GitHub Actions |
| Version control | Git and GitHub pull requests |

## Quick start: run the end-to-end demo

The demo uses **synthetic trades** and does **not** require Alpaca credentials, an active market session, or access to live market data.

### Prerequisites

- Python 3.14
- Docker Desktop with Docker Compose
- Git

### 1. Clone the repository

```bash
git clone https://github.com/willmarkevitch/real-time-market-data-platform.git
cd real-time-market-data-platform
```

### 2. Create a virtual environment and install dependencies

**Windows (Git Bash):**

```bash
python -m venv venv
source venv/Scripts/activate
python -m pip install -r requirements-dev.txt
```

**macOS/Linux:**

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements-dev.txt
```

### 3. Start Kafka

```bash
docker compose up -d
docker compose ps
```

The `kafka-init` service creates `market.trades`, `market.trades.dlq`, and `market.candles.1m`. Wait for topic initialization to finish before starting the demo. If necessary, rerun initialization:

```bash
docker compose run --rm kafka-init
```

### 4. Start the streaming service (terminal 1)

From the repository root, with your virtual environment active:

```bash
python -m src.streaming.streaming_service
```

Leave this process running. It consumes trades from `market.trades` and publishes finalized candles.

### 5. Replay synthetic trades (terminal 2)

Open a second terminal in the repository root, activate the same virtual environment, and run:

```bash
python -m src.ingestion.demo_replay
```

Expected confirmation:

```text
Successfully published 5 demo trades to Kafka.
```

The five deterministic AAPL trades have event timestamps in January 2026. Four belong to the 14:30 UTC minute; a fifth trade at 14:31:31 UTC advances the event-time watermark enough to finalize the earlier candle under the 30-second lateness allowance.

### 6. Inspect the output candle (terminal 3)

**Windows Git Bash:**

```bash
MSYS_NO_PATHCONV=1 docker exec market-data-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 \
  --topic market.candles.1m \
  --from-beginning \
  --timeout-ms 10000
```

**macOS/Linux:** Use the same command without the `MSYS_NO_PATHCONV=1` prefix.

Expected candle from a fresh demo run (JSON key ordering may differ):

```json
{
  "symbol": "AAPL",
  "window_start": "2026-01-05T14:30:00+00:00",
  "open": "250.00",
  "high": "252.00",
  "low": "249.00",
  "close": "251.00",
  "volume": 50,
  "vwap": "250.60"
}
```

The VWAP is the sum of each trade's price multiplied by its size, divided by the total size:

```text
(250.00 × 10 + 252.00 × 20 + 249.00 × 15 + 251.00 × 5) / 50 = 250.60
```

**Note:** The Kafka console consumer may print a `TimeoutException` after 10 seconds of inactivity. This is expected with `--timeout-ms 10000` and does not indicate that candle generation failed if the candle appeared. The `--from-beginning` option also shows any previously retained candles, not just this run's output.

### 7. Stop the services

Press **Ctrl+C** in terminal 1 to stop the Python streaming service gracefully. Then stop the Docker environment:

```bash
docker compose down
```

This retains Kafka's named data volume. **Do not run `docker compose down -v` unless you intend to delete the persisted Kafka data.**

### Repeating the demo from a clean Kafka state

Kafka retains messages and consumer-group offsets across ordinary restarts. Replaying the same synthetic timestamps into an existing broker may produce different visible results or duplicate candles because this pipeline is not idempotent or exactly-once.

For an isolated local demo, you can reset the **development-only** Kafka volume:

```bash
docker compose down -v
docker compose up -d
```

**Warning:** `down -v` permanently deletes the project's persisted Kafka messages and offsets. Do not use it on data you need to keep. Wait for topic initialization, then repeat steps 4–6.

## Running tests

Run broker-independent tests without Docker:

```bash
python -m pytest -m "not integration" -q
```

With Kafka running, run the marked broker-backed integration tests:

```bash
python -m pytest -m integration -v
```

Run the full suite:

```bash
python -m pytest -v
```

**Test milestone:** 144 passing tests, covering data models, validation, serialization, producers/consumers, aggregation, offset tracking, stream processing, dead-letter behavior, demo replay, and Kafka integration. This count is a point-in-time result, not a dynamically updated CI badge.

GitHub Actions runs the non-integration suite on pushes and pull requests. Kafka-backed integration tests currently run locally rather than in CI.

## Live ingestion configuration

The synthetic replay above needs no credentials. For Alpaca ingestion, copy `.env.example` to `.env` and set:

```dotenv
ALPACA_API_KEY=your_api_key_here
ALPACA_SECRET_KEY=your_secret_key_here
```

**Never commit `.env` or actual API credentials.** The repository's `.gitignore` excludes `.env`. The live-ingestion components are separate from the deterministic demo entry point; the quick-start instructions above specifically demonstrate the synthetic Kafka pipeline.

## Selected engineering decisions

**Why Kafka?** Kafka separates trade ingestion from downstream computation, supports replayable event logs and consumer groups, and allows partitioned processing.

**Why key by stock symbol?** Using the symbol as the Kafka message key preserves ordering for that symbol within a partition under the active partitioning scheme.

**Why `Decimal`?** Decimal arithmetic avoids binary floating-point representation artifacts that can be problematic when working with financial prices.

**Why event time rather than processing time?** A trade belongs to the minute represented by its event timestamp, not the minute in which the consumer happened to receive it. A watermark and allowed-lateness threshold determine when a window can be finalized.

**Why manual offset management?** A source offset should not be acknowledged before the corresponding record is handled. Tracking contiguous processed offsets per partition helps avoid committing past unfinished records.

**Why a dead-letter topic?** Invalid events can be inspected separately rather than silently discarded or repeatedly blocking normal processing.

**Why separate unit and integration tests?** Fast, deterministic tests run in CI without a broker; Kafka-backed tests verify interactions with a real broker in local development.

## Reliability and limitations

This is a **local portfolio and learning project**, not a production-grade trading system.

- **At-least-once, not exactly-once:** A crash after a candle is published but before the input offset is committed can lead to duplicate publication on replay.
- **In-memory processing state:** Aggregation and offset-tracking state are not durably checkpointed across process restarts.
- **Event-time watermark:** A candle may remain pending if no sufficiently later event arrives. Idle wall-clock time alone does not finalize it in this demo.
- **Local Kafka:** One broker and replication factor one provide no broker redundancy.
- **Demo replay:** Fixed timestamps and retained Kafka data mean repeated runs are not inherently isolated or idempotent.
- **Persistence and operations:** PostgreSQL storage, metrics, alerting, and production deployment are not yet implemented.

These limitations motivate future work on durable state, idempotent output handling, observability, and more robust deployment.

## Project layout

```text
real-time-market-data-platform/
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   ├── ingestion/
│   │   ├── demo_replay.py
│   │   ├── kafka_producer.py
│   │   ├── kafka_consumer.py
│   │   ├── dlq_publisher.py
│   │   ├── normalizer.py
│   │   ├── trade_serializer.py
│   │   ├── trade_deserializer.py
│   │   └── websocket_client.py
│   ├── models/
│   │   └── market_trade.py
│   └── streaming/
│       ├── streaming_service.py
│       ├── trade_aggregator.py
│       ├── trade_stream_processor.py
│       ├── offset_tracker.py
│       ├── kafka_candle_publisher.py
│       └── trade_bar_serializer.py
├── tests/
│   ├── integration/
│   └── test_*.py
├── compose.yaml
├── pytest.ini
├── requirements.txt
├── requirements-dev.txt
├── .env.example
└── README.md
```

This is a selected-file overview rather than an exhaustive listing.

## Roadmap

### Completed

- [x] Typed market-trade model, normalization, and validation
- [x] Alpaca WebSocket ingestion components
- [x] Kafka producer/consumer integration and dead-letter handling
- [x] One-minute event-time OHLCV and VWAP aggregation
- [x] Candle publishing to Kafka
- [x] Partition-aware offset tracking and commit retry handling
- [x] Deterministic end-to-end synthetic replay
- [x] Automated tests and GitHub Actions unit-test CI

### Planned

- [ ] PostgreSQL persistence and queryable historical candles
- [ ] Idempotent writes or other duplicate-mitigation strategy
- [ ] Durable processing state and improved recovery behavior
- [ ] Isolated Kafka integration tests in CI
- [ ] Structured logging, metrics, consumer-lag monitoring, and alerts
- [ ] Dashboard or API for recent market activity
- [ ] Deployment and scaling beyond a single local Kafka broker

## Development approach

This project is built incrementally through **test-driven development**, focused feature branches, code reviews via GitHub pull requests, and automated CI. Each milestone isolates an engineering concern—data contracts, Kafka delivery, fault handling, stream aggregation, offset management, or reproducible demonstrations—so the system remains explainable and testable as it grows.
