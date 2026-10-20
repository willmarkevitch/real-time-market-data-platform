
# Real-Time Market Data Platform

![Python](https://img.shields.io/badge/Python-3.14-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1.1-black)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Tests](https://img.shields.io/badge/Tests-77%20passing-brightgreen)

An event-driven data engineering project that ingests U.S. equity trade events from **Alpaca**, normalizes and validates them, and streams them through **Apache Kafka**. The project emphasizes reliable message delivery, explicit data contracts, failure handling, and test-driven development.

**Current status:** WebSocket ingestion, Kafka publishing and consumption, dead-letter handling, and automated tests are implemented. Streaming aggregations and PostgreSQL persistence are planned next.

## Architecture

```text
Alpaca market-data WebSocket
             |
             v
    Trade normalization
             |
             v
     MarketTrade model
             |
             v
     JSON serialization
             |
             v
       Kafka producer
             |
             v
   Kafka topic: market.trades
             |
             v
       Kafka consumer
             |
             v
  Deserialize and validate
             |
       +-----+------+
       |            |
       v            v
  Valid trade   Invalid payload
       |            |
       v            v
 Processing      DLQ publisher
  callback           |
       |             v
       |     market.trades.dlq
       |
       v
  Future: OHLCV/VWAP aggregation
          and PostgreSQL
```

A valid trade is committed **after successful downstream processing**. An invalid payload is committed **after confirmed publication to the dead-letter topic**. If processing or dead-letter publication fails, the processing function does not commit the source offset, allowing the record to be replayed after recovery. This is an **at-least-once** design, not exactly-once processing; downstream operations should be idempotent where necessary.

## Implemented Features

### Market-Data Ingestion and Data Contracts

- **Alpaca WebSocket integration:** Receives trade events from Alpaca's market-data stream.
- **Typed domain model:** Converts provider-specific events into a `MarketTrade` model with symbol, price, size, timestamp, and exchange.
- **Financial precision:** Represents prices with Python `Decimal` rather than binary floating-point arithmetic.
- **Input validation:** Rejects invalid prices, nonpositive or fractional sizes, missing metadata, and timestamps without timezone information.
- **Serialization:** Encodes normalized trades as UTF-8 JSON for Kafka transport and deserializes them on consumption.

### Kafka Messaging and Reliability

- **Kafka producer:** Publishes normalized trades to `market.trades`, using the stock symbol as the message key.
- **Delivery handling:** Uses delivery callbacks, failure tracking, and producer flushing during shutdown.
- **Kafka consumer:** Supports configurable bootstrap servers, consumer groups, and subscribed topics.
- **Manual offset management:** Disables automatic commits and commits only after successful processing or dead-letter delivery.
- **Dead-letter queue (DLQ):** Sends malformed trade records to `market.trades.dlq`, preserving the original payload and key.
- **Source metadata:** Adds `original_topic`, `original_partition`, and `original_offset` headers to dead-letter records for investigation.
- **Consumer runner:** Provides a continuous processing loop with cleanup on keyboard interruption.

### Local Infrastructure

- **Docker Compose:** Runs Apache Kafka 4.1.1 in single-node KRaft mode.
- **Topic initialization:** Creates `market.trades` and `market.trades.dlq` with three partitions each.
- **Persistent broker data:** Uses a named Docker volume so ordinary container restarts do not erase Kafka data.
- **Host/container networking:** Exposes Kafka to local Python processes on `127.0.0.1:9092` and uses an internal Docker listener for container communication.

> **Development environment:** Kafka runs with a replication factor of one. This setup is intended for local development, not high-availability production deployment.

## Technology Stack

| Area | Technology |
| --- | --- |
| Language | Python 3.14 |
| Market-data source | Alpaca API and WebSocket |
| Streaming platform | Apache Kafka 4.1.1 |
| Kafka Python client | `confluent-kafka` |
| Data representation | Python dataclasses, `Decimal`, JSON |
| Infrastructure | Docker, Docker Compose |
| Testing | pytest, unittest mocking, Kafka integration tests |
| Continuous integration | GitHub Actions |
| Version control | Git, GitHub pull requests |

## Getting Started

### Prerequisites

- Python 3.14
- Docker Desktop with Docker Compose
- An Alpaca account and market-data API credentials **for live ingestion**

Kafka and synthetic-message integration tests do not require Alpaca credentials.

### 1. Clone the Repository

```bash
git clone https://github.com/willmarkevitch/real-time-market-data-platform.git
cd real-time-market-data-platform
```

### 2. Create and Activate a Virtual Environment

On Windows using Git Bash:

```bash
python -m venv venv
source venv/Scripts/activate
```

On macOS/Linux:

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
python -m pip install -r requirements-dev.txt
```

`requirements-dev.txt` includes the application dependencies from `requirements.txt` plus pytest.

### 4. Configure Alpaca Credentials (for Live Ingestion)

Copy `.env.example` to a local `.env` file and replace the placeholders with your credentials:

```dotenv
ALPACA_API_KEY=your_api_key_here
ALPACA_SECRET_KEY=your_secret_key_here
```

**Never commit your `.env` file or API credentials.** The repository's `.gitignore` excludes `.env`.

### 5. Start Kafka

```bash
docker compose up -d
```

Check container status:

```bash
docker compose ps
```

The `kafka-init` service creates the two required topics. If you need to rerun topic initialization explicitly:

```bash
docker compose run --rm kafka-init
```

To stop the environment without deleting persisted Kafka data:

```bash
docker compose down
```

Avoid `docker compose down -v` unless you intentionally want to delete the broker's persistent volume.

### 6. Verify the Pipeline with Tests

Run the fast unit-test suite (no Kafka broker required):

```bash
python -m pytest -m "not integration" -q
```

With Kafka running, run the integration tests:

```bash
python -m pytest -m integration -v
```

To run everything:

```bash
python -m pytest -v
```

The repository also contains a simulated trade-pipeline smoke script at `tests/integration/smoke_trade_pipeline.py` for development verification. The current implementation is composed of reusable ingestion and consumer components; a unified production application entry point is not yet provided.

## Testing and CI

At the current milestone, the project has **77 passing tests**:

| Suite | Tests | What it verifies |
| --- | ---: | --- |
| Unit tests | 75 | Models, validation, normalization, serialization, Kafka wrappers, processing, DLQ behavior, and shutdown logic |
| Kafka integration tests | 2 | Real broker message consumption, dead-letter delivery, source metadata, and committed offsets |

GitHub Actions runs the unit-test suite on pushes and pull requests using Python 3.14 on Ubuntu:

```bash
python -m pytest -m "not integration" -q
```

The Kafka integration tests currently run locally against Docker and are **not** part of CI. They use shared development topics; isolated test topics and automated Kafka-backed CI are planned improvements.

## Selected Engineering Decisions

**Why Kafka?** Kafka decouples the incoming market-data feed from downstream processing. It also supports replayable events, consumer groups, and partitioned processing.

**Why key messages by symbol?** Kafka sends messages with the same key to the same partition under the active partitioning scheme, preserving per-partition order for a symbol while allowing multiple symbols to be distributed across partitions.

**Why `Decimal`?** Decimal arithmetic avoids binary floating-point representation artifacts in financial values.

**Why manual offset commits?** Committing only after successful processing or confirmed DLQ delivery avoids acknowledging a record before the application has handled it. This supports at-least-once recovery semantics, though failures between side effects and commits can still cause duplicates.

**Why a dead-letter topic?** Invalid payloads remain available for inspection and reprocessing instead of being silently dropped or repeatedly blocking normal consumption.

**Why separate unit and integration tests?** Fast, deterministic tests can run on every pull request, while broker-backed tests verify real Kafka behavior without requiring external services in the basic CI job.

## Project Layout

```text
real-time-market-data-platform/
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   ├── config/
│   ├── ingestion/
│   │   ├── consumer_runner.py
│   │   ├── dlq_publisher.py
│   │   ├── kafka_consumer.py
│   │   ├── kafka_producer.py
│   │   ├── normalizer.py
│   │   ├── trade_deserializer.py
│   │   ├── trade_processor.py
│   │   ├── trade_serializer.py
│   │   └── websocket_client.py
│   └── models/
│       ├── market_trade.py
│       └── trade_validation.py
├── tests/
│   ├── integration/
│   │   └── test_kafka_consumer_integration.py
│   └── test_*.py
├── compose.yaml
├── pytest.ini
├── requirements.txt
├── requirements-dev.txt
├── .env.example
└── README.md
```

The diagram highlights key files rather than every module or test.

## Roadmap

### Next: Streaming Aggregation

- [ ] Aggregate trades into **one-minute OHLCV candles** by symbol.
- [ ] Calculate **volume-weighted average price (VWAP)**.
- [ ] Define event-time windowing behavior and tests for out-of-order events.
- [ ] Connect aggregation to the Kafka consumer processing path.

### Next: Persistence and Querying

- [ ] Persist trade-derived data in PostgreSQL.
- [ ] Design schemas and idempotent writes for at-least-once consumption.
- [ ] Support queries by symbol and time range.

### Later Improvements

- [ ] Isolate Kafka integration tests and run them in CI.
- [ ] Add a runnable application entry point and operational configuration.
- [ ] Improve structured logging, metrics, and shutdown handling.
- [ ] Track consumer lag, delivery failures, and processing latency.
- [ ] Explore a dashboard or API for querying recent market activity.
- [ ] Evaluate deployment and scaling options beyond local Docker.

## Development Approach

This project is built incrementally using **test-driven development**, feature branches, pull requests, and GitHub Actions. Each milestone focuses on a specific engineering concern—data contracts, message delivery, fault handling, and, next, stream processing and persistence.

The goal is to build an understandable, testable streaming system while documenting the tradeoffs involved in moving from a basic market-data API integration toward a more complete data platform.
