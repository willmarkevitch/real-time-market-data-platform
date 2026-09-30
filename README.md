# Real-Time Market Data Platform

A real-time event-driven data pipeline for ingesting, validating, normalizing, and streaming U.S. equity market data from Alpaca through Apache Kafka.

The project is designed to explore production-oriented streaming data engineering concepts including WebSocket ingestion, typed domain models, event serialization, Kafka producers, partitioned event streams, fault handling, containerized infrastructure, and automated testing.

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
  - [Alpaca Market Data Integration](#alpaca-market-data-integration)
  - [Typed Trade Model](#typed-trade-model)
  - [Financial Data Precision](#financial-data-precision)
- [Real-Time Streaming](#real-time-streaming)
- [Apache Kafka](#apache-kafka)
- [Kafka Infrastructure](#kafka-infrastructure)
- [Kafka Producer](#kafka-producer)
- [Verified End-to-End Flow](#verified-end-to-end-flow)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Start Kafka](#start-kafka)
- [Running Tests](#running-tests)
- [Current Status](#current-status)
- [Roadmap](#roadmap)
  - [Kafka Consumer](#kafka-consumer)
  - [Persistent Trade Storage](#persistent-trade-storage)
  - [Stream Processing](#stream-processing)
  - [Interactive Dashboard](#interactive-dashboard)
  - [Observability](#observability)
  - [Workflow and Infrastructure Improvements](#workflow-and-infrastructure-improvements)
- [Future Architecture](#future-architecture)
- [Technologies](#technologies)
- [Why This Project?](#why-this-project)

---

## Architecture

```text
                  Alpaca Market Data
                         │
                    WebSocket
                         │
                         ▼
                 Streaming Client
                         │
                         ▼
                    Normalizer
                         │
                         ▼
                    MarketTrade
                   typed data model
                         │
                         ▼
                  JSON Serializer
                         │
                         ▼
                   Kafka Producer
                         │
                         ▼
                ┌─────────────────┐
                │  market.trades  │
                │   Kafka Topic   │
                └─────────────────┘
                         │
                         ▼
                 Kafka Consumers
                    [roadmap]
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
         Persistence           Live Analytics
          [roadmap]              [roadmap]
```

The currently implemented streaming path is:

```text
Alpaca WebSocket
      ↓
Trade normalization
      ↓
MarketTrade
      ↓
JSON serialization
      ↓
Kafka producer
      ↓
market.trades
```

---

# Features

## Alpaca Market Data Integration

The project integrates with Alpaca's market-data APIs for equity market data.

API credentials are loaded from environment variables rather than being stored in source control.

Expected configuration includes:

```text
ALPACA_API_KEY
ALPACA_SECRET_KEY
```

The project has worked with both historical/latest market-data requests and live WebSocket-based ingestion as the streaming architecture has developed.

---

## Typed Trade Model

Raw provider-specific events are normalized into an internal `MarketTrade` model.

Conceptually:

```python
MarketTrade(
    symbol="AAPL",
    price=Decimal("..."),
    size=...,
    timestamp=...,
    exchange="..."
)
```

Using an internal model prevents downstream components from depending directly on Alpaca's SDK objects.

This creates a boundary between:

```text
External provider format
        ↓
Internal domain model
        ↓
Downstream systems
```

That separation also makes it easier to support another market-data provider in the future.

---

## Financial Data Precision

Trade prices use Python's `Decimal` rather than binary floating-point values.

This avoids unnecessary floating-point representation issues when handling monetary values.

Trade normalization also validates incoming data before it can continue through the pipeline.

Validation includes conditions such as:

- Positive prices
- Positive trade sizes
- Finite numeric values
- Integer share sizes
- Required trade metadata

Invalid provider data is rejected rather than silently entering the streaming pipeline.

---

# Real-Time Streaming

The streaming layer consumes market events from Alpaca's WebSocket feed.

Incoming trades pass through the normalization layer before being published downstream.

```text
Alpaca Trade
     │
     ▼
handle_trade()
     │
     ▼
normalize_alpaca_trade()
     │
     ▼
MarketTrade
     │
     ▼
serialize
     │
     ▼
Kafka
```

Keeping normalization between the external WebSocket and Kafka ensures downstream consumers receive a consistent event format.

---

# Apache Kafka

Kafka provides the event backbone for the platform.

The development Kafka environment runs in Docker using Kafka's KRaft architecture.

The primary topic is:

```text
market.trades
```

Current development configuration includes:

```text
Partitions:         3
Replication factor: 1
```

Three partitions allow the project to demonstrate partitioned event processing while remaining lightweight enough for local development.

Trade messages use the stock symbol as the Kafka message key.

For example:

```text
Key:   AAPL

Value:
{
    "symbol": "AAPL",
    "price": "...",
    "size": 100,
    "timestamp": "...",
    "exchange": "..."
}
```

Keying events by symbol provides a natural strategy for maintaining ordering for trades belonging to the same symbol while allowing different symbols to be distributed across partitions.

---

# Kafka Infrastructure

Kafka runs through Docker Compose.

The development environment uses separate internal and external listeners so both containers and applications running on the host machine can communicate with the broker.

Conceptually:

```text
Python application
       │
       │ localhost
       ▼
127.0.0.1:9092
       │
       ▼
     Kafka
       ▲
       │
       │ Docker network
       │
   kafka:9094
```

A Kafka initialization service ensures the required topic exists when the development environment starts.

Kafka data is stored in a persistent Docker volume so stopping the containers does not automatically destroy broker state.

---

# Kafka Producer

Normalized trades are serialized to JSON and published using `confluent-kafka`.

The producer includes:

- JSON serialization
- Symbol-based message keys
- Delivery callbacks
- Delivery-failure tracking
- Graceful flushing during shutdown
- Testable producer abstractions

The pipeline therefore separates responsibilities:

```text
WebSocket Client
      ↓
Normalizer
      ↓
Serializer
      ↓
Producer
      ↓
Kafka Broker
```

rather than combining all streaming behavior into one component.

---

# Verified End-to-End Flow

The Kafka pipeline has been verified using a simulated trade.

The test path successfully executed:

```text
Simulated trade
      ↓
handle_trade()
      ↓
Normalizer
      ↓
Serializer
      ↓
Kafka producer
      ↓
market.trades
      ↓
Kafka consumer
```

The consumer successfully received the keyed JSON event from Kafka, demonstrating that the complete application-to-broker path works.

---

# Project Structure

The repository follows a modular source layout similar to:

```text
real-time-market-data-platform/
│
├── src/
│   ├── config/
│   ├── ingestion/
│   ├── models/
│   └── streaming/
│
├── tests/
│
├── compose.yaml
├── requirements.txt
├── requirements-dev.txt
├── .env.example
└── README.md
```

Responsibilities are intentionally separated between:

```text
configuration
     ↓
external ingestion
     ↓
domain models
     ↓
normalization
     ↓
serialization
     ↓
streaming infrastructure
```

---

# Setup

## 1. Clone the repository

```bash
git clone https://github.com/willmarkevitch/real-time-market-data-platform.git
cd real-time-market-data-platform
```

## 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it on Windows:

```bash
source venv/Scripts/activate
```

or:

```powershell
.\venv\Scripts\Activate.ps1
```

## 3. Install dependencies

```bash
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

## 4. Configure Alpaca credentials

Create a local `.env` file based on `.env.example`.

```text
ALPACA_API_KEY=your_api_key
ALPACA_SECRET_KEY=your_secret_key
```

Do not commit `.env`.

The repository's `.gitignore` prevents local credentials and virtual-environment files from being tracked.

---

# Start Kafka

Start the local streaming infrastructure:

```bash
docker compose up -d
```

Check its status:

```bash
docker compose ps
```

The initialization process ensures that:

```text
market.trades
```

exists before the application begins publishing events.

Kafka state is stored in a persistent named Docker volume.

Stopping the environment with:

```bash
docker compose down
```

stops the containers while preserving the Kafka volume.

Start it again later with:

```bash
docker compose up -d
```

---

# Running Tests

Run the complete automated test suite:

```bash
python -m pytest -v
```

Tests cover areas including:

- `MarketTrade` model behavior
- Alpaca trade normalization
- Invalid prices
- Invalid trade sizes
- Fractional sizes
- Zero and negative values
- NaN and infinite values
- Serialization
- WebSocket behavior
- Streaming reliability
- Kafka producer behavior
- Delivery failures
- Shutdown behavior

At the Kafka-producer milestone, the project contained **41 passing automated tests**.

---

# Current Status

The core producer-side streaming architecture is implemented:

```text
Alpaca
   ↓
WebSocket ingestion
   ↓
Normalization
   ↓
Typed MarketTrade
   ↓
JSON serialization
   ↓
Kafka producer
   ↓
market.trades
```

Completed components include:

- Alpaca configuration
- API credential management
- Typed `MarketTrade` model
- Trade normalization
- Financial-value validation
- WebSocket ingestion architecture
- Streaming reliability behavior
- Kafka Docker infrastructure
- Persistent Kafka storage
- Automatic topic initialization
- JSON event serialization
- Kafka producer
- Delivery callbacks
- Producer failure tracking
- Graceful producer shutdown
- Automated unit tests
- Simulated end-to-end Kafka verification

The next major architectural step is building the **consumer side** of the platform.

---

# Roadmap

## Kafka Consumer

Implement a dedicated consumer for:

```text
market.trades
```

The consumer will deserialize events and provide the boundary between the streaming system and downstream storage or analytics.

Target architecture:

```text
                    market.trades
                          │
                          ▼
                    Kafka Consumer
                          │
                 ┌────────┴────────┐
                 ▼                 ▼
             Database          Analytics
```

## Persistent Trade Storage

Add a database layer so streamed events can be queried after they leave Kafka.

This will enable questions such as:

```text
Latest trade for AAPL
Trades during a time range
Average price over a window
Trade volume by symbol
Recent market activity
```

## Stream Processing

Introduce rolling or windowed calculations such as:

- Trade count per minute
- Volume per minute
- Rolling average price
- Price changes
- High/low values
- Per-symbol event throughput

## Interactive Dashboard

Build a Streamlit dashboard where a user can select a stock ticker and view live or recently persisted market data.

Potential interface:

```text
Ticker: [ AAPL ▼ ]

Current price
Recent trades
Price history
Volume
Streaming status
```

## Observability

Add operational metrics for the pipeline itself rather than only the financial data.

Examples include:

- Events received per second
- Events published per second
- Producer failures
- Consumer lag
- WebSocket reconnects
- Processing latency
- Kafka availability

These metrics could eventually be visualized through tools such as Grafana.

## Workflow and Infrastructure Improvements

Longer-term improvements may include:

- Structured logging
- Schema evolution/versioning
- Dead-letter handling
- Consumer retry behavior
- PostgreSQL or time-series persistence
- CI/CD
- Integration testing against Kafka
- Cloud deployment
- Multiple consumers
- Additional market-data event types

---

# Future Architecture

The longer-term platform is intended to evolve toward:

```text
                         Alpaca
                           │
                      WebSocket
                           │
                           ▼
                       Ingestion
                           │
                           ▼
                      Normalization
                           │
                           ▼
                    Kafka Producer
                           │
                           ▼
                    market.trades
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
         Persistence    Analytics    Monitoring
          Consumer      Consumer      Consumer
              │            │            │
              ▼            ▼            ▼
          PostgreSQL    Streamlit      Metrics
              │                         │
              ▼                         ▼
       Historical API                Grafana
```

Kafka allows multiple downstream systems to consume the same trade stream independently without coupling them directly to the Alpaca WebSocket connection.

---

# Technologies

**Language**

Python

**Market Data**

Alpaca API / WebSocket

**Streaming**

Apache Kafka

**Kafka Client**

confluent-kafka

**Infrastructure**

Docker / Docker Compose

**Testing**

pytest

**Data Formats**

JSON

**Engineering Concepts**

Real-Time Data • Event-Driven Architecture • WebSockets • Kafka • Producers • Consumers • Partitioning • Serialization • Data Validation • Domain Models • Financial Data Precision • Fault Handling • Containerization • Automated Testing

---

# Why This Project?

The goal of this project is to go beyond simply requesting stock prices from an API.

The project demonstrates how a basic market-data application can evolve from:

```text
API → Python
```

into a real-time event-driven data platform:

```text
WebSocket
    ↓
Validation
    ↓
Domain Model
    ↓
Kafka
    ↓
Consumers
    ↓
Persistent Storage
    ↓
Analytics
    ↓
Observability
```

The architecture intentionally separates ingestion, normalization, transport, storage, and analytics so each component can evolve independently.

Development is performed incrementally using feature branches, pull requests, automated testing, and small architectural milestones to mirror a professional software and data engineering workflow.
