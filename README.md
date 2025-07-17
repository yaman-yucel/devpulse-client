┌────────────────────────────────────────────────────────────────────────┐
│ DevPulse System (Phase 1) │
├────────────────────────────────────────────────────────────────────────┤
│ CLIENT TIER │
│ ┌────────────┐ ┌─────────────┐ ┌─────────────┐ │
│ | Event Hooks|→→→|Normalizer |→→→|In‑Mem Queue | │
│ └────────────┘ └─────────────┘ └─────┬───────┘ │
│ Spill on backpressure │ │
│ ┌───────────────────────────▼────────┐ │
│ | Durable Local WAL (SQLite/AppendLog)| │
│ └───────────────────────────┬────────┘ │
│ │ │
│ Batch Flush / Retry Sender (HTTPS/gRPC) │
│ │ │
├─────────────────────────────────────────────────▼──────────────────────┤
│ EDGE / API TIER │
│ [Load Balancer / Reverse Proxy: Nginx/Envoy/Traefik] │
│ │ │
│ [Ingestion API: FastAPI or gRPC service] │
│ - Auth (bootstrap API key → exchange for token) │
│ - Rate / size limits │
│ - Batch endpoints (array payload) │
│ - Async write queue │
│ │ │
├─────────────────────────────────────────────────▼──────────────────────┤
│ SERVER DATA PIPELINE │
│ Option A (Simple @ this scale): │
│ FastAPI worker → batched COPY / bulk insert → PostgreSQL │
│ │
│ Option B (Future / scale): │
│ FastAPI enqueue → Message Broker (Kafka/RabbitMQ/Redpanda) → │
│ Consumer workers → Postgres (OLTP raw) + Data Lake/Warehouse │
│ │
├─────────────────────────────────────────────────▼──────────────────────┤
│ DATABASE & ANALYTICS LAYER │
│ PostgreSQL (partitioned) │
│ - Raw event tables (append only) │
│ - Rollup/summary tables (hour/day/user/app) │
│ - Materialized views / TimescaleDB ext (optional) │
│ PgBouncer for connection pooling │
│ ETL jobs → analytics warehouse / BI │
└────────────────────────────────────────────────────────────────────────┘

export PYTONPATH=./src
uv run -m devpulse_client
