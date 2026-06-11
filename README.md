# PulseQueue 🚀

A production-grade distributed message queue system built with FastAPI, PostgreSQL, and Redis — supporting topic-based pub/sub, consumer groups, idempotent publishing, exponential backoff retries, and dead letter queues.
## Architecture

```
┌──────────┐     ┌─────────────────┐     ┌──────────────────┐
│ Producer │────▶│   FastAPI API   │────▶│   PostgreSQL     │
└──────────┘     └────────┬────────┘     │                  │
                          │              │  topics          │
┌──────────┐              │              │  messages        │
│ Consumer │────▶         │              │  consumer_groups │
└──────────┘              │              │  offsets         │
                          ▼              │  dead_letters    │
                  ┌───────────────┐      └──────────────────┘
                  │     Redis     │
                  │               │
                  │  topic cache  │
                  │  group cache  │
                  │ offset cache  │
                  └───────────────┘
```


## Features

- **Topic-based Pub/Sub** — Create topics with configurable partitions
- **Idempotent Publishing** — Duplicate messages rejected via idempotency key
- **Consumer Groups** — Multiple independent consumers per topic
- **SKIP LOCKED** — Concurrent-safe message dequeue (no double processing)
- **Redis Cache-Aside** — Hot path optimization for topic/group/offset lookups
- **Exponential Backoff Retry** — Failed messages retry at 2s, 4s intervals
- **Dead Letter Queue** — Messages exceeding 3 retries moved to DLQ
- **Docker Compose** — One command spins up FastAPI + PostgreSQL + Redis

## Tech Stack

| Layer | Technology |
|-------|------------|
| API | FastAPI + uvicorn |
| Database | PostgreSQL 15 |
| Cache | Redis 7 |
| Driver | asyncpg |
| Containerization | Docker Compose |

## Quick Start

```bash
git clone https://github.com/Vedikajadhav/pulsequeue.git
cd pulsequeue
docker compose up --build
```

API → http://localhost:8001  
Swagger Docs → http://localhost:8001/docs

## API Reference

### Create Topic
```bash
POST /api/v1/topics
{"name": "payments", "partition_count": 3}
```

### Publish Message
```bash
POST /api/v1/topics/{name}/publish
{"payload": {"event": "payment_created"}, "idempotency_key": "pay-001"}
```

### Consume Messages
```bash
POST /api/v1/topics/{name}/consume
{"group_name": "payments-service", "max_messages": 10}
```

### Acknowledge Message
```bash
POST /api/v1/messages/ack
{"message_id": "uuid", "success": true}
```

## Retry Flow


Message fails ──▶ retry_count + 1
│
┌─────────┴──────────┐
│                    │
count < 3             count >= 3
│                    │
retry_after = NOW()      status = failed
+ 2^n seconds            dead_letters table


## Database Schema

| Table | Purpose |
|-------|---------|
| topics | Topic registry with partition config |
| messages | Message store with status tracking |
| consumer_groups | Independent consumer group registry |
| offsets | Per-partition offset tracking |
| dead_letters | Failed messages after max retries |