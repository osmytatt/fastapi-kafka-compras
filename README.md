# FastAPI + Kafka — event-driven mini store

A small example app that connects a FastAPI HTTP API with Kafka: every purchase received over HTTP is published to a Kafka topic, and a background consumer processes it — the same pattern used in real microservices.

## How it works

- **Producer** lives in `POST /compras`: each purchase is published to the `compras` topic, keyed by client
- **Consumer** runs as a background `asyncio` task started with the app lifespan: it reads the topic continuously, like `kafka-console-consumer` but inside your backend
- Messages are **keyed by client**, so all purchases from the same client land in the same partition and keep their order

## Run it

Requirements: a Kafka broker on `localhost:9092` with a `compras` topic (see my `kafka-desde-cero` repo for a one-command setup).

```bash
pip install -r requirements.txt
uvicorn app:app --reload
```

Then:

```bash
curl -X POST localhost:8000/compras \
  -H "Content-Type: application/json" \
  -d '{"cliente": "cliente1", "producto": "camisa"}'
```

Watch the terminal: the background consumer prints each message with its partition, offset and key.

## Stack

FastAPI · `aiokafka` (async producer/consumer) · Kafka
