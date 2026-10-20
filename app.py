"""
Tienda de ejemplo: FastAPI + Kafka.

- El PRODUCTOR vive en el endpoint POST /compras: cada compra que llega
  por HTTP se publica en el topic `compras` de tu broker local.
- El CONSUMIDOR vive en segundo plano (una tarea de asyncio que arranca
  con la app): lee el topic sin parar y muestra cada mensaje, igual que
  kafka-console-consumer pero dentro de tu backend.

Requisitos: un broker Kafka corriendo en localhost:9092
(por ejemplo, el de mi repo kafka-desde-cero) y el topic `compras`.
"""

import asyncio
import json
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI

BROKER = "localhost:9092"          # tu broker del curso (KRaft, contenedor kafka-curso)
TOPIC = "compras"                  # el topic de la lección 2 (3 particiones)
GROUP_ID = "fastapi-lectores"      # grupo propio: no pelea con tus consumidores de consola

producer = None  # se crea una sola vez al arrancar la app


async def leer_compras_en_fondo():
    """Bucle infinito que lee del topic y muestra cada mensaje."""
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",  # como --from-beginning: lee desde el inicio
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                f"recibido | particion={msg.partition} offset={msg.offset} "
                f"clave={msg.key} valor={msg.value}"
            )
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    await producer.start()
    # El consumidor NO va dentro de un endpoint: es un bucle infinito,
    # así que se lanza como tarea de fondo al arrancar la app.
    tarea = asyncio.create_task(leer_compras_en_fondo())
    yield
    tarea.cancel()
    try:
        await tarea
    except asyncio.CancelledError:
        pass
    await producer.stop()


app = FastAPI(title="Tienda + Kafka", lifespan=lifespan)


@app.get("/")
def raiz():
    return {"mensaje": "Tienda de ejemplo. POST /compras para registrar una compra."}


@app.post("/compras")
async def crear_compra(compra: dict):
    """Recibe {"cliente": "cliente1", "producto": "camisa"} y lo publica en Kafka.

    La clave es el cliente, igual que en la lección 2: así las compras del
    mismo cliente caen siempre en la misma partición y llegan en orden.
    """
    await producer.send_and_wait(
        TOPIC,
        key=compra["cliente"].encode("utf-8"),
        value=json.dumps(compra).encode("utf-8"),
    )
    return {"ok": True, "topic": TOPIC}
