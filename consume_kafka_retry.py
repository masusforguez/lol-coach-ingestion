"""Consumidor Kafka con reintentos y offsets persistentes entre ejecuciones."""
import os
import time

from ingestion.bus import KafkaBus
from ingestion.consumer import LakeConsumer

BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
LAKE = "data/lake_kafka"
STATE = "data/state_kafka"
GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "lake-loader-kafka")

bus = KafkaBus(BOOT)
consumer = LakeConsumer(bus, LAKE, STATE, group=GROUP)
total: dict[str, int] = {}

try:
    for attempt in range(1, 11):
        got = consumer.consume_once(max_messages=500)
        if got:
            print(f"Intento {attempt}: {got}", flush=True)
            for topic, n in got.items():
                total[topic] = total.get(topic, 0) + n
        else:
            print(f"Intento {attempt}: sin mensajes", flush=True)
        if attempt < 10:
            time.sleep(2)
finally:
    bus.close()

print("TOTAL:", total)