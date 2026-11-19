"""Consumidor Kafka con offsets persistentes entre ejecuciones.

Carga en el lago todo lo que haya retenido en los topics y confirma cada lote
solo después de que dlt lo escriba. El grupo conserva sus offsets, así que las
siguientes ejecuciones continúan donde se quedaron.

Un grupo nuevo tarda en unirse (rebalanceo): sus primeros poll() vuelven vacíos
aunque haya mensajes. LakeConsumer.drain() ya lo tiene en cuenta.
"""
import os

from ingestion.bus import KafkaBus
from ingestion.consumer import LakeConsumer

BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
LAKE = "data/lake_kafka"
STATE = "data/state_kafka"
GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "lake-loader-kafka")

bus = KafkaBus(BOOT)
consumer = LakeConsumer(bus, LAKE, STATE, group=GROUP)

try:
    total = consumer.drain(max_messages=500)
finally:
    bus.close()

for topic, n in total.items():
    print(f"{topic}: {n}")
print("TOTAL:", total)
