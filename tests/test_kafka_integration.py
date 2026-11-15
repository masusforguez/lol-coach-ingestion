"""Prueba contra un broker Kafka REAL. Se salta si no hay broker.
Uso:  docker compose up -d   y luego   KAFKA_BOOTSTRAP=localhost:9092 pytest tests/test_kafka_integration.py"""
import os
import uuid

import pytest

BOOT = os.getenv("KAFKA_BOOTSTRAP")
pytestmark = pytest.mark.skipif(not BOOT, reason="define KAFKA_BOOTSTRAP para probar contra un broker real")


def test_kafkabus_publish_poll_commit_redelivery():
    from ingestion.bus import KafkaBus
    topic, group = f"test.{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:8]}"
    bus = KafkaBus(BOOT)
    for i in range(5):
        bus.publish(topic, str(i), {"i": i})
    bus.flush()
    got = []
    for _ in range(10):                      # el primer poll puede tardar por el rebalanceo del grupo
        got += bus.poll(topic, group, max_messages=10, timeout=2.0)
        if len(got) >= 5:
            break
    assert [m.value["i"] for m in got[:5]] == [0, 1, 2, 3, 4]
    bus.commit(group, got[:3])               # solo se confirman 3
    bus.abort(group, topic)                  # simula reinicio del consumidor
    again = []
    for _ in range(10):
        again += bus.poll(topic, group, max_messages=10, timeout=2.0)
        if len(again) >= 2:
            break
    assert [m.value["i"] for m in again[:2]] == [3, 4]
    bus.close()
