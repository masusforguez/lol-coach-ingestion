"""Bus de mensajes: interfaz común + implementación en memoria + implementación Kafka.

Por qué un bus entre las APIs y el lago (el problema de sincronización):
  * Las fuentes tienen ritmos y límites distintos (429, créditos, cuota diaria).
  * Si el productor es más rápido que la carga, o la carga falla, no se pierde nada:
    los mensajes esperan en el topic.
  * El consumidor confirma (commit) el offset SOLO cuando dlt ha cargado el lote.
    Si falla antes, los mensajes se vuelven a entregar -> semántica "al menos una vez".
    Los posibles duplicados se eliminan después por clave primaria (idempotencia).

InMemoryBus permite desarrollar y testear sin broker. KafkaBus usa confluent-kafka
(el cliente de Kafka que también usa el origen verificado de dlt) contra un broker
local que arranca scripts/kafka-start.ps1, sin contenedores.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Message:
    topic: str
    key: str | None
    value: dict
    offset: int
    partition: int = 0


class MessageBus(Protocol):
    def publish(self, topic: str, key: str | None, value: dict) -> None: ...
    def flush(self) -> None: ...
    def poll(self, topic: str, group: str, max_messages: int = 500, timeout: float = 1.0) -> list[Message]: ...
    def commit(self, group: str, messages: list[Message]) -> None: ...
    def abort(self, group: str, topic: str) -> None: ...
    def close(self) -> None: ...


class InMemoryBus:
    """Bus mínimo con offsets por grupo. Un mensaje no confirmado se vuelve a entregar."""

    def __init__(self):
        self.topics: dict[str, list[tuple[str | None, dict]]] = {}
        self.committed: dict[tuple[str, str], int] = {}

    def publish(self, topic, key, value):
        self.topics.setdefault(topic, []).append((key, json.loads(json.dumps(value))))  # copia profunda

    def flush(self):
        pass

    def poll(self, topic, group, max_messages=500, timeout=1.0):
        start = self.committed.get((group, topic), 0)
        log = self.topics.get(topic, [])
        return [Message(topic, k, v, start + n) for n, (k, v) in enumerate(log[start:start + max_messages])]

    def commit(self, group, messages):
        for m in messages:
            cur = self.committed.get((group, m.topic), 0)
            self.committed[(group, m.topic)] = max(cur, m.offset + 1)

    def abort(self, group, topic):
        pass   # poll() siempre lee desde el offset confirmado

    def lag(self, group: str, topic: str) -> int:
        return len(self.topics.get(topic, [])) - self.committed.get((group, topic), 0)

    def close(self):
        pass


class KafkaBus:
    """Implementación sobre Apache Kafka (confluent-kafka).

    Productor idempotente con acks=all. Consumidor SIN auto-commit: el commit lo
    hace el código tras cargar con dlt. Se necesita un broker (scripts/kafka-start.ps1).
    """

    def __init__(self, bootstrap_servers: str, client_id: str = "lol-ingestion"):
        from confluent_kafka import Producer
        self.bootstrap = bootstrap_servers
        self.client_id = client_id
        self.producer = Producer({"bootstrap.servers": bootstrap_servers, "client.id": client_id,
                                  "enable.idempotence": True, "acks": "all"})
        self._consumers: dict[tuple[str, str], object] = {}

    def publish(self, topic, key, value):
        self.producer.produce(topic, key=key, value=json.dumps(value).encode())
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    def _consumer(self, group: str, topic: str):
        from confluent_kafka import Consumer
        k = (group, topic)
        if k not in self._consumers:
            c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": group,
                          "enable.auto.commit": False, "auto.offset.reset": "earliest"})
            c.subscribe([topic])
            self._consumers[k] = c
        return self._consumers[k]

    def poll(self, topic, group, max_messages=500, timeout=1.0):
        from confluent_kafka import KafkaError, KafkaException
        out = []
        for msg in self._consumer(group, topic).consume(num_messages=max_messages, timeout=timeout):
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            out.append(Message(msg.topic(), msg.key().decode() if msg.key() else None,
                               json.loads(msg.value()), msg.offset(), msg.partition()))
        return out

    def commit(self, group, messages):
        from confluent_kafka import TopicPartition
        best: dict[tuple[str, int], int] = {}
        for m in messages:
            best[(m.topic, m.partition)] = max(best.get((m.topic, m.partition), -1), m.offset)
        by_topic: dict[str, list] = {}
        for (topic, part), off in best.items():
            by_topic.setdefault(topic, []).append(TopicPartition(topic, part, off + 1))
        for topic, tps in by_topic.items():
            self._consumer(group, topic).commit(offsets=tps, asynchronous=False)

    def abort(self, group, topic):
        """Tras un fallo: descartar el consumidor para releer desde el último offset confirmado."""
        c = self._consumers.pop((group, topic), None)
        if c is not None:
            c.close()

    def close(self):
        for c in self._consumers.values():
            c.close()
        self._consumers.clear()
        self.producer.flush()
