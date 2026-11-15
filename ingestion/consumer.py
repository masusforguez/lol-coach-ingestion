"""Consumidor: lee de los topics y carga en el lago con dlt.

Regla clave: el offset se confirma (commit) SOLO si dlt ha cargado el lote sin
error. Si algo falla, no se confirma y el lote se vuelve a leer (al menos una vez).
"""
from __future__ import annotations

import dlt

from .bus import Message, MessageBus
from .pipeline import make_pipeline
from .producer import TOPIC_MATCHES, TOPIC_PRO, TOPIC_TIMELINES, TOPIC_VIDEOS
from .transform import timeline_rows


def _matches(rows):
    @dlt.resource(name="matches", primary_key="match_id", write_disposition="merge")
    def r():
        yield rows
    return r


def _timelines(rows):
    @dlt.resource(name="timelines", write_disposition="append")
    def r():
        for raw in rows:
            for table, row in timeline_rows(raw):
                yield dlt.mark.with_table_name(row, table)
    return r


def _pro(rows):
    @dlt.resource(name="pro_matches", primary_key="provider_match_id", write_disposition="merge")
    def r():
        yield rows
    return r


def _videos(rows):
    @dlt.resource(name="videos", primary_key="video_id", write_disposition="merge")
    def r():
        yield rows
    return r


# topic -> (dataset del lago, constructor del resource)
ROUTES = {
    TOPIC_MATCHES: ("riot", _matches),
    TOPIC_TIMELINES: ("riot", _timelines),
    TOPIC_PRO: ("provider", _pro),
    TOPIC_VIDEOS: ("video", _videos),
}


class LakeConsumer:
    def __init__(self, bus: MessageBus, lake_dir, pipelines_dir: str | None = None,
                 group: str = "lake-loader"):
        self.bus, self.lake_dir, self.pipelines_dir, self.group = bus, lake_dir, pipelines_dir, group

    def _load(self, topic: str, messages: list[Message]):
        dataset, build = ROUTES[topic]
        p = make_pipeline(f"lol_{topic.split('.')[-1]}_kafka", self.lake_dir, dataset, self.pipelines_dir)
        p.run(build([m.value for m in messages]), loader_file_format="parquet")

    def consume_once(self, max_messages: int = 500) -> dict[str, int]:
        """Una pasada por todos los topics. Devuelve mensajes cargados por topic."""
        loaded = {}
        for topic in ROUTES:
            msgs = self.bus.poll(topic, self.group, max_messages=max_messages, timeout=1.0)
            if not msgs:
                continue
            try:
                self._load(topic, msgs)
            except Exception:
                self.bus.abort(self.group, topic)      # releer desde el último offset confirmado
                raise
            self.bus.commit(self.group, msgs)          # solo tras una carga correcta
            loaded[topic] = len(msgs)
        return loaded

    def drain(self, max_messages: int = 500) -> dict[str, int]:
        """Consume hasta vaciar los topics."""
        total: dict[str, int] = {}
        while True:
            got = self.consume_once(max_messages)
            if not got:
                return total
            for k, v in got.items():
                total[k] = total.get(k, 0) + v
