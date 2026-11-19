"""Productor ("fetcher"): consulta las APIs respetando sus límites y publica en Kafka.

Publica el dato EN BRUTO tal como llega (principio EL). No carga nada en el lago:
eso lo hace el consumidor. Así, un límite de la API o un fallo de carga no se
propagan de una etapa a la otra.

Estado propio (cursores + timelines pendientes) en un JSON pequeño. Si el proceso
muere entre publicar y guardar el estado, se vuelve a publicar: duplicados
posibles, eliminados después por clave primaria.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import clients
from .bus import MessageBus
from .clients import QuotaExhausted, log
from .pipeline import Endpoints

TOPIC_MATCHES = "lol.raw.matches"
TOPIC_TIMELINES = "lol.raw.timelines"
TOPIC_PRO = "lol.raw.pro_matches"
TOPIC_VIDEOS = "lol.raw.videos"
ALL_TOPICS = [TOPIC_MATCHES, TOPIC_TIMELINES, TOPIC_PRO, TOPIC_VIDEOS]


class Fetcher:
    def __init__(self, ep: Endpoints, bus: MessageBus, state_path, page_size: int = 50,
                 max_timelines: int | None = None):
        self.ep, self.bus, self.page_size, self.max_timelines = ep, bus, page_size, max_timelines
        self.path = Path(state_path)
        self.state = {"matches": 0, "matches_id": "", "pro": 0, "video": 0, "pending_timelines": []}
        if self.path.exists():
            self.state.update(json.loads(self.path.read_text()))

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.state))

    # ---------------------------------------------------------------- matches
    def fetch_matches(self) -> int:
        n = 0
        try:
            for page in clients.riot_matches_pages(self.ep.base_url, self.ep.riot_token,
                                                   self.state["matches"], self.page_size,
                                                   self.state["matches_id"]):
                for m in page:
                    self.bus.publish(TOPIC_MATCHES, m["match_id"], m)
                    if m["match_id"] not in self.state["pending_timelines"]:
                        self.state["pending_timelines"].append(m["match_id"])
                    n += 1
                self.bus.flush()
                last = page[-1]
                self.state["matches"] = last["updated_at"]
                self.state["matches_id"] = last["match_id"]
                self._save()          # el cursor avanza solo tras publicar la página
        except QuotaExhausted as e:
            log.warning("riot: %s", e)
        return n

    # ---------------------------------------------------------------- timelines (1 petición por partida)
    def fetch_timelines(self) -> int:
        """Cada timeline cuesta una petición: se hace con presupuesto. Lo que no cabe
        queda pendiente para la siguiente pasada (las fuentes van a ritmos distintos)."""
        n = 0
        pending = self.state["pending_timelines"]
        while pending and (self.max_timelines is None or n < self.max_timelines):
            mid = pending[0]
            try:
                raw = clients.riot_timeline(self.ep.base_url, self.ep.riot_token, mid)
            except QuotaExhausted as e:
                log.warning("riot: %s", e)
                break
            pending.pop(0)
            if raw is not None:
                self.bus.publish(TOPIC_TIMELINES, mid, raw)
                n += 1
            self.bus.flush()
            self._save()
        return n

    # ---------------------------------------------------------------- proveedor y vídeo
    def fetch_pro(self) -> int:
        n = 0
        try:
            for page in clients.provider_pages(self.ep.base_url, self.ep.provider_token,
                                               self.state["pro"], self.page_size):
                for m in page:
                    self.bus.publish(TOPIC_PRO, m["provider_match_id"], m)
                    n += 1
                self.bus.flush()
                self.state["pro"] = max(self.state["pro"], max(m["ingested_at"] for m in page))
                self._save()
        except QuotaExhausted as e:
            log.warning("provider: %s", e)
        return n

    def fetch_videos(self) -> int:
        n = 0
        try:
            for page in clients.video_pages(self.ep.base_url, self.ep.video_key,
                                            self.state["video"], self.page_size):
                for v in page:
                    self.bus.publish(TOPIC_VIDEOS, v["video_id"], v)
                    n += 1
                self.bus.flush()
                self.state["video"] = max(self.state["video"], max(v["published_ms"] for v in page))
                self._save()
        except QuotaExhausted as e:
            log.warning("video: %s", e)
        return n

    def run_once(self) -> dict:
        return {"matches": self.fetch_matches(), "timelines": self.fetch_timelines(),
                "pro_matches": self.fetch_pro(), "videos": self.fetch_videos()}
