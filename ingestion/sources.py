"""Fuentes dlt (modo directo): dlt consulta cada API con carga incremental.

Patrón de los apuntes: @dlt.resource con primary_key + write_disposition="merge"
y un cursor dlt.sources.incremental. El cursor evita volver a pedir lo ya cargado;
la clave primaria evita duplicados lógicos.
"""
from __future__ import annotations

import dlt

from . import clients
from .clients import QuotaExhausted, log
from .transform import timeline_rows


@dlt.source(name="riot")
def riot_source(base_url: str, token: str, page_size: int = 50,
                with_timelines: bool = True, max_timelines: int | None = None):
    """Partidas (con participantes anidados) y, opcionalmente, su timeline."""

    @dlt.resource(name="matches", primary_key="match_id", write_disposition="merge")
    def matches(updated_at=dlt.sources.incremental("updated_at", initial_value=0)):
        # El cursor es updated_at (no game_creation) para capturar también las REVISIONES.
        try:
            yield from clients.riot_matches_pages(base_url, token, updated_at.start_value, page_size)
        except QuotaExhausted as e:
            log.warning("riot: %s; se continúa en la próxima ejecución", e)

    out = [matches]
    if with_timelines:
        budget = {"left": max_timelines}

        @dlt.transformer(data_from=matches, name="timelines", write_disposition="append")
        def timelines(page):
            for m in page:
                if budget["left"] is not None:
                    if budget["left"] <= 0:
                        return
                    budget["left"] -= 1
                raw = clients.riot_timeline(base_url, token, m["match_id"])
                if raw is None:
                    continue
                for table, row in timeline_rows(raw):
                    yield dlt.mark.with_table_name(row, table)

        out.append(timelines)
    return out


@dlt.source(name="provider")
def provider_source(base_url: str, token: str, page_size: int = 50):
    @dlt.resource(name="pro_matches", primary_key="provider_match_id", write_disposition="merge")
    def pro_matches(ingested_at=dlt.sources.incremental("ingested_at", initial_value=0)):
        try:
            yield from clients.provider_pages(base_url, token, ingested_at.start_value, page_size)
        except QuotaExhausted as e:
            log.warning("provider: %s; se continúa en la próxima ejecución", e)

    return pro_matches


@dlt.source(name="video")
def video_source(base_url: str, key: str, page_size: int = 50):
    @dlt.resource(name="videos", primary_key="video_id", write_disposition="merge")
    def videos(published_ms=dlt.sources.incremental("published_ms", initial_value=0)):
        try:
            yield from clients.video_pages(base_url, key, published_ms.start_value, page_size)
        except QuotaExhausted as e:
            log.warning("video: %s; se continúa en la próxima ejecución", e)

    return videos
