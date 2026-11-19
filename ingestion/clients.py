"""Clientes HTTP de las tres fuentes. Se comparten entre el modo directo (dlt
consulta la API) y el modo Kafka (el productor consulta la API).

Usan el cliente de dlt (dlt.sources.helpers.requests): reintenta solo ante
429 (respetando Retry-After) y errores 5xx, con espera creciente.
Los errores de cuota/crédito NO se reintentan: se convierten en QuotaExhausted
para parar con elegancia y continuar en la siguiente ejecución.
"""
from __future__ import annotations

import logging
from typing import Iterator

from dlt.sources.helpers import requests

log = logging.getLogger("ingestion")


class QuotaExhausted(Exception):
    """La fuente se ha quedado sin créditos o cuota. No es un fallo del pipeline."""


def _get(url: str, **kw):
    """GET con reintentos de dlt. 402/403-cuota -> QuotaExhausted; resto de 4xx -> HTTPError."""
    try:
        return requests.get(url, timeout=30, **kw)
    except requests.HTTPError as e:
        resp = e.response
        if resp is not None and resp.status_code == 402:
            raise QuotaExhausted("proveedor: créditos agotados (402)") from e
        if resp is not None and resp.status_code == 403 and "quotaExceeded" in resp.text:
            raise QuotaExhausted("vídeo: cuota diaria agotada (403 quotaExceeded)") from e
        raise


# ------------------------------------------------------------------ Riot
def riot_matches_pages(base_url: str, token: str, updated_after: int, page_size: int = 50,
                      match_id_after: str | None = None) -> Iterator[list]:
    """Pagina por offset, con cursor compuesto para reanudar entre marcas empatadas."""
    offset = 0
    while True:
        params = {"updated_after": updated_after, "limit": page_size, "offset": offset}
        if match_id_after is not None:
            params["match_id_after"] = match_id_after
        r = _get(f"{base_url}/riot/matches", headers={"X-Riot-Token": token},
                 params=params)
        items = r.json()["items"]
        if not items:
            return
        yield items
        offset += len(items)


def riot_timeline(base_url: str, token: str, match_id: str) -> dict | None:
    try:
        return _get(f"{base_url}/riot/matches/{match_id}/timeline",
                    headers={"X-Riot-Token": token}).json()
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return None
        raise


# ------------------------------------------------------------------ Proveedor de pago
def provider_pages(base_url: str, token: str, since: int, page_size: int = 50) -> Iterator[list]:
    """Pagina por cursor opaco (next_cursor)."""
    cursor = None
    while True:
        params = {"since": since, "page_size": page_size}
        if cursor:
            params["cursor"] = cursor
        body = _get(f"{base_url}/provider/pro-matches", headers={"Authorization": f"Bearer {token}"},
                    params=params).json()
        if body["items"]:
            yield body["items"]
        cursor = body.get("next_cursor")
        if not cursor:
            return


# ------------------------------------------------------------------ Vídeo
def video_pages(base_url: str, key: str, published_after: int, page_size: int = 50) -> Iterator[list]:
    token = None
    while True:
        params = {"key": key, "publishedAfter": published_after, "maxResults": page_size}
        if token:
            params["pageToken"] = token
        body = _get(f"{base_url}/youtube/videos", params=params).json()
        if body["items"]:
            yield body["items"]
        token = body.get("nextPageToken")
        if not token:
            return
