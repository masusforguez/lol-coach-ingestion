"""APIs sintéticas que simulan fuentes con capa gratuita limitada o de pago.

Se despliegan como UN servicio web (p. ej. en render.com). Tres "fuentes":

  /riot/...      límite de peticiones por ventana  -> HTTP 429 + Retry-After
  /provider/...  créditos de pago que se agotan    -> HTTP 402
  /youtube/...   cuota diaria de unidades          -> HTTP 403 quotaExceeded

Además hay un reloj simulado (/admin/clock/advance) para que "pasen" horas y
aparezcan datos nuevos o revisados sin esperar de verdad: sirve para comprobar
la carga incremental.
"""
from __future__ import annotations

import math
import os
import random
import time
from dataclasses import dataclass, field
from typing import Callable

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from . import generator as g

DAY_MS = 24 * 3600 * 1000


@dataclass
class Settings:
    riot_token: str = os.getenv("RIOT_TOKEN", "dev-riot-key")
    provider_token: str = os.getenv("PROVIDER_TOKEN", "dev-provider-key")
    video_key: str = os.getenv("VIDEO_KEY", "dev-video-key")
    admin_token: str = os.getenv("ADMIN_TOKEN", "dev-admin")
    rate_limit: int = int(os.getenv("RATE_LIMIT", "120"))          # peticiones por ventana
    rate_window_s: float = float(os.getenv("RATE_WINDOW_S", "60"))
    failure_rate: float = float(os.getenv("FAILURE_RATE", "0"))    # 503 aleatorios
    total_matches: int = int(os.getenv("TOTAL_MATCHES", "3000"))
    start_matches: int = int(os.getenv("START_MATCHES", "400"))    # partidas ya disponibles al arrancar
    provider_credits: int = int(os.getenv("PROVIDER_CREDITS", "200"))
    video_daily_quota: int = int(os.getenv("VIDEO_DAILY_QUOTA", "500"))
    clock: Callable[[], float] = time.monotonic                    # inyectable en tests


class State:
    """Estado en memoria (se pierde al reiniciar el servicio: es un simulador)."""

    def __init__(self, s: Settings):
        self.s = s
        self.reset()

    def reset(self):
        self.sim_now_ms = g.BASE_TIME_MS + self.s.start_matches * g.MATCH_INTERVAL_MS + 2_500_000
        self.hits: dict[str, list[float]] = {}
        self.credits = self.s.provider_credits
        self.quota_used = 0
        self.quota_day = self.sim_now_ms // DAY_MS
        self.rng = random.Random(1234)

    # --- límite de peticiones por token (ventana deslizante)
    def check_rate(self, token: str):
        now = self.s.clock()
        window = [t for t in self.hits.get(token, []) if now - t < self.s.rate_window_s]
        if len(window) >= self.s.rate_limit:
            retry = max(1, math.ceil(self.s.rate_window_s - (now - window[0])))
            self.hits[token] = window
            raise HTTPException(429, detail="rate limit exceeded", headers={"Retry-After": str(retry)})
        window.append(now)
        self.hits[token] = window

    def maybe_fail(self):
        if self.s.failure_rate and self.rng.random() < self.s.failure_rate:
            raise HTTPException(503, detail="service unavailable (simulated)")

    def roll_quota_day(self):
        day = self.sim_now_ms // DAY_MS
        if day != self.quota_day:
            self.quota_day, self.quota_used = day, 0


def create_app(settings: Settings | None = None) -> FastAPI:
    s = settings or Settings()
    st = State(s)
    app = FastAPI(title="LoL coach - APIs sintéticas", version="1.0")
    app.state.settings, app.state.st = s, st

    # ------------------------------------------------------------- utilidades
    def visible_match_indexes() -> list[int]:
        """Partidas ya terminadas en el reloj simulado.
        OJO: no se puede parar en la primera sin terminar; la duración varía y una
        partida creada después puede terminar antes."""
        return [i for i in range(s.total_matches) if g.match_ended_at(i) <= st.sim_now_ms]

    def auth(expected: str, header: str, value: str | None):
        if value != expected:
            raise HTTPException(401, detail=f"missing or invalid {header}")

    # ------------------------------------------------------------- admin / salud
    @app.get("/health")
    def health():
        return {"status": "ok"}

    def admin(x_admin_token: str | None = Header(None)):
        auth(s.admin_token, "X-Admin-Token", x_admin_token)

    @app.get("/admin/state", dependencies=[Depends(admin)])
    def admin_state():
        return {"sim_now_ms": st.sim_now_ms, "visible_matches": len(visible_match_indexes()),
                "provider_credits": st.credits, "video_quota_used": st.quota_used}

    @app.post("/admin/clock/advance", dependencies=[Depends(admin)])
    def advance(minutes: int = Query(..., ge=1, le=7 * 24 * 60)):
        st.sim_now_ms += minutes * 60_000
        st.roll_quota_day()
        return {"sim_now_ms": st.sim_now_ms, "visible_matches": len(visible_match_indexes())}

    @app.post("/admin/reset", dependencies=[Depends(admin)])
    def admin_reset():
        st.reset()
        return {"status": "reset"}

    # ------------------------------------------------------------- RIOT (límite de peticiones)
    def riot_guard(x_riot_token: str | None = Header(None)):
        auth(s.riot_token, "X-Riot-Token", x_riot_token)
        st.check_rate(x_riot_token)
        st.maybe_fail()

    @app.get("/riot/matches", dependencies=[Depends(riot_guard)])
    def riot_matches(updated_after: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=100),
                     offset: int = Query(0, ge=0)):
        rows = sorted(((g.updated_at(i, st.sim_now_ms), i) for i in visible_match_indexes()
                       if g.updated_at(i, st.sim_now_ms) > updated_after))
        page = rows[offset: offset + limit]
        return {"total": len(rows), "items": [g.generate_match(i, st.sim_now_ms) for _, i in page]}

    @app.get("/riot/matches/{mid}/timeline", dependencies=[Depends(riot_guard)])
    def riot_timeline(mid: str):
        try:
            i = g.match_index(mid)
        except Exception:
            raise HTTPException(404, detail="unknown match")
        if i < 0 or i >= s.total_matches or g.match_ended_at(i) > st.sim_now_ms:
            raise HTTPException(404, detail="unknown match")
        return g.generate_timeline(i, st.sim_now_ms)

    # ------------------------------------------------------------- PROVEEDOR DE PAGO (créditos)
    def provider_guard(authorization: str | None = Header(None)):
        auth("Bearer " + s.provider_token, "Authorization", authorization)
        st.maybe_fail()
        if st.credits <= 0:
            raise HTTPException(402, detail="no credits left")
        st.credits -= 1

    @app.get("/provider/pro-matches", dependencies=[Depends(provider_guard)])
    def provider_matches(since: int = Query(0, ge=0), cursor: str | None = None,
                         page_size: int = Query(50, ge=1, le=100)):
        try:
            start = g.decode_token(cursor) if cursor else 0
        except ValueError:
            raise HTTPException(400, detail="bad cursor")
        ks = [k for k in range(10_000) if g.pro_ingested_at(k) <= st.sim_now_ms]
        ks = [k for k in ks if g.pro_ingested_at(k) > since]
        page = ks[start: start + page_size]
        nxt = g.encode_token(start + page_size) if start + page_size < len(ks) else None
        return {"items": [g.generate_pro_match(k) for k in page], "next_cursor": nxt,
                "credits_remaining": st.credits}

    # ------------------------------------------------------------- VÍDEO (cuota diaria, estilo YouTube)
    def video_guard(key: str | None = Query(None)):
        auth(s.video_key, "key", key)
        st.maybe_fail()
        st.roll_quota_day()
        if st.quota_used + 1 > s.video_daily_quota:
            raise HTTPException(403, detail="quotaExceeded")
        st.quota_used += 1

    @app.get("/youtube/videos", dependencies=[Depends(video_guard)])
    def videos(publishedAfter: int = Query(0, ge=0), pageToken: str | None = None,
               maxResults: int = Query(50, ge=1, le=50)):
        try:
            start = g.decode_token(pageToken) if pageToken else 0
        except ValueError:
            raise HTTPException(400, detail="bad pageToken")
        ks = [k for k in range(10_000) if g.video_published_ms(k) <= st.sim_now_ms]
        ks = [k for k in ks if g.video_published_ms(k) > publishedAfter]
        page = ks[start: start + maxResults]
        nxt = g.encode_token(start + maxResults) if start + maxResults < len(ks) else None
        visible = len(visible_match_indexes())
        return {"items": [g.generate_video(k, visible) for k in page], "nextPageToken": nxt}

    # ------------------------------------------------------------- errores con forma uniforme
    @app.exception_handler(HTTPException)
    async def http_exc(_: Request, exc: HTTPException):
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code,
                            headers=getattr(exc, "headers", None))

    return app


app = create_app()
