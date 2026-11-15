"""Fixtures compartidas. Arranca la API sintética en un puerto local real,
porque dlt usa 'requests' contra una URL de verdad (igual que en producción)."""
import socket
import threading
import time

import pytest
import uvicorn

from synthetic_api.main import Settings, create_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LiveApi:
    def __init__(self, **overrides):
        base = dict(rate_limit=100_000, rate_window_s=60, failure_rate=0.0,
                    total_matches=400, start_matches=120, provider_credits=1000,
                    video_daily_quota=10_000)
        base.update(overrides)
        self.settings = Settings(**base)
        self.app = create_app(self.settings)
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        cfg = uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="error")
        self.server = uvicorn.Server(cfg)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self):
        self.thread.start()
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("la API de pruebas no arrancó")

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=5)

    def state(self) -> dict:
        import requests
        r = requests.get(f"{self.url}/admin/state", headers={"X-Admin-Token": self.settings.admin_token})
        r.raise_for_status()
        return r.json()

    def advance(self, minutes: int):
        import requests
        r = requests.post(f"{self.url}/admin/clock/advance", params={"minutes": minutes},
                          headers={"X-Admin-Token": self.settings.admin_token})
        r.raise_for_status()
        return r.json()


@pytest.fixture
def make_api():
    started = []

    def _make(**kw):
        api = LiveApi(**kw).start()
        started.append(api)
        return api

    yield _make
    for a in started:
        a.stop()


@pytest.fixture
def api(make_api):
    return make_api()
