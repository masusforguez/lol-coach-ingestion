import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

from synthetic_api.main import Settings, create_app


def local_client():
    return TestClient(
        create_app(Settings(dev_dashboard_enabled=True)),
        base_url="http://127.0.0.1",
        client=("127.0.0.1", 50000),
    )


def test_dashboard_is_disabled_by_default():
    client = TestClient(create_app(Settings(dev_dashboard_enabled=False)))
    assert client.get("/dev").status_code == 404
    assert client.post("/dev/run-tests").status_code == 404


def test_dashboard_is_served_when_enabled():
    response = local_client().get("/dev")
    assert response.status_code == 200
    assert "LoL Coach · Consola de pruebas" in response.text
    assert "URL de .dlt/config.toml" in response.text


def test_dashboard_rejects_non_loopback_clients():
    client = TestClient(create_app(Settings(dev_dashboard_enabled=True)), base_url="http://example.test")
    assert client.get("/dev").status_code == 403
    assert client.post("/dev/run-tests").status_code == 403


def test_request_history_omits_query_secrets_and_dashboard_polling():
    client = local_client()
    response = client.get("/youtube/videos", params={"key": "dev-video-key", "maxResults": 1})
    assert response.status_code == 200

    history = client.get("/dev/api-requests").json()
    assert len(history) == 1
    assert history[0]["path"] == "/youtube/videos"
    assert history[0]["status_code"] == 200
    assert "dev-video-key" not in str(history)
    assert client.post("/dev/api-requests/clear").json() == {"status": "cleared"}
    assert client.get("/dev/api-requests").json() == []


def test_remote_proxy_uses_local_dlt_secret_without_exposing_it(monkeypatch):
    import dlt

    secret = "render-riot-secret"
    captured = {}

    class FakeResponse:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self):
            return b'{"items":[]}'

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(dlt.config, "get", lambda _: "https://lol-synthetic-api.onrender.com")
    monkeypatch.setattr(
        dlt.secrets,
        "get",
        lambda name: secret if name == "sources.api.riot_token" else None,
    )
    monkeypatch.setattr("synthetic_api.main.urlopen", fake_urlopen)

    client = local_client()
    response = client.get("/dev/remote/riot/matches?limit=1")

    assert response.status_code == 200
    assert response.json() == {"items": []}
    assert captured["timeout"] == 90
    assert captured["request"].get_header("X-riot-token") == secret
    assert secret not in response.text
    history = client.get("/dev/api-requests").json()
    assert history[0]["path"] == "/riot/matches"
    assert history[0]["status_code"] == 200
    assert secret not in str(history)


def test_remote_video_key_is_only_added_server_side(monkeypatch):
    import dlt

    secret = "render-video-secret"
    captured = {}

    class FakeResponse:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self):
            return b'{"items":[]}'

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        return FakeResponse()

    monkeypatch.setattr(dlt.config, "get", lambda _: "https://lol-synthetic-api.onrender.com")
    monkeypatch.setattr(
        dlt.secrets,
        "get",
        lambda name: secret if name == "sources.api.video_key" else None,
    )
    monkeypatch.setattr("synthetic_api.main.urlopen", fake_urlopen)

    client = local_client()
    response = client.get("/dev/remote/youtube/videos?maxResults=1")

    assert response.status_code == 200
    assert f"key={secret}" in captured["url"]
    assert secret not in response.text
    assert secret not in str(client.get("/dev/api-requests").json())


def test_remote_proxy_rejects_mutations_and_unapproved_paths():
    client = local_client()
    assert client.post("/dev/remote/admin/reset").status_code == 405
    assert client.get("/dev/remote/admin/reset").status_code == 404


def test_remote_proxy_requires_secret_without_returning_values(monkeypatch):
    import dlt

    monkeypatch.setattr(dlt.config, "get", lambda _: "https://lol-synthetic-api.onrender.com")
    monkeypatch.setattr(dlt.secrets, "get", lambda _: None)

    response = local_client().get("/dev/remote/riot/matches?limit=1")

    assert response.status_code == 503
    assert response.json()["error"] == (
        "Missing local secret sources.api.riot_token in .dlt/secrets.toml or environment"
    )


def test_test_runner_uses_fixed_local_pytest_command(monkeypatch):
    calls = {}

    def fake_run(command, **kwargs):
        calls["command"] = command
        calls["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, stdout="4 passed")

    monkeypatch.setattr("synthetic_api.main.subprocess.run", fake_run)
    response = local_client().post("/dev/run-tests")

    assert response.status_code == 200
    assert response.json()["passed"] is True
    assert response.json()["output"] == "4 passed"
    assert calls["command"][-4:] == ["-m", "pytest", "tests", "-q"]
    assert calls["kwargs"]["cwd"] == Path(__file__).resolve().parents[1]
    assert calls["kwargs"]["timeout"] == 300
